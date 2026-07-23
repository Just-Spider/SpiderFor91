#!/usr/bin/env python3
"""
脚本名称: 91Pinse 爬虫（基于原 91Porn 脚本改造）
用途: 从 91pinse.com 列表页爬取视频标题、视频下载直链、封面图直链和唯一标识，
并按 crawler.v2 协议输出给后端入库。

说明:
 - 默认从 https://91pinse.com/v/hot/ 热门列表抓取视频
 - 列表页解析 article.video-card，详情页解析内嵌流地址并默认选取最高画质直链
 - 旧版镜像站仍保留 91Porn 风格解析作为回退逻辑
"""

import argparse
import base64
import requests
import re
import time
import random
import json
import os
import socket
import sys
import html
from urllib.parse import urljoin, unquote, urlparse
from datetime import datetime

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("错误: 缺少依赖库 beautifulsoup4", file=sys.stderr)
    print("请运行: pip install beautifulsoup4 lxml", file=sys.stderr)
    sys.exit(1)


def prefer_ipv4_for_plain_socks5_proxy():
    proxy_envs = (
        os.environ.get("HTTPS_PROXY", ""),
        os.environ.get("HTTP_PROXY", ""),
        os.environ.get("https_proxy", ""),
        os.environ.get("http_proxy", ""),
    )
    uses_plain_socks5 = any(v.strip().lower().startswith("socks5://") for v in proxy_envs)
    if not uses_plain_socks5 or getattr(socket, "_spider91_ipv4_first", False):
        return

    original_getaddrinfo = socket.getaddrinfo

    def getaddrinfo_ipv4_first(*args, **kwargs):
        infos = original_getaddrinfo(*args, **kwargs)
        return sorted(infos, key=lambda info: 0 if info[0] == socket.AF_INET else 1)

    socket.getaddrinfo = getaddrinfo_ipv4_first
    socket._spider91_ipv4_first = True


DEFAULT_SITE = "https://91pinse.com"
LIST_PATH = "/v/hot/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

MIN_PAGE_DELAY = 3.0
MAX_PAGE_DELAY = 6.0
MIN_DETAIL_DELAY = 2.0
MAX_DETAIL_DELAY = 5.0

MAX_RETRIES = 3
RETRY_DELAY = 5.0

OUTPUT_FILE = "91pinse_videos.json"
MAX_PAGES = None
RESUME = True
MAX_EMPTY_PAGES = 2
CRAWLER_NAME = "91Pinse"
CRAWLER_PROTOCOL = "crawler.v2"


def crawler_source_id(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
    return safe[:160]


def write_jsonl(event: dict):
    try:
        print(json.dumps(event, ensure_ascii=False), flush=True)
    except BrokenPipeError:
        sys.exit(0)


def positive_int(*values, default: int) -> int:
    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    return default


class PinseSpider:
    def __init__(
        self,
        site: str = None,
        output_file: str = None,
        start_page: int = 1,
        max_pages: int = None,
        resume: bool = None,
        max_empty_pages: int = None,
        quiet: bool = False,
        target_new: int = None,
        candidate_budget: int = None,
        seen_viewkeys: list = None,
        stream_output: bool = False,
        stream_protocol: str = "legacy",
        proxies: dict = None,
        job_mode: bool = False,
    ):
        self.site = (site or DEFAULT_SITE).rstrip('/')
        self.list_path = LIST_PATH if LIST_PATH.endswith('/') else LIST_PATH + '/'
        self.base_url = self.site + self.list_path
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.cookies.set("mode", "d")

        self.output_file = output_file if output_file is not None else OUTPUT_FILE
        self.start_page = max(1, int(start_page or 1))
        self.max_pages = max_pages if max_pages is None or max_pages > 0 else None
        self.resume = RESUME if resume is None else bool(resume)
        self.max_empty_pages = (
            MAX_EMPTY_PAGES if max_empty_pages is None else int(max_empty_pages)
        )
        self.candidate_budget = (
            candidate_budget
            if candidate_budget and candidate_budget > 0
            else None
        )
        self.target_new = target_new if target_new and target_new > 0 else None
        self.quiet = bool(quiet)
        self.stream_output = bool(stream_output)
        self.stream_protocol = stream_protocol or "legacy"
        self.job_mode = bool(job_mode)
        self.emitted = 0
        self.checked = 0
        self.limits = {}
        self._last_progress_at = time.monotonic()
        self._start_monotonic = time.monotonic()
        self._last_item_at = time.monotonic()
        if proxies:
            self.session.proxies.update(proxies)

        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            retry_strategy = Retry(
                total=MAX_RETRIES,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        except ImportError:
            pass

        self.results = []
        self.pages_crawled = 0
        self.processed_videos = 0
        self.skipped_videos = 0
        self.failed_videos = 0
        self.skip_viewkeys = set()

        if seen_viewkeys:
            for vk in seen_viewkeys:
                if not vk:
                    continue
                vk = vk.strip()
                if vk:
                    self.skip_viewkeys.add(vk)
                    safe_id = crawler_source_id(vk)
                    if safe_id:
                        self.skip_viewkeys.add(safe_id)

        if self.resume and os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                existing_videos = existing_data.get('videos', [])
                self.results = existing_videos
                for v in existing_videos:
                    vk = v.get('viewkey', '')
                    sid = v.get('source_id', '')
                    if vk:
                        self.skip_viewkeys.add(vk)
                    if sid:
                        self.skip_viewkeys.add(sid)
                self.processed_videos = existing_data.get('successful', 0)
                self.failed_videos = existing_data.get('failed', 0)
                self.log(f"加载已有数据: {len(self.results)} 个视频, 将跳过已处理项")
            except Exception:
                pass

    def log(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {message}"
        if self.stream_output or self.job_mode:
            print(line, file=sys.stderr, flush=True)
        else:
            print(line)

    def _output_budget(self) -> int:
        if self.candidate_budget:
            return self.candidate_budget
        if self.target_new:
            return self.target_new
        return 0

    def _reached_output_budget(self) -> bool:
        budget = self._output_budget()
        if not budget:
            return False
        if self.stream_output or self.job_mode:
            return self.emitted >= budget
        return self.processed_videos >= budget

    def _deadline_reached(self) -> bool:
        limits = self.limits or {}
        max_runtime = limits.get("max_runtime_seconds")
        if max_runtime:
            try:
                if time.monotonic() - self._start_monotonic >= float(max_runtime):
                    return True
            except (TypeError, ValueError):
                pass
        deadline_at = limits.get("deadline_at")
        if deadline_at:
            try:
                text = str(deadline_at).replace("Z", "+00:00")
                deadline = datetime.fromisoformat(text)
                if deadline.tzinfo is None:
                    return datetime.utcnow() >= deadline
                from datetime import timezone
                return datetime.now(timezone.utc) >= deadline.astimezone(timezone.utc)
            except Exception:
                pass
        idle = limits.get("candidate_idle_timeout_seconds")
        if idle:
            try:
                anchor = self._last_item_at if self.emitted > 0 else self._start_monotonic
                if time.monotonic() - anchor >= float(idle):
                    return True
            except (TypeError, ValueError):
                pass
        return False

    def _maybe_progress(self, message: str = ""):
        if not self.stream_output:
            return
        interval = 60
        try:
            interval = int((self.limits or {}).get("progress_interval_seconds") or 60)
        except (TypeError, ValueError):
            interval = 60
        if interval <= 0:
            interval = 60
        now = time.monotonic()
        if now - self._last_progress_at < interval and not message:
            return
        write_jsonl({
            "type": "progress",
            "checked": self.checked,
            "emitted": self.emitted,
            "message": message or f"checked={self.checked} emitted={self.emitted}",
        })
        self._last_progress_at = now

    def emit_stream_video(self, video: dict) -> bool:
        if not self.stream_output:
            return False
        try:
            if self.stream_protocol == "crawler.v2":
                source_id = crawler_source_id(video.get("source_id") or video.get("viewkey") or "")
                media_url = video.get("video_url") or ""
                title = (video.get("title") or "").strip()
                if not source_id or not media_url or not title:
                    self.log(
                        f"[stream] skip invalid item: source_id={source_id!r} "
                        f"media_url={bool(media_url)} title={bool(title)}"
                    )
                    return False
                referer = video.get("detail_url") or self.base_url
                event = {
                    "type": "item",
                    "source_id": source_id,
                    "title": title,
                    "media_url": media_url,
                    "thumbnail_url": video.get("thumb_url") or "",
                    "detail_url": video.get("detail_url") or "",
                    "headers": {
                        "Referer": referer,
                        "User-Agent": HEADERS["User-Agent"],
                    },
                }
                quality = video.get("quality")
                if quality:
                    event["quality"] = quality
                write_jsonl(event)
                self._last_item_at = time.monotonic()
                self._last_progress_at = time.monotonic()
                return True
            write_jsonl(video)
            return True
        except BrokenPipeError:
            sys.exit(0)
        except Exception as e:
            self.log(f"[stream] emit failed: {e}")
            return False

    def build_list_url(self, page_num: int) -> str:
        if page_num > 1:
            return f"{self.base_url}?page={page_num}"
        return self.base_url

    def _is_seen(self, video: dict) -> bool:
        viewkey = str(video.get("viewkey") or "").strip()
        source_id = str(video.get("source_id") or "").strip()
        if viewkey and viewkey in self.skip_viewkeys:
            return True
        if source_id and source_id in self.skip_viewkeys:
            return True
        safe_id = crawler_source_id(source_id or viewkey)
        return bool(safe_id and safe_id in self.skip_viewkeys)

    def _extract_post_id(self, url: str) -> str:
        match = re.search(r"/v/(\d+)", urlparse(url or "").path)
        return match.group(1) if match else ""

    def _extract_playback_api_url(self, html_text: str, detail_url: str = "") -> str:
        """Extract current-site playback API path from detail page."""
        patterns = (
            r"playbackApiUrl\s*=\s*['\"]([^'\"]+)['\"]",
            r"__jjPlaybackSourceRequest\s*=\s*\{[\s\S]*?url:\s*['\"]([^'\"]+)['\"]",
            r"['\"](/api/videos/\d+/playback)['\"]",
        )
        for pat in patterns:
            match = re.search(pat, html_text or "")
            if not match:
                continue
            path = match.group(1).strip()
            if not path:
                continue
            if path.startswith("http"):
                return path
            return urljoin(self.site + "/", path.lstrip("/"))

        post_id = self._extract_post_id(detail_url)
        if post_id:
            return f"{self.site}/api/videos/{post_id}/playback"
        return ""

    def _fetch_playback_api_urls(self, api_url: str, referer: str = "") -> list:
        """POST /api/videos/{id}/playback and collect stream candidates."""
        if not api_url:
            return []
        headers = {
            "User-Agent": HEADERS["User-Agent"],
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": referer or self.base_url,
            "Origin": self.site,
        }
        try:
            response = self.session.post(
                api_url,
                headers=headers,
                timeout=20,
                data=b"",
            )
            if response.status_code != 200:
                self.log(
                    f"  playback API status={response.status_code} url={api_url}"
                )
                return []
            data = response.json()
        except Exception as e:
            self.log(f"  playback API failed: {e}")
            return []

        urls = []
        if isinstance(data, dict):
            for key in ("url", "fallback_url", "media_url", "src"):
                value = str(data.get(key) or "").strip()
                if value.startswith("http"):
                    urls.append(value)
            nested = data.get("data")
            if isinstance(nested, dict):
                for key in ("url", "fallback_url", "media_url", "src"):
                    value = str(nested.get(key) or "").strip()
                    if value.startswith("http"):
                        urls.append(value)
        elif isinstance(data, str) and data.startswith("http"):
            urls.append(data)

        # de-dup preserve order
        seen = set()
        ordered = []
        for url in urls:
            if url not in seen:
                seen.add(url)
                ordered.append(url)
        return ordered

    def _extract_embedded_video_urls(self, html_text: str) -> list:
        urls = []
        seen = set()

        def add(url: str):
            value = (url or "").strip()
            if (
                value.startswith("http")
                and (".m3u8" in value or ".mp4" in value.lower())
                and value not in seen
            ):
                seen.add(value)
                urls.append(value)

        for script_match in re.finditer(r"<script[^>]*>([\s\S]*?)</script>", html_text):
            body = script_match.group(1)
            # New site may not use atob; still try base64 payloads when present.
            if "atob" in body or "loadSource" in body or "aHR0c" in body:
                for encoded_match in re.finditer(r"['\"](aHR0c[^'\"]+)['\"]", body):
                    encoded = encoded_match.group(1).replace("\\u003D", "=")
                    try:
                        url = base64.b64decode(encoded).decode("utf-8").strip()
                    except Exception:
                        continue
                    add(url)

            for match in re.finditer(
                r"https?://[^\s\"'<>]+\.(?:m3u8|mp4)[^\s\"'<>]*",
                body,
                re.I,
            ):
                add(match.group(0))

        return urls

    def _normalize_pinse_stream_url(self, url: str) -> str:
        if "jfly.xyz" in url and ".m3u8" in url and "hot=" in url:
            url = re.sub(r"hot=\d+", "hot=1", url)
        return url

    def _quality_hint_from_url(self, url: str) -> tuple:
        low = url.lower()
        for height in (2160, 1440, 1080, 720, 480, 360, 240):
            if re.search(rf"(?:^|[^0-9]){height}(?:p|x|$)", low):
                return height, f"{height}p"
        if ".mp4" in low:
            return 900, "mp4"
        if "hot=1" in low:
            return 800, "hls-hot"
        if "pl.m3u8" in low:
            return 700, "hls-playlist"
        if ".m3u8" in low:
            return 600, "hls"
        return 0, ""

    def _fetch_playlist_text(self, url: str, referer: str) -> str:
        headers = {
            "User-Agent": HEADERS["User-Agent"],
            "Referer": referer,
            "Accept": "*/*",
        }
        try:
            response = self.session.get(url, headers=headers, timeout=20)
            if response.status_code != 200:
                return ""
            text = response.text
            if "#EXTM3U" not in text:
                return ""
            return text
        except Exception:
            return ""

    def _parse_m3u8_master_variants(self, content: str, base_url: str) -> list:
        variants = []
        lines = content.splitlines()
        idx = 0
        while idx < len(lines):
            line = lines[idx].strip()
            if line.startswith("#EXT-X-STREAM-INF:"):
                bandwidth = 0
                height = 0
                bw_match = re.search(r"BANDWIDTH=(\d+)", line)
                if bw_match:
                    bandwidth = int(bw_match.group(1))
                res_match = re.search(r"RESOLUTION=(\d+)x(\d+)", line)
                if res_match:
                    height = int(res_match.group(2))
                idx += 1
                while idx < len(lines) and (
                    not lines[idx].strip() or lines[idx].strip().startswith("#")
                ):
                    idx += 1
                if idx < len(lines):
                    uri = lines[idx].strip()
                    if uri and not uri.startswith("#"):
                        variants.append({
                            "url": urljoin(base_url, uri),
                            "bandwidth": bandwidth,
                            "height": height,
                        })
            idx += 1
        return variants

    def _resolve_highest_m3u8(self, url: str, referer: str, depth: int = 0) -> tuple:
        if depth > 2:
            hint, label = self._quality_hint_from_url(url)
            return url, label

        content = self._fetch_playlist_text(url, referer)
        if not content:
            _, label = self._quality_hint_from_url(url)
            return url, label

        if "#EXT-X-STREAM-INF:" in content:
            variants = self._parse_m3u8_master_variants(content, url)
            if not variants:
                _, label = self._quality_hint_from_url(url)
                return url, label
            best = max(variants, key=lambda item: (item["height"], item["bandwidth"]))
            quality = f"{best['height']}p" if best["height"] else ""
            resolved_url, nested_quality = self._resolve_highest_m3u8(
                best["url"],
                referer,
                depth + 1,
            )
            return resolved_url, nested_quality or quality

        if ".ts" in content or ".m4s" in content:
            return url, "hls"
        if ".jpg" in content or ".webp" in content:
            return url, "hls-hot"
        _, label = self._quality_hint_from_url(url)
        return url, label

    def _select_highest_quality_url(self, candidates: list, referer: str) -> tuple:
        ranked = []
        for raw in candidates:
            url = self._normalize_pinse_stream_url(str(raw or "").strip())
            if not url.startswith("http"):
                continue

            hint, label = self._quality_hint_from_url(url)
            low = url.lower()

            if ".mp4" in low:
                ranked.append((1_000_000 + hint, url, label or "mp4"))
                continue

            if ".m3u8" in low:
                resolved, quality = self._resolve_highest_m3u8(url, referer)
                res_score = 0
                if quality and quality.endswith("p"):
                    try:
                        res_score = int(quality[:-1])
                    except ValueError:
                        res_score = 0
                hot_bonus = 50_000 if "hot=1" in resolved else 0
                playlist_bonus = 30_000 if "pl.m3u8" in resolved else 0
                ranked.append((
                    800_000 + res_score * 100 + hot_bonus + playlist_bonus + hint,
                    resolved,
                    quality or label or "hls",
                ))

        if not ranked:
            return "", ""

        ranked.sort(key=lambda item: item[0], reverse=True)
        return ranked[0][1], ranked[0][2]

    def _collect_video_url_candidates(self, html_text: str) -> list:
        candidates = []
        seen = set()

        def add(url: str):
            value = (url or "").strip()
            if value.startswith("http") and value not in seen:
                seen.add(value)
                candidates.append(value)

        for url in self._extract_embedded_video_urls(html_text):
            add(url)

        strencode_match = re.search(r'strencode2\(["\']([^"\']+)["\']\)', html_text)
        if strencode_match:
            try:
                decoded = unquote(strencode_match.group(1))
                src_match = re.search(r"src=['\"]([^'\"]+)['\"]", decoded)
                if src_match:
                    add(re.sub(r"(https?://[^/]+)//+", r"\1/", src_match.group(1)))
            except Exception:
                pass

        for match in re.finditer(r"https?://[^\s\"'<>]+\.mp4[^\s\"'<>]*", html_text, re.I):
            url = match.group(0)
            if "kwai" not in url and "ad-" not in url.lower():
                add(url)

        return candidates

    def random_sleep(self, min_sec: float, max_sec: float):
        delay = random.uniform(min_sec, max_sec)
        if not self.quiet:
            self.log(f"  随机延时 {delay:.2f} 秒...")
        time.sleep(delay)

    def fetch_page(self, url: str, description: str = "", referer: str = "") -> str:
        headers_extra = {}
        if referer:
            headers_extra["Referer"] = referer

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.log(f"正在请求: {description or url} (尝试 {attempt}/{MAX_RETRIES})")
                response = self.session.get(url, timeout=30, headers=headers_extra)

                if response.status_code == 403:
                    self.log("警告: 收到 403 Forbidden，可能被拦截")
                    if attempt < MAX_RETRIES:
                        self.random_sleep(RETRY_DELAY, RETRY_DELAY + 3)
                        continue
                    return ""

                response.raise_for_status()

                try:
                    html_content = response.content.decode('utf-8', errors='replace')
                except Exception:
                    html_content = response.text

                is_cf_challenge = (
                    "Just a moment" in html_content and
                    len(html_content) < 8000
                )
                if is_cf_challenge:
                    self.log("警告: 页面被Cloudflare挑战拦截，需要浏览器环境或正确cookie")
                    if attempt < MAX_RETRIES:
                        self.random_sleep(RETRY_DELAY, RETRY_DELAY + 5)
                        continue
                    return ""

                return html_content
            except requests.exceptions.HTTPError as e:
                self.log(f"HTTP错误: {e}")
                if attempt < MAX_RETRIES:
                    self.random_sleep(RETRY_DELAY, RETRY_DELAY + 3)
                else:
                    return ""
            except requests.exceptions.RequestException as e:
                self.log(f"请求失败: {e}")
                if attempt < MAX_RETRIES:
                    self.random_sleep(RETRY_DELAY, RETRY_DELAY + 3)
                else:
                    self.log(f"达到最大重试次数，放弃: {url}")
                    return ""
        return ""

    def parse_list_page(self, html: str) -> list:
        soup = BeautifulSoup(html, 'lxml')
        videos = self._parse_pinse_list_page(soup)
        if videos:
            return videos
        return self._parse_legacy_list_page(soup)

    def _parse_pinse_list_page(self, soup: BeautifulSoup) -> list:
        videos = []
        seen_cards = set()

        for card in soup.select("article.video-card"):
            link = card.find("a", href=re.compile(r"/v/\d+"))
            if not link:
                continue

            href = link.get("href", "").strip()
            if not href:
                continue

            post_id = self._extract_post_id(href)
            if not post_id:
                continue

            detail_url = urljoin(self.site + "/", href)
            img = card.find("img")
            thumb_url = ""
            title = ""
            if img:
                thumb_url = (img.get("src") or img.get("data-src") or "").strip()
                title = (img.get("alt") or "").strip()
            if not title:
                title = self._extract_title(link)

            if post_id in seen_cards:
                continue
            seen_cards.add(post_id)

            videos.append({
                "title": title,
                "detail_url": detail_url,
                "thumb_url": thumb_url,
                "viewkey": post_id,
                "source_id": post_id,
            })

        return videos

    def _parse_legacy_list_page(self, soup: BeautifulSoup) -> list:
        videos = []
        video_cards = soup.select('div.col-xs-12.col-sm-4.col-md-3.col-lg-3')

        if not video_cards:
            # 有些镜像使用稍有不同的类名
            video_cards = soup.select('div.video') or soup.select('li.playList')

        seen_cards = set()

        for card in video_cards:
            link = card.find('a', href=re.compile(r'view_video\.php\?viewkey='))
            if not link:
                link = card.find('a', href=re.compile(r'view_video\.php'))
            if not link:
                continue
            href = link.get('href', '')
            if not href:
                continue

            match = re.search(r'viewkey=([^&]+)', href)
            viewkey = match.group(1) if match else ''

            detail_url = urljoin(self.site + "/", href)

            title = self._extract_title(link)

            thumb_url = ""
            source_id = ""
            overlay = link.find(id=re.compile(r'^playvthumb_\d+$'))
            if overlay:
                source_id = overlay.get('id', '').rsplit('_', 1)[-1]
            img = link.find('img', class_=re.compile(r'img-responsive'))
            if img:
                thumb_url = img.get('src', '') or img.get('data-original', '')
                if thumb_url:
                    thumb_url = urljoin(self.site + "/", thumb_url)
            if not source_id and thumb_url:
                source_id = self._extract_thumb_source_id(thumb_url)

            card_key = source_id or detail_url
            if card_key in seen_cards:
                continue
            seen_cards.add(card_key)

            videos.append({
                "title": title,
                "detail_url": detail_url,
                "thumb_url": thumb_url,
                "viewkey": viewkey,
                "source_id": source_id
            })

        return videos

    def _extract_title(self, link) -> str:
        title_el = link.find('span', class_=re.compile(r'video-title'))
        if title_el:
            title = title_el.get_text(strip=True)
            if title:
                return html.unescape(title)

        title = link.get('title', '').strip()
        if title:
            return html.unescape(title)

        text = link.get_text(separator=' ', strip=True)
        text = re.sub(r'^(HD\s+|91\s+)?\d{2}:\d{2}:\d{2}\s*', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return html.unescape(text)[:120]

    def parse_detail_page(self, html: str, referer: str = "", detail_url: str = "") -> dict:
        result = {}

        if not html:
            return result

        title = self._extract_detail_title(html)
        if title:
            result["title"] = title

        candidates = self._collect_video_url_candidates(html)

        # Current 91pinse pages load streams via POST /api/videos/{id}/playback
        api_url = self._extract_playback_api_url(html, detail_url=detail_url or referer)
        if api_url:
            for url in self._fetch_playback_api_urls(api_url, referer=detail_url or referer):
                if url not in candidates:
                    candidates.append(url)

        video_url, quality = self._select_highest_quality_url(
            candidates,
            referer or detail_url or self.base_url,
        )
        if video_url:
            result["video_url"] = video_url
            if quality:
                result["quality"] = quality
            source_id = self._extract_source_id(video_url)
            if source_id:
                result["source_id"] = source_id
            return result

        return result

    def _extract_detail_title(self, html_text: str) -> str:
        soup = BeautifulSoup(html_text, 'lxml')
        title_el = soup.find('title')
        if not title_el:
            return ""
        title = title_el.get_text(" ", strip=True)
        title = re.sub(r'\s*-\s*91(?:porn|pinse).*$', '', title, flags=re.IGNORECASE).strip()
        return html.unescape(title)[:160]

    def _extract_source_id(self, video_url: str) -> str:
        path = urlparse(video_url or "").path
        name = os.path.basename(path)
        stem, ext = os.path.splitext(name)
        if ext.lower() not in {".mp4", ".m4v", ".mov", ".webm", ".mkv", ".avi"}:
            return ""
        source_id = re.sub(r'[^0-9]+', '', stem)
        if not source_id or source_id != stem:
            return ""
        return source_id

    def _extract_thumb_source_id(self, thumb_url: str) -> str:
        path = urlparse(thumb_url or "").path
        match = re.search(r'/thumb/(\d+)\.[A-Za-z0-9]+$', path)
        if match:
            return match.group(1)
        match = re.search(r'/images/(\d+)\.[A-Za-z0-9]+$', path)
        return match.group(1) if match else ""

    def _thumb_url_for_source(self, thumb_url: str, source_id: str) -> str:
        if not thumb_url or not source_id:
            return thumb_url
        parsed = urlparse(thumb_url)
        match = re.search(r'/thumb/([^/?#]+)\.[A-Za-z0-9]+$', parsed.path)
        if not match:
            return thumb_url
        current = match.group(1)
        if current == source_id:
            return thumb_url
        path = re.sub(
            r'/thumb/[^/?#]+\.[A-Za-z0-9]+$',
            f'/thumb/{source_id}.jpg',
            parsed.path,
        )
        return parsed._replace(path=path, query="", fragment="").geturl()

    def crawl(self):
        self.log("=" * 60)
        self.log(f"{CRAWLER_NAME} 视频爬虫启动 (site={self.site})")
        self.log("=" * 60)
        self.log(f"配置: 列表路径 {self.base_url}")
        self.log(f"配置: 列表页延时 {MIN_PAGE_DELAY}-{MAX_PAGE_DELAY}s, 详情页延时 {MIN_DETAIL_DELAY}-{MAX_DETAIL_DELAY}s")
        self.log(f"配置: 最大重试 {MAX_RETRIES} 次, 连续空页上限 {self.max_empty_pages}")
        self.log(f"配置: 起始页 {self.start_page}, 最大爬取页数 {self.max_pages if self.max_pages else '不限'}")
        budget = self._output_budget()
        if budget:
            self.log(f"配置: 候选输出上限 {budget}")
        self.log(f"配置: 输出文件 {os.path.abspath(self.output_file)}")
        if self.skip_viewkeys:
            self.log(f"配置: 已跳过 {len(self.skip_viewkeys)} 个已知 viewkey")
        self.log("")

        page_num = self.start_page
        consecutive_empty = 0
        crawled_in_session = 0

        while True:
            if self.max_pages is not None and crawled_in_session >= self.max_pages:
                self.log(f"达到配置的页数上限 {self.max_pages}，停止")
                break
            if consecutive_empty >= self.max_empty_pages:
                self.log(f"连续 {self.max_empty_pages} 页无结果，已达到末尾")
                break
            if self._reached_output_budget():
                self.log(f"已输出 {self.emitted if self.stream_output else self.processed_videos} 个候选，达到上限，停止")
                break
            if self._deadline_reached():
                self.log("达到 job limits 截止条件，停止")
                break

            page_url = self.build_list_url(page_num)

            if crawled_in_session > 0:
                self.log("")
                self.random_sleep(MIN_PAGE_DELAY, MAX_PAGE_DELAY)

            self.log(f"[页 {page_num}] 请求: {page_url}")
            page_html = self.fetch_page(page_url, f"列表页 第{page_num}页")

            if not page_html:
                self.log(f"[页 {page_num}] 获取失败，跳过")
                consecutive_empty += 1
                page_num += 1
                crawled_in_session += 1
                continue

            page_videos = self.parse_list_page(page_html)

            if not page_videos:
                self.log(f"[页 {page_num}] 页面无视频，可能已到末尾")
                consecutive_empty += 1
                page_num += 1
                crawled_in_session += 1
                continue

            consecutive_empty = 0

            new_videos = [v for v in page_videos if not self._is_seen(v)]
            skipped_on_page = len(page_videos) - len(new_videos)

            if skipped_on_page > 0:
                self.log(f"[页 {page_num}] 发现 {len(page_videos)} 个链接, 其中 {skipped_on_page} 个已处理, {len(new_videos)} 个新视频")
            else:
                self.log(f"[页 {page_num}] 发现 {len(new_videos)} 个视频")

            if new_videos:
                self._process_video_list(new_videos, referer=page_url)
            self.pages_crawled += 1
            self._maybe_progress(f"Scanning page {page_num}")
            page_num += 1
            crawled_in_session += 1

        if not self.job_mode:
            self._save_results()
            self._print_summary()

    def _process_video_list(self, videos: list, referer: str = ""):
        for idx, video in enumerate(videos, 1):
            if self._reached_output_budget():
                return
            if self._deadline_reached():
                return
            if self._is_seen(video):
                self.log(f"  [SKIP] 已处理过: {video.get('source_id') or video['viewkey']}")
                self.skipped_videos += 1
                self.checked += 1
                self._maybe_progress()
                continue

            self.checked += 1
            self.log(f"  处理视频 {idx}/{len(videos)}: {video['title'][:40]}...")

            if idx > 1:
                self.random_sleep(MIN_DETAIL_DELAY, MAX_DETAIL_DELAY)

            detail_html = self.fetch_page(video['detail_url'], f"详情页 viewkey={video['viewkey']}", referer=referer)

            if not detail_html:
                self.log(f"  [FAIL] 详情页获取失败: {video['viewkey']}")
                video["video_url"] = ""
                self.results.append(video)
                self.skip_viewkeys.add(video['viewkey'])
                self.failed_videos += 1
                continue

            detail_info = self.parse_detail_page(
                detail_html,
                referer=video["detail_url"],
                detail_url=video["detail_url"],
            )

            if detail_info.get("video_url"):
                video["video_url"] = detail_info["video_url"]
                if detail_info.get("quality"):
                    video["quality"] = detail_info["quality"]
                if detail_info.get("title"):
                    video["title"] = detail_info["title"]
                list_source_id = video.get("source_id", "")
                detail_source_id = detail_info.get("source_id", "")
                if list_source_id and detail_source_id and list_source_id != detail_source_id:
                    self.log(
                        f"  [FAIL] 详情页视频源不匹配: list_source_id={list_source_id} "
                        f"detail_source_id={detail_source_id} viewkey={video['viewkey']}"
                    )
                    self.failed_videos += 1
                    self.skip_viewkeys.add(video['viewkey'])
                    continue
                if not list_source_id and detail_source_id:
                    video["source_id"] = detail_source_id
                if video.get("source_id"):
                    video["thumb_url"] = self._thumb_url_for_source(
                        video.get("thumb_url", ""),
                        video["source_id"],
                    )
                    if video["source_id"] in self.skip_viewkeys:
                        self.log(f"  [SKIP] 已处理过 source_id: {video['source_id']}")
                        self.skipped_videos += 1
                        continue
                self.results.append(video)
                self.skip_viewkeys.add(video['viewkey'])
                if video.get("source_id"):
                    self.skip_viewkeys.add(video["source_id"])
                self.processed_videos += 1
                self.log(f"  [OK] 成功提取视频直链")
                if self.emit_stream_video(video):
                    self.emitted += 1
                self._maybe_progress()
                if self._reached_output_budget():
                    return
            else:
                self.log(f"  [FAIL] 未找到视频直链: {video['viewkey']}")
                video["video_url"] = ""
                self.results.append(video)
                self.skip_viewkeys.add(video['viewkey'])
                self.failed_videos += 1
                self._maybe_progress()

    def _save_results(self):
        output_data = {
            "crawl_time": datetime.now().isoformat(),
            "source_site": self.site,
            "source_url": self.base_url,
            "pages_crawled": self.pages_crawled,
            "total_videos": len(self.results),
            "successful": self.processed_videos,
            "skipped": self.skipped_videos,
            "failed": self.failed_videos,
            "videos": self.results
        }

        try:
            out_path = self.output_file
            parent = os.path.dirname(os.path.abspath(out_path))
            if parent:
                os.makedirs(parent, exist_ok=True)
            tmp_path = out_path + ".part"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, out_path)
            self.log(f"结果已保存到: {os.path.abspath(out_path)}")
        except Exception as e:
            self.log(f"保存文件失败: {e}")
            if not self.job_mode:
                backup_out = sys.stderr if self.stream_output else sys.stdout
                print("\n--- 备份输出 ---", file=backup_out, flush=True)
                print(json.dumps(output_data, ensure_ascii=False, indent=2), file=backup_out, flush=True)

    def _print_summary(self):
        self.log("")
        self.log("=" * 60)
        self.log("爬取完成!")
        self.log("=" * 60)
        self.log(f"爬取页数: {self.pages_crawled}")
        self.log(f"总视频数: {len(self.results)}")
        self.log(f"成功提取直链: {self.processed_videos}")
        self.log(f"跳过(已处理): {self.skipped_videos}")
        self.log(f"失败/缺失直链: {self.failed_videos}")
        self.log(f"输出文件: {os.path.abspath(self.output_file)}")
        self.log("=" * 60)


def print_help():
    print("""
================================================
    91pinse 视频爬虫
================================================

本脚本默认爬取 91pinse 热门列表 /v/hot/ 下的视频信息：
  - 视频名称
  - 封面图直链
  - 视频直链 (m3u8 / mp4)

依赖安装:
    pip install requests beautifulsoup4 lxml PySocks

使用方法:
    python3 91Pinse.py --job /path/to/job.json
    python 91Pinse.py --site https://91pinse.com

注意: 旧版镜像站 HTML 可能不同，如解析失败请调整 parse_list_page / parse_detail_page
""")


def run_job(job_path: str):
    try:
        with open(job_path, "r", encoding="utf-8") as f:
            job = json.load(f)
    except Exception as e:
        print(f"错误: 无法读取 job 文件: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    if job.get("protocol") != CRAWLER_PROTOCOL:
        print(
            f"错误: 不支持的协议: {job.get('protocol')!r}",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
    if job.get("mode") not in ("", None, "crawl"):
        print(
            f"错误: 不支持的 mode: {job.get('mode')!r}",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    candidate_budget = positive_int(
        job.get("candidate_budget"),
        job.get("target_new"),
        default=10,
    )
    unique_target = positive_int(job.get("unique_target"), default=0)
    print(
        f"[job] unique_target={unique_target or 'unknown'} candidate_budget={candidate_budget}",
        file=sys.stderr,
        flush=True,
    )

    config = job.get("config") if isinstance(job.get("config"), dict) else {}
    site = str(config.get("site") or DEFAULT_SITE).strip() or DEFAULT_SITE

    seen_file = job.get("seen_source_ids_file") or ""
    output_dir = job.get("output_dir") or ""
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    network = job.get("network") if isinstance(job.get("network"), dict) else {}
    proxy_url = str(network.get("proxy_url") or "").strip()
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        os.environ["http_proxy"] = proxy_url
        os.environ["https_proxy"] = proxy_url
        os.environ["NO_PROXY"] = ""
        os.environ["no_proxy"] = ""

    seen_source_ids = []
    if seen_file:
        try:
            with open(seen_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        seen_source_ids.append(line)
        except FileNotFoundError:
            print(f"警告: seen_source_ids_file 不存在: {seen_file}", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"警告: 读取 seen_source_ids_file 失败: {e}", file=sys.stderr, flush=True)

    prefer_ipv4_for_plain_socks5_proxy()
    spider = PinseSpider(
        site=site,
        start_page=1,
        max_pages=None,
        resume=False,
        quiet=True,
        candidate_budget=candidate_budget,
        seen_viewkeys=seen_source_ids,
        stream_output=True,
        stream_protocol="crawler.v2",
        proxies=proxies,
        job_mode=True,
    )
    spider.limits = job.get("limits") if isinstance(job.get("limits"), dict) else {}
    try:
        spider.crawl()
        write_jsonl({
            "type": "done",
            "stats": {
                "checked": spider.checked,
                "emitted": spider.emitted,
            },
        })
    except (KeyboardInterrupt, BrokenPipeError):
        sys.exit(0)
    except Exception as e:
        print(f"错误: 爬虫执行失败: {e}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        prog="91Pinse.py",
        description="91pinse 视频元数据爬虫",
    )
    parser.add_argument("--site", type=str, default=DEFAULT_SITE,
                        help="目标站点域名或完整 URL，例如 https://91pinse.com")
    parser.add_argument("--page", type=int, default=None,
                        help="只爬指定页（单页模式，配合 --output 用于定时任务）")
    parser.add_argument("--output", type=str, default=None,
                        help="输出 JSON 路径，覆盖默认 OUTPUT_FILE")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="单页模式下，从 --page 起最多再爬几页（默认 1）")
    parser.add_argument("--no-resume", action="store_true",
                        help="禁用断点续爬（单页模式默认禁用）")
    parser.add_argument("--quiet", action="store_true",
                        help="压缩日志，每条视频只输出关键事件")
    parser.add_argument("--target-new", type=int, default=None,
                        help="目标新增模式：从 page 1 起翻页直到累计处理这么多新源视频后停止")
    parser.add_argument("--seen-viewkeys-file", type=str, default=None,
                        help="文件路径，每行一个已处理过的 viewkey 或 mp4 源 ID；脚本会跳过这些视频")
    parser.add_argument("--stream-output", action="store_true",
                        help="流式模式：每解析一条视频直链就立即把它作为一行 JSON 写到 stdout 并 flush；日志改走 stderr。")
    parser.add_argument("--job", type=str, default=None,
                        help="crawler.v2 job JSON 路径；作为通用脚本爬虫运行。")

    args = parser.parse_args()
    if args.job:
        run_job(args.job)
        return

    prefer_ipv4_for_plain_socks5_proxy()

    seen_viewkeys = []
    if args.seen_viewkeys_file:
        try:
            with open(args.seen_viewkeys_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        seen_viewkeys.append(line)
        except FileNotFoundError:
            print(f"警告: --seen-viewkeys-file 不存在: {args.seen_viewkeys_file}", file=sys.stderr)
        except Exception as e:
            print(f"警告: 读取 --seen-viewkeys-file 失败: {e}", file=sys.stderr)

    if args.page is not None:
        start_page = max(1, args.page)
        max_pages = args.max_pages if args.max_pages and args.max_pages > 0 else 1
        spider = PinseSpider(
            site=args.site,
            output_file=args.output,
            start_page=start_page,
            max_pages=max_pages,
            resume=False,
            quiet=args.quiet,
            seen_viewkeys=seen_viewkeys,
            stream_output=args.stream_output,
        )
    else:
        spider = PinseSpider(
            site=args.site,
            output_file=args.output,
            resume=False if args.no_resume else None,
            quiet=args.quiet,
            seen_viewkeys=seen_viewkeys,
            stream_output=args.stream_output,
        )

    try:
        spider.crawl()
    except KeyboardInterrupt:
        spider.log("\n用户中断，正在保存已爬取的数据...")
        spider._save_results()
        spider._print_summary()
        sys.exit(0)
    except Exception as e:
        spider.log(f"发生未预料的错误: {e}")
        import traceback
        traceback.print_exc(file=sys.stderr)
        spider._save_results()
        raise


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
