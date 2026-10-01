#!/usr/bin/env python3
"""Single-file crawler.v3; dependencies: requests, beautifulsoup4 (PySocks for SOCKS proxies)."""

CRAWLER_NAME = "kanav"
CRAWLER_PROTOCOL = "crawler.v3"
CRAWLER_FEEDS = '[{"id":"latest","label":"最新自拍","default":true},{"id":"hot","label":"热门"}]'

import argparse
import base64
import datetime
import html
import json
import os
import queue
import re
import sys
import threading
import time
import traceback
import unicodedata
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from urllib3.util import Timeout


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"


def utf8_prefix(value, size):
    return str(value).encode("utf-8")[:size].decode("utf-8", errors="ignore")


def identifier(value, name):
    if (not isinstance(value, str) or not value or value != value.strip()
            or len(value.encode("utf-8")) > 512
            or any(unicodedata.category(c) == "Cc" for c in value)):
        raise ValueError(name + " must be a 1-512 byte identifier")
    return value


def http_url(value):
    if not isinstance(value, str) or len(value.encode("utf-8")) > 8192:
        raise ValueError("Invalid HTTP(S) URL")
    parsed = urlparse(value)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or any(c.isspace() or unicodedata.category(c) == "Cc" for c in value)):
        raise ValueError("Invalid HTTP(S) URL")
    parsed.port
    return value


def protocol_headers(headers):
    if not isinstance(headers, dict) or len(headers) > 64:
        raise ValueError("headers must be an object with at most 64 entries")
    for name, value in headers.items():
        if (not isinstance(name, str) or len(name.encode("utf-8")) > 256
                or not re.fullmatch(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+", name)
                or not isinstance(value, str) or len(value.encode("utf-8")) > 8192
                or any(c in value for c in "\r\n\0")):
            raise ValueError("Invalid HTTP header")
    return headers


class SourceError(Exception):
    def __init__(self, scope, code, message, retryable=False, retry_after=None):
        super().__init__(message)
        self.fields = dict(scope=scope, code=code,
                           message=utf8_prefix(message, 8192) or "Request failed",
                           retryable=bool(retryable))
        if retry_after is not None:
            self.fields["retry_after_seconds"] = max(0, min(86400, int(retry_after)))


class CommandDeadline:
    def __init__(self, deadline_at, scope):
        deadline = datetime.datetime.fromisoformat(deadline_at.replace("Z", "+00:00"))
        if deadline.tzinfo is None or deadline.utcoffset() is None:
            raise ValueError("deadline_at must include a timezone")
        seconds = (deadline - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
        self.expires_at = time.monotonic() + seconds - 0.05
        self.scope = scope
        self.cancelled = threading.Event()

    def remaining(self):
        value = self.expires_at - time.monotonic()
        if self.cancelled.is_set() or value <= 0:
            raise SourceError(self.scope, "source_unavailable", "Command deadline reached", True)
        return value


def run_before_deadline(operation, deadline, *args):
    # A socket read timeout alone does not bound slow, continuously streaming responses.
    outcomes = queue.Queue(maxsize=1)

    def execute():
        try:
            outcomes.put((operation(*args, deadline), None))
        except Exception as error:
            outcomes.put((None, error))

    deadline.remaining()
    threading.Thread(target=execute, daemon=True).start()
    try:
        result, error = outcomes.get(timeout=deadline.remaining())
    except queue.Empty:
        deadline.cancelled.set()
        raise SourceError(deadline.scope, "source_unavailable", "Command deadline reached", True)
    deadline.remaining()
    if error is not None:
        raise error
    return result


def retry_after(value):
    if not value:
        return None
    try:
        return max(0, min(86400, int(value)))
    except (ValueError, TypeError):
        try:
            when = parsedate_to_datetime(value)
            return max(0, min(86400, int((when - datetime.datetime.now(datetime.timezone.utc)).total_seconds())))
        except (ValueError, TypeError, OverflowError):
            return None


def duration_seconds(text):
    text = str(text or "").strip()
    if re.fullmatch(r"\d+(?::\d{1,2}){1,2}", text):
        total = 0
        for part in text.split(":"):
            total = total * 60 + int(part)
        return total if total <= 604800 else None
    match = re.fullmatch(r"P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?", text)
    if match:
        d, h, m, s = match.groups()
        total = int(float(d or 0) * 86400 + float(h or 0) * 3600 + float(m or 0) * 60 + float(s or 0))
        return total if total <= 604800 else None
    h = re.search(r"(\d+)\s*小时", text)
    m = re.search(r"(\d+)\s*分钟", text)
    s = re.search(r"(\d+)\s*秒", text)
    if h or m or s:
        total = (int(h[1]) * 3600 if h else 0) + (int(m[1]) * 60 if m else 0) + (int(s[1]) if s else 0)
        return total if total <= 604800 else None
    return int(text) if text.isdigit() and int(text) <= 604800 else None


def meta_value(soup, name):
    node = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"itemprop": re.compile("^" + re.escape(name) + "$", re.I)})
    return node.get("content", "").strip() if node else ""


def page_metadata(soup):
    title = meta_value(soup, "og:title")
    if not title:
        node = soup.select_one("h1") or soup.select_one("title")
        title = node.get_text(" ", strip=True) if node else ""
    return dict(title=title, thumbnail_url=meta_value(soup, "og:image"),
                duration_seconds=duration_seconds(meta_value(soup, "video:duration") or meta_value(soup, "duration")))


def image_url(node, base):
    if node is None:
        return ""
    for key in ("data-original", "data-lazy-src", "data-src", "src"):
        value = node.get(key, "").strip()
        if value and not value.startswith("data:"):
            return urljoin(base, value)
    return ""


class BaseCrawler:
    def __init__(self, job):
        self.feed_id = job["feed_id"]
        self.config = job.get("config", {})
        self.base_url = http_url(self.config.get("base_url", DEFAULT_BASE_URL)).rstrip("/")
        self.custom_headers = protocol_headers(self.config.get("headers", {}))
        self.session = requests.Session()  # requests' default adapter performs no automatic retries.
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
        self.session.headers.update(self.custom_headers)
        proxy = job.get("network", {}).get("proxy_url")
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})
        self.pages = {}
        self.metadata = {}
        self.state_lock = threading.Lock()

    def fetch_text(self, url, deadline, scope, referer=None, method="GET", headers=None):
        # Expired operations keep their own Session and cannot commit late cookies or cache entries.
        with requests.Session() as session:
            with self.state_lock:
                deadline.remaining()
                session.headers.update(self.session.headers)
                session.cookies.update(self.session.cookies)
                session.proxies.update(self.session.proxies)
                session.trust_env = self.session.trust_env
            request_headers = dict(headers or {})
            if referer:
                request_headers["Referer"] = referer
            for _ in range(11):
                deadline.remaining()
                try:
                    with session.request(method, http_url(url), headers=request_headers,
                                         data=b"" if method == "POST" else None,
                                         timeout=Timeout(total=min(30, deadline.remaining())),
                                         stream=True, allow_redirects=False) as response:
                        deadline.remaining()
                        status = response.status_code
                        if status in {301, 302, 303, 307, 308}:
                            location = response.headers.get("Location")
                            if not location:
                                raise SourceError(scope, "parse_failed", "Redirect has no Location header")
                            url = http_url(urljoin(url, location))
                            if status == 303 or (status in {301, 302} and method == "POST"):
                                method = "GET"
                            continue
                        if status >= 400:
                            code = {401: "auth_required", 403: "auth_required", 404: "not_found",
                                    410: "not_found", 429: "rate_limited"}.get(status, "source_unavailable")
                            error_scope = "source" if code in {"auth_required", "rate_limited"} else scope
                            raise SourceError(error_scope, code, "Source returned HTTP %d" % status,
                                              code in {"rate_limited", "source_unavailable"},
                                              retry_after(response.headers.get("Retry-After"))
                                              if code in {"rate_limited", "source_unavailable"} else None)
                        chunks, size = [], 0
                        for chunk in response.iter_content(chunk_size=65536):
                            deadline.remaining()
                            size += len(chunk)
                            if size > 8 * 1024 * 1024:
                                raise SourceError(scope, "parse_failed", "Source response exceeds 8 MiB")
                            chunks.append(chunk)
                        text = b"".join(chunks).decode("utf-8", errors="replace")
                        if ("cf-chl-" in text and "challenge-platform" in text) or ("Just a moment" in text and len(text) < 8000):
                            raise SourceError("source", "auth_required", "Source requires browser verification or a valid Cookie")
                        with self.state_lock:
                            deadline.remaining()
                            self.session.cookies.update(session.cookies)
                        return text
                except requests.exceptions.RequestException as error:
                    raise SourceError(scope, "source_unavailable", "Source network request failed", True) from error
            raise SourceError(scope, "source_unavailable", "Too many source redirects", True)

    def next_page(self, soup, page):
        base = soup.find("base", href=True)
        reference = urljoin(self.page_url(page), base["href"]) if base else self.page_url(page)
        for link in soup.select("a[href]"):
            if link.get("aria-disabled") == "true" or "disabled" in link.get("class", []):
                continue
            address = urlparse(urljoin(reference, link["href"]))
            if address.hostname != urlparse(self.base_url).hostname:
                continue
            number = self.page_number(address)
            if number is not None and number > page:
                return page + 1
        return None

    def discover(self, cursor, limit, deadline):
        deadline.remaining()
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer between 1 and 100")
        if cursor is None:
            page, offset = 1, 0
        else:
            identifier(cursor, "cursor")
            match = re.fullmatch(r"page:([1-9][0-9]*):(0|[1-9][0-9]*)", cursor)
            if not match:
                raise ValueError("Invalid pagination cursor")
            page, offset = map(int, match.groups())
        if page not in self.pages:
            text = self.fetch_text(self.page_url(page), deadline, "source", self.base_url + "/")
            soup = BeautifulSoup(text, "html.parser")
            rows = self.parse_listing(soup)
            if not rows and not (soup.select_one(".no-results, .no-videos, .empty-list")
                                 or re.search(r"no (?:videos|results|items)|暂无视频|没有视频", soup.get_text(), re.I)):
                raise SourceError("source", "parse_failed", "Unrecognized video list structure")
            previous = {key for number, cached in self.pages.items() if number < page for key in cached["keys"]}
            items, keys = [], set()
            for row in rows:
                source_id = identifier(str(row["id"]), "source_id")
                key = identifier("detail:" + source_id, "discovery_key")
                if key in keys:
                    continue
                keys.add(key)
                if key not in previous:
                    items.append(dict(discovery_key=key, source_id=source_id,
                                      locator=dict(id=source_id, detail_url=http_url(row["detail_url"]))))
            following = self.next_page(soup, page)
            with self.state_lock:
                deadline.remaining()
                self.pages[page] = dict(items=items, keys=keys, next_page=following)
                for row in rows:
                    self.metadata.setdefault(str(row["id"]), row)
        cached = self.pages[page]
        if offset > len(cached["items"]):
            raise ValueError("Cursor offset is outside the page")
        selected = cached["items"][offset:offset + limit]
        end = offset + len(selected)
        following = ("page:%d:%d" % (page, end) if end < len(cached["items"])
                     else "page:%d:0" % cached["next_page"] if cached["next_page"] is not None else None)
        deadline.remaining()
        return dict(items=selected, next_cursor=following)

    def media_object(self, url, referer):
        url = http_url(url)
        headers = {"User-Agent": self.session.headers["User-Agent"]}
        if referer:
            headers["Referer"] = referer
        headers.update(self.custom_headers)
        with self.state_lock:
            prepared = self.session.prepare_request(requests.Request("GET", url, headers=headers))
        if prepared.headers.get("Cookie"):
            headers["Cookie"] = prepared.headers["Cookie"]
        return dict(type="url", url=url, headers=protocol_headers(headers))

    def resolve(self, candidate, deadline):
        deadline.remaining()
        key = identifier(candidate["discovery_key"], "discovery_key")
        locator = candidate["locator"]
        if not isinstance(locator, dict) or len(json.dumps(locator, ensure_ascii=False).encode("utf-8")) > 16384:
            raise ValueError("locator must be an object of at most 16 KiB")
        source_id = identifier(candidate.get("source_id", locator["id"]), "source_id")
        if source_id != identifier(locator["id"], "locator.id"):
            raise ValueError("Candidate source_id does not match locator.id")
        detail = http_url(locator["detail_url"])
        data = self.resolve_detail(source_id, detail, deadline)
        row = self.metadata.get(source_id, {})
        title = utf8_prefix(str(data.get("title") or row.get("title") or "").strip(), 4096)
        if not title or not data.get("media_url"):
            raise SourceError("item", "parse_failed", "Missing video title or playable media URL")
        result = dict(discovery_key=key, source_id=source_id, title=title, detail_url=detail,
                      media=self.media_object(urljoin(detail, data["media_url"]), data.get("media_referer", detail)))
        thumbnail = data.get("thumbnail_url") or row.get("thumbnail_url")
        if thumbnail:
            result["thumbnail"] = self.media_object(urljoin(detail, thumbnail), data.get("thumbnail_referer", detail))
        for field, size in (("author", 1024), ("description", 65536)):
            value = str(data.get(field) or row.get(field) or "").strip()
            if value:
                result[field] = utf8_prefix(value, size)
        duration = data.get("duration_seconds")
        if duration is None:
            duration = row.get("duration_seconds")
        if type(duration) is int and 0 <= duration <= 604800:
            result["duration_seconds"] = duration
        # Site tags cannot create project tags. Only explicitly supplied existing names are eligible.
        existing = self.config.get("existing_tags", [])
        if isinstance(existing, list):
            tags = list(dict.fromkeys(t for t in data.get("tags", []) if isinstance(t, str)
                                     and t.strip() and t in existing and len(t.encode("utf-8")) <= 256))[:100]
            if tags:
                result["tags"] = tags
        deadline.remaining()
        return result


def unique_keys(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate JSON key: " + key)
        value[key] = item
    return value


def load_json(text):
    def reject_constant(value):
        raise ValueError("Invalid JSON constant: " + value)
    value = json.loads(text, object_pairs_hook=unique_keys, parse_constant=reject_constant)
    if not isinstance(value, dict):
        raise ValueError("JSON must be an object")
    return value


class ProtocolWriter:
    def __init__(self):
        self.total_bytes = 0

    def write(self, response):
        line = json.dumps(response, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"
        size = len(line.encode("utf-8"))
        if size > 1024 * 1024 or self.total_bytes + size > 64 * 1024 * 1024:
            raise ValueError("Protocol output size limit reached")
        sys.stdout.write(line)
        sys.stdout.flush()
        self.total_bytes += size


def main():
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=CRAWLER_NAME + " crawler.v3")
    parser.add_argument("--job", required=True)
    args = parser.parse_args()
    with open(args.job, encoding="utf-8") as file:
        job = load_json(file.read())
    if job.get("protocol") != CRAWLER_PROTOCOL:
        raise ValueError("Unsupported protocol")
    if job.get("feed_id") not in {f["id"] for f in json.loads(CRAWLER_FEEDS)}:
        raise ValueError("Unsupported feed_id")
    for field in ("task_id", "crawler_id"):
        if not isinstance(job.get(field), str) or not job[field].strip():
            raise ValueError(field + " must be a nonempty string")
    if not isinstance(job.get("work_dir"), str) or not os.path.isabs(job["work_dir"]):
        raise ValueError("work_dir must be an absolute path")
    for field in ("config", "network"):
        job.setdefault(field, {})
        if not isinstance(job[field], dict):
            raise ValueError(field + " must be an object")
    proxy = job["network"].get("proxy_url")
    if proxy is not None and (not isinstance(proxy, str) or not proxy.strip()):
        raise ValueError("network.proxy_url must be a nonempty string")
    if proxy:
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ[name] = proxy
        os.environ["NO_PROXY"] = os.environ["no_proxy"] = ""
    crawler = Crawler(job)
    writer = ProtocolWriter()
    try:
        while True:
            line = sys.stdin.readline(1024 * 1024 + 1)
            if not line:
                raise EOFError("stdin closed before stop")
            if len(line.encode("utf-8")) > 1024 * 1024:
                raise ValueError("Command exceeds 1 MiB")
            command = load_json(line)
            request_id = identifier(command.get("request_id"), "request_id")
            kind = command.get("type")
            response = dict(request_id=request_id)
            if kind == "stop":
                writer.write(dict(type="stopped", **response))
                return
            scope = "source" if kind == "discover" else "item"
            deadline = None
            try:
                deadline = CommandDeadline(command["deadline_at"], scope)
                if kind == "discover":
                    fields = run_before_deadline(crawler.discover, deadline, command["cursor"], command["limit"])
                    response.update(type="page", **fields)
                elif kind == "resolve":
                    fields = run_before_deadline(crawler.resolve, deadline, command["candidate"])
                    response.update(type="item", **fields)
                else:
                    raise ValueError("Unknown command type")
            except SourceError as error:
                response.update(type="error", **error.fields)
            except Exception as error:
                traceback.print_exc(file=sys.stderr)
                response.update(type="error", scope=scope, code="parse_failed",
                                message=utf8_prefix(error, 8192) or "Parse failed", retryable=False)
            finally:
                if deadline is not None:
                    deadline.cancelled.set()
            writer.write(response)
    finally:
        crawler.session.close()



DEFAULT_BASE_URL = "https://kanav.ad"


class Crawler(BaseCrawler):
    def __init__(self, job):
        super().__init__(job)
        category = str(self.config.get("category_id", 22))
        if not category.isdigit() or int(category) < 1:
            raise ValueError("config.category_id must be a positive integer")
        self.category_id = str(int(category))

    def page_url(self, page):
        if self.feed_id == "hot":
            return self.base_url + "/index.php/label/hot.html"
        suffix = "/page/%d" % page if page > 1 else ""
        return self.base_url + "/index.php/vod/show/id/" + self.category_id + suffix + ".html"

    def page_number(self, address):
        if self.feed_id == "hot":
            return None
        match = re.fullmatch(r"/index.php/vod/show/id/" + self.category_id + r"/page/(\d+)\.html", address.path)
        return int(match[1]) if match else None

    def parse_listing(self, soup):
        rows = []
        for card in soup.select("div.video-item"):
            link = card.find("a", href=re.compile(r"/vod/play/id/\d+"))
            if not link:
                continue
            source_id = re.search(r"/vod/play/id/(\d+)", link["href"])[1]
            image = card.find("img")
            duration = card.select_one(".model-view")
            rows.append(dict(id=source_id, detail_url=urljoin(self.base_url, link["href"]),
                             title=image.get("alt", "") if image else link.get_text(" ", strip=True),
                             thumbnail_url=image_url(image, self.base_url),
                             duration_seconds=duration_seconds(duration.get_text(strip=True)) if duration else None))
        return rows

    def resolve_detail(self, source_id, detail, deadline):
        text = self.fetch_text(detail, deadline, "item", self.base_url + "/")
        soup = BeautifulSoup(text, "html.parser")
        result = page_metadata(soup)
        match = re.search(r"\bvar\s+player_\w+\s*=\s*(\{)", text)
        if not match:
            raise SourceError("item", "parse_failed", "MacCMS player configuration not found")
        player, _ = json.JSONDecoder().raw_decode(text[match.start(1):])
        if player.get("id") and str(player["id"]) != source_id:
            raise SourceError("item", "parse_failed", "Player does not match the requested video")
        media = player.get("url", "")
        encryption = int(player.get("encrypt", 0))
        if encryption == 2:
            media = unquote(base64.b64decode(unquote(media)).decode("utf-8"))
        elif encryption == 1:
            media = unquote(media)
        result["media_url"] = urljoin(detail, media.split("$$$")[0].strip()) if media else ""
        vod = player.get("vod_data", {})
        if isinstance(vod, dict):
            result["title"] = vod.get("vod_name") or result["title"]
            result["author"] = vod.get("vod_actor", "")
        result["thumbnail_referer"] = ""
        return result


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, BrokenPipeError):
        os._exit(0)
