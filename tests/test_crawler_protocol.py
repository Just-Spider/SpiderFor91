"""Offline regression checks: python -B -m unittest discover -s tests -v."""

import datetime
import importlib.util
import json
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = (
    "91Pinse/91Pinse.py",
    "91Porn/91Porn.py",
    "BaddiesOnly/BaddiesOnly.py",
    "Fapnut/fapnut.py",
    "Kanav/kanav.py",
    "MemoJav/MemoJav.py",
    "Pimpbunny/Pimpbunny.py",
    "WatchPorn/WatchPorn.py",
)


class SourceHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.requests[self.path] += 1
        if self.path == "/disconnect":
            self.close_connection = True
            return
        if self.path == "/redirect-loop":
            self.send_response(302)
            self.send_header("Location", self.path)
        else:
            self.send_response(int(self.path.rsplit("/", 1)[1]))
            self.send_header("Retry-After", "60")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


class CrawlerProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.modules = {}
        for index, relative in enumerate(SCRIPTS):
            spec = importlib.util.spec_from_file_location(
                "protocol_test_crawler_%d" % index, ROOT / relative)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            cls.modules[relative] = module
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), SourceHandler)
        cls.server.daemon_threads = True
        cls.server.requests = Counter()
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.base_url = "http://127.0.0.1:%d" % cls.server.server_port

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=2)

    @contextmanager
    def crawler(self, module, config=None):
        feed = next(f["id"] for f in json.loads(module.CRAWLER_FEEDS) if f.get("default"))
        crawler_class = (module.Porn91ProtocolCrawler if hasattr(module, "Porn91ProtocolCrawler")
                         else module.Crawler)
        crawler = crawler_class(dict(feed_id=feed, config={"base_url": self.base_url, **(config or {})}))
        crawler.session.trust_env = False
        try:
            yield crawler
        finally:
            crawler.session.close()

    @staticmethod
    def deadline(module, scope="source"):
        when = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=10)
        return module.CommandDeadline(when.isoformat(), scope)

    def fetch(self, crawler, module, path, scope):
        deadline = self.deadline(module, scope)
        if hasattr(crawler, "fetch_text"):
            return crawler.fetch_text(self.base_url + path, deadline, scope, self.base_url)
        return crawler.fetch_page(self.base_url + path, deadline, scope, self.base_url)

    def test_pagination_continues_after_duplicate_or_empty_page(self):
        for relative, module in self.modules.items():
            for middle in (["1", "2", "3"], []):
                with self.subTest(crawler=relative, middle=middle), self.crawler(module) as crawler:
                    rows = {1: ["1", "2", "3"], 2: middle, 3: ["2", "4", "5"]}
                    fetched = []

                    def fetch_page(url, *args):
                        page = int(url.rsplit("/", 1)[1])
                        fetched.append(page)
                        return '<div class="row no-videos" data-page="%d"></div>' % page

                    def parse_listing(text, *args):
                        page = int(re.search(r'data-page="(\d+)"', str(text))[1])
                        return [dict(id=i, viewkey=i, title="Video " + i,
                                     detail_url=self.base_url + "/detail/" + i) for i in rows[page]]

                    crawler.page_url = lambda page: self.base_url + "/page/" + str(page)
                    crawler.next_page = lambda text, page: page + 1 if page < 3 else None
                    if hasattr(crawler, "spider"):
                        crawler.fetch_page = fetch_page
                        crawler.spider.parse_list_page = parse_listing
                    else:
                        crawler.fetch_text = fetch_page
                        crawler.parse_listing = parse_listing

                    first = crawler.discover(None, 2, self.deadline(module))
                    self.assertEqual([i["source_id"] for i in first["items"]], ["1", "2"])
                    self.assertEqual(first["next_cursor"], "page:1:2")
                    self.assertEqual(first, crawler.discover(None, 2, self.deadline(module)))

                    tail = crawler.discover(first["next_cursor"], 2, self.deadline(module))
                    self.assertEqual([i["source_id"] for i in tail["items"]], ["3"])
                    self.assertEqual(tail["next_cursor"], "page:2:0")

                    empty = crawler.discover(tail["next_cursor"], 2, self.deadline(module))
                    self.assertEqual(empty, dict(items=[], next_cursor="page:3:0"))
                    self.assertEqual(empty, crawler.discover(tail["next_cursor"], 2, self.deadline(module)))

                    final = crawler.discover(empty["next_cursor"], 2, self.deadline(module))
                    self.assertEqual([i["source_id"] for i in final["items"]], ["4", "5"])
                    self.assertIsNone(final["next_cursor"])
                    self.assertEqual(final, crawler.discover(empty["next_cursor"], 2, self.deadline(module)))
                    self.assertEqual(fetched, [1, 2, 3])

    def test_http_errors_keep_operation_scope_without_automatic_retries(self):
        cases = (
            (401, "auth_required", False),
            (403, "auth_required", False),
            (404, "not_found", False),
            (410, "not_found", False),
            (429, "rate_limited", True),
            (500, "source_unavailable", True),
            (503, "source_unavailable", True),
        )
        for relative, module in self.modules.items():
            for scope in ("item", "source"):
                for status, code, retryable in cases:
                    with self.subTest(crawler=relative, scope=scope, status=status), self.crawler(module) as crawler:
                        path = "/status/%d" % status
                        before = self.server.requests[path]
                        with self.assertRaises(module.SourceError) as raised:
                            self.fetch(crawler, module, path, scope)
                        fields = raised.exception.fields
                        expected_scope = "source" if code in {"auth_required", "rate_limited"} else scope
                        self.assertEqual(fields["scope"], expected_scope)
                        self.assertEqual(fields["code"], code)
                        self.assertEqual(fields["retryable"], retryable)
                        if retryable:
                            self.assertEqual(fields["retry_after_seconds"], 60)
                        self.assertEqual(self.server.requests[path] - before, 1)

    def test_connection_and_redirect_failures_keep_operation_scope(self):
        for relative, module in self.modules.items():
            for scope in ("item", "source"):
                for path, count in (("/disconnect", 1), ("/redirect-loop", 11)):
                    with self.subTest(crawler=relative, scope=scope, path=path), self.crawler(module) as crawler:
                        before = self.server.requests[path]
                        with self.assertRaises(module.SourceError) as raised:
                            self.fetch(crawler, module, path, scope)
                        fields = raised.exception.fields
                        self.assertEqual(fields["scope"], scope)
                        self.assertEqual(fields["code"], "source_unavailable")
                        self.assertTrue(fields["retryable"])
                        self.assertEqual(self.server.requests[path] - before, count)

    def test_91porn_job_ignores_legacy_max_pages(self):
        module = self.modules["91Porn/91Porn.py"]
        for max_pages in (1, 2, 0, "legacy"):
            for feed_id, category in module.FEED_CATEGORIES.items():
                with self.subTest(max_pages=max_pages, feed=feed_id), self.crawler(module, {"max_pages": max_pages}) as crawler:
                    crawler.category = category
                    html = '<a href="v.php?category=%s&amp;page=10">Last page</a>' % category
                    self.assertEqual(crawler.next_page(html, 1), 2)
                    self.assertEqual(crawler.next_page(html, 9), 10)
                    self.assertIsNone(crawler.next_page(html, 10))

    def test_91porn_standalone_still_stops_at_max_pages(self):
        module = self.modules["91Porn/91Porn.py"]
        for max_pages in (1, 2):
            with self.subTest(max_pages=max_pages):
                spider = module.Porn91Spider(max_pages=max_pages, max_empty_pages=4, resume=False)
                try:
                    spider.log = lambda *args: None
                    spider.random_sleep = lambda *args: None
                    spider.fetch_page = Mock(return_value="<empty />")
                    spider.parse_list_page = Mock(return_value=[])
                    spider._save_results = Mock()
                    spider._print_summary = lambda: None
                    spider.crawl()
                    self.assertEqual(spider.fetch_page.call_count, max_pages)
                    spider._save_results.assert_called_once_with()
                finally:
                    spider.session.close()

    def test_copied_single_file_accepts_stop(self):
        with tempfile.TemporaryDirectory(prefix="crawler-protocol-test-") as temp:
            work = Path(temp)
            for relative, module in self.modules.items():
                with self.subTest(crawler=relative):
                    copied = work / Path(relative).name
                    shutil.copyfile(ROOT / relative, copied)
                    feed = next(f["id"] for f in json.loads(module.CRAWLER_FEEDS) if f.get("default"))
                    job = dict(protocol="crawler.v3", task_id="test-task", crawler_id="test-crawler",
                               feed_id=feed, work_dir=str(work), config={"base_url": self.base_url})
                    job_file = work / "job.json"
                    job_file.write_text(json.dumps(job), encoding="utf-8")
                    stop = dict(type="stop", request_id="test-stop", reason="any-reason",
                                deadline_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
                    result = subprocess.run(
                        [sys.executable, "-B", str(copied), "--job", str(job_file)],
                        cwd=work, input=json.dumps(stop) + "\n", capture_output=True,
                        encoding="utf-8", timeout=5)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(len(result.stdout.splitlines()), 1)
                    self.assertEqual(json.loads(result.stdout), dict(type="stopped", request_id="test-stop"))


if __name__ == "__main__":
    unittest.main()
