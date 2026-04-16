"""
Local server that mimics GitHub Pages project-site URLs: /AIiScope-SG/... → web/...

Use to verify 404.html redirect + ?job= query preservation.
"""

from __future__ import annotations

import argparse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

BASE = Path(__file__).resolve().parent.parent
WEB = BASE / "web"
DEFAULT_PREFIX = "/AIiScope-SG"


class GHProjectSiteHandler(SimpleHTTPRequestHandler):
    repo_prefix = DEFAULT_PREFIX

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WEB), **kwargs)

    def log_message(self, fmt: str, *args) -> None:
        print(f"[simulate] {self.address_string()} - {fmt % args}")

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        raw_path = unquote(parsed.path)
        query = f"?{parsed.query}" if parsed.query else ""
        fragment = f"#{parsed.fragment}" if parsed.fragment else ""

        if raw_path in ("", "/"):
            self.send_response(302)
            loc = self.repo_prefix.rstrip("/") + "/"
            if query:
                loc += query
            if fragment:
                loc += fragment
            self.send_header("Location", loc)
            self.end_headers()
            return

        if raw_path != self.repo_prefix and not raw_path.startswith(self.repo_prefix + "/"):
            self.send_response(404)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Use project prefix, e.g. /AIiScope-SG/\n")
            return

        rel = raw_path[len(self.repo_prefix) :] or "/"
        if rel == "/":
            rel = "/index.html"

        fs_path = (WEB / rel.lstrip("/")).resolve()
        try:
            fs_path.relative_to(WEB)
        except ValueError:
            self.send_error(403)
            return

        if fs_path.is_file():
            self.path = rel + query + fragment
            return super().do_GET()

        # Missing asset under prefix → behave like GH Pages custom 404
        self.send_response(404)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        not_found = WEB / "404.html"
        if not_found.is_file():
            self.wfile.write(not_found.read_bytes())
        else:
            self.wfile.write(b"404 Not Found\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate GitHub Pages /REPO/ layout locally.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="Leading path segment, e.g. /AIiScope-SG")
    args = parser.parse_args()
    prefix = args.prefix if args.prefix.startswith("/") else f"/{args.prefix}"
    GHProjectSiteHandler.repo_prefix = prefix.rstrip("/") or DEFAULT_PREFIX

    httpd = ThreadingHTTPServer((args.host, args.port), GHProjectSiteHandler)
    print(f"[simulate] Serving {WEB} at http://{args.host}:{args.port}{GHProjectSiteHandler.repo_prefix}/")
    print(f"[simulate] Try: http://{args.host}:{args.port}{GHProjectSiteHandler.repo_prefix}/does-not-exist?job=123")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
