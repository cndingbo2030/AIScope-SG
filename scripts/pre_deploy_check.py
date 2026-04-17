"""
Pre-deploy: normalize absolute paths under web/ to relative paths, then verify SEO.

Run before GitHub Pages deploy so cndingbo2030.github.io/AIScope-SG/ and other subpaths work.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
WEB = BASE / "web"
INDEX = WEB / "index.html"
APP_JS = WEB / "app.js"
I18N_JSON = WEB / "data" / "i18n.json"

RECENCY_META = """<meta name="aiscope-recency" content="AI Exposure Model v2026.4 · Updated Apr 2026">
<meta name="aiscope-methodology" content="./methodology.html">
"""

TICKER_TEXT = "● Live Data: AI Exposure Model v2026.4 (Updated Apr 2026)"


def inject_index_recency(html: str) -> tuple[str, bool]:
    """Insert machine-readable recency + default ticker (i18n overrides in app.js)."""
    changed = False
    if "<!--AISCOPE_RECENCY_META-->" in html:
        html = html.replace("<!--AISCOPE_RECENCY_META-->", RECENCY_META, 1)
        changed = True
    if "__AISCOPE_TICKER__" in html:
        html = html.replace("__AISCOPE_TICKER__", TICKER_TEXT, 1)
        changed = True
    return html, changed


# Replace root-absolute same-origin paths (not // URLs) with ./ prefix for static hosting.
# Examples: href="/data/x" -> href="./data/x", fetch("/data/ -> fetch("./data/
REPLACEMENTS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r'(\bhref\s*=\s*")/'), r'\1./'),
    (re.compile(r"(\bhref\s*=\s*')/"), r"\1./"),
    (re.compile(r'(\bsrc\s*=\s*")/'), r'\1./'),
    (re.compile(r"(\bsrc\s*=\s*')/"), r"\1./"),
    (re.compile(r'(\burl\s*\(\s*")/'), r'\1./'),
    (re.compile(r"(\burl\s*\(\s*')/"), r"\1./"),
    (re.compile(r'(\bfetch\s*\(\s*")/'), r'\1./'),
    (re.compile(r"(\bfetch\s*\(\s*')/"), r"\1./"),
    (re.compile(r'(\bimport\s+.*?from\s*")/'), r'\1./'),
    (re.compile(r"(\bimport\s+.*?from\s*')/"), r"\1./"),
]


def scan_files() -> list[Path]:
    paths: list[Path] = []
    for ext in (".html", ".htm", ".js", ".css"):
        paths.extend(WEB.rglob(f"*{ext}"))
    return sorted(set(paths))


def fix_file(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    original = text
    for pattern, repl in REPLACEMENTS:
        text = pattern.sub(repl, text)
    # Catch remaining obvious same-document absolute refs (avoid //)
    text = re.sub(r'(?<!["\'])(?<![:/])href="/(?!/)', 'href="./', text)
    text = re.sub(r'(?<!["\'])(?<![:/])src="/(?!/)', 'src="./', text)
    if text != original:
        path.write_text(text, encoding="utf-8")
        print(f"[pre-deploy] fixed paths -> {path.relative_to(BASE)}")
        return True
    return False


def audit_html_root_absolute_paths() -> list[str]:
    """Fail if any web/**/*.html uses same-origin root-absolute href/src (breaks /repo/ Pages)."""
    errs: list[str] = []
    bad = re.compile(r"""(?:href|src)\s*=\s*["']/(?!/|https?:)""")
    for path in sorted(WEB.rglob("*.html")):
        text = path.read_text(encoding="utf-8")
        for m in bad.finditer(text):
            snippet = text[m.start() : m.start() + 48].replace("\n", " ")
            errs.append(f"{path.relative_to(BASE)}: root-absolute ref near {snippet!r}")
    return errs


def _t_keys_from_app_js() -> set[str]:
    if not APP_JS.exists():
        return set()
    text = APP_JS.read_text(encoding="utf-8")
    return set(re.findall(r'\bt\(\s*"([a-z0-9_]+)"\s*\)', text, flags=re.IGNORECASE))


def _data_i18n_keys_from_index() -> set[str]:
    if not INDEX.exists():
        return set()
    text = INDEX.read_text(encoding="utf-8")
    return set(re.findall(r'data-i18n\s*=\s*"([^"]+)"', text))


def verify_i18n_json_coverage() -> list[str]:
    errs: list[str] = []
    if not I18N_JSON.exists():
        errs.append(f"missing {I18N_JSON.relative_to(BASE)}")
        return errs
    try:
        bundle: dict[str, object] = json.loads(I18N_JSON.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"i18n.json invalid JSON: {exc}"]

    needed = _t_keys_from_app_js() | _data_i18n_keys_from_index()
    missing = sorted(k for k in needed if k not in bundle)
    if missing:
        sample = ", ".join(missing[:24])
        errs.append(f"i18n.json missing {len(missing)} key(s) used by UI (e.g. {sample})")

    for key, row in bundle.items():
        if not isinstance(row, dict):
            errs.append(f"i18n.json[{key!r}] must be an object with en/zh strings")
            continue
        if "en" not in row or "zh" not in row:
            errs.append(f"i18n.json[{key!r}] must include both 'en' and 'zh'")
    return errs


def verify_index() -> list[str]:
    errors: list[str] = []
    if not INDEX.exists():
        errors.append("missing web/index.html")
        return errors

    html = INDEX.read_text(encoding="utf-8")

    if 'property="og:title"' not in html and "property='og:title'" not in html:
        errors.append("index.html: missing og:title")
    if 'property="og:description"' not in html:
        errors.append("index.html: missing og:description")
    if 'property="og:image"' not in html:
        errors.append("index.html: missing og:image")

    if "rel=\"canonical\"" not in html and "rel='canonical'" not in html:
        errors.append("index.html: missing canonical link")
    elif "https://cndingbo2030.github.io/AIScope-SG" not in html:
        errors.append("index.html: canonical/OG should reference GitHub Pages base URL")

    if 'name="aiscope-recency"' not in html and "name='aiscope-recency'" not in html:
        errors.append("index.html: missing aiscope-recency meta (run pre_deploy_check to inject)")

    # Reject obvious root-absolute asset refs that break under /repo/
    bad = re.findall(r'(?:href|src)\s*=\s*"/(?!/)', html)
    if bad:
        errors.append(f"index.html: still contains root-absolute href/src ({len(bad)} hits)")

    return errors


def main() -> int:
    if not WEB.is_dir():
        print("ERROR: web/ directory not found", file=sys.stderr)
        return 1

    cname_path = WEB / "CNAME"
    if cname_path.is_file():
        print(
            "ERROR: web/CNAME is present. GitHub Pages will treat it as a custom domain and "
            "redirect github.io visitors. Remove web/CNAME and manage domains only via "
            "repository Settings → Pages when you are ready for production DNS.",
            file=sys.stderr,
        )
        return 1

    if INDEX.exists():
        idx_html = INDEX.read_text(encoding="utf-8")
        idx_new, idx_changed = inject_index_recency(idx_html)
        if idx_changed:
            INDEX.write_text(idx_new, encoding="utf-8")
            print("[pre-deploy] injected recency meta + default ticker into index.html")

    changed = 0
    for path in scan_files():
        if fix_file(path):
            changed += 1

    print(f"[pre-deploy] scanned {len(scan_files())} files, {changed} updated")

    errors = verify_index()
    errors.extend(audit_html_root_absolute_paths())
    errors.extend(verify_i18n_json_coverage())
    if errors:
        print("ERROR: pre-deploy verification failed:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    print("[pre-deploy] index.html SEO checks passed (OG + canonical -> github.io/AIScope-SG)")
    print("[pre-deploy] HTML path audit + i18n key coverage OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
