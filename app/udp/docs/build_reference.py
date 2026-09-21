#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Render README.md into a Google Cloud themed, single-file HTML reference.

Why a generator instead of hand-written HTML: README.md is the single source of
truth for the platform specification. Re-running this script after any README
edit keeps the published document byte-for-byte in sync, so the two can never
drift apart.

Usage:
    python3 app/udp/docs/build_reference.py             # writes docs/index.html
    python3 app/udp/docs/build_reference.py --serve     # build, then serve on :8848
"""

from __future__ import annotations

import argparse
import datetime
import html
import os
import re
from pathlib import Path

import markdown
from pygments.formatters import HtmlFormatter

# Paths are anchored to this script's own directory rather than a fixed depth
# from the repo root, so the docs folder can be relocated without edits here.
DOCS_DIR = Path(__file__).resolve().parent
REPO_ROOT = DOCS_DIR.parents[2]
README = DOCS_DIR / "README.md"
TEMPLATE = DOCS_DIR / "_reference_template.html"
OUTPUT = DOCS_DIR / "index.html"
DIAGRAM_DIR = DOCS_DIR / "assets" / "diagrams"

# Absolute file:// links in the README are rewritten to paths relative to the
# docs folder so they stay clickable when the page is served over HTTP.
ABSOLUTE_PREFIX = "file://" + str(REPO_ROOT) + "/"
REL_TO_ROOT = os.path.relpath(REPO_ROOT, DOCS_DIR) + "/"

ALERT_STYLES = {
    "NOTE": ("note", "Note"),
    "TIP": ("tip", "Tip"),
    "IMPORTANT": ("important", "Important"),
    "WARNING": ("warning", "Warning"),
    "CAUTION": ("caution", "Caution"),
}

# The README carries box-drawing ASCII diagrams so it stays readable as plain
# text on GitHub. For the rendered page they are swapped for hand-drawn SVGs
# that follow the Google Cloud visual language. Each entry is matched against
# the *contents* of a fenced block, so the README can be reflowed freely
# without breaking the mapping.
#   fingerprint -> (svg filename, figure number, caption)
DIAGRAMS: list[tuple[str, str, str, str]] = [
    (
        "UPSTREAM SOURCE SYSTEMS",
        "medallion-lifecycle.svg",
        "Figure 1",
        "End-to-end medallion lifecycle. Records never move directly from a source "
        "system into BigQuery — every row lands immutably in GCS first.",
    ),
    (
        "TaskGroup: ingestion_group",
        "dag-anatomy.svg",
        "Figure 2",
        "Anatomy of a generated DAG. The factory emits the same two-TaskGroup "
        "topology for all 30 onboarded entities.",
    ),
    (
        "Source System isolation",
        "storage-hierarchy.svg",
        "Figure 3",
        "GCS Bronze bucket hierarchy, from the bucket root down to a single "
        "date-partitioned Parquet object.",
    ),
    (
        "Composer Airflow DAG",
        "cloudrun-lifecycle.svg",
        "Figure 4",
        "Cloud Run Job execution lifecycle, from Composer dispatch through the "
        "BigQuery Bronze load.",
    ),
    (
        "Cloud NAT Gateway",
        "network-topology.svg",
        "Figure 5",
        "Networking topology. Corporate sources are reached over a static NAT "
        "egress IP; Google APIs are reached with IAM credentials.",
    ),
]


def strip_inline_toc(md_text: str) -> str:
    """Drop the markdown Table of Contents; the sidebar supersedes it."""
    lines = md_text.splitlines()
    out, skipping = [], False
    for line in lines:
        if not skipping and re.match(r"^##\s+.*Table of Contents", line):
            skipping = True
            continue
        if skipping:
            # The TOC block ends at the horizontal rule that follows it.
            if line.strip() == "---":
                skipping = False
            continue
        out.append(line)
    return "\n".join(out)


def fix_broken_latex(md_text: str) -> str:
    """Replace the unrendered $$...$$ path formula with a readable code line.

    Drive/GitHub never rendered this LaTeX; it displayed as escaped literals.
    """
    formula = (
        "gs://{raw_landing_bucket}/{source_system}/raw/{target_table}"
        "/dt={YYYY-MM-DD}/data_{YYYYMMDD_HHMMSS}.parquet"
    )
    pattern = re.compile(r"^\$\$.*raw\\?_landing\\?_bucket.*\$\$$", re.MULTILINE)
    return pattern.sub("```text\n" + formula + "\n```", md_text)


def convert_github_alerts(md_text: str) -> str:
    """Turn GitHub '> [!NOTE]' blockquotes into styled callout divs."""
    lines = md_text.splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        match = re.match(r"^>\s*\[!(\w+)\]\s*$", lines[i])
        if not match or match.group(1).upper() not in ALERT_STYLES:
            out.append(lines[i])
            i += 1
            continue

        kind, label = ALERT_STYLES[match.group(1).upper()]
        i += 1
        body: list[str] = []
        while i < len(lines) and lines[i].startswith(">"):
            body.append(re.sub(r"^>\s?", "", lines[i]))
            i += 1

        inner = markdown.markdown("\n".join(body), extensions=["extra"])
        out.append(
            f'<div class="callout callout-{kind}" markdown="0">'
            f'<div class="callout-label">{label}</div>{inner}</div>'
        )
    return "\n".join(out)


DIAGRAM_TOKEN = "@@UDPDIAGRAM-{}@@"

# A fenced block's closing fence must carry the same indentation as its
# opening fence. Without the backreference an indented ```python block nested
# in a list item pairs with the next *top-level* fence instead of its own,
# which silently swallows whatever sits between them.
FENCE_RE = re.compile(
    r"^(?P<indent>[ \t]*)```[a-zA-Z]*\n.*?\n(?P=indent)```[ \t]*$",
    re.DOTALL | re.MULTILINE,
)


def swap_ascii_diagrams(md_text: str) -> str:
    """Replace ASCII-art fenced blocks with placeholder tokens.

    Done on the markdown rather than the HTML so the block never reaches the
    code highlighter in the first place.
    """

    def repl(match: re.Match) -> str:
        block = match.group(0)
        for index, (fingerprint, _, _, _) in enumerate(DIAGRAMS):
            if fingerprint in block:
                return "\n" + DIAGRAM_TOKEN.format(index) + "\n"
        return block

    return FENCE_RE.sub(repl, md_text)


def inline_diagrams(html_text: str) -> str:
    """Expand each placeholder into an inline, theme-aware <figure>.

    The SVGs are inlined rather than referenced with <img> so they inherit the
    page's CSS custom properties and therefore repaint correctly in dark mode.
    Their <style> blocks are stripped because styles inside inline SVG are
    document-scoped: five copies would otherwise fight over the same class
    names. The shared .dgm-* vocabulary lives in the template instead.
    """
    for index, (_, filename, number, caption) in enumerate(DIAGRAMS):
        token = DIAGRAM_TOKEN.format(index)
        svg = (DIAGRAM_DIR / filename).read_text(encoding="utf-8")
        svg = re.sub(r"<style>.*?</style>", "", svg, flags=re.DOTALL).strip()
        figure = (
            f'<figure class="diagram" id="figure-{index + 1}">{svg}'
            f'<figcaption><b>{number}</b> — {html.escape(caption)}</figcaption></figure>'
        )
        html_text = html_text.replace(f"<p>{token}</p>", figure).replace(token, figure)
    return html_text


def rewrite_links(html_text: str) -> str:
    """Point absolute file:// links at repo-relative paths and tag them."""
    html_text = html_text.replace('href="' + ABSOLUTE_PREFIX, 'href="' + REL_TO_ROOT)
    escaped = re.escape(REL_TO_ROOT)
    return re.sub(
        r'<a href="' + escaped, '<a class="repo-link" href="' + REL_TO_ROOT, html_text
    )


def wrap_tables(html_text: str) -> str:
    """Allow wide tables to scroll horizontally instead of breaking layout."""
    html_text = html_text.replace("<table>", '<div class="table-wrap"><table>')
    return html_text.replace("</table>", "</table></div>")


def decorate_headings(html_text: str) -> tuple[str, list[dict]]:
    """Add permalink anchors and collect the sidebar navigation model."""
    nav: list[dict] = []

    def repl(match: re.Match) -> str:
        level, anchor, inner = match.group(1), match.group(2), match.group(3)
        label = html.unescape(re.sub(r"<[^>]+>", "", inner)).strip()
        nav.append({"level": int(level), "id": anchor, "label": label})
        permalink = f'<a class="anchor" href="#{anchor}" aria-label="Link to section">#</a>'
        return f'<h{level} id="{anchor}">{inner}{permalink}</h{level}>'

    html_text = re.sub(r'<h([23]) id="([^"]+)">(.*?)</h\1>', repl, html_text, flags=re.DOTALL)
    return html_text, nav


def add_code_toolbar(html_text: str) -> str:
    """Attach a copy button to every highlighted code block."""
    return html_text.replace(
        '<div class="chl">',
        '<div class="chl"><button class="copy-btn" type="button">Copy</button>',
    )


def build_nav(nav: list[dict]) -> str:
    """Render the sidebar as nested, filterable, clickable links."""
    parts: list[str] = []
    open_sub = False
    for item in nav:
        label = html.escape(item["label"])
        if item["level"] == 2:
            if open_sub:
                parts.append("</div>")
                open_sub = False
            number = ""
            text = label
            num_match = re.match(r"^(\d+)\.\s+(.*)$", label)
            if num_match:
                number = f'<span class="nav-num">{num_match.group(1)}</span>'
                text = num_match.group(2)
            parts.append(
                f'<a class="nav-item nav-h2" href="#{item["id"]}" data-target="{item["id"]}">'
                f"{number}<span>{text}</span></a>"
            )
            parts.append('<div class="nav-sub">')
            open_sub = True
        else:
            parts.append(
                f'<a class="nav-item nav-h3" href="#{item["id"]}" data-target="{item["id"]}">'
                f"<span>{label}</span></a>"
            )
    if open_sub:
        parts.append("</div>")
    return "\n".join(parts)


# Short labels for the condensed topbar, keyed by README section number. Only
# the number is hardcoded; the anchor itself is read back from the generated
# heading so a retitled section can never leave a dead link behind.
TOPBAR_SECTIONS = {
    2: "Architecture",
    3: "Orchestration",
    5: "Ingestion",
    9: "Transformation",
    12: "Runbooks",
}


def build_topnav(nav: list[dict]) -> str:
    """Render the condensed topbar links from real heading anchors."""
    by_number: dict[int, str] = {}
    for item in nav:
        if item["level"] != 2:
            continue
        match = re.match(r"^(\d+)\.\s", item["label"])
        if match:
            by_number[int(match.group(1))] = item["id"]

    links = []
    for number, label in TOPBAR_SECTIONS.items():
        anchor = by_number.get(number)
        if anchor:
            links.append(f'<a href="#{anchor}">{html.escape(label)}</a>')
    return "\n    ".join(links)


def verify_anchors(page: str) -> None:
    """Fail the build on dangling in-page links rather than shipping them."""
    ids = set(re.findall(r'id="([^"]+)"', page))
    targets = {t for t in re.findall(r'href="#([^"]+)"', page) if t}
    missing = sorted(targets - ids - {"top"})
    if missing:
        raise SystemExit("Dangling in-page anchors: " + ", ".join(missing))


def render() -> str:
    md_text = README.read_text(encoding="utf-8")

    title_match = re.match(r"^#\s+(.*)$", md_text.splitlines()[0])
    raw_title = title_match.group(1) if title_match else "Technical Reference"
    if "—" in raw_title:
        product, subtitle = (part.strip() for part in raw_title.split("—", 1))
    else:
        product, subtitle = raw_title, ""

    body_md = "\n".join(md_text.splitlines()[1:])
    body_md = strip_inline_toc(body_md)
    body_md = fix_broken_latex(body_md)
    body_md = swap_ascii_diagrams(body_md)
    body_md = convert_github_alerts(body_md)

    md = markdown.Markdown(
        extensions=["extra", "toc", "sane_lists", "attr_list", "codehilite", "md_in_html"],
        extension_configs={
            "codehilite": {"css_class": "chl", "guess_lang": False},
            "toc": {"anchorlink": False},
        },
    )
    body_html = md.convert(body_md)
    body_html = inline_diagrams(body_html)
    body_html = rewrite_links(body_html)
    body_html = wrap_tables(body_html)
    body_html, nav = decorate_headings(body_html)
    body_html = add_code_toolbar(body_html)

    pygments_css = HtmlFormatter(style="monokai").get_style_defs(".chl")
    sections = sum(1 for item in nav if item["level"] == 2)
    built = datetime.datetime.now().strftime("%d %b %Y")

    template = TEMPLATE.read_text(encoding="utf-8")
    page = (
        template.replace("__PRODUCT__", html.escape(product))
        .replace("__SUBTITLE__", html.escape(subtitle))
        .replace("__TOPNAV__", build_topnav(nav))
        .replace("__NAV__", build_nav(nav))
        .replace("__BODY__", body_html)
        .replace("__PYGMENTS__", pygments_css)
        .replace("__SECTIONS__", str(sections))
        .replace("__BUILT__", built)
    )
    verify_anchors(page)
    return page


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the UDP HTML reference")
    parser.add_argument("--serve", action="store_true", help="serve docs/ after building")
    parser.add_argument("--port", type=int, default=8848)
    args = parser.parse_args()

    OUTPUT.write_text(render(), encoding="utf-8")
    size_kb = OUTPUT.stat().st_size / 1024
    print(f"Wrote {OUTPUT.relative_to(REPO_ROOT)} ({size_kb:.1f} KB)")

    if args.serve:
        import functools
        import http.server
        import socketserver

        handler = functools.partial(
            http.server.SimpleHTTPRequestHandler, directory=str(OUTPUT.parent)
        )
        # Without this a quick restart fails while the old socket lingers in
        # TIME_WAIT.
        socketserver.TCPServer.allow_reuse_address = True
        with socketserver.TCPServer(("", args.port), handler) as httpd:
            print(f"Serving http://localhost:{args.port}/{OUTPUT.name}")
            httpd.serve_forever()


if __name__ == "__main__":
    main()
