#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["playwright>=1.42"]
# ///
"""Build docs/ARCHITECTURE.pdf from docs/ARCHITECTURE.md.

The PDF is a build artifact, not a hand-made document. Regenerate it whenever
the markdown changes, or the two silently drift apart.

    uv run scripts/build_architecture_pdf.py

Why it is not just "print the markdown": the five mermaid diagrams disagree
about shape. Three are tall top-to-bottom flowcharts, the sequence diagram is
wide, and the SCD2 lifecycle is a narrow column. One page size for the whole
document shrinks something to illegibility, so each figure gets a CSS named
page cut to its own aspect ratio and sits at full size. Prose stays on Letter
in between, so it still reads as a document. Everything is vector -- the
mermaid SVGs are inlined rather than rasterized, so zoom stays sharp.

Requirements beyond the declared Python dependency:
  * node / npx -- fetches `marked` (markdown) and `mermaid-cli` (diagrams)
  * Google Chrome installed -- Playwright drives it via channel="chrome"
    rather than downloading a browser. The `chrome --print-to-pdf` CLI cannot
    be used instead: it has no way to set `preferCSSPageSize`, which is what
    makes the per-figure named pages take effect.

Expected output: 12 pages, 21 embedded font subsets, tagged for accessibility.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SOURCE = REPO / "docs" / "ARCHITECTURE.md"
OUTPUT = REPO / "docs" / "ARCHITECTURE.pdf"

MARKED = "marked@15"
MERMAID = "@mermaid-js/mermaid-cli@11"
MERMAID_THEME = "neutral"  # the default purple-and-yellow looks garish on paper

# Figure sizing, in mm. A figure is scaled up until it hits one of these caps,
# then the page is cut to fit it exactly.
MAX_FIG_W = 400.0
MAX_FIG_H = 700.0
PAGE_MARGIN = 15.0  # per side
LABEL_BLOCK = 18.0  # .figlabel height (12) + its margin-bottom (6)
PAGE_PAD_W = 2 * PAGE_MARGIN
PAGE_PAD_H = 2 * PAGE_MARGIN + LABEL_BLOCK

MERMAID_BLOCK = re.compile(r"^```mermaid\n(.*?)^```\n", re.DOTALL | re.MULTILINE)
HEADING = re.compile(r"^#{2,4}\s+(.*)$", re.MULTILINE)
SVG_ROOT_NOISE = re.compile(r'\s(?:width|height)="[^"]*"|\sstyle="[^"]*"')

BASE_CSS = """* { box-sizing: border-box; }
html { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
body { margin:0; color:#14181c; background:#fff;
       font: 10.5pt/1.55 "Source Serif Pro", Palatino, Georgia, serif; }
h1,h2,h3,h4 { font-family:"Helvetica Neue",Helvetica,Arial,sans-serif; font-weight:600;
              line-height:1.2; color:#0d1114; break-after:avoid; margin:1.6em 0 .55em; }
h1 { font-size:24pt; letter-spacing:-.02em; margin-top:0; }
h2 { font-size:15pt; letter-spacing:-.01em; padding-bottom:.3em;
     border-bottom:1px solid #d8dfe2; margin-top:2em; }
h3 { font-size:11.5pt; }
p,li { orphans:3; widows:3; }
p { margin:0 0 .8em; }
ul,ol { margin:0 0 .9em; padding-left:1.3em; }
li { margin-bottom:.35em; }
code { font:9pt/1.5 "SF Mono",Menlo,Consolas,monospace; background:#f0f3f4;
       padding:.1em .32em; border-radius:2px; }
pre { background:#f5f7f8; border:1px solid #dfe6e9; border-left:3px solid #12626e;
      padding:.85em 1em; break-inside:avoid; margin:0 0 1em; }
pre code { background:none; padding:0; font-size:8.6pt; white-space:pre-wrap; }
blockquote { margin:0 0 1.1em; padding:.7em 1.1em; background:#f4f7f8;
             border-left:3px solid #9fb2b8; color:#37474e; font-size:9.8pt; }
blockquote p:last-child { margin-bottom:0; }
table { border-collapse:collapse; width:100%; margin:.4em 0 1.3em;
        font-size:9pt; break-inside:avoid; }
th,td { border:1px solid #d5dee1; padding:.5em .65em; text-align:left; vertical-align:top; }
th { background:#eef2f4; font-family:"Helvetica Neue",Helvetica,Arial,sans-serif;
     font-weight:600; font-size:8.4pt; letter-spacing:.02em; }
hr { border:0; border-top:1px solid #dde4e7; margin:2em 0; }
a { color:#0f5560; text-decoration:none; }
figure.fig { margin:0; break-before:page; break-after:page; break-inside:avoid; }
figure.fig svg { display:block; }
"""

LABEL_CSS = """.figlabel { font:600 8.5pt/1.3 "Helvetica Neue",Helvetica,Arial,sans-serif;
  letter-spacing:.09em; text-transform:uppercase; color:#5d7078;
  margin:0 0 6mm; padding-bottom:2mm; height:12mm; border-bottom:1px solid #d5dee1; }
"""


def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if proc.returncode != 0:
        sys.exit(f"command failed: {' '.join(cmd)}\n{proc.stderr.strip()}")
    return proc


def preceding_heading(markdown: str, pos: int) -> str:
    """The nearest heading above `pos`, with any leading section number dropped."""
    titles = [m.group(1) for m in HEADING.finditer(markdown, 0, pos)]
    return re.sub(r"^\d+\.\s+", "", titles[-1]) if titles else ""


def extract_diagrams(markdown: str) -> tuple[str, list[dict]]:
    """Swap each mermaid block for a placeholder comment marked's HTML passes through."""
    diagrams: list[dict] = []

    def swap(match: re.Match) -> str:
        n = len(diagrams) + 1
        diagrams.append({
            "n": n,
            "source": match.group(1),
            "label": preceding_heading(markdown, match.start()),
        })
        return f"<!--DIAGRAM{n}-->\n\n"

    return MERMAID_BLOCK.sub(swap, markdown), diagrams


def render_diagram(diagram: dict, work: Path) -> dict:
    """Render one mermaid block to an inlineable SVG and size its page."""
    n = diagram["n"]
    mmd = work / f"d{n}.mmd"
    svg_path = work / f"d{n}.svg"
    mmd.write_text(diagram["source"], encoding="utf-8")
    run(["npx", "-y", MERMAID, "-t", MERMAID_THEME, "-i", str(mmd), "-o", str(svg_path)])

    svg = svg_path.read_text(encoding="utf-8")
    # mermaid-cli scopes the SVG's embedded <style> on the root id, so the id has
    # to be rewritten everywhere it appears, not only on the root element.
    svg = svg.replace("my-svg", f"figsvg{n}")
    head, rest = svg.split(">", 1)
    svg = SVG_ROOT_NOISE.sub("", head) + ">" + rest  # CSS owns the size now

    # Sequence diagrams carry a non-zero viewBox origin, so take the last two
    # numbers rather than assuming "0 0 w h".
    viewbox = re.search(r'viewBox="(-?[\d.]+) (-?[\d.]+) ([\d.]+) ([\d.]+)"', svg)
    if not viewbox:
        sys.exit(f"diagram {n}: mermaid produced no viewBox to size the page from")
    aspect = float(viewbox.group(3)) / float(viewbox.group(4))

    width = min(MAX_FIG_W, MAX_FIG_H * aspect)
    height = width / aspect
    return {
        **diagram,
        "svg": svg,
        "w": width,
        "h": height,
        "page_w": width + PAGE_PAD_W,
        "page_h": height + PAGE_PAD_H,
    }


def build_html(body: str, figures: list[dict]) -> str:
    page_rules = "".join(
        f"@page fig{f['n']} {{ size: {f['page_w']:.1f}mm {f['page_h']:.1f}mm;"
        f" margin: {PAGE_MARGIN:.0f}mm; }}\n"
        for f in figures
    )
    size_rules = "".join(
        f".fig{f['n']} svg {{ width: {f['w']:.1f}mm; height: {f['h']:.1f}mm; }}\n"
        for f in figures
    )
    assign_rules = "".join(f".fig{f['n']} {{ page: fig{f['n']}; }}\n" for f in figures)

    for f in figures:
        figure = (
            f'<figure class="fig fig{f["n"]}">'
            f'<p class="figlabel">Figure {f["n"]} — {f["label"]}</p>'
            f'{f["svg"]}</figure>'
        )
        body = body.replace(f"<!--DIAGRAM{f['n']}-->", figure)

    css = (
        "@page { size: Letter; margin: 20mm 22mm 18mm; }\n"
        + page_rules
        + BASE_CSS
        + size_rules
        + LABEL_CSS
        + assign_rules
    )
    return (
        '<!doctype html><html><head><meta charset="utf-8">'
        f"<title>KnackELT Architecture</title><style>{css}</style></head>"
        f"<body>{body}</body></html>"
    )


def print_pdf(html: Path, out: Path) -> None:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(channel="chrome")
        page = browser.new_page()
        page.goto(html.resolve().as_uri(), wait_until="load")
        page.emulate_media(media="print")
        # prefer_css_page_size is what makes the named @page rules apply at all;
        # print_background keeps the blockquote, table and code fills.
        page.pdf(
            path=str(out),
            prefer_css_page_size=True,
            print_background=True,
            tagged=True,
            outline=False,
        )
        browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-o", "--output", type=Path, default=OUTPUT)
    parser.add_argument("-s", "--source", type=Path, default=SOURCE)
    parser.add_argument("--keep", action="store_true", help="keep the build directory")
    args = parser.parse_args()

    if not shutil.which("npx"):
        sys.exit("npx not found -- node is needed for marked and mermaid-cli")

    markdown = args.source.read_text(encoding="utf-8")
    prose, diagrams = extract_diagrams(markdown)
    if not diagrams:
        sys.exit(f"no mermaid blocks found in {args.source}")

    work = Path(tempfile.mkdtemp(prefix="architecture-pdf-"))
    try:
        figures = [render_diagram(d, work) for d in diagrams]
        for f in figures:
            print(f"figure {f['n']}: {f['w']:.1f} x {f['h']:.1f}mm "
                  f"-> page {f['page_w']:.1f} x {f['page_h']:.1f}mm  «{f['label']}»")

        (work / "body.md").write_text(prose, encoding="utf-8")
        body = run(["npx", "-y", MARKED, "--gfm"], input=prose).stdout

        html = work / "print.html"
        html.write_text(build_html(body, figures), encoding="utf-8")
        print_pdf(html, args.output)
        print(f"wrote {args.output} ({args.output.stat().st_size:,} bytes)")
    finally:
        if args.keep:
            print(f"build directory kept at {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
