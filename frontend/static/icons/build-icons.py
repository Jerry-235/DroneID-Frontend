#!/usr/bin/env python3
"""Generates the DroneID favicon / bookmark icon set.

Three themed marks are defined below, all built from the same delta the live
map draws for a drone, in the app's own palette:

  a  marker   the bare dart. Most legible at 16px; the shipped default.
  b  tracked  the dart inside a ring.
  c  lock     the dart inside reticle corners.

    python3 build-icons.py [a|b|c]

Rewrites favicon.svg and every PNG/ICO from the chosen mark, so switching
themes is one command and index.html never has to change. PNG rasterizing
goes through the Chromium that Playwright ships, since librsvg/ImageMagick's
SVG support isn't dependable; if Playwright isn't installed, the SVGs are
still written and only the PNG/ICO step is skipped.
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# straight from the app's :root palette in static/index.html
BG      = "#16212C"   # --panel
EDGE    = "#263544"   # --hairline
AMBER   = "#E8A33D"   # --amber, primary accent
DIM     = "#7A5A28"   # --amber-dim
ORANGE  = "#F97316"   # drone identity orange used on the map

# The map marker's polygon, rescaled from its 26-unit viewBox onto 64.
DART = "32,7 50.5,56 32,43.5 13.5,56"


def dart(scale=1.0):
    return ('<g transform="translate(32,32) scale(%s) translate(-32,-32)">'
            '<polygon points="%s" fill="%s"/></g>') % (scale, DART, AMBER)


def tick(d):
    return ('<path d="%s" fill="none" stroke="%s" stroke-width="5" '
            'stroke-linecap="round"/>') % (d, ORANGE)


def mark(variant):
    """Just the artwork at full size, no background plate. Internal
    proportions are fixed here; scaling for the masked platforms happens in
    svg() and scales the mark as a whole, so an outer element (mark c's
    reticle corners, say) can't be left behind at the edges where a circular
    mask would crop it."""
    if variant == "b":
        return ('<circle cx="32" cy="32" r="25" fill="none" stroke="%s" stroke-width="4"/>'
                % DIM) + dart(0.62)
    if variant == "c":
        return (tick("M9 20 V11 H18") + tick("M46 11 H55 V20")
                + tick("M55 44 V53 H46") + tick("M18 53 H9 V44")
                + dart(0.62))
    return dart(1.0)


def svg(variant, rounded=True, edge=True, scale=1.0):
    """rounded/edge off + a smaller mark gives the full-bleed, padded artwork
    Apple and Android want, since both apply their own mask and would clip a
    rounded plate's corners or crop artwork that runs to the edge. At
    scale=0.68 the artwork sits inside the central 68% of the canvas, well
    within Android's 80% maskable safe circle."""
    stroke = ' stroke="%s" stroke-width="2"' % EDGE if edge else ''
    inset = '1' if edge else '0'
    size = '62' if edge else '64'
    plate = '<rect x="%s" y="%s" width="%s" height="%s" rx="%d" fill="%s"%s/>' % (
        inset, inset, size, size, 12 if rounded else 0, BG, stroke)
    art = mark(variant)
    if scale != 1.0:
        art = ('<g transform="translate(32,32) scale(%s) translate(-32,-32)">%s</g>'
               % (scale, art))
    return ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" '
            'width="64" height="64">\n%s\n%s\n</svg>\n' % (plate, art))


# (filename, svg source, pixel size)
def targets(variant):
    tab = svg(variant)                                   # rounded plate, for the tab
    full = svg(variant, rounded=False, edge=False, scale=0.68)  # masked platforms
    return [
        ("favicon-16.png", tab, 16),
        ("favicon-32.png", tab, 32),
        ("favicon-48.png", tab, 48),
        ("apple-touch-icon.png", full, 180),
        ("icon-192.png", full, 192),
        ("icon-512.png", full, 512),
    ]


def rasterize(jobs):
    """jobs: list of (outfile, svg_source, size). One Chromium launch for all."""
    script = os.path.join(HERE, "_rasterize.js")
    payload = [{"out": os.path.join(HERE, o), "svg": s, "size": n} for o, s, n in jobs]
    import json
    with open(script, "w") as f:
        f.write("""
const { chromium } = require('playwright');
const jobs = %s;
(async () => {
  const b = await chromium.launch({ executablePath: process.env.PW_CHROME });
  for (const j of jobs) {
    const p = await b.newPage({ viewport: { width: j.size, height: j.size } });
    await p.setContent(`<html><body style="margin:0">${j.svg.replace('width="64" height="64"',
      `width="${j.size}" height="${j.size}"`)}</body></html>`);
    await p.screenshot({ path: j.out, omitBackground: true });
    await p.close();
  }
  await b.close();
})();
""" % json.dumps(payload))
    chrome = os.environ.get("PW_CHROME") or "/opt/pw-browsers/chromium-1194/chrome-linux/chrome"
    subprocess.run(["node", script], check=True, cwd=HERE,
                   env={**os.environ, "PW_CHROME": chrome})
    os.remove(script)


def build_ico():
    """One .ico holding 16/32/48, for older browsers and Windows bookmarks.

    Built from the three separately-rendered PNGs rather than by downscaling
    one of them: each was rasterized at its final size, so the small ones are
    sharper than any resample would be. ImageMagick packs an ICO from several
    input files directly; PIL's ICO writer can only resize a single image
    (append_images is a no-op for this format), so that's the fallback."""
    srcs = [os.path.join(HERE, "favicon-%d.png" % n) for n in (16, 32, 48)]
    out = os.path.join(HERE, "favicon.ico")
    try:
        subprocess.run(["convert"] + srcs + [out], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    except (OSError, subprocess.CalledProcessError):
        pass
    from PIL import Image
    Image.open(srcs[-1]).convert("RGBA").save(
        out, sizes=[(16, 16), (32, 32), (48, 48)])


MANIFEST = """{
  "name": "DroneID Live Map",
  "short_name": "DroneID",
  "icons": [
    { "src": "/static/icons/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable" },
    { "src": "/static/icons/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable" }
  ],
  "theme_color": "#0F1720",
  "background_color": "#0F1720",
  "display": "standalone",
  "start_url": "/"
}
"""


def main():
    variant = (sys.argv[1] if len(sys.argv) > 1 else "a").lower()
    if variant not in ("a", "b", "c"):
        sys.exit("variant must be a, b or c")

    # Keep all three sources around so the alternatives stay easy to preview.
    for v in ("a", "b", "c"):
        open(os.path.join(HERE, "mark-%s.svg" % v), "w").write(svg(v))
    open(os.path.join(HERE, "favicon.svg"), "w").write(svg(variant))
    open(os.path.join(HERE, "site.webmanifest"), "w").write(MANIFEST)

    try:
        rasterize(targets(variant))
        build_ico()
    except Exception as e:
        print("SVG + manifest written; raster step skipped (%s)" % e)
        return
    print("Built icon set from mark '%s'." % variant)


if __name__ == "__main__":
    main()
