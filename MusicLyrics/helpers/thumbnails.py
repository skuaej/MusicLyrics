"""4K thumbnail generation for music playback cards.

Renders a 3840×2160 (UHD-4K) card with:
  * Full-frame blurred album art background
  * Soft vignette + colour gradient overlay
  * Circular, glowing album art on the left
  * Vinyl-style decoration ring around the cover
  * Large title (auto-shrink + wrap), artist, duration progress bar,
    requester chip, and bold bottom-right brand mark.

Output is saved as a high-quality JPEG to stay well under Telegram's
10 MB photo limit while still preserving 4K detail.
"""

from __future__ import annotations

import os
import textwrap
from io import BytesIO

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps
import aiohttp

from config import Config

_DOWNLOADS = Config.DOWNLOADS_DIR
_THUMB_DIR = os.path.join(_DOWNLOADS, "thumbnails")
os.makedirs(_THUMB_DIR, exist_ok=True)

# ── Canvas: true 4K UHD ──────────────────────────────────────────────────────
WIDTH, HEIGHT = 3840, 2160

# ── Palette ──────────────────────────────────────────────────────────────────
BG_COLOR = (12, 12, 18)
TEXT_COLOR = (255, 255, 255)
SUB_TEXT_COLOR = (210, 210, 220)
MUTED_COLOR = (170, 170, 185)
ACCENT_COLOR = (29, 215, 96)        # Spotify-ish green
ACCENT_GLOW = (29, 215, 96, 110)
BRAND_COLOR = (255, 215, 64)        # Premium gold
RING_COLOR = (255, 255, 255, 60)
CHIP_BG = (255, 255, 255, 28)
CHIP_BORDER = (255, 255, 255, 60)


def _get_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Return a TrueType font, falling back to the default bitmap font."""
    font_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
        if bold
        else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf"
        if bold
        else "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
    for path in font_paths:
        if os.path.isfile(path):
            return ImageFont.truetype(path, size)
    try:
        return ImageFont.truetype("arial.ttf", size)
    except OSError:
        return ImageFont.load_default()


def _format_dur(seconds: int) -> str:
    mins, secs = divmod(int(seconds or 0), 60)
    hours, mins = divmod(mins, 60)
    if hours:
        return f"{hours}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"


def _fit_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font_path_size: tuple[bool, int],
    max_width: int,
    min_size: int = 72,
) -> ImageFont.FreeTypeFont:
    """Shrink the font until the longest word in *text* fits *max_width*."""
    bold, size = font_path_size
    font = _get_font(size, bold=bold)
    while size > min_size:
        bbox = draw.textbbox((0, 0), text, font=font)
        if bbox[2] - bbox[0] <= max_width:
            return font
        size -= 6
        font = _get_font(size, bold=bold)
    return font


def _circle_crop(img: Image.Image, size: int) -> Image.Image:
    """Return a square *img* cropped into a transparent circle of *size* px."""
    img = ImageOps.fit(img, (size, size), Image.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(img.convert("RGB"), (0, 0), mask)
    return out


def _vertical_gradient(
    size: tuple[int, int],
    top: tuple[int, int, int, int],
    bottom: tuple[int, int, int, int],
) -> Image.Image:
    """Build a vertical RGBA gradient."""
    w, h = size
    grad = Image.new("RGBA", (1, h))
    for y in range(h):
        t = y / max(1, h - 1)
        r = int(top[0] * (1 - t) + bottom[0] * t)
        g = int(top[1] * (1 - t) + bottom[1] * t)
        b = int(top[2] * (1 - t) + bottom[2] * t)
        a = int(top[3] * (1 - t) + bottom[3] * t)
        grad.putpixel((0, y), (r, g, b, a))
    return grad.resize((w, h))


def _radial_glow(
    size: int,
    color: tuple[int, int, int, int],
) -> Image.Image:
    """Build a soft circular glow of *size* px."""
    glow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(glow)
    steps = 18
    for i in range(steps, 0, -1):
        alpha = int(color[3] * (i / steps) * 0.35)
        r = int(size / 2 * (i / steps))
        cx = cy = size // 2
        d.ellipse(
            (cx - r, cy - r, cx + r, cy + r),
            fill=(color[0], color[1], color[2], alpha),
        )
    return glow.filter(ImageFilter.GaussianBlur(radius=size // 18))


async def gen_thumbnail(
    title: str,
    artist: str,
    duration: int,
    thumbnail_url: str,
    requester: str,
) -> str:
    """Render a 4K playback card and return its absolute file path."""
    # -- 1. Download cover art ------------------------------------------------
    cover: Image.Image | None = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                thumbnail_url,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    raw = await resp.read()
                    cover = Image.open(BytesIO(raw)).convert("RGB")
    except Exception:
        cover = None

    # -- 2. Background: blurred cover + dark vignette + gradient --------------
    if cover is not None:
        bg = ImageOps.fit(cover, (WIDTH, HEIGHT), Image.LANCZOS)
        bg = bg.filter(ImageFilter.GaussianBlur(radius=60))
        # Darken the blur so foreground text reads well
        dark = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 165))
        bg = Image.alpha_composite(bg.convert("RGBA"), dark)
    else:
        bg = Image.new("RGBA", (WIDTH, HEIGHT), BG_COLOR + (255,))

    # Diagonal-ish gradient: deeper at the bottom-right for branding contrast
    gradient = _vertical_gradient(
        (WIDTH, HEIGHT),
        top=(0, 0, 0, 0),
        bottom=(0, 0, 0, 220),
    )
    bg = Image.alpha_composite(bg, gradient)

    canvas = bg

    # -- 3. Glow + vinyl ring + circular cover (left half) --------------------
    COVER_SIZE = 1500           # diameter in px
    cx, cy = 1080, HEIGHT // 2  # cover centre

    # Soft accent glow behind the cover
    glow = _radial_glow(COVER_SIZE + 600, ACCENT_GLOW)
    canvas.alpha_composite(
        glow,
        dest=(cx - (COVER_SIZE + 600) // 2, cy - (COVER_SIZE + 600) // 2),
    )

    # White soft ring (vinyl record vibe)
    ring_layer = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
    rd = ImageDraw.Draw(ring_layer)
    outer = COVER_SIZE + 110
    rd.ellipse(
        (cx - outer // 2, cy - outer // 2, cx + outer // 2, cy + outer // 2),
        outline=RING_COLOR,
        width=10,
    )
    inner_ring = COVER_SIZE + 40
    rd.ellipse(
        (
            cx - inner_ring // 2,
            cy - inner_ring // 2,
            cx + inner_ring // 2,
            cy + inner_ring // 2,
        ),
        outline=(255, 255, 255, 130),
        width=6,
    )
    canvas = Image.alpha_composite(canvas, ring_layer)

    # Circular cover (fall back to a flat accent disc when art missing)
    if cover is not None:
        disc = _circle_crop(cover, COVER_SIZE)
    else:
        disc = Image.new("RGBA", (COVER_SIZE, COVER_SIZE), (0, 0, 0, 0))
        ImageDraw.Draw(disc).ellipse(
            (0, 0, COVER_SIZE, COVER_SIZE),
            fill=ACCENT_COLOR + (255,),
        )
    canvas.alpha_composite(disc, dest=(cx - COVER_SIZE // 2, cy - COVER_SIZE // 2))

    # Centre vinyl hole
    hole = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
    hd = ImageDraw.Draw(hole)
    hole_r = 60
    hd.ellipse(
        (cx - hole_r, cy - hole_r, cx + hole_r, cy + hole_r),
        fill=(15, 15, 20, 230),
        outline=(255, 255, 255, 80),
        width=6,
    )
    canvas = Image.alpha_composite(canvas, hole)

    draw = ImageDraw.Draw(canvas)

    # -- 4. Right-side text column --------------------------------------------
    TEXT_X = 1980
    TEXT_W = WIDTH - TEXT_X - 180  # right margin

    # "NOW PLAYING" eyebrow chip
    eyebrow = "♪  NOW PLAYING"
    f_eyebrow = _get_font(58, bold=True)
    bbox = draw.textbbox((0, 0), eyebrow, font=f_eyebrow)
    ew, eh = bbox[2] - bbox[0], bbox[3] - bbox[1]
    chip_pad_x, chip_pad_y = 40, 24
    chip_w, chip_h = ew + chip_pad_x * 2, eh + chip_pad_y * 2
    chip_y = 380
    chip_layer = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
    cd = ImageDraw.Draw(chip_layer)
    cd.rounded_rectangle(
        (TEXT_X, chip_y, TEXT_X + chip_w, chip_y + chip_h),
        radius=chip_h // 2,
        fill=CHIP_BG,
        outline=CHIP_BORDER,
        width=3,
    )
    canvas = Image.alpha_composite(canvas, chip_layer)
    draw = ImageDraw.Draw(canvas)
    draw.text(
        (TEXT_X + chip_pad_x, chip_y + chip_pad_y - 8),
        eyebrow,
        fill=ACCENT_COLOR,
        font=f_eyebrow,
    )

    # Title — auto-shrink to fit, wrap to max 2 lines
    title_y = chip_y + chip_h + 60
    f_title = _fit_text(draw, title, (True, 150), TEXT_W, min_size=92)
    wrapped = textwrap.wrap(title, width=22) or [title]
    if len(wrapped) > 2:
        wrapped = wrapped[:2]
        wrapped[-1] = wrapped[-1].rstrip() + "…"
    line_h = f_title.size + 16
    for i, ln in enumerate(wrapped):
        draw.text(
            (TEXT_X, title_y + i * line_h),
            ln,
            fill=TEXT_COLOR,
            font=f_title,
        )
    cursor_y = title_y + len(wrapped) * line_h + 30

    # Artist
    if artist:
        f_artist = _fit_text(draw, artist, (False, 86), TEXT_W, min_size=58)
        draw.text(
            (TEXT_X, cursor_y),
            artist,
            fill=SUB_TEXT_COLOR,
            font=f_artist,
        )
        cursor_y += f_artist.size + 60

    # Duration progress bar (decorative — visualises 40% played)
    bar_x0, bar_x1 = TEXT_X, TEXT_X + TEXT_W
    bar_y = cursor_y + 20
    bar_h = 22
    bar_layer = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
    bd = ImageDraw.Draw(bar_layer)
    bd.rounded_rectangle(
        (bar_x0, bar_y, bar_x1, bar_y + bar_h),
        radius=bar_h // 2,
        fill=(255, 255, 255, 60),
    )
    fill_w = int((bar_x1 - bar_x0) * 0.4)
    bd.rounded_rectangle(
        (bar_x0, bar_y, bar_x0 + fill_w, bar_y + bar_h),
        radius=bar_h // 2,
        fill=ACCENT_COLOR + (255,),
    )
    # Knob
    knob_r = 30
    knob_cx = bar_x0 + fill_w
    knob_cy = bar_y + bar_h // 2
    bd.ellipse(
        (knob_cx - knob_r, knob_cy - knob_r, knob_cx + knob_r, knob_cy + knob_r),
        fill=(255, 255, 255, 255),
    )
    canvas = Image.alpha_composite(canvas, bar_layer)
    draw = ImageDraw.Draw(canvas)

    # Time labels under the bar
    f_time = _get_font(60, bold=True)
    draw.text(
        (bar_x0, bar_y + bar_h + 24),
        "00:00",
        fill=MUTED_COLOR,
        font=f_time,
    )
    dur_text = _format_dur(duration) if duration else "LIVE"
    dbbox = draw.textbbox((0, 0), dur_text, font=f_time)
    draw.text(
        (bar_x1 - (dbbox[2] - dbbox[0]), bar_y + bar_h + 24),
        dur_text,
        fill=MUTED_COLOR,
        font=f_time,
    )
    cursor_y = bar_y + bar_h + 24 + f_time.size + 60

    # Requester chip
    if requester:
        req_text = f"👤  {requester}"
        f_req = _get_font(60, bold=True)
        rbbox = draw.textbbox((0, 0), req_text, font=f_req)
        rw, rh = rbbox[2] - rbbox[0], rbbox[3] - rbbox[1]
        pad_x, pad_y = 40, 22
        req_layer = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
        rqd = ImageDraw.Draw(req_layer)
        rqd.rounded_rectangle(
            (TEXT_X, cursor_y, TEXT_X + rw + pad_x * 2, cursor_y + rh + pad_y * 2),
            radius=(rh + pad_y * 2) // 2,
            fill=CHIP_BG,
            outline=CHIP_BORDER,
            width=3,
        )
        canvas = Image.alpha_composite(canvas, req_layer)
        draw = ImageDraw.Draw(canvas)
        draw.text(
            (TEXT_X + pad_x, cursor_y + pad_y - 6),
            req_text,
            fill=TEXT_COLOR,
            font=f_req,
        )

    # -- 5. Brand mark (bottom-right) -----------------------------------------
    brand_text = Config.BOT_NAME
    f_brand = _get_font(96, bold=True)
    bbox = draw.textbbox((0, 0), brand_text, font=f_brand)
    bw, bh = bbox[2] - bbox[0], bbox[3] - bbox[1]
    bx = WIDTH - bw - 160
    by = HEIGHT - bh - 140
    # Soft glow behind brand
    bglow = _radial_glow(max(bw, bh) + 260, (255, 215, 64, 90))
    canvas.alpha_composite(
        bglow,
        dest=(
            bx + bw // 2 - (max(bw, bh) + 260) // 2,
            by + bh // 2 - (max(bw, bh) + 260) // 2,
        ),
    )
    draw = ImageDraw.Draw(canvas)
    draw.text((bx, by), brand_text, fill=BRAND_COLOR, font=f_brand)

    # Thin top + bottom highlight stripes for a premium frame
    line_layer = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
    ld = ImageDraw.Draw(line_layer)
    ld.rectangle((0, 0, WIDTH, 8), fill=ACCENT_COLOR + (220,))
    ld.rectangle((0, HEIGHT - 8, WIDTH, HEIGHT), fill=BRAND_COLOR + (220,))
    canvas = Image.alpha_composite(canvas, line_layer)

    # -- 6. Save as high-quality JPEG (stays under Telegram's 10 MB cap) ------
    out_path = os.path.join(_THUMB_DIR, f"thumb_{hash(title) & 0xFFFFFFFF}.jpg")
    canvas.convert("RGB").save(
        out_path,
        "JPEG",
        quality=92,
        optimize=True,
        progressive=True,
        subsampling=1,
    )
    return out_path
