from django.http import HttpResponse
from django.utils.html import escape
from django.conf import settings
from files.models import Media

def video_sitemap(request):
    qs = (
        Media.objects
        .filter(media_type="video")
        .exclude(state="private")
        .order_by("-add_date")
    )

    items = []
    for m in qs.iterator():
        watch_url = f"{settings.FRONTEND_HOST}{m.get_absolute_url()}"

        thumb = m.poster_url or m.thumbnail_url
        content = m.trim_video_url

        if not (thumb and content):
            continue

        title = (m.title or "").strip()[:100]
        desc = ((m.description or "").strip())[:2048]
        duration = int(m.duration or 0)
        esc = lambda s: escape(s or "")

        items.append(f"""
  <url>
    <loc>{esc(watch_url)}</loc>
    <video:video>
      <video:thumbnail_loc>{esc(thumb)}</video:thumbnail_loc>
      <video:title>{esc(title)}</video:title>
      <video:description>{esc(desc)}</video:description>
      <video:content_loc>{esc(content)}</video:content_loc>
      {"<video:duration>%d</video:duration>" % duration if duration else ""}
    </video:video>
  </url>""")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:video="http://www.google.com/schemas/sitemap-video/1.1">
{''.join(items)}
</urlset>"""

    return HttpResponse(xml, content_type="application/xml; charset=utf-8")
