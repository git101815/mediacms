export function getVideoDownloadPageUrl(mediaId) {
  let url = '/download/' + encodeURIComponent(mediaId) + '/';

  try {
    const pageUrl = new URL(window.location.href);

    if (pageUrl.searchParams.get('playback') === 'premium') {
      url += '?playback=premium';
    }
  } catch {}

  return url;
}
