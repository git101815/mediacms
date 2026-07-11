import React from 'react';
import { MediaPageStore } from '../../utils/stores/';
import { getVideoDownloadPageUrl } from '../../utils/mediaDownload';
import { CircleIconButton, MaterialIcon } from '../_shared/';
import { translateString } from '../../utils/helpers/';

export function VideoMediaDownloadLink() {
  const mediaId = MediaPageStore.get('media-id');

  if (!mediaId) {
    return null;
  }

  return (
    <div className="video-downloads hidden-only-in-small">
      <button
        type="button"
        onClick={() => {
          window.location.href = getVideoDownloadPageUrl(mediaId);
        }}
      >
        <CircleIconButton type="span">
          <MaterialIcon type="arrow_downward" />
        </CircleIconButton>
        <span>{translateString("DOWNLOAD")}</span>
      </button>
    </div>
  );
}