import React, { useRef, useEffect } from 'react';
import PropTypes from 'prop-types';
import urlParse from 'url-parse';

import MediaPlayer from 'mediacms-player/dist/mediacms-player.js';
import 'mediacms-player/dist/mediacms-player.css';

import './VideoPlayer.scss';
import 'videojs-contrib-ads/dist/videojs-contrib-ads.css';
import 'videojs-ima/dist/videojs.ima.css';

export function formatInnerLink(url, baseUrl) {
  let link = urlParse(url, {});

  if ('' === link.origin || 'null' === link.origin || !link.origin) {
    link = urlParse(baseUrl + '/' + url.replace(/^\//g, ''), {});
  }

  return link.toString();
}

function hlsSourceForQuality(info, quality) {
  const item = info && info[quality];

  if (!item || !Array.isArray(item.format) || !Array.isArray(item.url)) {
    return null;
  }

  const hlsIndex = item.format.indexOf('hls');

  return -1 === hlsIndex || !item.url[hlsIndex] ? null : item.url[hlsIndex];
}

function qualityFromSources(sources, info) {
  if (!Array.isArray(sources) || !info || 'object' !== typeof info) {
    return null;
  }

  const sourceUrls = sources
    .map((source) => (source && source.src ? source.src : null))
    .filter((source) => null !== source);

  const qualities = Object.keys(info);

  for (let qualityIndex = 0; qualityIndex < qualities.length; qualityIndex += 1) {
    const quality = qualities[qualityIndex];
    const item = info[quality];

    if (!item || !Array.isArray(item.url)) {
      continue;
    }

    for (let urlIndex = 0; urlIndex < item.url.length; urlIndex += 1) {
      if (-1 !== sourceUrls.indexOf(item.url[urlIndex])) {
        return quality;
      }
    }
  }

  return null;
}

function initialVideoQuality(sources, info, requestedQuality) {
  const sourceQuality = qualityFromSources(sources, info);

  if (null !== sourceQuality) {
    return sourceQuality;
  }

  if (
    null !== requestedQuality &&
    void 0 !== requestedQuality &&
    info &&
    void 0 !== info[requestedQuality]
  ) {
    return requestedQuality;
  }

  if (info && void 0 !== info.Auto) {
    return 'Auto';
  }

  const qualities = info && 'object' === typeof info ? Object.keys(info) : [];

  return qualities.length ? qualities[0] : 'Auto';
}

function normalizedHlsVideoInfo(info) {
  const normalized = {};
  const qualities = info && 'object' === typeof info ? Object.keys(info) : [];

  for (let qualityIndex = 0; qualityIndex < qualities.length; qualityIndex += 1) {
    const quality = qualities[qualityIndex];
    const item = info[quality] || {};

    normalized[quality] = Object.assign({}, item, {
      format: Array.isArray(item.format) ? item.format.slice() : [],
      url: Array.isArray(item.url) ? item.url.slice() : [],
    });
  }

  const fallbackMasterUrl = hlsSourceForQuality(normalized, 'Auto');
  const hasExplicitMasters = qualities.some(
    (quality) => !!normalized[quality].hlsMaster
  );

  for (let qualityIndex = 0; qualityIndex < qualities.length; qualityIndex += 1) {
    const item = normalized[qualities[qualityIndex]];
    const masterUrl =
      item.hlsMaster || (!hasExplicitMasters ? fallbackMasterUrl : null);

    if (!masterUrl) {
      continue;
    }

    for (let formatIndex = 0; formatIndex < item.format.length; formatIndex += 1) {
      if ('hls' === item.format[formatIndex]) {
        item.url[formatIndex] = masterUrl;
      }
    }
  }

  return normalized;
}

function videoJsHlsController(videoJsPlayer) {
  if (!videoJsPlayer) {
    return null;
  }

  let tech = videoJsPlayer.tech_ || null;

  if (!tech && 'function' === typeof videoJsPlayer.tech) {
    try {
      tech = videoJsPlayer.tech({ IWillNotUseThisInPlugins: true });
    } catch (error) {
      tech = null;
    }
  }

  return tech ? tech.vhs || tech.hls || null : null;
}

function applyHlsQuality(videoJsPlayer, quality) {
  const controller = videoJsHlsController(videoJsPlayer);

  if (!controller || 'function' !== typeof controller.representations) {
    return false;
  }

  const representations = controller.representations();

  if (!representations || !representations.length) {
    return false;
  }

  const automatic = 'Auto' === quality;
  const targetHeight = parseInt(quality, 10);

  if (!automatic && isNaN(targetHeight)) {
    return false;
  }

  const matchingRepresentations = automatic
    ? representations
    : representations.filter(
        (representation) => parseInt(representation.height, 10) === targetHeight
      );

  if (!automatic && !matchingRepresentations.length) {
    return false;
  }

  for (let index = 0; index < representations.length; index += 1) {
    const representation = representations[index];

    if ('function' === typeof representation.enabled) {
      representation.enabled(
        automatic || -1 !== matchingRepresentations.indexOf(representation)
      );
    }
  }

  return true;
}

export function VideoPlayerError(props) {
  return (
    <div className="error-container">
      <div className="error-container-inner">
        <span className="icon-wrap">
          <i className="material-icons">error_outline</i>
        </span>
        <span className="msg-wrap">{props.errorMessage}</span>
      </div>
    </div>
  );
}

VideoPlayerError.propTypes = {
  errorMessage: PropTypes.string.isRequired,
};

export function VideoPlayer(props) {
  const videoElemRef = useRef(null);

  let player = null;
  let hlsQualityApplyToken = 0;

  const selectedInitialQuality = initialVideoQuality(
    props.sources,
    props.info,
    props.videoQuality
  );
  const videoInfo = normalizedHlsVideoInfo(props.info);
  const initialHlsSource = hlsSourceForQuality(videoInfo, selectedInitialQuality);
  const videoSources = initialHlsSource ? [{ src: initialHlsSource }] : props.sources;

  const playerStates = {
    playerVolume: props.playerVolume,
    playerSoundMuted: props.playerSoundMuted,
    videoQuality: selectedInitialQuality,
    videoPlaybackSpeed: props.videoPlaybackSpeed,
    inTheaterMode: props.inTheaterMode,
  };

  playerStates.playerVolume =
    null === playerStates.playerVolume ? 1 : Math.max(Math.min(Number(playerStates.playerVolume), 1), 0);
  playerStates.playerSoundMuted = null !== playerStates.playerSoundMuted ? playerStates.playerSoundMuted : !1;
  playerStates.videoQuality = null !== playerStates.videoQuality ? playerStates.videoQuality : 'Auto';
  playerStates.videoPlaybackSpeed = null !== playerStates.videoPlaybackSpeed ? playerStates.videoPlaybackSpeed : !1;
  playerStates.inTheaterMode = null !== playerStates.inTheaterMode ? playerStates.inTheaterMode : !1;

  function applySelectedHlsQuality(quality) {
    const videoJsPlayer = player && player.player ? player.player : null;
    const token = hlsQualityApplyToken + 1;

    hlsQualityApplyToken = token;

    if (!videoJsPlayer) {
      return;
    }

    let attempts = 0;
    let retryTimer = null;

    function apply() {
      if (token !== hlsQualityApplyToken) {
        return false;
      }

      return applyHlsQuality(videoJsPlayer, quality);
    }

    function retry() {
      if (token !== hlsQualityApplyToken) {
        return;
      }

      attempts += 1;

      if (!apply() && attempts < 30) {
        retryTimer = window.setTimeout(retry, 100);
      }
    }

    videoJsPlayer.one('loadedmetadata', apply);
    videoJsPlayer.one('loadeddata', apply);

    retryTimer = window.setTimeout(retry, 0);

    videoJsPlayer.one('dispose', function () {
      if (retryTimer) {
        window.clearTimeout(retryTimer);
      }
    });
  }

  function onClickNext() {
    if (void 0 !== props.onClickNextCallback) {
      props.onClickNextCallback();
    }
  }

  function onClickPrevious() {
    if (void 0 !== props.onClickPreviousCallback) {
      props.onClickPreviousCallback();
    }
  }

  function onPlayerStateUpdate(newState) {
    if (playerStates.playerVolume !== newState.volume) {
      playerStates.playerVolume = newState.volume;
    }

    if (playerStates.playerSoundMuted !== newState.soundMuted) {
      playerStates.playerSoundMuted = newState.soundMuted;
    }

    if (playerStates.videoQuality !== newState.quality) {
      playerStates.videoQuality = newState.quality;
      applySelectedHlsQuality(newState.quality);
    }

    if (playerStates.videoPlaybackSpeed !== newState.playbackSpeed) {
      playerStates.videoPlaybackSpeed = newState.playbackSpeed;
    }

    if (playerStates.inTheaterMode !== newState.theaterMode) {
      playerStates.inTheaterMode = newState.theaterMode;
    }

    if (void 0 !== props.onStateUpdateCallback) {
      props.onStateUpdateCallback(newState);
    }
  }

  function initPlayer() {
    if (null !== player || null !== props.errorMessage) {
      return;
    }

    if (!props.inEmbed) {
      window.removeEventListener('focus', initPlayer);
      document.removeEventListener('visibilitychange', initPlayer);
    }

    if (!videoElemRef.current) {
      return;
    }

    if (!props.inEmbed) {
      videoElemRef.current.focus(); // Focus on player before instance init.
    }

    const subtitles = {
      on: false,
    };

    if (void 0 !== props.subtitlesInfo && null !== props.subtitlesInfo && props.subtitlesInfo.length) {
      subtitles.languages = [];

      let i = 0;
      while (i < props.subtitlesInfo.length) {
        if (
          void 0 !== props.subtitlesInfo[i].src &&
          void 0 !== props.subtitlesInfo[i].srclang &&
          void 0 !== props.subtitlesInfo[i].label
        ) {
          subtitles.languages.push({
            src: formatInnerLink(props.subtitlesInfo[i].src, props.siteUrl),
            srclang: props.subtitlesInfo[i].srclang,
            label: props.subtitlesInfo[i].label,
          });
        }

        i += 1;
      }

      if (subtitles.languages.length) {
        subtitles.on = true;
      }
    }

    player = new MediaPlayer(
      videoElemRef.current,
      {
        enabledTouchControls: true,
        sources: videoSources,
        poster: props.poster,
        autoplay: props.enableAutoplay,
        bigPlayButton: true,
        controlBar: {
          theaterMode: props.hasTheaterMode,
          pictureInPicture: false,
          next: props.hasNextLink ? true : false,
          previous: props.hasPreviousLink ? true : false,
        },
        subtitles: subtitles,
        cornerLayers: props.cornerLayers,
        videoPreviewThumb: props.previewSprite,
      },
      {
        volume: playerStates.playerVolume,
        soundMuted: playerStates.playerSoundMuted,
        theaterMode: playerStates.inTheaterMode,
        theSelectedQuality: playerStates.videoQuality,
        theSelectedPlaybackSpeed: playerStates.videoPlaybackSpeed || 1,
      },
      videoInfo,
      [0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2],
      onPlayerStateUpdate,
      onClickNext,
      onClickPrevious
    );

    applySelectedHlsQuality(playerStates.videoQuality);

    if (void 0 !== props.onPlayerInitCallback) {
      props.onPlayerInitCallback(player, videoElemRef.current);
    }
  }

  function unsetPlayer() {
    hlsQualityApplyToken += 1;

    if (null === player) {
      return;
    }

    videojs(videoElemRef.current).dispose();
    player = null;
  }

  useEffect(() => {
    if (props.inEmbed || document.hasFocus() || 'visible' === document.visibilityState) {
      initPlayer();
    } else {
      window.addEventListener('focus', initPlayer);
      document.addEventListener('visibilitychange', initPlayer);
    }

    /*
      // We don't need this because we have a custom function in frontend/src/static/js/components/media-viewer/VideoViewer/index.js:617
      player && player.player.one('loadedmetadata', () => {
      const urlParams = new URLSearchParams(window.location.search);
      const paramT = Number(urlParams.get('t'));
      const timestamp = !isNaN(paramT) ? paramT : 0;
      player.player.currentTime(timestamp);
    }); */

    return () => {
      unsetPlayer();

      if (void 0 !== props.onUnmountCallback) {
        props.onUnmountCallback();
      }
    };
  }, []);

  return null === props.errorMessage ? (
  <video
  ref={videoElemRef}
  crossOrigin="use-credentials"
  className="video-js vjs-mediacms native-dimensions"
  />
  ) : (
    <div className="error-container">
      <div className="error-container-inner">
        <span className="icon-wrap">
          <i className="material-icons">error_outline</i>
        </span>
        <span className="msg-wrap">{props.errorMessage}</span>
      </div>
    </div>
  );
}

VideoPlayer.propTypes = {
  playerVolume: PropTypes.string,
  playerSoundMuted: PropTypes.bool,
  videoQuality: PropTypes.string,
  videoPlaybackSpeed: PropTypes.number,
  inTheaterMode: PropTypes.bool,
  siteId: PropTypes.string.isRequired,
  siteUrl: PropTypes.string.isRequired,
  errorMessage: PropTypes.string,
  cornerLayers: PropTypes.object,
  subtitlesInfo: PropTypes.array.isRequired,
  inEmbed: PropTypes.bool.isRequired,
  sources: PropTypes.array.isRequired,
  info: PropTypes.object.isRequired,
  enableAutoplay: PropTypes.bool.isRequired,
  hasTheaterMode: PropTypes.bool.isRequired,
  hasNextLink: PropTypes.bool.isRequired,
  hasPreviousLink: PropTypes.bool.isRequired,
  poster: PropTypes.string,
  previewSprite: PropTypes.object,
  onClickPreviousCallback: PropTypes.func,
  onClickNextCallback: PropTypes.func,
  onPlayerInitCallback: PropTypes.func,
  onStateUpdateCallback: PropTypes.func,
  onUnmountCallback: PropTypes.func,
};

VideoPlayer.defaultProps = {
  errorMessage: null,
  cornerLayers: {},
};
