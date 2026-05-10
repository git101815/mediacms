import React from 'react';
import { formatViewsNumber } from '../../utils/helpers/';
import { PageStore, MediaPageStore } from '../../utils/stores/';
import { MemberContext, PlaylistsContext } from '../../utils/contexts/';
import { MediaLikeIcon, MediaDislikeIcon, OtherMediaDownloadLink, VideoMediaDownloadLink, MediaSaveButton, MediaShareButton, MediaMoreOptionsIcon } from '../media-actions/';
import ViewerInfoTitleBanner from './ViewerInfoTitleBanner';
import { translateString } from '../../utils/helpers/';

export default class ViewerInfoVideoTitleBanner extends ViewerInfoTitleBanner {
  getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) {
      return parts.pop().split(';').shift();
    }
    return '';
  }

  openUnlockedPlayback(event) {
    event.preventDefault();
    const baseUrl = window.location.pathname;
    window.location.href = baseUrl + '?playback=premium';
  }

  purchaseWithTokens(event, purchaseUrl) {
    event.preventDefault();

    fetch(purchaseUrl, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        Accept: 'application/json',
        'X-CSRFToken': this.getCookie('csrftoken'),
      },
    })
      .then((response) => response.json().then((payload) => ({ response, payload })))
      .then(({ response, payload }) => {
        if (!response.ok || !payload.ok) {
          alert(payload.error || 'Purchase failed');
          return;
        }

        window.location.href = window.location.pathname + '?playback=premium';
      })
      .catch(() => {
        alert('Purchase failed');
      });
  }
  render() {
    const displayViews = PageStore.get('config-options').pages.media.displayViews && void 0 !== this.props.views;

    const mediaState = MediaPageStore.get('media-data').state;

    const mediaData = MediaPageStore.get('media-data') || {};
    const dfansUrl = mediaData.author_dfans_url;
    const premium = mediaData.premium || {};
    const variant   = 'cart';
    let refCode = 'none';
    try { refCode = new URL(dfansUrl).searchParams.get('ref') || 'none'; } catch {}
    const plausibleClasses = [
        'action-btn', 'action-btn--primary', 'action-btn--dfans',
        'plausible-event-name=e2',
        `plausible-event-variant=${variant}`,
        `plausible-event-ref_code=${refCode}`,
    ].join(' ');
    let stateTooltip = '';

    switch (mediaState) {
      case 'private':
        stateTooltip = 'The site admins have to make its access public';
        break;
      case 'unlisted':
        stateTooltip = 'The site admins have to make it appear on listings';
        break;
    }

    return (
      <div className="media-title-banner">
        {displayViews && PageStore.get('config-options').pages.media.categoriesWithTitle
          ? this.mediaCategories(true)
          : null}

        {void 0 !== this.props.title ? (
          <div className="media-title-row">
            <h1 className="media-title">{this.props.title}</h1>
            {displayViews ? (
              <span className="media-views-inline only-mobile">
                {formatViewsNumber(this.props.views, true)}{' '}
                {1 >= this.props.views ? translateString('view') : translateString('views')}
              </span>
            ) : null}
          </div>
         ) : null}

        {'public' !== mediaState ? (
          <div className="media-labels-area">
            <div className="media-labels-area-inner">
              <span className="media-label-state">
                <span>{mediaState}</span>
              </span>
              <span className="helper-icon" data-tooltip={stateTooltip}>
                <i className="material-icons">help_outline</i>
              </span>
            </div>
          </div>
        ) : null}

        <div
          className={
            'media-views-actions' +
            (this.state.likedMedia ? ' liked-media' : '') +
            (this.state.dislikedMedia ? ' disliked-media' : '')
          }
        >
          {!displayViews && PageStore.get('config-options').pages.media.categoriesWithTitle
            ? this.mediaCategories()
            : null}

          {displayViews ? (
            <div className="media-views media-views--desktop">
              {formatViewsNumber(this.props.views, true)}{' '}
              {1 >= this.props.views ? translateString('view') : translateString('views')}
            </div>
          ) : null}

          <div className="media-actions">
            <div>
              {premium.enabled && premium.viewer_has_unlock ? (
                <a
                  className="action-btn action-btn--primary"
                  href={window.location.pathname + '?playback=premium'}
                  data-icon="play_arrow"
                  data-short="Unlocked"
                  title="Watch unlocked video"
                  onClick={(event) => this.openUnlockedPlayback(event)}
                >
                  Watch unlocked
                </a>
              ) : null}

              {premium.enabled && !premium.viewer_has_unlock && premium.purchase_url ? (
                <button
                  type="button"
                  className="action-btn action-btn--primary"
                  data-icon="lock_open"
                  data-short="Tokens"
                  title="Pay with tokens"
                  onClick={(event) => this.purchaseWithTokens(event, premium.purchase_url)}
                >
                  Pay with tokens · {premium.price_display}
                </button>
              ) : null}

              {dfansUrl ? (
                <a
                  className={plausibleClasses}
                  href={dfansUrl}
                  data-icon={variant}
                  data-short="DFans"
                  target="_blank"
                  rel="nofollow noopener noreferrer sponsored"
                  title="Pay on DFans"
                  onClick={(e) => {
                    if (typeof window.plausible === 'function') {
                      window.plausible('e2', {
                        props: {
                          variant,
                          ref_code: refCode,
                          page_path: window.location.pathname,
                        },
                      });
                    }
                  }}
                >
                  Pay on DFans
                </a>
              ) : null}
              {MemberContext._currentValue.can.likeMedia ? <MediaLikeIcon /> : null}
              {MemberContext._currentValue.can.dislikeMedia ? <MediaDislikeIcon /> : null}
              {MemberContext._currentValue.can.shareMedia ? <MediaShareButton isVideo={true} /> : null}

              {!MemberContext._currentValue.is.anonymous &&
                MemberContext._currentValue.can.saveMedia &&
                -1 < PlaylistsContext._currentValue.mediaTypes.indexOf(MediaPageStore.get('media-type')) ? (
                <MediaSaveButton />
              ) : null}

              {!this.props.allowDownload || !MemberContext._currentValue.can.downloadMedia ? null : !this
                .downloadLink ? (
                <VideoMediaDownloadLink />
              ) : (
                <OtherMediaDownloadLink link={this.downloadLink} title={this.props.title} />
              )}

              <MediaMoreOptionsIcon allowDownload={this.props.allowDownload} />
            </div>
          </div>
        </div>
      </div>
    );
  }
}
