import React from 'react';
import { formatViewsNumber } from '../../utils/helpers/';
import { PageStore, MediaPageStore } from '../../utils/stores/';
import { MemberContext, PlaylistsContext } from '../../utils/contexts/';
import { MediaLikeIcon, MediaDislikeIcon, OtherMediaDownloadLink, VideoMediaDownloadLink, MediaSaveButton, MediaShareButton, MediaMoreOptionsIcon } from '../media-actions/';
import ViewerInfoTitleBanner from './ViewerInfoTitleBanner';
import { translateString } from '../../utils/helpers/';

export default class ViewerInfoVideoTitleBanner extends ViewerInfoTitleBanner {
  render() {
    const displayViews = PageStore.get('config-options').pages.media.displayViews && void 0 !== this.props.views;

    const mediaState = MediaPageStore.get('media-data').state;

    const dfansUrl  = MediaPageStore.get('media-data')?.author_dfans_url;
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
              {dfansUrl ? (
                <a
                  className={plausibleClasses}
                  href={dfansUrl}
                  data-icon={variant}
                  data-short="Full Video"
                  target="_blank"
                  rel="nofollow noopener noreferrer sponsored"
                  title="Get the Full Video here"
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
                >Get the Full Video here!</a>
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
