import React from 'react';
import { formatViewsNumber } from '../../utils/helpers/';
import { PageStore, MediaPageStore } from '../../utils/stores/';
import { MemberContext, PlaylistsContext } from '../../utils/contexts/';
import {
  MediaLikeIcon,
  MediaDislikeIcon,
  OtherMediaDownloadLink,
  VideoMediaDownloadLink,
  MediaSaveButton,
  MediaShareButton,
  MediaMoreOptionsIcon,
} from '../media-actions/';
import ViewerInfoTitleBanner from './ViewerInfoTitleBanner';
import { translateString } from '../../utils/helpers/';
import './PremiumActions.scss';

export default class ViewerInfoVideoTitleBanner extends ViewerInfoTitleBanner {
  constructor(props) {
    super(props);

    this.state = {
      ...this.state,
      premiumModalOpen: false,
      premiumPurchasing: false,
      premiumError: '',
    };

    this.openPremiumModal = this.openPremiumModal.bind(this);
    this.closePremiumModal = this.closePremiumModal.bind(this);
    this.openUnlockedPlayback = this.openUnlockedPlayback.bind(this);
  }

  getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) {
      return parts.pop().split(';').shift();
    }
    return '';
  }

  openPremiumModal(event) {
    event.preventDefault();

    this.setState({
      premiumModalOpen: true,
      premiumError: '',
    });
  }

  closePremiumModal(event) {
    if (event) {
      event.preventDefault();
    }

    if (this.state.premiumPurchasing) {
      return;
    }

    this.setState({
      premiumModalOpen: false,
      premiumError: '',
    });
  }

  openUnlockedPlayback(event) {
    event.preventDefault();
    window.location.href = window.location.pathname + '?playback=premium';
  }

  openLoginForPurchase(event) {
    event.preventDefault();
    window.location.href = '/accounts/login?next=' + encodeURIComponent(window.location.pathname);
  }

  purchaseWithTokens(event, purchaseUrl) {
    event.preventDefault();

    if (this.state.premiumPurchasing) {
      return;
    }

    this.setState({
      premiumPurchasing: true,
      premiumError: '',
    });

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
          this.setState({
            premiumPurchasing: false,
            premiumError: payload.error || 'Purchase failed.',
          });
          return;
        }

        window.location.href = window.location.pathname + '?playback=premium';
      })
      .catch(() => {
        this.setState({
          premiumPurchasing: false,
          premiumError: 'Purchase failed.',
        });
      });
  }

  renderPremiumModal({ premium, dfansUrl, plausibleClasses, variant, refCode }) {
    if (!this.state.premiumModalOpen) {
      return null;
    }

    const isAnonymous = MemberContext._currentValue.is.anonymous;

    return (
      <div className="premium-modal-backdrop" onClick={(event) => this.closePremiumModal(event)}>
        <div className="premium-modal" onClick={(event) => event.stopPropagation()}>
          <button
            type="button"
            className="premium-modal__close"
            onClick={(event) => this.closePremiumModal(event)}
            disabled={this.state.premiumPurchasing}
            aria-label="Close"
          >
            ×
          </button>

          <div className="premium-modal__header">
            <div className="premium-modal__eyebrow">Premium video</div>
            <h2>Get the full video</h2>
            <p>Choose how you want to unlock this video.</p>
          </div>

          <div className="premium-modal__options">
                        {premium.enabled && premium.purchase_url ? (
              <button
                type="button"
                className="premium-option premium-option--tokens"
                disabled={this.state.premiumPurchasing}
                onClick={(event) =>
                  isAnonymous
                    ? this.openLoginForPurchase(event)
                    : this.purchaseWithTokens(event, premium.purchase_url)
                }
              >
                <span className="premium-option__icon">
                  <i className="material-icons">lock_open</i>
                </span>
                <span className="premium-option__body">
                  <span className="premium-option__title">
                    {isAnonymous ? 'Log in to pay with tokens' : 'Pay with tokens'}
                  </span>
                  <span className="premium-option__subtitle">
                    Unlock permanently on this site
                  </span>
                </span>
                <span className="premium-option__price">
                  {premium.price_display || '—'}
                </span>
              </button>
            ) : (
              <button
                type="button"
                className="premium-option premium-option--tokens premium-option--disabled"
                disabled
              >
                <span className="premium-option__icon">
                  <i className="material-icons">lock</i>
                </span>
                <span className="premium-option__body">
                  <span className="premium-option__title">Pay with tokens</span>
                  <span className="premium-option__subtitle">
                    Not available for this video yet
                  </span>
                </span>
                <span className="premium-option__price">—</span>
              </button>
            )}

            {dfansUrl ? (
              <a
                className="premium-option premium-option--dfans"
                href={dfansUrl}
                target="_blank"
                rel="nofollow noopener noreferrer sponsored"
                onClick={() => {
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
                <span className="premium-option__icon">
                  <i className="material-icons">open_in_new</i>
                </span>
                <span className="premium-option__body">
                  <span className="premium-option__title">Pay on DFans</span>
                  <span className="premium-option__subtitle">
                    Continue to the creator page
                  </span>
                </span>
                <span className="premium-option__price">DFans</span>
              </a>
            ) : null}
          </div>

          {this.state.premiumPurchasing ? (
            <div className="premium-modal__status">Processing purchase…</div>
          ) : null}

          {this.state.premiumError ? (
            <div className="premium-modal__error">{this.state.premiumError}</div>
          ) : null}

          <div className="premium-modal__footer">
            <a href="/unlocked">View unlocked videos</a>
          </div>
        </div>
      </div>
    );
  }

  render() {
    const displayViews = PageStore.get('config-options').pages.media.displayViews && void 0 !== this.props.views;

    const mediaData = MediaPageStore.get('media-data') || {};
    const mediaState = mediaData.state;

    const dfansUrl = mediaData.author_dfans_url;
    const premium = mediaData.premium || {};
    const premiumManageUrl = premium.manage_url;
    const variant = 'cart';

    let refCode = 'none';
    try {
      refCode = new URL(dfansUrl).searchParams.get('ref') || 'none';
    } catch {}

    const plausibleClasses = [
      'action-btn',
      'action-btn--primary',
      'action-btn--dfans',
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

    const hasPremium = !!premium.enabled;
    const hasUnlock = !!premium.viewer_has_unlock;

    return (
      <div className="media-title-banner">
        {displayViews && PageStore.get('config-options').pages.media.categoriesWithTitle
          ? this.mediaCategories(true)
          : null}

        {void 0 !== this.props.title ? (
          <div className="media-title-row">
            <h1 className="media-title">{this.props.title}</h1>
            {hasPremium ? (
              <span className={'premium-badge' + (hasUnlock ? ' premium-badge--unlocked' : '')}>
                {hasUnlock ? 'Unlocked' : 'Premium'}
              </span>
            ) : null}
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
              {hasPremium && hasUnlock ? (
                <a
                  className="action-btn action-btn--primary action-btn--premium-unlocked"
                  href={window.location.pathname + '?playback=premium'}
                  data-icon="play_arrow"
                  data-short="Unlocked"
                  title="Watch unlocked video"
                  onClick={(event) => this.openUnlockedPlayback(event)}
                >
                  Watch unlocked
                </a>
              ) : null}

              {!hasUnlock && (hasPremium || dfansUrl) ? (
                <button
                  type="button"
                  className="action-btn action-btn--primary action-btn--dfans action-btn--premium"
                  data-icon="cart"
                  data-short="Full Video"
                  title="Get the full video"
                  onClick={(event) => this.openPremiumModal(event)}
                >
                  Get full video
                </button>
              ) : null}
               {premiumManageUrl ? (
                  <a
                    className="action-btn"
                    href={premiumManageUrl}
                    data-icon="settings"
                    data-short="Premium"
                    title="Manage premium video"
                  >
                    Manage premium
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

        {this.renderPremiumModal({
          premium,
          dfansUrl,
          plausibleClasses,
          variant,
          refCode,
        })}
      </div>
    );
  }
}