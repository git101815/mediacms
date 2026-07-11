import React, { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import PropTypes from 'prop-types';

import { MemberContext } from '../../utils/contexts/';

import './CreatorSubscribeButton.scss';


function getCookie(name) {
  const prefix = name + '=';
  const parts = document.cookie ? document.cookie.split(';') : [];

  for (let i = 0; i < parts.length; i += 1) {
    const item = parts[i].trim();
    if (item.indexOf(prefix) === 0) {
      return decodeURIComponent(item.substring(prefix.length));
    }
  }

  return '';
}


function readJsonResponse(response) {
  return response
    .json()
    .catch(() => ({}))
    .then((payload) => ({ response, payload }));
}


function formatPeriodEnd(value) {
  if (!value) {
    return '';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '';
  }

  return date.toLocaleDateString();
}


export default function CreatorSubscribeButton(props) {
  const [portalTarget, setPortalTarget] = useState(null);
  const [plan, setPlan] = useState(null);
  const [subscription, setSubscription] = useState(null);
  const [loaded, setLoaded] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  const member = MemberContext._currentValue;
  const anonymous = member.is.anonymous;
  const viewerUsername = member.username;
  const isOwnProfile = !anonymous && viewerUsername === props.username;

  function loadOffer() {
    return fetch('/api/v1/creators/' + encodeURIComponent(props.username) + '/subscription-plans', {
      credentials: 'same-origin',
      headers: {
        Accept: 'application/json',
      },
    })
      .then(readJsonResponse)
      .then(({ response, payload }) => {
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || 'Could not load subscription information.');
        }

        const plans = payload.plans || [];
        setPlan(plans.length ? plans[0] : null);
        setSubscription(payload.subscription || null);
      });
  }

  useEffect(() => {
    let cancelled = false;
    let animationFrame = null;

    function findPortalTarget() {
      if (cancelled) {
        return;
      }

      setPortalTarget(document.querySelector(props.portalTargetSelector));
    }

    findPortalTarget();
    animationFrame = window.requestAnimationFrame(findPortalTarget);

    return () => {
      cancelled = true;
      if (animationFrame) {
        window.cancelAnimationFrame(animationFrame);
      }
    };
  }, [props.portalTargetSelector, props.username]);

  useEffect(() => {
    let cancelled = false;

    setLoaded(false);
    setPlan(null);
    setSubscription(null);
    setError('');
    setModalOpen(false);

    loadOffer()
      .catch(() => {
        if (!cancelled) {
          setPlan(null);
          setSubscription(null);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoaded(true);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [props.username]);

  const activeSubscription = !!(subscription && subscription.active);
  const hasOffer = !!plan || activeSubscription;
  const cancellationPending = !!(
    activeSubscription && subscription.cancel_at_period_end
  );

  function openSubscription(event) {
    event.preventDefault();
    event.stopPropagation();

    if (anonymous) {
      window.location.href =
        '/accounts/login/?next=' +
        encodeURIComponent(window.location.pathname + window.location.search);
      return;
    }

    setError('');
    setModalOpen(true);
  }

  function closeModal(event) {
    if (event) {
      event.preventDefault();
      event.stopPropagation();
    }

    if (submitting) {
      return;
    }

    setError('');
    setModalOpen(false);
  }

  function submitAction(event) {
    event.preventDefault();
    event.stopPropagation();

    if (submitting) {
      return;
    }

    let actionUrl = '';

    if (activeSubscription) {
      actionUrl = cancellationPending
        ? subscription.resume_url
        : subscription.cancel_url;
    } else if (plan) {
      actionUrl = plan.subscribe_url;
    }

    if (!actionUrl) {
      return;
    }

    setSubmitting(true);
    setError('');

    fetch(actionUrl, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        Accept: 'application/json',
        'X-CSRFToken': getCookie('csrftoken'),
      },
    })
      .then(readJsonResponse)
      .then(({ response, payload }) => {
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || 'Subscription update failed.');
        }

        return loadOffer();
      })
      .then(() => {
        setModalOpen(false);
      })
      .catch((requestError) => {
        setError(requestError.message || 'Subscription update failed.');
      })
      .finally(() => {
        setSubmitting(false);
      });
  }

  if (!portalTarget || !loaded || isOwnProfile || !hasOffer) {
    return null;
  }

  const creatorName = props.creatorName || props.username;
  const priceDisplay = activeSubscription
    ? subscription.price_display
    : plan.price_display;
  const billingPeriodDays = activeSubscription
    ? subscription.billing_period_days
    : plan.billing_period_days;
  const periodEnd = activeSubscription
    ? formatPeriodEnd(subscription.current_period_end)
    : '';
  const actionLabel = activeSubscription
    ? cancellationPending
      ? 'Resume renewal'
      : 'Cancel renewal'
    : 'Subscribe for ' + priceDisplay + ' tokens';

  const content = (
    <div
      className={
        'creator-subscribe-slot creator-subscribe-slot--' + props.placement
      }
    >
      <button
        type="button"
        className={
          'creator-subscribe-button' +
          (activeSubscription ? ' creator-subscribe-button--active' : '')
        }
        onClick={openSubscription}
      >
        {activeSubscription ? 'Subscribed' : 'Subscribe'}
      </button>

      {modalOpen ? (
        <div
          className="creator-subscribe-modal-backdrop"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              closeModal(event);
            }
          }}
        >
          <div
            className="creator-subscribe-modal"
            role="dialog"
            aria-modal="true"
            aria-label="Creator subscription"
            onMouseDown={(event) => event.stopPropagation()}
          >
            <button
              type="button"
              className="creator-subscribe-modal__close"
              aria-label="Close"
              disabled={submitting}
              onClick={closeModal}
            >
              ×
            </button>

            <div className="creator-subscribe-modal__header">
              <span className="creator-subscribe-modal__eyebrow">
                Creator membership
              </span>
              <h2>
                {activeSubscription
                  ? cancellationPending
                    ? 'Renewal is turned off'
                    : 'You are subscribed'
                  : 'Subscribe to ' + creatorName}
              </h2>
              <p>
                Premium videos released during each paid period are permanently
                unlocked for your account.
              </p>
            </div>

            <div className="creator-subscribe-modal__summary">
              <span>Membership price</span>
              <strong>
                {priceDisplay} tokens every {billingPeriodDays} days
              </strong>
            </div>

            {activeSubscription && periodEnd ? (
              <div className="creator-subscribe-modal__period">
                <span>Current paid period ends</span>
                <strong>{periodEnd}</strong>
              </div>
            ) : null}

            {cancellationPending ? (
              <p className="creator-subscribe-modal__notice">
                No further renewal is scheduled. Access already earned remains
                available.
              </p>
            ) : null}

            {error ? (
              <div className="creator-subscribe-modal__error">
                <span>{error}</span>
                {error.toLowerCase().indexOf('insufficient token balance') !== -1 ? (
                  <a href="/wallet">Buy tokens</a>
                ) : null}
              </div>
            ) : null}

            <div className="creator-subscribe-modal__actions">
              <button
                type="button"
                className="creator-subscribe-modal__dismiss"
                disabled={submitting}
                onClick={closeModal}
              >
                Close
              </button>
              <button
                type="button"
                className={
                  'creator-subscribe-modal__confirm' +
                  (activeSubscription && !cancellationPending
                    ? ' creator-subscribe-modal__confirm--secondary'
                    : '')
                }
                disabled={submitting}
                onClick={submitAction}
              >
                {submitting ? 'Processing…' : actionLabel}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );

  return createPortal(content, portalTarget);
}


CreatorSubscribeButton.propTypes = {
  username: PropTypes.string.isRequired,
  creatorName: PropTypes.string,
  placement: PropTypes.oneOf(['profile', 'media']).isRequired,
  portalTargetSelector: PropTypes.string.isRequired,
};


CreatorSubscribeButton.defaultProps = {
  creatorName: '',
};
