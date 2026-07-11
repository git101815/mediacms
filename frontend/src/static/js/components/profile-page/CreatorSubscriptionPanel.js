import React, { useEffect, useState } from 'react';
import PropTypes from 'prop-types';

import { MemberContext } from '../../utils/contexts/';

import './CreatorSubscriptionPanel.scss';


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
  return response.json().then((payload) => ({ response, payload }));
}


export default function CreatorSubscriptionPanel(props) {
  const [plans, setPlans] = useState([]);
  const [subscription, setSubscription] = useState(null);
  const [loading, setLoading] = useState(true);
  const [submittingPlanId, setSubmittingPlanId] = useState(null);
  const [error, setError] = useState('');

  const anonymous = MemberContext._currentValue.is.anonymous;
  const viewerUsername = MemberContext._currentValue.username;
  const isOwnProfile = !anonymous && viewerUsername === props.username;

  function loadPlans() {
    setLoading(true);
    setError('');

    return fetch('/api/v1/creators/' + encodeURIComponent(props.username) + '/subscription-plans', {
      credentials: 'same-origin',
      headers: {
        Accept: 'application/json',
      },
    })
      .then(readJsonResponse)
      .then(({ response, payload }) => {
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || 'Could not load subscription plans.');
        }

        setPlans(payload.plans || []);
        setSubscription(payload.subscription || null);
      })
      .catch((requestError) => {
        setError(requestError.message || 'Could not load subscription plans.');
      })
      .finally(() => {
        setLoading(false);
      });
  }

  useEffect(() => {
    loadPlans();
  }, [props.username]);

  function submitAction(url, planId) {
    setSubmittingPlanId(planId);
    setError('');

    fetch(url, {
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
        return loadPlans();
      })
      .catch((requestError) => {
        setError(requestError.message || 'Subscription update failed.');
      })
      .finally(() => {
        setSubmittingPlanId(null);
      });
  }

  if (isOwnProfile || (!loading && plans.length === 0 && !error)) {
    return null;
  }

  return (
    <section className="creator-subscription-panel" aria-label="Creator memberships">
      {loading ? <span className="creator-subscription-loading">Loading memberships…</span> : null}

      {plans.map((plan) => {
        const currentPlan = subscription && subscription.plan_id === plan.id;
        const activeOnAnotherPlan = subscription && subscription.active && !currentPlan;
        const busy = submittingPlanId === plan.id;

        let label = 'SUBSCRIBE · ' + plan.price_display + ' TOKENS / ' + plan.billing_period_days + ' DAYS';
        let actionUrl = plan.subscribe_url;
        let disabled = busy || !plan.can_subscribe;

        if (currentPlan && subscription.active && subscription.cancel_at_period_end) {
          label = 'RESUME MEMBERSHIP';
          actionUrl = subscription.resume_url;
          disabled = busy;
        } else if (currentPlan && subscription.active) {
          label = 'CANCEL RENEWAL';
          actionUrl = subscription.cancel_url;
          disabled = busy;
        } else if (activeOnAnotherPlan) {
          label = 'ALREADY SUBSCRIBED';
          disabled = true;
        }

        return (
          <div className="creator-subscription-plan" key={plan.id}>
            <div className="creator-subscription-plan-copy">
              <strong>{plan.name}</strong>
              <span>
                Includes premium access earned for videos released during each paid period.
              </span>
            </div>

            {anonymous ? (
              <a
                className="button-link creator-subscription-action"
                href={'/accounts/login/?next=' + encodeURIComponent(window.location.pathname + window.location.search)}
              >
                SIGN IN TO SUBSCRIBE
              </a>
            ) : (
              <button
                type="button"
                className="button-link creator-subscription-action"
                disabled={disabled}
                onClick={() => submitAction(actionUrl, plan.id)}
              >
                {busy ? 'PROCESSING…' : label}
              </button>
            )}

            {currentPlan && subscription.active ? (
              <span className="creator-subscription-renews">
                Paid access period ends {new Date(subscription.current_period_end).toLocaleDateString()}
              </span>
            ) : null}
          </div>
        );
      })}

      {error ? <div className="creator-subscription-error">{error}</div> : null}
    </section>
  );
}


CreatorSubscriptionPanel.propTypes = {
  username: PropTypes.string.isRequired,
};
