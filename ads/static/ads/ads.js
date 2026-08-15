(() => {
  const sidebar = document.querySelector('.sidebar');
  const page = document.querySelector('.active-page');
  const toggle = document.querySelector('.toggle-sidebar-button');

  if (sidebar && page && toggle) {
    toggle.addEventListener('click', () => {
      const closed = sidebar.classList.toggle('--closed');
      page.classList.toggle('--sidebar-closed', closed);
      toggle.classList.toggle('--closed', closed);
    });
  }

  document.querySelectorAll('.general-form .group').forEach((group) => {
    const control = group.querySelector(
      'input:not([type="file"]), select, textarea'
    );
    if (!control) return;
    const sync = () => {
      const value = control.value == null
        ? ''
        : String(control.value).trim();
      group.classList.toggle('has-value', value.length > 0);
    };
    control.addEventListener('input', sync);
    control.addEventListener('change', sync);
    sync();
  });

  const imageInput = document.querySelector(
    'input[type="file"][name="image"], input[type="file"][name="creative"]'
  );
  const preview = document.querySelector('[data-creative-preview]');
  if (imageInput && preview) {
    imageInput.addEventListener('change', () => {
      const selected = imageInput.files && imageInput.files[0];
      if (!selected) return;
      const url = URL.createObjectURL(selected);
      preview.innerHTML = '';
      const img = document.createElement('img');
      img.src = url;
      img.alt = 'Creative preview';
      img.onload = () => URL.revokeObjectURL(url);
      preview.appendChild(img);
    });
  }

  const placement = document.querySelector(
    '#campaign-form [name="placement"]'
  );
  const picker = document.querySelector('[data-creative-picker]');
  const pickerEmpty = document.querySelector(
    '[data-creative-picker-empty]'
  );
  if (placement && picker) {
    const syncPicker = () => {
      let visible = 0;
      picker.querySelectorAll(
        '[data-creative-placement]'
      ).forEach((item) => {
        const matches = (
          item.getAttribute('data-creative-placement')
          === placement.value
        );
        item.hidden = !matches;
        if (!matches) {
          const checkbox = item.querySelector(
            'input[type="checkbox"]'
          );
          if (checkbox) checkbox.checked = false;
        } else {
          visible += 1;
        }
      });
      if (pickerEmpty) pickerEmpty.hidden = visible !== 0;
    };
    placement.addEventListener('change', syncPicker);
    syncPicker();
  }

  // ads-min-bid-ui-v1
  const campaignForm = document.querySelector('#campaign-form');
  if (campaignForm) {
    const campaignPlacement = campaignForm.querySelector(
      '[name="placement"]'
    );
    const campaignPricing = campaignForm.querySelector(
      '[name="pricing_model"]'
    );
    const campaignBid = campaignForm.querySelector(
      '[name="bid_usd"]'
    );

    const syncCampaignBidMinimum = () => {
      if (!campaignPlacement || !campaignPricing || !campaignBid) {
        return;
      }

      const bannerPlacements = new Set([
        'home_leaderboard',
        'media_sidebar_rectangle',
      ]);
      const adType = bannerPlacements.has(campaignPlacement.value)
        ? 'banner'
        : campaignPlacement.value;
      const key = (
        'data-min-'
        + adType
        + '-'
        + campaignPricing.value
      );
      const minimum = campaignBid.getAttribute(key);
      if (minimum) {
        campaignBid.setAttribute('min', minimum);
      }
    };

    if (campaignPlacement) {
      campaignPlacement.addEventListener(
        'change',
        syncCampaignBidMinimum
      );
    }
    if (campaignPricing) {
      campaignPricing.addEventListener(
        'change',
        syncCampaignBidMinimum
      );
    }
    syncCampaignBidMinimum();
  }

  const copyButton = document.querySelector('[data-copy-address]');
  if (copyButton) {
    copyButton.addEventListener('click', async () => {
      const value = copyButton.getAttribute(
        'data-copy-address'
      ) || '';
      if (!value) return;
      try {
        await navigator.clipboard.writeText(value);
        const previous = copyButton.textContent;
        copyButton.textContent = 'copied';
        window.setTimeout(() => {
          copyButton.textContent = previous;
        }, 1200);
      } catch (_) {}
    });
  }

  const statusNode = document.querySelector(
    '[data-deposit-status-url]'
  );
  if (statusNode) {
    const statusUrl = statusNode.getAttribute(
      'data-deposit-status-url'
    );
    const label = document.querySelector(
      '[data-deposit-status-label]'
    );
    const terminal = new Set([
      'transaction_complete',
      'canceled',
      'failed',
      'expired',
    ]);
    const poll = async () => {
      try {
        const response = await fetch(
          statusUrl,
          {
            credentials: 'same-origin',
            cache: 'no-store',
          }
        );
        if (!response.ok) return;
        const payload = await response.json();
        if (label && payload.status_label) {
          label.textContent = payload.status_label;
        }
        if (terminal.has(payload.status)) return;
      } catch (_) {}
      window.setTimeout(poll, 3000);
    };
    window.setTimeout(poll, 1500);
  }

  const financeForm = document.querySelector(
    '[data-finance-buy-form]'
  );
  if (!financeForm) return;

  const packInput = financeForm.querySelector(
    '[data-finance-selected-pack]'
  );
  const routeInput = financeForm.querySelector(
    '[data-finance-selected-route]'
  );

  const state = {
    packCode: '',
    packLabel: '',
    packPriceLabel: '',
    packGrossCanonical: 0,
    method: null,
    assetKey: '',
    asset: null,
    route: null,
    reviewBackStep: 2,
  };

  const routes = Array.from(
    financeForm.querySelectorAll(
      '[data-finance-route-option]'
    )
  ).map((node) => ({
    key: node.getAttribute('data-option-key') || '',
    paymentMethodKey:
      node.getAttribute('data-payment-method-key') || '',
    paymentMethodLabel:
      node.getAttribute('data-payment-method-label') || '',
    paymentMethodType:
      node.getAttribute('data-payment-method-type') || '',
    paymentGroupKey:
      node.getAttribute('data-payment-group-key') || '',
    paymentGroupLabel:
      node.getAttribute('data-payment-group-label') || '',
    paymentGroupIcon:
      node.getAttribute('data-payment-group-icon') || '',
    paymentGroupIconUrl:
      node.getAttribute('data-payment-group-icon-url') || '',
    priceBps: Number(
      node.getAttribute('data-payment-price-bps') || 0
    ),
    priceFixedCanonical: Number(
      node.getAttribute(
        'data-payment-price-fixed-canonical'
      ) || 0
    ),
    paymentCurrency:
      node.getAttribute('data-payment-currency') || 'USD',
    paymentCurrencySymbol:
      node.getAttribute(
        'data-payment-currency-symbol'
      ) || '$',
    paymentCurrencyUsdRate: Number(
      node.getAttribute(
        'data-payment-currency-usd-rate'
      ) || 1
    ),
    paymentRequiresRouteSelection:
      node.getAttribute(
        'data-payment-requires-route-selection'
      ) === 'true',
    paymentOpenNewTab:
      node.getAttribute(
        'data-payment-open-new-tab'
      ) === 'true',
    assetCode: node.getAttribute('data-asset-code') || '',
    assetGroupKey:
      node.getAttribute('data-asset-group-key') || '',
    assetGroupLabel:
      node.getAttribute('data-asset-group-label') || '',
    assetGroupIconUrl:
      node.getAttribute('data-asset-group-icon-url') || '',
    assetGroupOrder: Number(
      node.getAttribute('data-asset-group-order') || 100
    ),
    chain: node.getAttribute('data-chain') || '',
    networkLabel:
      node.getAttribute('data-network-label') || '',
    networkGroupKey:
      node.getAttribute('data-network-group-key') || '',
    networkGroupLabel:
      node.getAttribute('data-network-group-label') || '',
    networkGroupIconUrl:
      node.getAttribute('data-network-group-icon-url') || '',
    networkGroupOrder: Number(
      node.getAttribute('data-network-group-order') || 100
    ),
    minAmountCanonical: Number(
      node.getAttribute('data-min-amount-canonical') || 0
    ),
  }));

  const stepPanels = Array.from(
    financeForm.querySelectorAll(
      '[data-finance-step-panel]'
    )
  );
  const indicators = Array.from(
    financeForm.querySelectorAll(
      '[data-finance-step-indicator]'
    )
  );

  function showStep(step) {
    stepPanels.forEach((panel) => {
      panel.hidden = (
        panel.getAttribute('data-finance-step-panel')
        !== String(step)
      );
    });
    indicators.forEach((indicator) => {
      indicator.classList.toggle(
        'is-active',
        indicator.getAttribute(
          'data-finance-step-indicator'
        ) === String(step)
      );
      const indicatorStep = Number(
        indicator.getAttribute(
          'data-finance-step-indicator'
        )
      );
      indicator.classList.toggle(
        'is-done',
        indicatorStep < step
      );
    });
  }

  function setRouteIndicatorsVisible(visible) {
    financeForm.querySelectorAll(
      '.finance-stepper__route'
    ).forEach((node) => {
      node.hidden = !visible;
    });
  }

  function adjustedCanonical(route) {
    const base = Number(
      state.packGrossCanonical || 0
    );
    return (
      base
      + Number(route.priceFixedCanonical || 0)
      + Math.round(
        base * Number(route.priceBps || 0) / 10000
      )
    );
  }

  function availableRoutes() {
    return routes.filter((route) => {
      const minimum = Number(
        route.minAmountCanonical || 0
      );
      return (
        minimum <= 0
        || adjustedCanonical(route) >= minimum
      );
    });
  }

  function makeChoiceCard({
    label,
    detail,
    iconUrl,
    iconText,
    onClick,
  }) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'finance-choice-card';

    const icon = document.createElement(
      iconUrl ? 'img' : 'span'
    );
    if (iconUrl) {
      icon.src = iconUrl;
      icon.alt = '';
      icon.className = 'finance-choice-card__logo';
    } else {
      icon.className = (
        'finance-choice-card__logo '
        + 'finance-choice-card__logo--fallback'
      );
      icon.textContent = (
        iconText || label || '?'
      ).slice(0, 4).toUpperCase();
    }

    const copy = document.createElement('span');
    copy.className = 'finance-choice-card__copy';
    const strong = document.createElement('strong');
    strong.textContent = label;
    const small = document.createElement('small');
    small.textContent = detail || '';
    copy.append(strong, small);

    button.append(icon, copy);
    button.addEventListener('click', () => onClick(button));
    return button;
  }

  function markChoiceSelected(container, selectedButton) {
    container.querySelectorAll('.finance-choice-card').forEach((button) => {
      const selected = button === selectedButton;
      button.classList.toggle('is-selected', selected);
      button.setAttribute('aria-pressed', selected ? 'true' : 'false');
    });
  }

  function paymentMethods() {
    const map = new Map();
    availableRoutes().forEach((route) => {
      const key = (
        route.paymentGroupKey
        || route.paymentMethodKey
      );
      if (!key) return;

      if (!map.has(key)) {
        map.set(key, {
          key,
          label: (
            route.paymentGroupLabel
            || route.paymentMethodLabel
            || route.assetCode
          ),
          iconText: (
            route.paymentGroupIcon
            || route.paymentGroupLabel
            || ''
          ),
          iconUrl: route.paymentGroupIconUrl,
          type: route.paymentMethodType,
          routes: [],
        });
      }
      map.get(key).routes.push(route);
    });
    return Array.from(map.values());
  }

  function renderPaymentMethods() {
    const container = financeForm.querySelector(
      '[data-finance-payment-methods]'
    );
    container.innerHTML = '';
    const continueButton = financeForm.querySelector(
      '[data-finance-payment-continue]'
    );
    if (continueButton) continueButton.disabled = true;

    paymentMethods().forEach((method) => {
      const requiresRoute = method.routes.some(
        (route) => (
          route.paymentRequiresRouteSelection
          || route.paymentMethodType === 'crypto'
        )
      );
      const detail = requiresRoute
        ? 'Choose asset and network'
        : 'Continue to checkout';

      container.appendChild(
        makeChoiceCard({
          label: method.label,
          detail,
          iconUrl: method.iconUrl,
          iconText: method.iconText,
          onClick: (button) => {
            markChoiceSelected(container, button);
            state.method = method;
            state.assetKey = '';
            state.asset = null;
            state.route = null;
            routeInput.value = '';
            setRouteIndicatorsVisible(requiresRoute);
            const continueButton = financeForm.querySelector(
              '[data-finance-payment-continue]'
            );
            if (continueButton) continueButton.disabled = false;
          },
        })
      );
    });
  }

  function assetChoices() {
    const map = new Map();
    const methodRoutes = (
      state.method ? state.method.routes : []
    );

    methodRoutes.forEach((route) => {
      const key = (
        route.assetGroupKey
        || route.assetCode
      );
      if (!key) return;
      if (!map.has(key)) {
        map.set(key, {
          key,
          label: (
            route.assetGroupLabel
            || route.assetCode
          ),
          iconUrl: route.assetGroupIconUrl,
          order: route.assetGroupOrder,
          routes: [],
        });
      }
      map.get(key).routes.push(route);
    });

    return Array.from(map.values()).sort(
      (a, b) => (
        a.order - b.order
        || a.label.localeCompare(b.label)
      )
    );
  }

  function renderAssets() {
    const container = financeForm.querySelector(
      '[data-finance-assets]'
    );
    container.innerHTML = '';
    const continueButton = financeForm.querySelector(
      '[data-finance-asset-continue]'
    );
    if (continueButton) continueButton.disabled = true;

    assetChoices().forEach((asset) => {
      container.appendChild(
        makeChoiceCard({
          label: asset.label,
          detail: 'Select network next',
          iconUrl: asset.iconUrl,
          iconText: asset.label,
          onClick: (button) => {
            markChoiceSelected(container, button);
            state.assetKey = asset.key;
            state.asset = asset;
            state.route = null;
            routeInput.value = '';
            const continueButton = financeForm.querySelector(
              '[data-finance-asset-continue]'
            );
            if (continueButton) continueButton.disabled = false;
          },
        })
      );
    });
  }

  function renderNetworks(assetRoutes) {
    const container = financeForm.querySelector(
      '[data-finance-networks]'
    );
    container.innerHTML = '';
    const continueButton = financeForm.querySelector(
      '[data-finance-network-continue]'
    );
    if (continueButton) continueButton.disabled = true;

    [...assetRoutes]
      .sort((a, b) => (
        a.networkGroupOrder
        - b.networkGroupOrder
      ))
      .forEach((route) => {
        const label = (
          route.networkGroupLabel
          || route.networkLabel
          || route.chain
        );
        container.appendChild(
          makeChoiceCard({
            label,
            detail: route.assetGroupLabel
              || route.assetCode,
            iconUrl: route.networkGroupIconUrl,
            iconText: label,
            onClick: (button) => {
              markChoiceSelected(container, button);
              state.route = route;
              routeInput.value = route.key;
              const continueButton = financeForm.querySelector(
                '[data-finance-network-continue]'
              );
              if (continueButton) continueButton.disabled = false;
            },
          })
        );
      });
  }

  function renderReview() {
    const pack = financeForm.querySelector(
      '[data-finance-review-pack]'
    );
    const payment = financeForm.querySelector(
      '[data-finance-review-payment]'
    );
    const route = financeForm.querySelector(
      '[data-finance-review-route]'
    );
    const routeRow = financeForm.querySelector(
      '[data-finance-review-route-row]'
    );
    const price = financeForm.querySelector(
      '[data-finance-review-price]'
    );

    if (pack) pack.textContent = state.packLabel || '—';
    if (payment) {
      payment.textContent = (
        state.method ? state.method.label : '—'
      );
    }
    if (price) {
      price.textContent = state.packPriceLabel || '—';
    }

    const requiresRoute = Boolean(
      state.method
      && state.method.routes.some(
        (item) => (
          item.paymentRequiresRouteSelection
          || item.paymentMethodType === 'crypto'
        )
      )
    );
    if (routeRow) routeRow.hidden = !requiresRoute;
    if (route) {
      route.textContent = state.route
        ? [
            (
              state.route.assetGroupLabel
              || state.route.assetCode
            ),
            (
              state.route.networkGroupLabel
              || state.route.networkLabel
            ),
          ].filter(Boolean).join(' · ')
        : '—';
    }

    const chosenRoute = state.route;
    if (
      chosenRoute
      && (
        chosenRoute.paymentOpenNewTab
        || chosenRoute.paymentMethodType === 'provider'
      )
    ) {
      financeForm.setAttribute('target', '_blank');
    } else {
      financeForm.removeAttribute('target');
    }
  }

  function selectFinancePack(input) {
    if (!input) return;

    state.packCode = (
      input.getAttribute('data-pack-code')
      || ''
    );
    state.packLabel = (
      input.getAttribute('data-pack-label')
      || ''
    );
    state.packPriceLabel = (
      input.getAttribute('data-pack-price-label')
      || ''
    );
    state.packGrossCanonical = Number(
      input.getAttribute(
        'data-pack-gross-canonical'
      ) || 0
    );
    packInput.value = state.packCode;

    state.method = null;
    state.assetKey = '';
    state.asset = null;
    state.route = null;
    routeInput.value = '';
    setRouteIndicatorsVisible(false);
  }

  const financePackInputs = Array.from(
    financeForm.querySelectorAll(
      '[data-finance-pack]'
    )
  );

  financePackInputs.forEach((input) => {
    input.addEventListener('change', () => {
      if (input.checked) {
        selectFinancePack(input);
      }
    });
  });

  const initialFinancePack = financePackInputs.find(
    (input) => input.checked
  ) || financePackInputs[0];

  if (initialFinancePack) {
    initialFinancePack.checked = true;
    selectFinancePack(initialFinancePack);
  }

  const financePackContinue = financeForm.querySelector(
    '[data-finance-pack-continue]'
  );
  if (financePackContinue) {
    financePackContinue.addEventListener('click', () => {
      if (!state.packCode) return;
      renderPaymentMethods();
      showStep(2);
    });
  }

  const financePaymentContinue = financeForm.querySelector(
    '[data-finance-payment-continue]'
  );
  if (financePaymentContinue) {
    financePaymentContinue.addEventListener('click', () => {
      if (!state.method) return;

      const requiresRoute = state.method.routes.some(
        (route) => (
          route.paymentRequiresRouteSelection
          || route.paymentMethodType === 'crypto'
        )
      );

      if (requiresRoute) {
        renderAssets();
        showStep(3);
        return;
      }

      state.route = state.method.routes[0] || null;
      if (!state.route) return;
      routeInput.value = state.route.key;
      state.reviewBackStep = 2;
      renderReview();
      showStep(5);
    });
  }

  const financeAssetContinue = financeForm.querySelector(
    '[data-finance-asset-continue]'
  );
  if (financeAssetContinue) {
    financeAssetContinue.addEventListener('click', () => {
      if (!state.asset) return;
      renderNetworks(state.asset.routes);
      showStep(4);
    });
  }

  const financeNetworkContinue = financeForm.querySelector(
    '[data-finance-network-continue]'
  );
  if (financeNetworkContinue) {
    financeNetworkContinue.addEventListener('click', () => {
      if (!state.route) return;
      state.reviewBackStep = 4;
      renderReview();
      showStep(5);
    });
  }

  financeForm.querySelectorAll(
    '[data-finance-back]'
  ).forEach((button) => {
    button.addEventListener('click', () => {
      showStep(Number(
        button.getAttribute('data-finance-back')
      ));
    });
  });

  const reviewBack = financeForm.querySelector(
    '[data-finance-review-back]'
  );
  if (reviewBack) {
    reviewBack.addEventListener('click', () => {
      showStep(state.reviewBackStep);
    });
  }

  financeForm.addEventListener('submit', (event) => {
    if (!state.packCode || !state.route) {
      event.preventDefault();
      return;
    }
    packInput.value = state.packCode;
    routeInput.value = state.route.key;
  });

  setRouteIndicatorsVisible(false);
  showStep(1);
})();
