(function () {
  const root = document.querySelector('[data-wallet-ui]');
  if (!root) {
    return;
  }

  // public-wallet-preview-v1
  const requiresAuthentication =
    root.getAttribute('data-wallet-auth-required') === 'true';
  const walletLoginUrl =
    root.getAttribute('data-wallet-login-url') || '';

  if (requiresAuthentication && walletLoginUrl) {
    const redirectGuestToLogin = function (event) {
      event.preventDefault();
      event.stopPropagation();
      if (typeof event.stopImmediatePropagation === 'function') {
        event.stopImmediatePropagation();
      }
      window.location.assign(walletLoginUrl);
    };

    document.addEventListener('click', function (event) {
      const eventTarget = event.target;
      const interactive = eventTarget && eventTarget.closest
        ? eventTarget.closest(
          'button, a, input, select, textarea, label'
        )
        : null;

      if (!interactive) {
        return;
      }
      if (interactive.closest('[data-wallet-guest-allowed]')) {
        return;
      }
      if (
        !interactive.closest('[data-wallet-ui]') &&
        !interactive.closest('.wallet-modal')
      ) {
        return;
      }

      redirectGuestToLogin(event);
    }, true);

    document.addEventListener('submit', function (event) {
      const form = event.target;
      if (
        !form ||
        !form.closest ||
        (
          !form.closest('[data-wallet-ui]') &&
          !form.closest('.wallet-modal')
        )
      ) {
        return;
      }
      redirectGuestToLogin(event);
    }, true);
  }

  // wallet-dashboard-view-switching-v2
  function inferDashboardViewFromUrl() {
    const url = new URL(window.location.href);
    if (url.searchParams.get('view') === 'activity') {
      return 'activity';
    }
    if (url.searchParams.has('tab') || url.searchParams.has('status') || url.searchParams.has('page')) {
      return 'activity';
    }
    return 'home';
  }

  function setDashboardView(value, updateHistory) {
    const view = value === 'activity' ? 'activity' : 'home';
    document.querySelectorAll('[data-wallet-dashboard-view]').forEach(function (node) {
      node.hidden = node.getAttribute('data-wallet-dashboard-view') !== view;
    });
    if (updateHistory) {
      const url = new URL(window.location.href);
      if (view === 'activity') {
        url.searchParams.set('view', 'activity');
      } else {
        ['view', 'tab', 'status', 'page'].forEach(function (key) { url.searchParams.delete(key); });
      }
      window.history.pushState({ walletDashboardView: view }, '', url);
    }
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  setDashboardView(
    requiresAuthentication ? 'home' : inferDashboardViewFromUrl(),
    false
  );
  window.addEventListener('popstate', function () {
    setDashboardView(
      requiresAuthentication ? 'home' : inferDashboardViewFromUrl(),
      false
    );
  });

  function getModal(name) {
    return document.querySelector('.wallet-modal[data-wallet-modal="' + name + '"]');
  }

  function syncLock() {
    const anyOpen = Array.from(document.querySelectorAll('.wallet-modal[data-wallet-modal]')).some(function (node) {
      return !node.hidden;
    });

    document.documentElement.classList.toggle('wallet-modal-open', anyOpen);
    document.body.classList.toggle('wallet-modal-open', anyOpen);
  }

  const buyState = {
    step: 1,
    packCode: '',
    packLabel: '',
    packGrossCanonical: 0,
    paymentMethodKey: '',
    paymentMethodLabel: '',
    paymentMethodType: '',
    providerKey: '',
    paymentOpenNewTab: false,
    assetKey: '',
    routeKey: '',
  };

  const p2pPriceCache = new Map();
  const p2pPriceRequests = new Map();

  function getBuyForm() {
    return document.querySelector('[data-wallet-buy-form]');
  }

  function syncBuyFormTarget() {
    const form = getBuyForm();
    if (!form) {
      return;
    }

    if (buyState.paymentOpenNewTab) {
      form.setAttribute('target', '_blank');
    } else {
      form.removeAttribute('target');
    }
  }

  function escapeHtml(value) {
    return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  function formatCanonicalStableAmount(value) {
    const cents = Math.round(Number(value || 0) / 10000);
    return (cents / 100).toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: 2,
    });
  }

  function renderChoiceIcon(iconPath, fallbackLabel, className) {
    if (iconPath) {
      return '<img class="' + className + '" src="' + escapeHtml(iconPath) + '" alt="">';
    }

    return '<span class="wallet-buy-flow__choice-icon-fallback">' + escapeHtml(fallbackLabel || '') + '</span>';
  }

  function getMethodDefinitions() {
    const form = getBuyForm();
    if (!form) {
      return [];
    }

    return Array.from(form.querySelectorAll('[data-wallet-checkout-method]')).map(function (node) {
      return {
        key: node.getAttribute('data-method-key') || '',
        label: node.getAttribute('data-method-label') || '',
        subtitle: node.getAttribute('data-method-subtitle') || '',
        icon: node.getAttribute('data-method-icon') || '',
        iconPath: node.getAttribute('data-method-icon-path') || '',
        order: Number(node.getAttribute('data-method-order') || 100),
      };
    }).sort(function (a, b) {
      if (a.order !== b.order) {
        return a.order - b.order;
      }
      return String(a.label || '').localeCompare(String(b.label || ''));
    });
  }

  function getRouteOptions() {
    const form = getBuyForm();
    if (!form) {
      return [];
    }

    return Array.from(form.querySelectorAll('[data-wallet-route-option]')).map(function (node) {
      const assetCode = node.getAttribute('data-asset-code') || '';
      const assetGroupKey = node.getAttribute('data-asset-group-key') || assetCode;
      const checkoutMethodKeys = String(
        node.getAttribute('data-checkout-method-keys') || ''
      ).split(/\s+/).filter(Boolean);

      return {
        key: node.getAttribute('data-option-key') || '',
        paymentMethodKey: node.getAttribute('data-payment-method-key') || '',
        paymentMethodLabel: node.getAttribute('data-payment-method-label') || '',
        paymentMethodType: node.getAttribute('data-payment-method-type') || '',
        paymentGroupKey: node.getAttribute('data-payment-group-key') || '',
        paymentGroupLabel: node.getAttribute('data-payment-group-label') || '',
        paymentGroupIcon: node.getAttribute('data-payment-group-icon') || '',
        paymentGroupIconPath: node.getAttribute('data-payment-group-icon-path') || '',
        checkoutMethodKeys: checkoutMethodKeys,
        checkoutProviderKey: node.getAttribute('data-checkout-provider-key') || '',
        checkoutProviderLabel: node.getAttribute('data-checkout-provider-label') || '',
        checkoutProviderOrder: Number(node.getAttribute('data-checkout-provider-order') || 100),
        checkoutProviderOptionLabel: node.getAttribute('data-checkout-provider-option-label') || '',
        paymentPriceFixedCanonical: Number(node.getAttribute('data-payment-price-fixed-canonical') || 0),
        paymentPriceBps: Number(node.getAttribute('data-payment-price-bps') || 0),
        paymentCurrency: node.getAttribute('data-payment-currency') || 'USD',
        paymentCurrencySymbol: node.getAttribute('data-payment-currency-symbol') || '$',
        paymentCurrencyUsdRate: Number(node.getAttribute('data-payment-currency-usd-rate') || 1),
        paymentOpenNewTab:
          node.getAttribute('data-payment-open-new-tab') === 'true',
        paymentPriceMode: node.getAttribute('data-payment-price-mode') || 'fixed',
        assetCode: assetCode,
        assetGroupKey: assetGroupKey,
        assetGroupLabel: node.getAttribute('data-asset-group-label') || assetGroupKey,
        assetGroupIconPath: node.getAttribute('data-asset-group-icon-path') || '',
        assetGroupOrder: Number(node.getAttribute('data-asset-group-order') || 100),
        chain: node.getAttribute('data-chain') || '',
        networkLabel: node.getAttribute('data-network-label') || '',
        networkGroupKey: node.getAttribute('data-network-group-key') || node.getAttribute('data-chain') || '',
        networkGroupLabel: node.getAttribute('data-network-group-label') || node.getAttribute('data-network-label') || '',
        networkGroupIconPath: node.getAttribute('data-network-group-icon-path') || '',
        networkGroupOrder: Number(node.getAttribute('data-network-group-order') || 100),
        minAmountCanonical: Number(
          node.getAttribute('data-min-amount-canonical') || 0
        ),
      };
    });
  }

  function getAdjustedCanonicalAmountForOption(option) {
    const base = Number(buyState.packGrossCanonical || 0);
    const bps = Number((option && option.paymentPriceBps) || 0);
    const fixed = Number(
      (option && option.paymentPriceFixedCanonical) || 0
    );

    return base + fixed + Math.round(base * bps / 10000);
  }

  function routeSupportsSelectedPack(option) {
    const minimum = Number(option.minAmountCanonical || 0);
    if (!Number.isFinite(minimum) || minimum <= 0) {
      return true;
    }

    return getAdjustedCanonicalAmountForOption(option) >= minimum;
  }

  function getAvailableRouteOptions() {
    return getRouteOptions().filter(routeSupportsSelectedPack);
  }

  function getRoutesForPaymentMethod(paymentMethodKey) {
    return getAvailableRouteOptions().filter(function (option) {
      return option.checkoutMethodKeys.indexOf(paymentMethodKey) !== -1;
    });
  }

  function getPaymentMethods() {
    return getMethodDefinitions().map(function (definition) {
      const routes = getRoutesForPaymentMethod(definition.key);
      if (!routes.length) {
        return null;
      }
      const providerKeys = new Set();
      const assetKeys = new Set();
      routes.forEach(function (route) {
        if (route.checkoutProviderKey) {
          providerKeys.add(route.checkoutProviderKey);
        }
        if (route.assetGroupKey || route.assetCode) {
          assetKeys.add(route.assetGroupKey || route.assetCode);
        }
      });
      return {
        key: definition.key,
        label: definition.label,
        subtitle: definition.subtitle,
        icon: definition.icon,
        iconPath: definition.iconPath,
        order: definition.order,
        type: routes.every(function (route) {
          return route.paymentMethodType === 'crypto';
        }) ? 'crypto' : 'provider',
        providerCount: providerKeys.size,
        assetCount: assetKeys.size,
        routes: routes,
      };
    }).filter(Boolean);
  }

  function getRouteByKey(routeKey) {
    return getAvailableRouteOptions().find(function (option) {
      return option.key === routeKey;
    }) || null;
  }

  function getProvidersForPaymentMethod(paymentMethodKey) {
    const map = new Map();

    getRoutesForPaymentMethod(paymentMethodKey).forEach(function (option) {
      const key = option.checkoutProviderKey;
      if (!key) {
        return;
      }
      if (!map.has(key)) {
        map.set(key, {
          key: key,
          label: option.checkoutProviderLabel || key,
          order: option.checkoutProviderOrder || 100,
          routes: [],
        });
      }
      map.get(key).routes.push(option);
    });

    return Array.from(map.values()).sort(function (a, b) {
      if (a.order !== b.order) {
        return a.order - b.order;
      }
      return String(a.label || '').localeCompare(String(b.label || ''));
    });
  }

  function getRoutesForSelectedProvider() {
    return getRoutesForPaymentMethod(buyState.paymentMethodKey).filter(function (option) {
      return option.checkoutProviderKey === buyState.providerKey;
    });
  }

  function getAssetsForPaymentMethod(paymentMethodKey) {
    const map = new Map();

    getRoutesForPaymentMethod(paymentMethodKey)
      .filter(function (option) {
        return option.paymentMethodType === 'crypto';
      })
      .forEach(function (option) {
        const key = option.assetGroupKey || option.assetCode;
        if (!key) {
          return;
        }

        if (!map.has(key)) {
          map.set(key, {
            key: key,
            label: option.assetGroupLabel || option.assetCode,
            iconPath: option.assetGroupIconPath || '',
            order: option.assetGroupOrder || 100,
          });
        }
      });

    return Array.from(map.values()).sort(function (a, b) {
      if (a.order !== b.order) {
        return a.order - b.order;
      }

      return String(a.label || '').localeCompare(String(b.label || ''));
    });
  }

  function getRoutesForSelectedAsset() {
    return getRoutesForPaymentMethod(buyState.paymentMethodKey).filter(function (option) {
      return (option.assetGroupKey || option.assetCode) === buyState.assetKey;
    });
  }

  function updateStepIndicators(step) {
    document.querySelectorAll('[data-wallet-step-indicator]').forEach(function (node) {
      node.classList.toggle(
        'wallet-buy-flow__step--active',
        node.getAttribute('data-wallet-step-indicator') === String(step)
      );
    });
  }

  function updatePanels(step) {
    document.querySelectorAll('[data-wallet-step-panel]').forEach(function (node) {
      node.hidden = node.getAttribute('data-wallet-step-panel') !== String(step);
    });
  }

  function setSelectedPackFromInput(input) {
    if (!input) {
      return;
    }

    buyState.packCode = input.getAttribute('data-pack-code') || input.value || '';
    buyState.packGrossCanonical = Number(input.getAttribute('data-pack-gross-canonical') || 0);

    const tokenDisplay = input.getAttribute('data-pack-token-display') || '';
    buyState.packLabel = tokenDisplay + ' tokens';

    const hidden = document.querySelector('[data-wallet-selected-pack]');
    if (hidden) {
      hidden.value = buyState.packCode;
    }
  }

  function setSelectedPaymentMethod(method) {
    buyState.paymentMethodKey = method ? method.key : '';
    buyState.paymentMethodLabel = method ? method.label : '';
    buyState.paymentMethodType = method ? method.type : '';

    const hiddenKey = document.querySelector('[data-wallet-selected-payment-method-key]');
    const hiddenType = document.querySelector('[data-wallet-selected-payment-method-type]');
    if (hiddenKey) {
      hiddenKey.value = buyState.paymentMethodKey;
    }
    if (hiddenType) {
      hiddenType.value = buyState.paymentMethodType;
    }
  }

  function setSelectedProvider(providerKey) {
    buyState.providerKey = providerKey || '';
  }

  function setSelectedAsset(assetKey) {
    buyState.assetKey = assetKey || '';

    const hidden = document.querySelector('[data-wallet-selected-asset]');
    if (hidden) {
      hidden.value = buyState.assetKey;
    }
  }

  function syncHiddenPaymentFromRoute(route) {
    if (!route) {
      return;
    }

    const hiddenKey = document.querySelector('[data-wallet-selected-payment-method-key]');
    const hiddenType = document.querySelector('[data-wallet-selected-payment-method-type]');

    if (hiddenKey) {
      hiddenKey.value = route.paymentMethodKey || buyState.paymentMethodKey;
    }
    if (hiddenType) {
      hiddenType.value = route.paymentMethodType || buyState.paymentMethodType;
    }
  }

  function setSelectedRoute(routeKey) {
    buyState.routeKey = routeKey || '';
    const route = getRouteByKey(buyState.routeKey);
    buyState.paymentOpenNewTab = Boolean(route && route.paymentOpenNewTab);
    syncBuyFormTarget();

    const hidden = document.querySelector('[data-wallet-selected-route]');
    if (hidden) {
      hidden.value = buyState.routeKey;
    }

    syncHiddenPaymentFromRoute(route);
  }

  function resetDownstreamSelection() {
    setSelectedProvider('');
    setSelectedAsset('');
    setSelectedRoute('');
  }

  function selectDefaultAssetForPaymentMethod() {
    const assets = getAssetsForPaymentMethod(buyState.paymentMethodKey);
    setSelectedAsset(assets[0] ? assets[0].key : '');
  }

  function selectDefaultProviderForPaymentMethod() {
    const providers = getProvidersForPaymentMethod(buyState.paymentMethodKey);
    setSelectedProvider(providers[0] ? providers[0].key : '');
  }

  function selectDefaultRouteForCryptoAsset() {
    const routes = getRoutesForSelectedAsset();
    setSelectedRoute(routes[0] ? routes[0].key : '');
  }

  function selectDefaultRouteForProvider() {
    const routes = getRoutesForSelectedProvider();
    setSelectedRoute(routes[0] ? routes[0].key : '');
  }

  function getP2PPriceCacheKey(option) {
    return String(buyState.packCode || '') + '|' + String((option && option.key) || '');
  }

  function getP2PPreviewUrl() {
    const form = getBuyForm();
    return form ? (form.getAttribute('data-p2p-preview-url') || '') : '';
  }

  function setP2PExpectedTransactionAmount(value) {
    const form = getBuyForm();
    const input = form
      ? form.querySelector('[data-wallet-p2p-expected-transaction-amount]')
      : null;
    if (!input) {
      return;
    }
    const amount = Number(value || 0);
    input.value = Number.isFinite(amount) && amount > 0 ? String(Math.trunc(amount)) : '';
  }

  function requestP2PPrice(option) {
    if (!option || option.paymentPriceMode !== 'p2p_dynamic') {
      return Promise.resolve({ available: true, display: '' });
    }

    const cacheKey = getP2PPriceCacheKey(option);
    if (p2pPriceCache.has(cacheKey)) {
      return Promise.resolve(p2pPriceCache.get(cacheKey));
    }
    if (p2pPriceRequests.has(cacheKey)) {
      return p2pPriceRequests.get(cacheKey);
    }

    const previewUrl = getP2PPreviewUrl();
    if (!previewUrl || !buyState.packCode || !option.key) {
      return Promise.resolve({ available: false, display: 'Unavailable' });
    }

    const params = new URLSearchParams({
      token_pack_key: buyState.packCode,
      deposit_option_key: option.key,
    });
    const request = fetch(previewUrl + '?' + params.toString(), {
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
    })
      .then(function (response) {
        return response.json().catch(function () { return {}; }).then(function (body) {
          if (!response.ok) {
            return {
              available: false,
              display: 'Unavailable',
              detail: body.detail || 'P2P pricing is unavailable.',
            };
          }
          return {
            available: true,
            display: body.transaction_value_display || '',
            transactionAmount: Number(body.transaction_amount || 0),
          };
        });
      })
      .catch(function () {
        return {
          available: false,
          display: 'Unavailable',
          detail: 'P2P pricing is unavailable.',
        };
      })
      .then(function (result) {
        p2pPriceCache.set(cacheKey, result);
        return result;
      })
      .finally(function () {
        p2pPriceRequests.delete(cacheKey);
      });

    p2pPriceRequests.set(cacheKey, request);
    return request;
  }

  function refreshP2PPrice(option, rerender) {
    if (!option || option.paymentPriceMode !== 'p2p_dynamic') {
      return;
    }
    const cacheKey = getP2PPriceCacheKey(option);
    if (p2pPriceCache.has(cacheKey) || p2pPriceRequests.has(cacheKey)) {
      return;
    }
    const expectedPack = buyState.packCode;
    requestP2PPrice(option).then(function () {
      if (buyState.packCode === expectedPack) {
        rerender();
      }
    });
  }

  function getRoutePriceDisplay(option) {
    if (option && option.paymentPriceMode === 'p2p_dynamic') {
      const result = p2pPriceCache.get(getP2PPriceCacheKey(option));
      return result ? result.display : '…';
    }
    const base = Number(buyState.packGrossCanonical || 0);
    const bps = Number((option && option.paymentPriceBps) || 0);
    const fixed = Number((option && option.paymentPriceFixedCanonical) || 0);
    const currency = String((option && option.paymentCurrency) || 'USD').toUpperCase();
    const currencySymbol = String((option && option.paymentCurrencySymbol) || currency + ' ');
    const currencyUsdRate = Number((option && option.paymentCurrencyUsdRate) || 1);

    const percentageFee = Math.round(base * bps / 10000);
    const adjusted = base + fixed + percentageFee;
    const normalizedRate = Number.isFinite(currencyUsdRate) && currencyUsdRate > 0
      ? currencyUsdRate
      : 1;
    const fiatAmount = (adjusted / 1000000) / normalizedRate;
    const roundedFiatAmount = Math.ceil((fiatAmount * 100) - 1e-9) / 100;

    return currencySymbol + roundedFiatAmount.toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: 2,
    });
  }

  function renderPaymentMethodChoices() {
    const container = document.querySelector('[data-wallet-payment-method-choices]');
    if (!container) {
      return;
    }

    const methods = getPaymentMethods();
    let selectedMethod = methods.find(function (method) {
      return method.key === buyState.paymentMethodKey;
    });

    if (!selectedMethod) {
      selectedMethod = methods[0] || null;
      setSelectedPaymentMethod(selectedMethod);
      resetDownstreamSelection();
    }

    container.innerHTML = '';

    methods.forEach(function (method) {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'wallet-buy-flow__choice' + (
        buyState.paymentMethodKey === method.key ? ' wallet-buy-flow__choice--selected' : ''
      );
      button.setAttribute('data-wallet-payment-method-choice', method.key);

      const countText = method.type === 'crypto'
        ? method.assetCount + ' ' + (method.assetCount === 1 ? 'coin' : 'coins')
        : method.providerCount + ' ' + (method.providerCount === 1 ? 'provider' : 'providers');
      const subtitle = [countText, method.subtitle].filter(Boolean).join(' · ');

      button.innerHTML =
        '<span class="wallet-buy-flow__choice-icon">' +
          renderChoiceIcon(method.iconPath, method.icon, 'wallet-buy-flow__choice-icon-image') +
        '</span>' +
        '<span class="wallet-buy-flow__choice-copy">' +
          '<span class="wallet-buy-flow__choice-title">' + escapeHtml(method.label) + '</span>' +
          '<span class="wallet-buy-flow__choice-subtitle">' + escapeHtml(subtitle) + '</span>' +
        '</span>';

      container.appendChild(button);
    });
  }

  function renderStep3Choices() {
    const container = document.querySelector('[data-wallet-step-3-choices]');
    const title = document.querySelector('[data-wallet-step-3-title]');
    if (!container) {
      return;
    }

    const isCrypto = buyState.paymentMethodKey === 'crypto';
    if (title) {
      title.textContent = isCrypto ? 'Choose coin' : 'Choose provider';
    }
    container.innerHTML = '';

    if (isCrypto) {
      const assets = getAssetsForPaymentMethod(buyState.paymentMethodKey);
      if (!buyState.assetKey || !assets.some(function (item) { return item.key === buyState.assetKey; })) {
        setSelectedAsset(assets[0] ? assets[0].key : '');
      }
      assets.forEach(function (asset) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'wallet-buy-flow__choice' + (
          buyState.assetKey === asset.key ? ' wallet-buy-flow__choice--selected' : ''
        );
        button.setAttribute('data-wallet-asset-choice', asset.key);
        button.innerHTML =
          '<span class="wallet-buy-flow__choice-icon">' +
            renderChoiceIcon(asset.iconPath, asset.label, 'wallet-buy-flow__choice-icon-image') +
          '</span>' +
          '<span class="wallet-buy-flow__choice-copy">' +
            '<span class="wallet-buy-flow__choice-title">' + escapeHtml(asset.label) + '</span>' +
          '</span>';
        container.appendChild(button);
      });
      return;
    }

    const providers = getProvidersForPaymentMethod(buyState.paymentMethodKey);
    if (!buyState.providerKey || !providers.some(function (item) { return item.key === buyState.providerKey; })) {
      setSelectedProvider(providers[0] ? providers[0].key : '');
    }

    providers.forEach(function (provider) {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'wallet-buy-flow__choice' + (
        buyState.providerKey === provider.key ? ' wallet-buy-flow__choice--selected' : ''
      );
      button.setAttribute('data-wallet-provider-choice', provider.key);
      const hasMultipleOptions = provider.routes.length > 1;
      const subtitle = hasMultipleOptions
        ? provider.routes.length + ' options'
        : (provider.routes[0].networkLabel || provider.routes[0].paymentGroupLabel || '');
      const price = hasMultipleOptions ? '' : getRoutePriceDisplay(provider.routes[0]);
      button.innerHTML =
        '<span class="wallet-buy-flow__choice-copy">' +
          '<span class="wallet-buy-flow__choice-title">' + escapeHtml(provider.label) + '</span>' +
          (subtitle ? '<span class="wallet-buy-flow__choice-subtitle">' + escapeHtml(subtitle) + '</span>' : '') +
        '</span>' +
        (price ? '<span class="wallet-buy-flow__choice-price">' + escapeHtml(price) + '</span>' : '');
      container.appendChild(button);
      if (!hasMultipleOptions && provider.routes[0].paymentPriceMode === 'p2p_dynamic') {
        refreshP2PPrice(provider.routes[0], renderStep3Choices);
      }
    });
  }

  function renderStep4Choices() {
    const container = document.querySelector('[data-wallet-step-4-choices]');
    const title = document.querySelector('[data-wallet-step-4-title]');
    if (!container) {
      return;
    }

    const isCrypto = buyState.paymentMethodKey === 'crypto';
    const routes = isCrypto ? getRoutesForSelectedAsset() : getRoutesForSelectedProvider();
    if (!buyState.routeKey || !routes.some(function (item) { return item.key === buyState.routeKey; })) {
      setSelectedRoute(routes[0] ? routes[0].key : '');
    }

    if (title) {
      if (isCrypto) {
        title.textContent = 'Choose network';
      } else if (buyState.providerKey === 'mtpelerin') {
        title.textContent = 'Choose currency';
      } else {
        title.textContent = 'Choose option';
      }
    }

    container.innerHTML = '';
    routes.forEach(function (item) {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'wallet-buy-flow__choice' + (
        buyState.routeKey === item.key ? ' wallet-buy-flow__choice--selected' : ''
      );
      button.setAttribute('data-wallet-route-choice', item.key);

      const label = isCrypto
        ? (item.networkGroupLabel || item.networkLabel)
        : (item.checkoutProviderOptionLabel || item.paymentCurrency || item.paymentMethodLabel);
      const iconPath = isCrypto ? (item.networkGroupIconPath || '') : '';
      const price = isCrypto ? '' : getRoutePriceDisplay(item);
      button.innerHTML =
        '<span class="wallet-buy-flow__choice-icon">' +
          renderChoiceIcon(iconPath, label, 'wallet-buy-flow__choice-icon-image') +
        '</span>' +
        '<span class="wallet-buy-flow__choice-copy">' +
          '<span class="wallet-buy-flow__choice-title">' + escapeHtml(label) + '</span>' +
        '</span>' +
        (price ? '<span class="wallet-buy-flow__choice-price">' + escapeHtml(price) + '</span>' : '');
      container.appendChild(button);
      if (item.paymentPriceMode === 'p2p_dynamic') {
        refreshP2PPrice(item, renderStep4Choices);
      }
    });
  }

  function goToStep(step) {
    buyState.step = step;
    updateStepIndicators(step);
    updatePanels(step);

    if (step >= 2) {
      renderPaymentMethodChoices();
    }
    if (step >= 3) {
      renderStep3Choices();
    }
    if (step >= 4) {
      renderStep4Choices();
    }
  }

  function resetBuyFlow() {
    const checkedPack = document.querySelector('input[name="token_pack_choice"]:checked');
    setSelectedPackFromInput(checkedPack);

    const methods = getPaymentMethods();
    setSelectedPaymentMethod(methods[0] || null);
    resetDownstreamSelection();
    if (buyState.paymentMethodKey === 'crypto') {
      selectDefaultAssetForPaymentMethod();
      selectDefaultRouteForCryptoAsset();
    } else {
      selectDefaultProviderForPaymentMethod();
      selectDefaultRouteForProvider();
    }

    renderPaymentMethodChoices();
    renderStep3Choices();
    renderStep4Choices();
    goToStep(1);
  }

  function openModal(name) {
    document.querySelectorAll('.wallet-modal[data-wallet-modal]').forEach(function (node) {
      node.hidden = true;
    });

    const modal = getModal(name);
    if (!modal) {
      return;
    }

    modal.hidden = false;
    if (name === 'deposit') {
      resetBuyFlow();
    }
    syncLock();
  }

  function closeModal(name) {
    const modal = getModal(name);
    if (!modal) {
      return;
    }

    modal.hidden = true;
    syncLock();
  }


  // reward-chest-preview-v2
  const rewardChestCatalogNode = document.getElementById(
    'wallet-reward-chest-catalog'
  );
  let rewardChestCatalog = new Map();

  if (rewardChestCatalogNode) {
    try {
      const parsedCatalog = JSON.parse(
        rewardChestCatalogNode.textContent || '[]'
      );
      rewardChestCatalog = new Map(
        parsedCatalog
          .filter(function (row) {
            return row && row.key;
          })
          .map(function (row) {
            return [row.key, row];
          })
      );
    } catch (error) {
      rewardChestCatalog = new Map();
    }
  }

  function normalizeRewardPreviewRarity(value) {
    const rarity = String(value || 'common').toLowerCase();
    return rarity === 'jackpot' ? 'legendary' : rarity;
  }



  function openRewardChestPreview(chestKey) {
    const chest = rewardChestCatalog.get(chestKey);
    const modal = getModal('reward-chest-preview');

    if (!chest || !modal) {
      return;
    }

    const title = modal.querySelector(
      '[data-reward-chest-modal-title]'
    );
    const drops = modal.querySelector(
      '[data-reward-chest-modal-drops]'
    );

    if (!title || !drops) {
      return;
    }

    title.textContent = chest.label || 'Reward Chest';
    drops.innerHTML = '';

    const rarityRanks = {
      common: 1,
      uncommon: 2,
      rare: 3,
      epic: 4,
      legendary: 5,
    };
    const previewDrops = Array.isArray(chest.drops)
      ? chest.drops.slice().sort(function (left, right) {
        const leftRarity = normalizeRewardPreviewRarity(
          left && left.rarity
        );
        const rightRarity = normalizeRewardPreviewRarity(
          right && right.rarity
        );

        return (
          (rarityRanks[rightRarity] || 0) -
          (rarityRanks[leftRarity] || 0)
        );
      })
      : [];

    previewDrops.forEach(function (drop, index) {
      const rarity = normalizeRewardPreviewRarity(
        drop.rarity
      );
      const fullLabel = String(drop.label || '');
      const amountLabel = fullLabel.replace(
        /\s+tokens?$/i,
        ''
      );

      const row = document.createElement('li');
      row.className = 'wallet-reward-chest-modal__drop';
      row.setAttribute('data-rarity', rarity);
      row.setAttribute('aria-label', fullLabel);
      row.style.setProperty('--drop-order', String(index));

      row.innerHTML =
        '<span class="wallet-reward-chest-modal__badge-wrap">' +
          '<img class="wallet-reward-chest-modal__badge" src="' +
            escapeHtml(drop.rarity_image_url || '') +
            '" alt="">' +
        '</span>' +
        '<span class="wallet-reward-chest-modal__drop-copy">' +
          '<span class="wallet-reward-chest-modal__rarity">' +
            escapeHtml(
              drop.rarity_label ||
              rarity.toUpperCase()
            ) +
          '</span>' +
          '<span class="wallet-reward-chest-modal__value">' +
            '<strong class="wallet-reward-chest-modal__amount">' +
              escapeHtml(amountLabel) +
            '</strong>' +
            '<span class="wallet-reward-chest-modal__unit">' +
              'TOKENS' +
            '</span>' +
          '</span>' +
        '</span>' +
        '<span class="wallet-reward-chest-modal__art-frame">' +
          '<img class="wallet-reward-chest-modal__art" src="' +
            escapeHtml(drop.image_url || '') +
            '" alt="">' +
        '</span>';

      drops.appendChild(row);
    });

    openModal('reward-chest-preview');
  }


  // interactive-chest-opening-v2
  const chestOpeningOverlay = document.querySelector(
    '[data-wallet-chest-opening]'
  );
  const chestOpeningTrigger = chestOpeningOverlay
    ? chestOpeningOverlay.querySelector(
      '[data-wallet-chest-opening-trigger]'
    )
    : null;
  const chestOpeningCollect = chestOpeningOverlay
    ? chestOpeningOverlay.querySelector(
      '[data-wallet-chest-opening-collect]'
    )
    : null;
  const chestOpeningBackdrop = chestOpeningOverlay
    ? chestOpeningOverlay.querySelector(
      '.wallet-chest-opening__backdrop'
    )
    : null;
  let chestOpeningTimers = [];
  let chestOpeningRarity = 'reward';
  let chestOpeningPending = null;

  function clearChestOpeningTimers() {
    chestOpeningTimers.forEach(function (timerId) {
      window.clearTimeout(timerId);
    });
    chestOpeningTimers = [];
  }

  function setChestOpeningText(selector, value) {
    if (!chestOpeningOverlay) {
      return;
    }
    const node = chestOpeningOverlay.querySelector(selector);
    if (node) {
      node.textContent = value;
    }
  }

  function setChestOpeningImage(selector, value) {
    if (!chestOpeningOverlay) {
      return;
    }
    const node = chestOpeningOverlay.querySelector(selector);
    if (node) {
      node.src = value || '';
    }
  }

  function resetChestOpeningOutcome() {
    chestOpeningRarity = 'reward';
    setChestOpeningText(
      '[data-wallet-chest-opening-source]',
      ''
    );
    setChestOpeningText(
      '[data-wallet-chest-opening-amount]',
      '0'
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-drop-image]',
      ''
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-rarity-image]',
      ''
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-token-icon]',
      ''
    );
  }

  function applyChestOpeningResult(opening) {
    chestOpeningRarity = String(
      opening.rarity || 'reward'
    ).toLowerCase();

    setChestOpeningText(
      '[data-wallet-chest-opening-source]',
      opening.rarity_label ||
        String(opening.rarity || 'reward').toUpperCase()
    );
    setChestOpeningText(
      '[data-wallet-chest-opening-amount]',
      opening.amount_display ||
        String(opening.amount_tokens || 0)
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-closed]',
      opening.closed_image_url
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-opened]',
      opening.opened_image_url
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-drop-image]',
      opening.drop_image_url
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-rarity-image]',
      opening.rarity_image_url
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-token-icon]',
      opening.token_icon_url
    );
  }

  function showChestOpening(opening, form) {
    if (
      !chestOpeningOverlay ||
      !opening ||
      opening.pending !== true ||
      !opening.grant_public_id ||
      !form
    ) {
      window.location.reload();
      return;
    }

    clearChestOpeningTimers();
    chestOpeningPending = {
      form: form,
      grantPublicId: String(opening.grant_public_id),
    };

    chestOpeningOverlay.hidden = false;
    chestOpeningOverlay.setAttribute('aria-hidden', 'false');
    chestOpeningOverlay.setAttribute('data-state', 'closed');
    chestOpeningOverlay.setAttribute('data-rarity', 'mystery');

    resetChestOpeningOutcome();
    setChestOpeningText(
      '[data-wallet-chest-opening-hint]',
      'Tap the chest to open'
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-closed]',
      opening.closed_image_url
    );
    setChestOpeningImage(
      '[data-wallet-chest-opening-opened]',
      opening.opened_image_url
    );

    if (chestOpeningCollect) {
      chestOpeningCollect.classList.remove(
        'wallet-chest-opening__collect--visible'
      );
      chestOpeningCollect.setAttribute('aria-hidden', 'true');
    }
    if (chestOpeningTrigger) {
      chestOpeningTrigger.disabled = false;
    }

    document.documentElement.classList.add(
      'wallet-chest-opening-active'
    );
    document.body.classList.add(
      'wallet-chest-opening-active'
    );
  }

  function scheduleChestOpeningReveal(elapsedMs) {
    const openedDelay = Math.max(0, 620 - elapsedMs);

    chestOpeningTimers.push(window.setTimeout(function () {
      chestOpeningOverlay.setAttribute(
        'data-rarity',
        chestOpeningRarity
      );
      chestOpeningOverlay.setAttribute('data-state', 'opened');
    }, openedDelay));

    chestOpeningTimers.push(window.setTimeout(function () {
      chestOpeningOverlay.setAttribute('data-state', 'revealed');
      setChestOpeningText(
        '[data-wallet-chest-opening-hint]',
        ''
      );
    }, openedDelay + 310));

    chestOpeningTimers.push(window.setTimeout(function () {
      if (chestOpeningCollect) {
        chestOpeningCollect.classList.add(
          'wallet-chest-opening__collect--visible'
        );
        chestOpeningCollect.setAttribute('aria-hidden', 'false');
      }
    }, openedDelay + 800));
  }

  async function confirmChestOpening() {
    if (
      !chestOpeningOverlay ||
      !chestOpeningPending ||
      chestOpeningOverlay.getAttribute('data-state') !== 'closed'
    ) {
      return;
    }

    const pending = chestOpeningPending;
    const startedAt = Date.now();

    chestOpeningOverlay.setAttribute('data-state', 'opening');
    setChestOpeningText(
      '[data-wallet-chest-opening-hint]',
      'Opening...'
    );
    if (chestOpeningTrigger) {
      chestOpeningTrigger.disabled = true;
    }

    try {
      const formData = new FormData(pending.form);
      formData.set('confirm_open', '1');
      formData.set('grant_public_id', pending.grantPublicId);

      const response = await window.fetch(pending.form.action, {
        method: 'POST',
        body: formData,
        credentials: 'same-origin',
        headers: {
          'Accept': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
        },
      });

      let payload = null;
      try {
        payload = await response.json();
      } catch (error) {
        payload = null;
      }

      if (!response.ok || !payload || !payload.ok) {
        throw new Error(
          payload && payload.error
            ? payload.error
            : 'Could not open the Reward Chest.'
        );
      }
      if (!payload.opening || payload.opening.pending === true) {
        throw new Error('The Reward Chest result is missing.');
      }

      applyChestOpeningResult(payload.opening);
      chestOpeningPending = null;
      scheduleChestOpeningReveal(Date.now() - startedAt);
    } catch (error) {
      chestOpeningOverlay.setAttribute('data-state', 'closed');
      chestOpeningOverlay.setAttribute('data-rarity', 'mystery');
      setChestOpeningText(
        '[data-wallet-chest-opening-hint]',
        'Tap the chest to try again'
      );
      if (chestOpeningTrigger) {
        chestOpeningTrigger.disabled = false;
      }
      window.alert(
        error && error.message
          ? error.message
          : 'Could not open the Reward Chest.'
      );
    }
  }

  function finishChestOpening() {
    window.location.reload();
  }

  async function submitChestClaimForm(form) {
    if (
      !form ||
      form.getAttribute('data-wallet-chest-submitting') === 'true'
    ) {
      return;
    }

    form.setAttribute('data-wallet-chest-submitting', 'true');
    const submitButton = form.querySelector(
      'button[type="submit"], input[type="submit"]'
    );
    if (submitButton) {
      submitButton.disabled = true;
    }

    try {
      const response = await window.fetch(form.action, {
        method: 'POST',
        body: new FormData(form),
        credentials: 'same-origin',
        headers: {
          'Accept': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
        },
      });

      let payload = null;
      try {
        payload = await response.json();
      } catch (error) {
        payload = null;
      }

      if (!response.ok || !payload || !payload.ok) {
        throw new Error(
          payload && payload.error
            ? payload.error
            : 'Could not prepare the Reward Chest.'
        );
      }

      if (payload.reload) {
        window.location.reload();
        return;
      }
      if (!payload.opening || payload.opening.pending !== true) {
        throw new Error('Could not prepare the Reward Chest.');
      }

      showChestOpening(payload.opening, form);
    } catch (error) {
      form.removeAttribute('data-wallet-chest-submitting');
      if (submitButton) {
        submitButton.disabled = false;
      }
      window.alert(
        error && error.message
          ? error.message
          : 'Could not prepare the Reward Chest.'
      );
    }
  }

  document.addEventListener('submit', function (event) {
    const form = event.target && event.target.closest
      ? event.target.closest('[data-wallet-chest-claim-form]')
      : null;

    if (!form || requiresAuthentication) {
      return;
    }

    event.preventDefault();
    submitChestClaimForm(form);
  });

  if (chestOpeningTrigger) {
    chestOpeningTrigger.addEventListener(
      'click',
      confirmChestOpening
    );
  }

  if (chestOpeningCollect) {
    chestOpeningCollect.addEventListener(
      'click',
      finishChestOpening
    );
  }

  if (chestOpeningBackdrop) {
    chestOpeningBackdrop.addEventListener('click', function () {
      if (
        chestOpeningOverlay &&
        chestOpeningOverlay.getAttribute('data-state') === 'revealed'
      ) {
        finishChestOpening();
      }
    });
  }

  function getReferralUrl() {
    const module = document.querySelector('[data-wallet-module="referral"]');
    return module ? module.getAttribute('data-referral-url') || '' : '';
  }

  function flashReferralLabel(button, text) {
    const label = button.querySelector('[data-wallet-referral-label]');
    if (!label) {
      return;
    }
    const original = label.textContent;
    label.textContent = text;
    window.setTimeout(function () {
      label.textContent = original;
    }, 1600);
  }

  function copyReferralUrl(button) {
    const url = getReferralUrl();
    if (!url) {
      return;
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(url).then(function () {
        flashReferralLabel(button, 'Copied');
      });
      return;
    }

    const input = document.createElement('textarea');
    input.value = url;
    input.setAttribute('readonly', '');
    input.style.position = 'fixed';
    input.style.opacity = '0';
    document.body.appendChild(input);
    input.select();
    document.execCommand('copy');
    document.body.removeChild(input);
    flashReferralLabel(button, 'Copied');
  }

  document.addEventListener('click', function (event) {
    const referralCopy = event.target.closest('[data-wallet-referral-copy]');
    const chestInfoButton = event.target.closest(
      '[data-reward-chest-trigger]'
    );
    if (chestInfoButton) {
      event.preventDefault();
      event.stopPropagation();
      openRewardChestPreview(
        chestInfoButton.getAttribute(
          'data-reward-chest-trigger'
        ) || ''
      );
      return;
    }

    const clickableChest = event.target.closest(
      '.wallet-game-reward[data-reward-chest], ' +
      '[data-reward-chest-click]'
    );
    if (clickableChest) {
      const nestedControl = event.target.closest(
        'a, button, input, select, textarea, label, form'
      );

      if (!nestedControl) {
        event.preventDefault();
        openRewardChestPreview(
          clickableChest.getAttribute('data-reward-chest') ||
          clickableChest.getAttribute('data-reward-chest-click') ||
          ''
        );
        return;
      }
    }

    if (referralCopy) {
      event.preventDefault();
      copyReferralUrl(referralCopy);
      return;
    }

    const referralShare = event.target.closest('[data-wallet-referral-share]');
    if (referralShare) {
      event.preventDefault();
      const url = getReferralUrl();
      if (!url) {
        return;
      }
      if (navigator.share) {
        navigator.share({
          title: 'Join me',
          text: 'Join using my referral link.',
          url: url,
        }).catch(function () {});
      } else {
        copyReferralUrl(referralShare);
      }
      return;
    }

    const dashboardViewButton = event.target.closest('[data-wallet-dashboard-open]');
    if (dashboardViewButton) {
      event.preventDefault();
      setDashboardView(dashboardViewButton.getAttribute('data-wallet-dashboard-open') || 'home', true);
      return;
    }

    const scrollButton = event.target.closest('[data-wallet-scroll-to]');
    if (scrollButton) {
      event.preventDefault();
      const target = document.getElementById(scrollButton.getAttribute('data-wallet-scroll-to'));
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
      return;
    }

    const openButton = event.target.closest('[data-wallet-open]');
    if (openButton) {
      event.preventDefault();
      openModal(openButton.getAttribute('data-wallet-open'));
      return;
    }

    const closeButton = event.target.closest('[data-wallet-close]');
    if (closeButton) {
      event.preventDefault();
      closeModal(closeButton.getAttribute('data-wallet-close'));
      return;
    }

    const nextButton = event.target.closest('[data-wallet-next-step]');
    if (nextButton) {
      event.preventDefault();

      const step = nextButton.getAttribute('data-wallet-next-step');

      if (step === '2') {
        const checkedPack = document.querySelector('input[name="token_pack_choice"]:checked');
        setSelectedPackFromInput(checkedPack);
        renderPaymentMethodChoices();
        goToStep(2);
        return;
      }

      if (step === '3') {
        const selectedMethod = getPaymentMethods().find(function (item) {
          return item.key === buyState.paymentMethodKey;
        });
        if (!selectedMethod) {
          renderPaymentMethodChoices();
          return;
        }

        resetDownstreamSelection();
        if (selectedMethod.type === 'crypto') {
          selectDefaultAssetForPaymentMethod();
          selectDefaultRouteForCryptoAsset();
        } else {
          selectDefaultProviderForPaymentMethod();
          selectDefaultRouteForProvider();
        }
        renderStep3Choices();
        goToStep(3);
        return;
      }

      if (step === '4') {
        if (buyState.paymentMethodKey === 'crypto') {
          const routes = getRoutesForSelectedAsset();
          if (!routes.length) {
            return;
          }
          selectDefaultRouteForCryptoAsset();
          renderStep4Choices();
          goToStep(4);
          return;
        }

        const routes = getRoutesForSelectedProvider();
        if (!routes.length) {
          return;
        }
        if (routes.length === 1) {
          setSelectedRoute(routes[0].key);
          const form = getBuyForm();
          if (!form) {
            return;
          }

          if (routes[0].paymentPriceMode === 'p2p_dynamic') {
            const expectedRoute = routes[0].key;
            const expectedPack = buyState.packCode;
            requestP2PPrice(routes[0]).then(function (result) {
              if (
                !result.available ||
                buyState.routeKey !== expectedRoute ||
                buyState.packCode !== expectedPack
              ) {
                renderStep3Choices();
                return;
              }
              setP2PExpectedTransactionAmount(result.transactionAmount);
              syncBuyFormTarget();
              form.submit();
            });
            return;
          }

          syncBuyFormTarget();
          form.submit();
          if (buyState.paymentOpenNewTab) {
            closeModal('deposit');
          }
          return;
        }

        selectDefaultRouteForProvider();
        renderStep4Choices();
        goToStep(4);
        return;
      }

      return;
    }

    const prevButton = event.target.closest('[data-wallet-prev-step]');
    if (prevButton) {
      event.preventDefault();
      goToStep(Number(prevButton.getAttribute('data-wallet-prev-step')));
      return;
    }

    const paymentMethodChoice = event.target.closest('[data-wallet-payment-method-choice]');
    if (paymentMethodChoice) {
      event.preventDefault();

      const methodKey = paymentMethodChoice.getAttribute('data-wallet-payment-method-choice');
      const selectedMethod = getPaymentMethods().find(function (item) {
        return item.key === methodKey;
      });
      setSelectedPaymentMethod(selectedMethod || null);
      resetDownstreamSelection();
      renderPaymentMethodChoices();
      return;
    }

    const providerChoice = event.target.closest('[data-wallet-provider-choice]');
    if (providerChoice) {
      event.preventDefault();
      setSelectedProvider(providerChoice.getAttribute('data-wallet-provider-choice'));
      selectDefaultRouteForProvider();
      renderStep3Choices();
      return;
    }

    const assetChoice = event.target.closest('[data-wallet-asset-choice]');
    if (assetChoice) {
      event.preventDefault();
      setSelectedAsset(assetChoice.getAttribute('data-wallet-asset-choice'));
      selectDefaultRouteForCryptoAsset();
      renderStep3Choices();
      return;
    }

    const routeChoice = event.target.closest('[data-wallet-route-choice]');
    if (routeChoice) {
      event.preventDefault();
      setSelectedRoute(routeChoice.getAttribute('data-wallet-route-choice'));
      renderStep4Choices();
    }
  });

  const buyForm = getBuyForm();
  if (buyForm) {
    buyForm.addEventListener('submit', function (event) {
      const route = getRouteByKey(buyState.routeKey);
      if (route && route.paymentPriceMode === 'p2p_dynamic') {
        const cacheKey = getP2PPriceCacheKey(route);
        const cached = p2pPriceCache.get(cacheKey);
        if (!cached || !cached.available || !cached.transactionAmount) {
          event.preventDefault();
          requestP2PPrice(route).then(function (result) {
            if (!result.available || !result.transactionAmount) {
              renderStep4Choices();
              return;
            }
            setP2PExpectedTransactionAmount(result.transactionAmount);
            buyForm.requestSubmit();
          });
          return;
        }
        setP2PExpectedTransactionAmount(cached.transactionAmount);
      } else {
        setP2PExpectedTransactionAmount(0);
      }

      syncBuyFormTarget();
      if (buyState.paymentOpenNewTab) {
        window.setTimeout(function () {
          closeModal('deposit');
        }, 0);
      }
    });
  }

  document.addEventListener('change', function (event) {
    if (!event.target.matches('input[name="token_pack_choice"]')) {
      return;
    }

    setSelectedPackFromInput(event.target);
    renderPaymentMethodChoices();
  });

  document.addEventListener('keydown', function (event) {
    if (event.key !== 'Escape') {
      return;
    }

    const openModalNode = document.querySelector('.wallet-modal[data-wallet-modal]:not([hidden])');
    if (!openModalNode) {
      return;
    }

    closeModal(openModalNode.getAttribute('data-wallet-modal'));
  });

  const withdrawForm = document.querySelector('[data-wallet-withdraw-form]');
  if (withdrawForm) {
    const amountInput = withdrawForm.querySelector('[data-wallet-withdraw-amount]');
    const assetSelect = withdrawForm.querySelector('[data-wallet-withdraw-asset]');
    const networkSelect = withdrawForm.querySelector('[data-wallet-withdraw-network]');
    const percentButtons = Array.from(
      withdrawForm.querySelectorAll('[data-wallet-withdraw-percent]')
    );

    function syncWithdrawalNetworks() {
      if (!assetSelect || !networkSelect) {
        return;
      }

      const selectedAsset = String(assetSelect.value || '').trim().toUpperCase();
      Array.from(networkSelect.options).forEach(function (option) {
        if (!option.value) {
          option.hidden = false;
          option.disabled = false;
          return;
        }

        const supportedAssets = String(
          option.getAttribute('data-wallet-supported-assets') || ''
        ).trim().split(/\s+/).filter(Boolean);
        const compatible = Boolean(selectedAsset) && supportedAssets.indexOf(selectedAsset) !== -1;
        option.hidden = !compatible;
        option.disabled = !compatible;
      });

      const selectedNetworkOption = networkSelect.options[networkSelect.selectedIndex];
      if (!selectedAsset || !selectedNetworkOption || selectedNetworkOption.disabled) {
        networkSelect.value = '';
      }
    }

    if (assetSelect && networkSelect) {
      assetSelect.addEventListener('change', syncWithdrawalNetworks);
      syncWithdrawalNetworks();
    }

    function formatUnitsToDisplayAmount(units) {
      const normalizedUnits = Math.max(0, parseInt(units || 0, 10));
      const integerPart = Math.floor(normalizedUnits / 1000000);
      const fractionalPart = String(normalizedUnits % 1000000).padStart(6, '0');
      return (integerPart + '.' + fractionalPart).replace(/\.?0+$/, '');
    }

    percentButtons.forEach(function (button) {
      button.addEventListener('click', function () {
        const availableUnits = parseInt(withdrawForm.getAttribute('data-wallet-withdraw-available-units') || 0, 10);
        const percent = parseInt(button.getAttribute('data-wallet-withdraw-percent') || 0, 10);
        if (!amountInput || !availableUnits || !percent) {
          return;
        }

        amountInput.value = formatUnitsToDisplayAmount(Math.floor(availableUnits * percent / 100));
      });
    });
  }
})();