(function () {
  const root = document.querySelector('[data-wallet-ui]');
  if (!root) {
    return;
  }

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
    assetKey: '',
    routeKey: '',
  };

  function getBuyForm() {
    return document.querySelector('[data-wallet-buy-form]');
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

  function getRouteOptions() {
    const form = getBuyForm();
    if (!form) {
      return [];
    }

    return Array.from(form.querySelectorAll('[data-wallet-route-option]')).map(function (node) {
      const assetCode = node.getAttribute('data-asset-code') || '';
      const assetGroupKey = node.getAttribute('data-asset-group-key') || assetCode;

      return {
        key: node.getAttribute('data-option-key') || '',
        paymentMethodKey: node.getAttribute('data-payment-method-key') || '',
        paymentMethodLabel: node.getAttribute('data-payment-method-label') || '',
        paymentMethodType: node.getAttribute('data-payment-method-type') || '',
        paymentGroupKey: node.getAttribute('data-payment-group-key') || '',
        paymentGroupLabel: node.getAttribute('data-payment-group-label') || '',
        paymentGroupIcon: node.getAttribute('data-payment-group-icon') || '',
        paymentGroupIconPath: node.getAttribute('data-payment-group-icon-path') || '',
        paymentPriceFixedCanonical: Number(node.getAttribute('data-payment-price-fixed-canonical') || 0),
        paymentPriceBps: Number(node.getAttribute('data-payment-price-bps') || 0),
        assetCode: assetCode,
        assetGroupKey: assetGroupKey,
        assetGroupLabel: node.getAttribute('data-asset-group-label') || assetGroupKey,
        assetGroupIconPath: node.getAttribute('data-asset-group-icon-path') || '',
        assetGroupOrder: Number(node.getAttribute('data-asset-group-order') || 100),
        chain: node.getAttribute('data-chain') || '',
        networkLabel: node.getAttribute('data-network-label') || '',
        minAmount: node.getAttribute('data-min-amount') || '',
      };
    });
  }

  function getPaymentMethods() {
    const map = new Map();

    getRouteOptions().forEach(function (option) {
      const groupKey = option.paymentGroupKey || option.paymentMethodKey;
      if (!groupKey) {
        return;
      }

      if (!map.has(groupKey)) {
        map.set(groupKey, {
          key: groupKey,
          label: option.paymentGroupLabel || option.paymentMethodLabel || option.assetCode,
          icon: option.paymentGroupIcon || option.paymentGroupLabel || option.assetCode,
          iconPath: option.paymentGroupIconPath || '',
          type: option.paymentMethodType || 'crypto',
          priceBps: option.paymentPriceBps || 0,
          priceFixedCanonical: option.paymentPriceFixedCanonical || 0,
          routes: [],
        });
      }

      map.get(groupKey).routes.push(option);
    });

    return Array.from(map.values());
  }

  function getRoutesForPaymentMethod(paymentMethodKey) {
    return getRouteOptions().filter(function (option) {
      return (option.paymentGroupKey || option.paymentMethodKey) === paymentMethodKey;
    });
  }

  function getRouteByKey(routeKey) {
    return getRouteOptions().find(function (option) {
      return option.key === routeKey;
    }) || null;
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

    const hidden = document.querySelector('[data-wallet-selected-route]');
    if (hidden) {
      hidden.value = buyState.routeKey;
    }

    syncHiddenPaymentFromRoute(getRouteByKey(buyState.routeKey));
  }

  function selectDefaultAssetForPaymentMethod() {
    const assets = getAssetsForPaymentMethod(buyState.paymentMethodKey);
    setSelectedAsset(assets[0] ? assets[0].key : '');
  }

  function selectDefaultRouteForPaymentMethod() {
    const methodRoutes = getRoutesForPaymentMethod(buyState.paymentMethodKey);

    if (buyState.paymentMethodType !== 'crypto') {
      setSelectedRoute(methodRoutes[0] ? methodRoutes[0].key : '');
      return;
    }

    if (!buyState.assetKey) {
      selectDefaultAssetForPaymentMethod();
    }

    const assetRoutes = getRoutesForSelectedAsset();
    setSelectedRoute(assetRoutes[0] ? assetRoutes[0].key : '');
  }

  function getPaymentMethodPriceDisplay(method) {
    const base = Number(buyState.packGrossCanonical || 0);
    const bps = Number((method && method.priceBps) || 0);
    const fixed = Number((method && method.priceFixedCanonical) || 0);

    const percentageFee = Math.round(base * bps / 10000);
    const adjusted = base + fixed + percentageFee;

    return '$' + formatCanonicalStableAmount(adjusted);
  }

  function renderPaymentMethodChoices() {
    const container = document.querySelector('[data-wallet-payment-method-choices]');
    if (!container) {
      return;
    }

    const methods = getPaymentMethods();
    if (!buyState.paymentMethodKey && methods.length) {
      setSelectedPaymentMethod(methods[0]);
      selectDefaultAssetForPaymentMethod();
      selectDefaultRouteForPaymentMethod();
    }

    container.innerHTML = '';

    methods.forEach(function (method) {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'wallet-buy-flow__choice' + (
        buyState.paymentMethodKey === method.key ? ' wallet-buy-flow__choice--selected' : ''
      );
      button.setAttribute('data-wallet-payment-method-choice', method.key);

      button.innerHTML =
        '<span class="wallet-buy-flow__choice-icon">' +
          renderChoiceIcon(method.iconPath, method.icon, 'wallet-buy-flow__choice-icon-image') +
        '</span>' +
        '<span class="wallet-buy-flow__choice-copy">' +
          '<span class="wallet-buy-flow__choice-title">' + escapeHtml(method.label) + '</span>' +
        '</span>' +
        '<span class="wallet-buy-flow__choice-price">' + escapeHtml(getPaymentMethodPriceDisplay(method)) + '</span>';

      container.appendChild(button);
    });
  }

  function renderAssetChoices() {
    const container = document.querySelector('[data-wallet-asset-choices]');
    if (!container) {
      return;
    }

    const assets = getAssetsForPaymentMethod(buyState.paymentMethodKey);

    if (!buyState.assetKey || !assets.some(function (item) { return item.key === buyState.assetKey; })) {
      setSelectedAsset(assets[0] ? assets[0].key : '');
    }

    container.innerHTML = '';

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
  }

  function renderNetworkChoices() {
    const container = document.querySelector('[data-wallet-network-choices]');
    if (!container) {
      return;
    }

    const routes = getRoutesForSelectedAsset();

    if (!buyState.routeKey || !routes.some(function (item) { return item.key === buyState.routeKey; })) {
      setSelectedRoute(routes[0] ? routes[0].key : '');
    }

    container.innerHTML = '';

    routes.forEach(function (item) {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'wallet-buy-flow__choice' + (
        buyState.routeKey === item.key ? ' wallet-buy-flow__choice--selected' : ''
      );
      button.setAttribute('data-wallet-route-choice', item.key);

      button.innerHTML =
        '<span class="wallet-buy-flow__choice-copy">' +
          '<span class="wallet-buy-flow__choice-title">' + escapeHtml(item.networkLabel) + '</span>' +
        '</span>';

      container.appendChild(button);
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
      renderAssetChoices();
    }

    if (step >= 4) {
      renderNetworkChoices();
    }
  }

  function resetBuyFlow() {
    const checkedPack = document.querySelector('input[name="token_pack_choice"]:checked');
    setSelectedPackFromInput(checkedPack);

    const methods = getPaymentMethods();
    setSelectedPaymentMethod(methods[0] || null);
    selectDefaultAssetForPaymentMethod();
    selectDefaultRouteForPaymentMethod();

    renderPaymentMethodChoices();
    renderAssetChoices();
    renderNetworkChoices();
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

  document.addEventListener('click', function (event) {
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
        const methods = getPaymentMethods();
        const selectedMethod = methods.find(function (item) {
          return item.key === buyState.paymentMethodKey;
        });

        if (!selectedMethod) {
          renderPaymentMethodChoices();
          return;
        }

        const routes = getRoutesForPaymentMethod(selectedMethod.key);
        if (!routes.length) {
          return;
        }

        if (selectedMethod.type !== 'crypto') {
          setSelectedRoute(routes[0].key);
          const form = getBuyForm();
          if (form) {
            form.submit();
          }
          return;
        }

        selectDefaultAssetForPaymentMethod();
        selectDefaultRouteForPaymentMethod();
        renderAssetChoices();
        goToStep(3);
        return;
      }

      if (step === '4') {
        renderNetworkChoices();
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
      const methods = getPaymentMethods();
      const selectedMethod = methods.find(function (item) {
        return item.key === methodKey;
      });

      setSelectedPaymentMethod(selectedMethod || null);
      selectDefaultAssetForPaymentMethod();
      selectDefaultRouteForPaymentMethod();

      renderPaymentMethodChoices();
      renderAssetChoices();
      renderNetworkChoices();
      return;
    }

    const assetChoice = event.target.closest('[data-wallet-asset-choice]');
    if (assetChoice) {
      event.preventDefault();

      setSelectedAsset(assetChoice.getAttribute('data-wallet-asset-choice'));
      selectDefaultRouteForPaymentMethod();

      renderAssetChoices();
      renderNetworkChoices();
      return;
    }

    const routeChoice = event.target.closest('[data-wallet-route-choice]');
    if (routeChoice) {
      event.preventDefault();

      setSelectedRoute(routeChoice.getAttribute('data-wallet-route-choice'));
      renderNetworkChoices();
    }
  });

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
    const percentButtons = Array.from(
      withdrawForm.querySelectorAll('[data-wallet-withdraw-percent]')
    );

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