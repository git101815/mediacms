(function () {
  if (window.__questBoardTrackingStarted) {
    return;
  }
  window.__questBoardTrackingStarted = true;

  const NAVIGATION_ENDPOINT = '/api/weekly-quests/navigation';
  let fingerprintPromise = null;
  let lastReportedPage = '';

  function getCookie(name) {
    const parts = (`; ${document.cookie}`).split(`; ${name}=`);
    return parts.length === 2 ? parts.pop().split(';').shift() : '';
  }

  function fallbackHash(value) {
    let hash = 2166136261;
    for (let index = 0; index < value.length; index += 1) {
      hash ^= value.charCodeAt(index);
      hash = Math.imul(hash, 16777619);
    }
    return (`00000000${(hash >>> 0).toString(16)}`).slice(-8).repeat(4);
  }

  function canvasValue() {
    try {
      const canvas = document.createElement('canvas');
      const context = canvas.getContext('2d');
      context.textBaseline = 'top';
      context.font = '14px Arial';
      context.fillText('MediaCMS quest fingerprint', 2, 2);
      return canvas.toDataURL();
    } catch (error) {
      return '';
    }
  }

  function webglValue() {
    try {
      const canvas = document.createElement('canvas');
      const gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
      if (!gl) {
        return '';
      }
      const extension = gl.getExtension('WEBGL_debug_renderer_info');
      if (!extension) {
        return `${gl.getParameter(gl.VENDOR)}|${gl.getParameter(gl.RENDERER)}`;
      }
      return [
        gl.getParameter(extension.UNMASKED_VENDOR_WEBGL),
        gl.getParameter(extension.UNMASKED_RENDERER_WEBGL),
      ].join('|');
    } catch (error) {
      return '';
    }
  }

  function fingerprintMaterial() {
    const screenValue = window.screen
      ? [window.screen.width, window.screen.height, window.screen.colorDepth].join('x')
      : '';
    return JSON.stringify({
      userAgent: navigator.userAgent || '',
      platform: navigator.platform || '',
      languages: navigator.languages || [navigator.language || ''],
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || '',
      screen: screenValue,
      pixelRatio: window.devicePixelRatio || 1,
      hardwareConcurrency: navigator.hardwareConcurrency || 0,
      deviceMemory: navigator.deviceMemory || 0,
      touchPoints: navigator.maxTouchPoints || 0,
      canvas: canvasValue(),
      webgl: webglValue(),
    });
  }

  function getFingerprint() {
    if (fingerprintPromise) {
      return fingerprintPromise;
    }
    const material = fingerprintMaterial();
    if (window.crypto && window.crypto.subtle && window.TextEncoder) {
      fingerprintPromise = window.crypto.subtle
        .digest('SHA-256', new TextEncoder().encode(material))
        .then(function (buffer) {
          return Array.from(new Uint8Array(buffer))
            .map(function (byte) { return byte.toString(16).padStart(2, '0'); })
            .join('');
        })
        .catch(function () { return fallbackHash(material); });
    } else {
      fingerprintPromise = Promise.resolve(fallbackHash(material));
    }
    return fingerprintPromise;
  }

  window.MediaCMSWeeklyQuests = {
    getFingerprint: getFingerprint,
  };

  function currentPage() {
    return window.location.pathname + window.location.search;
  }

  function reportNavigation() {
    if (document.visibilityState && document.visibilityState !== 'visible') {
      return;
    }
    const page = currentPage();
    if (page === lastReportedPage) {
      return;
    }
    lastReportedPage = page;
    getFingerprint().then(function (fingerprint) {
      return window.fetch(NAVIGATION_ENDPOINT, {
        method: 'POST',
        credentials: 'same-origin',
        keepalive: true,
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
        },
        body: JSON.stringify({ fingerprint: fingerprint, page: page }),
      });
    }).catch(function () {});
  }

  function wrapHistory(name) {
    const original = window.history && window.history[name];
    if (!original) {
      return;
    }
    window.history[name] = function () {
      const result = original.apply(this, arguments);
      window.setTimeout(reportNavigation, 0);
      return result;
    };
  }

  wrapHistory('pushState');
  wrapHistory('replaceState');
  window.addEventListener('popstate', reportNavigation);
  window.addEventListener('pageshow', reportNavigation);
  document.addEventListener('visibilitychange', reportNavigation);

  function copyUrl(button, url) {
    const original = button.textContent;
    function done() {
      button.textContent = 'Copied';
      window.setTimeout(function () { button.textContent = original; }, 1500);
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(url).then(done).catch(function () {});
      return;
    }
    const field = document.createElement('textarea');
    field.value = url;
    field.setAttribute('readonly', '');
    field.style.position = 'fixed';
    field.style.opacity = '0';
    document.body.appendChild(field);
    field.select();
    document.execCommand('copy');
    document.body.removeChild(field);
    done();
  }

  function shareSite(button) {
    const endpoint = button.getAttribute('data-wallet-weekly-share-site') || '';
    if (!endpoint) {
      return;
    }
    button.disabled = true;
    getFingerprint()
      .then(function (fingerprint) {
        return window.fetch(endpoint, {
          method: 'POST',
          credentials: 'same-origin',
          headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
            'X-CSRFToken': getCookie('csrftoken'),
            'X-Requested-With': 'XMLHttpRequest',
          },
          body: JSON.stringify({ fingerprint: fingerprint }),
        });
      })
      .then(function (response) {
        if (!response.ok) {
          throw new Error('Share link unavailable');
        }
        return response.json();
      })
      .then(function (payload) {
        const url = payload && payload.url ? payload.url : '';
        if (!url) {
          throw new Error('Share link unavailable');
        }
        if (navigator.share) {
          return navigator.share({
            title: document.title,
            text: 'Take a look at this site.',
            url: url,
          }).catch(function () { copyUrl(button, url); });
        }
        copyUrl(button, url);
        return null;
      })
      .catch(function () {})
      .finally(function () { button.disabled = false; });
  }

  function formatCountdown(date) {
    const seconds = Math.max(0, Math.floor((date.getTime() - Date.now()) / 1000));
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return days > 0 ? `${days}d ${hours}h` : `${hours}h ${minutes}m`;
  }

  function initWalletBoard() {
    const module = document.querySelector('[data-wallet-module="quests"]');
    if (!module) {
      return;
    }

    document.addEventListener('click', function (event) {
      const button = event.target && event.target.closest
        ? event.target.closest('[data-wallet-weekly-share-site]')
        : null;
      if (!button) {
        return;
      }
      event.preventDefault();
      shareSite(button);
    });

    const countdown = module.querySelector('[data-wallet-quest-countdown]');
    const endDate = new Date(module.getAttribute('data-wallet-quest-ends-at') || '');
    const statusUrl = module.getAttribute('data-wallet-quest-status-url') || '';
    const cycleKey = module.getAttribute('data-wallet-quest-cycle') || '';
    const revision = module.getAttribute('data-wallet-quest-revision') || '';

    function updateCountdown() {
      if (!countdown || Number.isNaN(endDate.getTime())) {
        return;
      }
      countdown.textContent = formatCountdown(endDate);
      if (endDate.getTime() <= Date.now()) {
        window.location.reload();
      }
    }

    function poll() {
      if (!statusUrl || document.hidden) {
        return;
      }
      window.fetch(statusUrl, {
        credentials: 'same-origin',
        headers: { Accept: 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
      })
        .then(function (response) { return response.ok ? response.json() : null; })
        .then(function (payload) {
          if (payload && (payload.cycle_key !== cycleKey || payload.revision !== revision)) {
            window.location.reload();
          }
        })
        .catch(function () {});
    }

    updateCountdown();
    window.setInterval(updateCountdown, 30000);
    window.setInterval(poll, 60000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      reportNavigation();
      initWalletBoard();
    }, { once: true });
  } else {
    reportNavigation();
    initWalletBoard();
  }
}());
