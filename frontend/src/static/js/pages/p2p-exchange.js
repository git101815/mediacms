(function () {
  const root = document.querySelector('[data-p2p-exchange]');
  if (!root) {
    return;
  }

  const websocketPath = root.getAttribute('data-websocket-path') || '';
  const messagesNode = root.querySelector('[data-p2p-messages]');
  const emptyNode = root.querySelector('[data-p2p-empty]');
  const form = root.querySelector('[data-p2p-composer]');
  const input = root.querySelector('[data-p2p-message-input]');
  const sendButton = root.querySelector('[data-p2p-send]');
  const statusNode = root.querySelector('[data-p2p-order-status]');
  const readonlyNode = root.querySelector('[data-p2p-readonly]');

  const renderedIds = new Set();
  let socket = null;
  let reconnectTimer = null;
  let reconnectAttempt = 0;
  let stopping = false;
  let orderCanSend = root.getAttribute('data-can-send') === 'true';
  let socketReady = false;
  let normalPlaceholder = input ? input.getAttribute('placeholder') || 'Write a message…' : '';

  function humanizeStatus(status) {
    switch (status) {
      case 'completed':
        return 'Completed';
      case 'canceled':
        return 'Canceled';
      case 'disputed':
        return 'Disputed';
      case 'open':
      default:
        return 'Open';
    }
  }

  function setStatus(status) {
    if (!statusNode || !status) return;
    statusNode.textContent = humanizeStatus(status);
    statusNode.setAttribute('data-status', status);
    statusNode.className = 'p2p-exchange__status p2p-exchange__status--' + status;
  }

  function applyComposerState() {
    const writable = orderCanSend && socketReady;
    if (input) {
      input.disabled = !writable;
      if (!orderCanSend) {
        input.placeholder = 'This conversation is read-only.';
      } else if (!socketReady) {
        input.placeholder = 'Reconnecting…';
      } else {
        input.placeholder = normalPlaceholder;
      }
    }
    if (sendButton) sendButton.disabled = !writable;
    if (readonlyNode) readonlyNode.hidden = orderCanSend;
  }

  function setWritable(canSend) {
    orderCanSend = Boolean(canSend);
    applyComposerState();
  }

  function setSocketReady(ready) {
    socketReady = Boolean(ready);
    applyComposerState();
  }

  function nearBottom() {
    if (!messagesNode) return true;
    return messagesNode.scrollHeight - messagesNode.scrollTop - messagesNode.clientHeight < 90;
  }

  function scrollToBottom(force) {
    if (!messagesNode) return;
    if (force || nearBottom()) {
      messagesNode.scrollTop = messagesNode.scrollHeight;
    }
  }

  function formatTime(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return new Intl.DateTimeFormat(undefined, {
      hour: '2-digit',
      minute: '2-digit',
    }).format(date);
  }

  function appendMessage(message, forceScroll) {
    if (!message || renderedIds.has(message.id) || !messagesNode) return;
    renderedIds.add(message.id);
    if (emptyNode) emptyNode.hidden = true;

    const wasNearBottom = nearBottom();
    const row = document.createElement('div');
    row.className = 'p2p-chat__message';

    if (message.kind === 'system') {
      row.classList.add('p2p-chat__message--system');
      const systemText = document.createElement('span');
      systemText.textContent = message.body || '';
      row.appendChild(systemText);
    } else {
      row.classList.add(message.is_mine ? 'p2p-chat__message--mine' : 'p2p-chat__message--peer');

      const bubble = document.createElement('div');
      bubble.className = 'p2p-chat__bubble';

      const meta = document.createElement('div');
      meta.className = 'p2p-chat__meta';
      const sender = document.createElement('span');
      sender.textContent = message.is_mine ? 'You' : (message.sender_name || 'Peer');
      const time = document.createElement('time');
      time.dateTime = message.created_at || '';
      time.textContent = formatTime(message.created_at);
      meta.appendChild(sender);
      meta.appendChild(time);

      const body = document.createElement('div');
      body.className = 'p2p-chat__body';
      body.textContent = message.body || '';

      bubble.appendChild(meta);
      bubble.appendChild(body);
      row.appendChild(bubble);
    }

    messagesNode.appendChild(row);
    if (forceScroll || wasNearBottom) scrollToBottom(true);
  }

  function buildSocketUrl() {
    if (!websocketPath) return '';
    const scheme = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return scheme + '//' + window.location.host + websocketPath;
  }

  function nextReconnectDelay() {
    const base = Math.min(15000, 1000 * Math.pow(2, reconnectAttempt));
    reconnectAttempt += 1;
    return base;
  }

  function scheduleReconnect(immediate) {
    window.clearTimeout(reconnectTimer);
    if (stopping) return;
    const delay = immediate ? 0 : nextReconnectDelay();
    reconnectTimer = window.setTimeout(connect, delay);
  }

  function handlePayload(payload) {
    if (!payload || typeof payload !== 'object') return;

    if (payload.type === 'snapshot') {
      (payload.messages || []).forEach(function (message) {
        appendMessage(message, false);
      });
      setStatus(payload.order_status);
      setWritable(Boolean(payload.can_send));
      scrollToBottom(true);
      return;
    }

    if (payload.type === 'message') {
      appendMessage(payload.message, true);
      return;
    }

    if (payload.type === 'status') {
      setStatus(payload.order_status);
      setWritable(Boolean(payload.can_send));
      return;
    }

    if (payload.type === 'error') {
      if (payload.code === 'read_only') {
        setStatus(payload.order_status);
        setWritable(false);
      }
      return;
    }
  }

  function connect() {
    if (stopping || !websocketPath) return;
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
      return;
    }

    setSocketReady(false);
    const url = buildSocketUrl();
    if (!url) return;

    socket = new WebSocket(url);

    socket.addEventListener('open', function () {
      reconnectAttempt = 0;
      setSocketReady(true);
    });

    socket.addEventListener('message', function (event) {
      try {
        handlePayload(JSON.parse(event.data));
      } catch (error) {
        // Ignore malformed server frames; reconnect semantics remain intact.
      }
    });

    socket.addEventListener('close', function (event) {
      setSocketReady(false);
      socket = null;
      if (stopping) return;
      if (event.code === 4401 || event.code === 4404) {
        return;
      }
      scheduleReconnect(false);
    });

    socket.addEventListener('error', function () {
      // The close event owns reconnect scheduling.
    });
  }

  function makeClientId() {
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
      return window.crypto.randomUUID();
    }
    return String(Date.now()) + '-' + Math.random().toString(16).slice(2);
  }

  if (form) {
    form.addEventListener('submit', function (event) {
      event.preventDefault();
      if (!input || input.disabled || !orderCanSend) return;
      const message = input.value.trim();
      if (!message) return;

      if (!socket || socket.readyState !== WebSocket.OPEN) {
        setSocketReady(false);
        scheduleReconnect(true);
        return;
      }

      socket.send(JSON.stringify({
        type: 'message.send',
        message: message,
        client_id: makeClientId(),
      }));
      input.value = '';
      input.style.height = '';
      input.focus();
    });
  }

  if (input) {
    input.addEventListener('input', function () {
      input.style.height = 'auto';
      input.style.height = Math.min(input.scrollHeight, 120) + 'px';
    });

    input.addEventListener('keydown', function (event) {
      if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        if (form) form.requestSubmit();
      }
    });
  }

  document.addEventListener('visibilitychange', function () {
    if (!document.hidden && (!socket || socket.readyState === WebSocket.CLOSED)) {
      reconnectAttempt = 0;
      scheduleReconnect(true);
    }
  });

  window.addEventListener('beforeunload', function () {
    stopping = true;
    window.clearTimeout(reconnectTimer);
    if (socket) socket.close(1000, 'page unload');
  });

  setStatus(statusNode ? statusNode.getAttribute('data-status') : 'open');
  setWritable(orderCanSend);
  setSocketReady(false);
  connect();
})();
