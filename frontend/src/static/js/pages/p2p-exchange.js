
(function () {
  const root = document.querySelector('[data-p2p-exchange]');
  if (!root) return;

  const websocketPath = root.getAttribute('data-websocket-path') || '';
  const role = root.getAttribute('data-p2p-role') || '';
  const pretrade = root.getAttribute('data-p2p-pretrade') === 'true';
  const messagesNode = root.querySelector('[data-p2p-messages]');
  const emptyNode = root.querySelector('[data-p2p-empty]');
  const form = root.querySelector('[data-p2p-composer]');
  const input = root.querySelector('[data-p2p-message-input]');
  const sendButton = root.querySelector('[data-p2p-send]');
  const statusNode = root.querySelector('[data-p2p-order-status]');
  const readonlyNode = root.querySelector('[data-p2p-readonly]');
  const sentButton = root.querySelector('[data-p2p-fiat-sent]');
  const receivedButton = root.querySelector('[data-p2p-fiat-received]');
  const actionError = root.querySelector('[data-p2p-action-error]');

  const renderedIds = new Set();
  let socket = null;
  let reconnectTimer = null;
  let reconnectAttempt = 0;
  let stopping = false;
  let orderCanSend = root.getAttribute('data-can-send') === 'true';
  let socketReady = false;
  const normalPlaceholder = input ? input.getAttribute('placeholder') || 'Write a message…' : '';

  function humanizeStatus(status) {
    const labels = {
      open: 'Open',
      waiting_agent: 'Waiting for P2P agent',
      waiting_new_agent: 'Waiting for a new P2P agent',
      no_agent_available: 'No P2P agent available',
      chat_open: 'Exchange open',
      fiat_sent: 'Money sent',
      completed: 'Completed',
      canceled: 'Canceled',
      disputed: 'Disputed',
    };
    return labels[status] || status || '';
  }

  function currentStatus() {
    return statusNode ? statusNode.getAttribute('data-status') || '' : '';
  }

  function setStatus(status) {
    if (!statusNode || !status) return;
    statusNode.textContent = humanizeStatus(status);
    statusNode.setAttribute('data-status', status);
    statusNode.className = 'p2p-exchange__status p2p-exchange__status--' + status;
  }

  function applyActions(actions, status) {
    const data = actions || {};
    if (sentButton) sentButton.hidden = !(data.can_mark_sent || (role === 'buyer' && status === 'chat_open'));
    if (receivedButton) receivedButton.hidden = !(data.can_mark_received || (role === 'agent' && status === 'fiat_sent'));
  }

  function applyComposerState() {
    const writable = orderCanSend && socketReady;
    if (input) {
      input.disabled = !writable;
      input.placeholder = !orderCanSend ? 'This conversation is read-only.' : (!socketReady ? 'Reconnecting…' : normalPlaceholder);
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

  function showActionError(text) {
    if (!actionError) return;
    actionError.textContent = text || '';
    actionError.hidden = !text;
  }

  function nearBottom() {
    if (!messagesNode) return true;
    return messagesNode.scrollHeight - messagesNode.scrollTop - messagesNode.clientHeight < 90;
  }

  function scrollToBottom(force) {
    if (!messagesNode) return;
    if (force || nearBottom()) messagesNode.scrollTop = messagesNode.scrollHeight;
  }

  function formatTime(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' }).format(date);
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
      const text = document.createElement('span');
      text.textContent = message.body || '';
      row.appendChild(text);
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
    return (window.location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + window.location.host + websocketPath;
  }

  function nextReconnectDelay() {
    const base = Math.min(15000, 1000 * Math.pow(2, reconnectAttempt));
    reconnectAttempt += 1;
    return base;
  }

  function scheduleReconnect(immediate) {
    window.clearTimeout(reconnectTimer);
    if (stopping) return;
    reconnectTimer = window.setTimeout(connect, immediate ? 0 : nextReconnectDelay());
  }

  function handleStatusPayload(payload) {
    const previous = currentStatus();
    const status = payload.order_status || previous;
    setStatus(status);
    setWritable(Boolean(payload.can_send));
    applyActions(payload.actions, status);
    if (pretrade && status !== previous) {
      window.location.reload();
      return;
    }
    if (!pretrade && status === 'completed' && previous !== 'completed') {
      window.location.reload();
    }
  }

  function handlePayload(payload) {
    if (!payload || typeof payload !== 'object') return;
    if (payload.type === 'snapshot') {
      (payload.messages || []).forEach(function (message) { appendMessage(message, false); });
      handleStatusPayload(payload);
      scrollToBottom(true);
      return;
    }
    if (payload.type === 'message') {
      appendMessage(payload.message, true);
      return;
    }
    if (payload.type === 'status') {
      handleStatusPayload(payload);
      return;
    }
    if (payload.type === 'action_ack') {
      showActionError('');
      return;
    }
    if (payload.type === 'error') {
      showActionError(payload.detail || 'Unable to complete this action.');
      if (payload.code === 'read_only') setWritable(false);
    }
  }

  function connect() {
    if (stopping || !websocketPath) return;
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;
    setSocketReady(false);
    const url = buildSocketUrl();
    if (!url) return;
    socket = new WebSocket(url);
    socket.addEventListener('open', function () { reconnectAttempt = 0; setSocketReady(true); });
    socket.addEventListener('message', function (event) {
      try { handlePayload(JSON.parse(event.data)); } catch (error) { /* ignore malformed frames */ }
    });
    socket.addEventListener('close', function (event) {
      setSocketReady(false);
      socket = null;
      if (stopping || event.code === 4401 || event.code === 4404) return;
      scheduleReconnect(false);
    });
  }

  function makeClientId() {
    if (window.crypto && typeof window.crypto.randomUUID === 'function') return window.crypto.randomUUID();
    return String(Date.now()) + '-' + Math.random().toString(16).slice(2);
  }

  function sendOrderAction(type) {
    showActionError('');
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      showActionError('Connection unavailable. Reconnecting…');
      scheduleReconnect(true);
      return;
    }
    socket.send(JSON.stringify({ type: type }));
  }

  if (sentButton) sentButton.addEventListener('click', function () { sendOrderAction('order.fiat_sent'); });
  if (receivedButton) receivedButton.addEventListener('click', function () { sendOrderAction('order.fiat_received'); });

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
      socket.send(JSON.stringify({ type: 'message.send', message: message, client_id: makeClientId() }));
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

  setStatus(currentStatus());
  applyActions({}, currentStatus());
  setWritable(orderCanSend);
  setSocketReady(false);
  connect();
})();
