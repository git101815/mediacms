(function () {
  const root = document.querySelector('[data-p2p-exchange]');
  if (!root) {
    return;
  }

  const messagesUrl = root.getAttribute('data-messages-url') || '';
  const sendUrl = root.getAttribute('data-send-url') || '';
  const messagesNode = root.querySelector('[data-p2p-messages]');
  const emptyNode = root.querySelector('[data-p2p-empty]');
  const form = root.querySelector('[data-p2p-composer]');
  const input = root.querySelector('[data-p2p-message-input]');
  const sendButton = root.querySelector('[data-p2p-send]');
  const connectionNode = root.querySelector('[data-p2p-connection-status]');
  const readonlyNode = root.querySelector('[data-p2p-readonly]');
  const csrfInput = form ? form.querySelector('input[name="csrfmiddlewaretoken"]') : null;

  let afterId = 0;
  let pollTimer = null;
  let polling = false;
  let stopped = false;
  let consecutiveErrors = 0;
  const renderedIds = new Set();

  function setConnection(text, tone) {
    if (!connectionNode) return;
    connectionNode.textContent = text;
    connectionNode.setAttribute('data-tone', tone || 'neutral');
  }

  function setWritable(canSend) {
    if (input) input.disabled = !canSend;
    if (sendButton) sendButton.disabled = !canSend;
    if (readonlyNode) readonlyNode.hidden = canSend;
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
    afterId = Math.max(afterId, Number(message.id) || 0);
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

  function schedulePoll(delay) {
    window.clearTimeout(pollTimer);
    if (stopped) return;
    pollTimer = window.setTimeout(poll, delay);
  }

  async function poll() {
    if (polling || stopped || !messagesUrl) return;
    polling = true;
    try {
      const separator = messagesUrl.indexOf('?') === -1 ? '?' : '&';
      const response = await fetch(messagesUrl + separator + 'after_id=' + encodeURIComponent(afterId), {
        credentials: 'same-origin',
        headers: { 'Accept': 'application/json' },
      });
      if (!response.ok) throw new Error('messages request failed');
      const payload = await response.json();
      (payload.messages || []).forEach(function (message) {
        appendMessage(message, afterId === 0);
      });
      setWritable(Boolean(payload.can_send));
      consecutiveErrors = 0;
      setConnection('Live updates', 'ok');
    } catch (error) {
      consecutiveErrors += 1;
      setConnection(consecutiveErrors > 2 ? 'Connection problem' : 'Reconnecting…', 'warning');
    } finally {
      polling = false;
      schedulePoll(document.hidden ? 6000 : 2000);
    }
  }

  if (form) {
    form.addEventListener('submit', async function (event) {
      event.preventDefault();
      if (!input || input.disabled || !sendUrl) return;
      const message = input.value.trim();
      if (!message) return;

      input.disabled = true;
      if (sendButton) sendButton.disabled = true;
      try {
        const response = await fetch(sendUrl, {
          method: 'POST',
          credentials: 'same-origin',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-CSRFToken': csrfInput ? csrfInput.value : '',
          },
          body: JSON.stringify({ message: message }),
        });
        const payload = await response.json();
        if (!response.ok) {
          if (response.status === 409) setWritable(false);
          throw new Error(payload.detail || 'Unable to send message');
        }
        input.value = '';
        input.style.height = '';
        appendMessage(payload.message, true);
        setConnection('Live updates', 'ok');
      } catch (error) {
        setConnection(error.message || 'Unable to send', 'warning');
      } finally {
        if (!readonlyNode || readonlyNode.hidden) {
          input.disabled = false;
          if (sendButton) sendButton.disabled = false;
          input.focus();
        }
      }
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
    if (!document.hidden) schedulePoll(0);
  });
  window.addEventListener('beforeunload', function () {
    stopped = true;
    window.clearTimeout(pollTimer);
  });

  setWritable(root.getAttribute('data-can-send') === 'true');
  poll();
})();
