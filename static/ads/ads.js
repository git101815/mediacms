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
    const control = group.querySelector('input:not([type="file"]), select, textarea');
    if (!control) return;
    const sync = () => {
      const value = control.value == null ? '' : String(control.value).trim();
      group.classList.toggle('has-value', value.length > 0);
    };
    control.addEventListener('input', sync);
    control.addEventListener('change', sync);
    sync();
  });

  const file = document.querySelector('input[type="file"][name="creative"]');
  const preview = document.querySelector('[data-creative-preview]');
  if (file && preview) {
    file.addEventListener('change', () => {
      const selected = file.files && file.files[0];
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

  const copyButton = document.querySelector('[data-copy-address]');
  if (copyButton) {
    copyButton.addEventListener('click', async () => {
      const value = copyButton.getAttribute('data-copy-address') || '';
      if (!value) return;
      try {
        await navigator.clipboard.writeText(value);
        const previous = copyButton.textContent;
        copyButton.textContent = 'copied';
        window.setTimeout(() => { copyButton.textContent = previous; }, 1200);
      } catch (_) {}
    });
  }

  const statusNode = document.querySelector('[data-deposit-status-url]');
  if (statusNode) {
    const statusUrl = statusNode.getAttribute('data-deposit-status-url');
    const label = document.querySelector('[data-deposit-status-label]');
    const terminal = new Set(['transaction_complete', 'canceled', 'failed', 'expired']);
    const poll = async () => {
      try {
        const response = await fetch(statusUrl, {credentials: 'same-origin', cache: 'no-store'});
        if (!response.ok) return;
        const payload = await response.json();
        if (label && payload.status_label) label.textContent = payload.status_label;
        if (terminal.has(payload.status)) return;
      } catch (_) {}
      window.setTimeout(poll, 3000);
    };
    window.setTimeout(poll, 1500);
  }
})();
