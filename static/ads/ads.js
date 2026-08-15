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
})();
