(() => {
  const file = document.querySelector('input[type="file"][name="creative"]');
  const preview = document.querySelector('[data-creative-preview]');
  const label = document.querySelector('[data-creative-label]');
  if (file && preview) {
    file.addEventListener('change', () => {
      const selected = file.files && file.files[0];
      if (!selected) return;
      if (label) label.textContent = selected.name;
      const url = URL.createObjectURL(selected);
      preview.innerHTML = '';
      const img = document.createElement('img');
      img.src = url;
      img.alt = 'Creative preview';
      img.onload = () => URL.revokeObjectURL(url);
      preview.appendChild(img);
    });
  }

  const model = document.querySelector('[name="pricing_model"]');
  const help = document.querySelector('[data-bid-help]');
  const updateBidHelp = () => {
    if (!model || !help) return;
    help.textContent = model.value === 'cpc'
      ? 'Tokens charged for each valid click.'
      : 'Tokens charged for every 1,000 delivered impressions.';
  };
  if (model) {
    model.addEventListener('change', updateBidHelp);
    updateBidHelp();
  }
})();
