(function () {
  const root = document.querySelector("[data-ai-generation-root]");
  if (!root) return;

  const createUrl =
    root.dataset.createUrl || "/api/v1/ai/generations";
  const detailBaseUrl =
    root.dataset.detailBaseUrl || "/api/v1/ai/generations/";

  const form = root.querySelector("#ai-generation-form");
  const promptInput = root.querySelector("#ai-generation-prompt");
  const promptLength = root.querySelector("#ai-generation-prompt-length");
  const submit = root.querySelector("#ai-generation-submit");
  const errorBox = root.querySelector("#ai-generation-error");
  const statusBox = root.querySelector("#ai-generation-status");
  const metaBox = root.querySelector("#ai-generation-meta");
  const previewBox = root.querySelector("#ai-generation-preview");
  const downloadButton = root.querySelector("#ai-generation-download");
  const csrfInput = form ? form.querySelector("[name=csrfmiddlewaretoken]") : null;

  let currentGeneration = null;
  let currentImageObjectUrl = null;
  let imageLoadVersion = 0;

  function escapeHtml(value) {
    const div = document.createElement("div");
    div.textContent = value == null ? "" : String(value);
    return div.innerHTML;
  }

  function currentResolutionValue() {
    const selected = root.querySelector('input[name="resolution"]:checked');
    return selected ? selected.value : "512x768";
  }

  function updatePromptLength() {
    if (promptInput && promptLength) {
      promptLength.textContent = String(promptInput.value.length);
    }
  }

  function setStatus(status) {
    const value = status || "ready";
    statusBox.className = "ai-studio-badge";

    if (["queued", "running", "success", "failed"].includes(value)) {
      statusBox.className += " ai-studio-badge-" + value;
    }

    statusBox.textContent = value;
  }

  function setMeta(text) {
    metaBox.textContent = text || "";
  }

  function revokeCurrentImageObjectUrl() {
    if (currentImageObjectUrl) {
      URL.revokeObjectURL(currentImageObjectUrl);
      currentImageObjectUrl = null;
    }
  }

  function hideDownload() {
    downloadButton.hidden = true;
    downloadButton.removeAttribute("href");
  }

  function showDownload(url, item, contentType) {
    const extensions = {
      "image/jpeg": "jpg",
      "image/png": "png",
      "image/webp": "webp"
    };
    const extension = extensions[contentType] || "img";

    downloadButton.href = url;
    downloadButton.download =
      "generated-" + String(item.id || "image") + "." + extension;
    downloadButton.hidden = false;
  }

  async function loadSuccessfulImage(item, version) {
    previewBox.innerHTML =
      '<div class="ai-studio-progress">' +
        '<div class="ai-studio-spinner"></div>' +
        '<h3>Loading image...</h3>' +
      '</div>';

    try {
      const response = await fetch(item.image_url, {
        credentials: "same-origin",
        cache: "no-store"
      });

      if (!response.ok) {
        throw new Error("Generated image is unavailable.");
      }

      const blob = await response.blob();

      if (
        version !== imageLoadVersion ||
        !currentGeneration ||
        currentGeneration.id !== item.id
      ) {
        return;
      }

      revokeCurrentImageObjectUrl();
      currentImageObjectUrl = URL.createObjectURL(blob);

      previewBox.innerHTML =
        '<img src="' +
        escapeHtml(currentImageObjectUrl) +
        '" alt="Generated image">';

      showDownload(
        currentImageObjectUrl,
        item,
        blob.type
      );
    } catch (error) {
      if (version !== imageLoadVersion) return;

      previewBox.innerHTML =
        '<div class="ai-studio-failure">' +
          '<h3>Image unavailable</h3>' +
          '<p>' +
            escapeHtml(error.message || "Generated image is unavailable.") +
          '</p>' +
        '</div>';
    }
  }

  function renderEmpty() {
    imageLoadVersion += 1;
    revokeCurrentImageObjectUrl();
    currentGeneration = null;
    setStatus("ready");
    setMeta("No active generation");
    hideDownload();

    previewBox.innerHTML =
      '<div class="ai-studio-empty">' +
        '<h3>Image preview</h3>' +
        '<p>Your generated image will appear here.</p>' +
      '</div>';
  }

  function renderGeneration(item) {
    if (!item) {
      renderEmpty();
      return;
    }

    const previousId = currentGeneration ? currentGeneration.id : null;
    if (previousId !== item.id) {
      imageLoadVersion += 1;
      revokeCurrentImageObjectUrl();
    }

    currentGeneration = item;
    setStatus(item.status);
    setMeta(item.resolution || "Generation in progress");
    hideDownload();

    if (item.status === "success" && item.image_url) {
      const version = ++imageLoadVersion;
      loadSuccessfulImage(item, version);
      return;
    }

    if (item.status === "failed") {
      previewBox.innerHTML =
        '<div class="ai-studio-failure">' +
          '<h3>Generation failed</h3>' +
          '<p>' + escapeHtml(item.error_message || "Generation failed.") + '</p>' +
          '<p class="ai-studio-prompt-echo">' + escapeHtml(item.prompt || "") + '</p>' +
        '</div>';
      return;
    }

    const statusText =
      item.status === "running"
        ? "Generating image..."
        : "Waiting in queue...";

    previewBox.innerHTML =
      '<div class="ai-studio-progress">' +
        '<div class="ai-studio-spinner"></div>' +
        '<h3>' + escapeHtml(statusText) + '</h3>' +
        '<p class="ai-studio-prompt-echo">' + escapeHtml(item.prompt || "") + '</p>' +
      '</div>';
  }

  async function refreshActive() {
    if (!currentGeneration) return;

    if (
      currentGeneration.status !== "queued" &&
      currentGeneration.status !== "running"
    ) {
      return;
    }

    try {
      const response = await fetch(
        detailBaseUrl +
          encodeURIComponent(currentGeneration.id),
        { credentials: "same-origin" }
      );

      if (!response.ok) return;

      const payload = await response.json();

      if (payload && payload.generation) {
        renderGeneration(payload.generation);
      }
    } catch (_error) {
    }
  }

  if (promptInput) {
    promptInput.addEventListener("input", updatePromptLength);
    updatePromptLength();
  }

  if (form) {
    form.addEventListener("submit", async function (event) {
      event.preventDefault();

      errorBox.hidden = true;
      submit.disabled = true;
      submit.textContent = "Generating...";

      const outgoing = {
        prompt: promptInput.value,
        resolution: currentResolutionValue()
      };

      renderGeneration({
        id: "",
        status: "queued",
        prompt: outgoing.prompt,
        resolution: outgoing.resolution,
        error_message: "",
        image_url: ""
      });

      try {
        const response = await fetch(createUrl, {
          method: "POST",
          credentials: "same-origin",
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfInput ? csrfInput.value : ""
          },
          body: JSON.stringify(outgoing)
        });

        const payload = await response.json();

        if (!response.ok || !payload.success) {
          throw new Error(
            payload.error || "Could not create generation."
          );
        }

        renderGeneration(payload.generation);
        promptInput.value = "";
        updatePromptLength();
      } catch (error) {
        renderEmpty();
        errorBox.textContent =
          error.message || "Could not create generation.";
        errorBox.hidden = false;
      } finally {
        submit.disabled = false;
        submit.textContent =
          submit.dataset.defaultLabel || "Generate";
      }
    });
  }

  window.addEventListener("pagehide", function () {
    imageLoadVersion += 1;
    revokeCurrentImageObjectUrl();
  });

  renderEmpty();
  window.setInterval(refreshActive, 3000);
})();
