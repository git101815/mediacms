(function () {
  var cfg = (window.__MC_GATE_CONFIG__ || {});
  var COOKIE_NAME = cfg.cookieName || "mc_gate";
  var CDN_HOST = cfg.cdnHost || "medias.celebfakes.ru";
  var PLACEHOLDER = cfg.placeholder || "data:image/gif;base64,R0lGODlhAQABAAAAACw=";

  var observer = null;
  var active = false;

  function getCookie(name) {
    var m = document.cookie.match(new RegExp("(?:^|; )" + name.replace(/[$()*+./?[\\\]^{|}-]/g, "\\$&") + "=([^;]*)"));
    return m ? decodeURIComponent(m[1]) : null;
  }

  function isCdnUrl(u) {
    try {
      var url = new URL(u, window.location.href);
      return url.host === CDN_HOST;
    } catch (e) {
      return false;
    }
  }

  function srcsetTouchesCdn(srcset) {
    if (!srcset) return false;
    return srcset.indexOf(CDN_HOST) !== -1;
  }

  function styleTouchesCdn(styleAttr) {
    if (!styleAttr) return false;
    return styleAttr.indexOf(CDN_HOST) !== -1 && styleAttr.indexOf("background-image") !== -1;
  }

  function holdImg(img) {
    var src = img.getAttribute("src");
    if (src && isCdnUrl(src) && !img.dataset.mcSrc) {
      img.dataset.mcSrc = src;
      img.setAttribute("src", PLACEHOLDER);
    }
    var srcset = img.getAttribute("srcset");
    if (srcset && srcsetTouchesCdn(srcset) && !img.dataset.mcSrcset) {
      img.dataset.mcSrcset = srcset;
      img.removeAttribute("srcset");
    }
  }

  function holdSource(source) {
    var srcset = source.getAttribute("srcset");
    if (srcset && srcsetTouchesCdn(srcset) && !source.dataset.mcSrcset) {
      source.dataset.mcSrcset = srcset;
      source.removeAttribute("srcset");
    }
  }

  function holdVideo(video) {
    var poster = video.getAttribute("poster");
    if (poster && isCdnUrl(poster) && !video.dataset.mcPoster) {
      video.dataset.mcPoster = poster;
      video.removeAttribute("poster");
    }
  }

  function holdDataSrc(el) {
    var ds = el.getAttribute("data-src");
    if (ds && isCdnUrl(ds) && !el.dataset.mcDataSrc) {
      el.dataset.mcDataSrc = ds;
      el.removeAttribute("data-src");
    }
  }

  function holdBackground(el) {
    var s = el.getAttribute("style") || "";
    if (styleTouchesCdn(s) && !el.dataset.mcBg) {
      var bg = el.style.backgroundImage;
      if (bg && bg.indexOf(CDN_HOST) !== -1) {
        el.dataset.mcBg = bg;
        el.style.backgroundImage = "none";
      }
    }
  }

  function holdNode(node) {
    if (!node || node.nodeType !== 1) return;

    if (node.tagName === "IMG") holdImg(node);
    else if (node.tagName === "SOURCE") holdSource(node);
    else if (node.tagName === "VIDEO") holdVideo(node);

    if (node.hasAttribute && node.hasAttribute("data-src")) holdDataSrc(node);
    if (node.getAttribute && node.getAttribute("style")) holdBackground(node);

    var imgs = node.querySelectorAll ? node.querySelectorAll("img[src], img[srcset]") : [];
    for (var i = 0; i < imgs.length; i++) holdImg(imgs[i]);

    var sources = node.querySelectorAll ? node.querySelectorAll("source[srcset]") : [];
    for (var j = 0; j < sources.length; j++) holdSource(sources[j]);

    var videos = node.querySelectorAll ? node.querySelectorAll("video[poster]") : [];
    for (var k = 0; k < videos.length; k++) holdVideo(videos[k]);

    var dataSrcEls = node.querySelectorAll ? node.querySelectorAll("[data-src]") : [];
    for (var d = 0; d < dataSrcEls.length; d++) holdDataSrc(dataSrcEls[d]);

    var styled = node.querySelectorAll ? node.querySelectorAll('[style*="' + CDN_HOST + '"]') : [];
    for (var t = 0; t < styled.length; t++) holdBackground(styled[t]);
  }

  function holdExisting() {
    holdNode(document.documentElement);
  }

  function releaseExisting() {
    var imgs = document.querySelectorAll("img[data-mc-src], img[data-mc-srcset]");
    for (var i = 0; i < imgs.length; i++) {
      var img = imgs[i];
      if (img.dataset.mcSrc) {
        img.setAttribute("src", img.dataset.mcSrc);
        delete img.dataset.mcSrc;
      }
      if (img.dataset.mcSrcset) {
        img.setAttribute("srcset", img.dataset.mcSrcset);
        delete img.dataset.mcSrcset;
      }
    }

    var sources = document.querySelectorAll("source[data-mc-srcset]");
    for (var j = 0; j < sources.length; j++) {
      var s = sources[j];
      s.setAttribute("srcset", s.dataset.mcSrcset);
      delete s.dataset.mcSrcset;
    }

    var videos = document.querySelectorAll("video[data-mc-poster]");
    for (var k = 0; k < videos.length; k++) {
      var v = videos[k];
      v.setAttribute("poster", v.dataset.mcPoster);
      delete v.dataset.mcPoster;
    }

    var dataSrcEls = document.querySelectorAll("[data-mc-data-src]");
    for (var d = 0; d < dataSrcEls.length; d++) {
      var el = dataSrcEls[d];
      el.setAttribute("data-src", el.dataset.mcDataSrc);
      delete el.dataset.mcDataSrc;
    }

    var bgEls = document.querySelectorAll("[data-mc-bg]");
    for (var b = 0; b < bgEls.length; b++) {
      var e = bgEls[b];
      e.style.backgroundImage = e.dataset.mcBg;
      delete e.dataset.mcBg;
    }
  }

  function start() {
    if (active) return;
    if (getCookie(COOKIE_NAME)) return;

    active = true;
    holdExisting();

    observer = new MutationObserver(function (mutations) {
      if (!active) return;
      if (getCookie(COOKIE_NAME)) return;
      for (var i = 0; i < mutations.length; i++) {
        var m = mutations[i];
        if (m.type === "childList") {
          for (var j = 0; j < m.addedNodes.length; j++) holdNode(m.addedNodes[j]);
        } else if (m.type === "attributes") {
          holdNode(m.target);
        }
      }
    });

    observer.observe(document.documentElement, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ["src", "srcset", "poster", "style", "data-src"]
    });
  }

  function stop() {
    active = false;
    if (observer) {
      observer.disconnect();
      observer = null;
    }
  }

  function release() {
    stop();
    releaseExisting();
  }

  window.mcGateHoldStart = start;
  window.mcGateRelease = release;

  start();
})();
