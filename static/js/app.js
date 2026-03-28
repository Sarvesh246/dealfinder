(function () {
  var PricePulse = window.PricePulse || {};

  function dismissFlash(flash) {
    if (!flash || flash.classList.contains("fade-out")) return;
    flash.classList.add("fade-out");
    window.setTimeout(function () {
      flash.remove();
    }, 420);
  }

  function initFlashes(root) {
    var flashList = (root || document).getElementById
      ? (root || document).getElementById("flash-list")
      : document.getElementById("flash-list");
    if (!flashList || flashList.dataset.bound === "true") return;
    flashList.dataset.bound = "true";

    flashList.querySelectorAll(".flash-dismiss").forEach(function (button) {
      button.addEventListener("click", function () {
        dismissFlash(button.closest(".flash"));
      });
    });

    flashList.querySelectorAll(".flash[data-auto-dismiss='true']").forEach(function (flash) {
      window.setTimeout(function () {
        dismissFlash(flash);
      }, 6000);
    });
  }

  function initMobileNav() {
    var body = document.body;
    var navToggle = document.getElementById("mobile-nav-toggle");
    var navPanel = document.getElementById("mobile-nav-panel");
    var navBackdrop = document.getElementById("mobile-nav-backdrop");
    if (!navToggle || !navPanel || !navBackdrop || navToggle.dataset.bound === "true") return;

    navToggle.dataset.bound = "true";
    var lastActiveElement = null;

    function openNav() {
      lastActiveElement = document.activeElement;
      body.classList.add("nav-open");
      navToggle.setAttribute("aria-expanded", "true");
      navPanel.setAttribute("aria-hidden", "false");
      navBackdrop.hidden = false;
      window.setTimeout(function () {
        navPanel.focus();
      }, 20);
    }

    function closeNav() {
      body.classList.remove("nav-open");
      navToggle.setAttribute("aria-expanded", "false");
      navPanel.setAttribute("aria-hidden", "true");
      navBackdrop.hidden = true;
      if (lastActiveElement && typeof lastActiveElement.focus === "function") {
        lastActiveElement.focus();
      }
    }

    navToggle.addEventListener("click", function () {
      if (body.classList.contains("nav-open")) {
        closeNav();
      } else {
        openNav();
      }
    });

    navBackdrop.addEventListener("click", closeNav);
    navPanel.querySelectorAll("a").forEach(function (link) {
      link.addEventListener("click", closeNav);
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && body.classList.contains("nav-open")) {
        closeNav();
      }
    });
  }

  function bindCopyButtons(root) {
    (root || document).querySelectorAll("[data-copy-url]").forEach(function (button) {
      if (button.dataset.bound === "true") return;
      button.dataset.bound = "true";
      button.addEventListener("click", async function (event) {
        event.preventDefault();
        event.stopPropagation();
        var copyUrl = button.getAttribute("data-copy-url");
        if (!copyUrl) return;
        try {
          await navigator.clipboard.writeText(copyUrl);
          var original = button.dataset.originalLabel || button.textContent;
          button.dataset.originalLabel = original;
          button.textContent = "Copied";
          window.setTimeout(function () {
            button.textContent = original;
          }, 1400);
        } catch (error) {
          window.prompt("Copy this link:", copyUrl);
        }
      });
    });
  }

  function bindCardLinks(root) {
    function shouldIgnoreClick(target) {
      return !!target.closest("a, button, form, input, select, textarea, label");
    }

    (root || document).querySelectorAll(".product-card[data-detail-url]").forEach(function (card) {
      if (card.dataset.bound === "true") return;
      card.dataset.bound = "true";

      function navigate() {
        var href = card.getAttribute("data-detail-url");
        if (href) window.location.href = href;
      }

      card.addEventListener("click", function (event) {
        if (shouldIgnoreClick(event.target)) return;
        navigate();
      });

      card.addEventListener("keydown", function (event) {
        if ((event.key !== "Enter" && event.key !== " ") || shouldIgnoreClick(event.target)) {
          return;
        }
        event.preventDefault();
        navigate();
      });
    });
  }

  function initFragment(root) {
    bindCopyButtons(root || document);
    bindCardLinks(root || document);
  }

  function init() {
    initMobileNav();
    initFlashes(document);
    initFragment(document);
  }

  PricePulse.init = init;
  PricePulse.initFragment = initFragment;
  PricePulse.dismissFlash = dismissFlash;
  window.PricePulse = PricePulse;

  document.addEventListener("DOMContentLoaded", init);
})();
