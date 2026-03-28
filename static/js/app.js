(function () {
  var PricePulse = window.PricePulse || {};
  var activeCustomSelect = null;
  var customSelectEventsBound = false;

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

  function closeActiveCustomSelect(restoreFocus) {
    if (!activeCustomSelect) return;
    var instance = activeCustomSelect;
    activeCustomSelect = null;
    instance.root.classList.remove("is-open");
    instance.trigger.setAttribute("aria-expanded", "false");
    instance.menu.hidden = true;
    if (restoreFocus && instance.trigger && typeof instance.trigger.focus === "function") {
      instance.trigger.focus();
    }
  }

  function bindCustomSelectGlobals() {
    if (customSelectEventsBound) return;
    customSelectEventsBound = true;

    document.addEventListener("mousedown", function (event) {
      if (!activeCustomSelect) return;
      if (!activeCustomSelect.root.contains(event.target)) {
        closeActiveCustomSelect(false);
      }
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && activeCustomSelect) {
        event.preventDefault();
        closeActiveCustomSelect(true);
      }
    });
  }

  function syncCustomSelect(select, rebuild) {
    if (select && typeof select._ppSync === "function") {
      select._ppSync(!!rebuild);
    }
  }

  function initCustomSelects(root) {
    bindCustomSelectGlobals();

    (root || document).querySelectorAll("select[data-pp-select]").forEach(function (select) {
      if (select.dataset.ppEnhanced === "true") {
        syncCustomSelect(select, false);
        return;
      }
      select.dataset.ppEnhanced = "true";

      var variant = select.getAttribute("data-pp-select") || "default";
      var controlId = select.id ? select.id + "-control" : "";
      var selectedIndex = select.selectedIndex >= 0 ? select.selectedIndex : 0;

      var wrapper = document.createElement("div");
      wrapper.className = "pp-select pp-select--" + variant;
      wrapper.setAttribute("data-pp-select-root", "");

      var trigger = document.createElement("button");
      trigger.type = "button";
      trigger.className = "pp-select-trigger";
      trigger.setAttribute("aria-haspopup", "listbox");
      trigger.setAttribute("aria-expanded", "false");
      if (controlId) {
        trigger.id = controlId;
      }
      trigger.innerHTML =
        '<span class="pp-select-label"></span>' +
        '<span class="pp-select-chevron" aria-hidden="true">' +
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">' +
        '<polyline points="6 9 12 15 18 9"></polyline>' +
        "</svg>" +
        "</span>";

      var menu = document.createElement("div");
      menu.className = "pp-select-menu";
      menu.hidden = true;
      menu.setAttribute("role", "listbox");
      if (select.id) {
        menu.id = select.id + "-menu";
        trigger.setAttribute("aria-controls", menu.id);
      }

      wrapper.appendChild(trigger);
      wrapper.appendChild(menu);
      select.insertAdjacentElement("afterend", wrapper);
      select.classList.add("pp-native-select", "is-enhanced");

      var externalLabel = select.id ? document.querySelector('label[for="' + select.id + '"]') : null;
      if (externalLabel) {
        if (!externalLabel.id) {
          externalLabel.id = select.id + "-label";
        }
        externalLabel.htmlFor = controlId;
        trigger.setAttribute("aria-labelledby", externalLabel.id + " " + controlId);
      }

      function enabledOptions() {
        return Array.from(menu.querySelectorAll(".pp-select-option:not([disabled])"));
      }

      function selectedOptionButton() {
        return menu.querySelector('.pp-select-option[aria-selected="true"]');
      }

      function focusOption(target) {
        if (target && typeof target.focus === "function") {
          target.focus();
        }
      }

      function moveFocus(step) {
        var options = enabledOptions();
        if (!options.length) return;
        var activeIndex = options.indexOf(document.activeElement);
        if (activeIndex === -1) {
          var selected = selectedOptionButton();
          activeIndex = selected ? options.indexOf(selected) : 0;
        }
        activeIndex = (activeIndex + step + options.length) % options.length;
        focusOption(options[activeIndex]);
      }

      function openMenu(focusSelected) {
        if (activeCustomSelect && activeCustomSelect.root !== wrapper) {
          closeActiveCustomSelect(false);
        }
        activeCustomSelect = { root: wrapper, trigger: trigger, menu: menu };
        wrapper.classList.add("is-open");
        trigger.setAttribute("aria-expanded", "true");
        menu.hidden = false;
        if (focusSelected) {
          focusOption(selectedOptionButton() || enabledOptions()[0]);
        }
      }

      function closeMenu(restoreFocus) {
        if (activeCustomSelect && activeCustomSelect.root === wrapper) {
          closeActiveCustomSelect(restoreFocus);
          return;
        }
        wrapper.classList.remove("is-open");
        trigger.setAttribute("aria-expanded", "false");
        menu.hidden = true;
        if (restoreFocus) {
          trigger.focus();
        }
      }

      function renderOptions() {
        var labelNode = trigger.querySelector(".pp-select-label");
        menu.innerHTML = "";
        Array.from(select.options).forEach(function (option, index) {
          var optionButton = document.createElement("button");
          optionButton.type = "button";
          optionButton.className = "pp-select-option";
          optionButton.setAttribute("role", "option");
          optionButton.setAttribute("data-value", option.value);
          optionButton.setAttribute("data-index", String(index));
          optionButton.textContent = option.textContent;
          if (option.disabled) {
            optionButton.disabled = true;
          }
          if (index === selectedIndex) {
            optionButton.setAttribute("aria-selected", "true");
            optionButton.classList.add("is-selected");
          } else {
            optionButton.setAttribute("aria-selected", "false");
          }
          optionButton.addEventListener("click", function () {
            if (option.disabled) return;
            if (select.selectedIndex !== index) {
              select.selectedIndex = index;
              selectedIndex = index;
              select.dispatchEvent(new Event("change", { bubbles: true }));
            } else {
              syncCustomSelect(select, false);
            }
            closeMenu(true);
          });
          menu.appendChild(optionButton);
        });
        var selectedOption = select.options[select.selectedIndex >= 0 ? select.selectedIndex : 0];
        labelNode.textContent = selectedOption ? selectedOption.textContent : "";
      }

      select._ppSync = function (rebuild) {
        selectedIndex = select.selectedIndex >= 0 ? select.selectedIndex : 0;
        if (rebuild) {
          renderOptions();
          return;
        }
        var labelNode = trigger.querySelector(".pp-select-label");
        var selectedOption = select.options[selectedIndex];
        labelNode.textContent = selectedOption ? selectedOption.textContent : "";
        menu.querySelectorAll(".pp-select-option").forEach(function (optionButton) {
          var isSelected = parseInt(optionButton.getAttribute("data-index"), 10) === selectedIndex;
          optionButton.setAttribute("aria-selected", isSelected ? "true" : "false");
          optionButton.classList.toggle("is-selected", isSelected);
        });
        trigger.disabled = !!select.disabled;
      };

      renderOptions();
      select._ppSync(false);
      select.addEventListener("change", function () {
        select._ppSync(false);
      });

      trigger.addEventListener("click", function () {
        if (wrapper.classList.contains("is-open")) {
          closeMenu(false);
        } else {
          openMenu(false);
        }
      });

      trigger.addEventListener("keydown", function (event) {
        if (event.key === "ArrowDown") {
          event.preventDefault();
          openMenu(true);
        } else if (event.key === "ArrowUp") {
          event.preventDefault();
          openMenu(true);
          var options = enabledOptions();
          if (options.length) {
            focusOption(options[options.length - 1]);
          }
        } else if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          if (wrapper.classList.contains("is-open")) {
            closeMenu(false);
          } else {
            openMenu(false);
          }
        }
      });

      menu.addEventListener("keydown", function (event) {
        if (event.key === "ArrowDown") {
          event.preventDefault();
          moveFocus(1);
        } else if (event.key === "ArrowUp") {
          event.preventDefault();
          moveFocus(-1);
        } else if (event.key === "Home") {
          event.preventDefault();
          focusOption(enabledOptions()[0]);
        } else if (event.key === "End") {
          event.preventDefault();
          var options = enabledOptions();
          focusOption(options[options.length - 1]);
        } else if (event.key === "Tab") {
          closeMenu(false);
        }
      });
    });
  }

  function initFragment(root) {
    initCustomSelects(root || document);
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
  PricePulse.initCustomSelects = initCustomSelects;
  PricePulse.syncCustomSelect = syncCustomSelect;
  PricePulse.dismissFlash = dismissFlash;
  window.PricePulse = PricePulse;

  document.addEventListener("DOMContentLoaded", init);
})();
