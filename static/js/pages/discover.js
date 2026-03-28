(function () {
  function initDiscoverPage() {
    var form = document.getElementById("discover-form");
    var queryInput = document.getElementById("search-query");
    var priceInput = document.getElementById("max-price");
    var hiddenCat = document.getElementById("hidden-category-id");
    var overlay = document.getElementById("loading-overlay");
    var loadText = document.getElementById("loading-text");
    var loadSub = document.getElementById("loading-sub");
    var chipWrap = document.getElementById("discover-source-chips");
    var allInput = document.getElementById("discover-search-all-sources");
    var loadingTimer = null;

    if (!form || !queryInput || form.dataset.bound === "true") return;
    form.dataset.bound = "true";

    function sourceIdInputs() {
      return form.querySelectorAll("input.discover-source-id-cb");
    }

    function applyAllMode(on) {
      if (!chipWrap || !allInput) return;
      chipWrap.classList.toggle("discover-source-chips--all-mode", on);
      sourceIdInputs().forEach(function (inp) {
        inp.disabled = on;
        if (on) inp.checked = false;
      });
    }

    if (allInput) {
      allInput.addEventListener("change", function () {
        applyAllMode(allInput.checked);
      });
    }

    sourceIdInputs().forEach(function (inp) {
      inp.addEventListener("change", function () {
        if (inp.checked && allInput) allInput.checked = false;
        if (allInput && !allInput.checked) applyAllMode(false);
      });
    });

    if (allInput) applyAllMode(!!allInput.checked);

    var STORAGE_KEY = "pp_recent_searches";
    var MAX_RECENT = 8;

    document.querySelectorAll(".chip[data-query]").forEach(function (chip) {
      chip.addEventListener("click", function () {
        queryInput.value = chip.getAttribute("data-query");
        queryInput.focus();
        hiddenCat.value = "";
      });
    });

    function getRecent() {
      try {
        var raw = localStorage.getItem(STORAGE_KEY);
        return raw ? JSON.parse(raw) : [];
      } catch (e) {
        return [];
      }
    }

    function saveRecent(list) {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
      } catch (e) {}
    }

    function addRecent(query, maxPrice) {
      var list = getRecent().filter(function (r) {
        return r.q.toLowerCase() !== query.toLowerCase();
      });
      list.unshift({ q: query, p: maxPrice || "" });
      if (list.length > MAX_RECENT) list = list.slice(0, MAX_RECENT);
      saveRecent(list);
    }

    function renderRecent() {
      var section = document.getElementById("recent-section");
      var row = document.getElementById("recent-chips");
      if (!section || !row) return;
      var list = getRecent();

      if (!list.length) {
        section.style.display = "none";
        return;
      }

      section.style.display = "";
      var label = row.querySelector(".chip-label");
      row.innerHTML = "";
      if (label) row.appendChild(label);

      list.forEach(function (r) {
        var el = document.createElement("span");
        el.className = "chip chip-ghost";
        el.textContent = r.q + (r.p ? " <=$" + r.p : "");
        el.addEventListener("click", function () {
          queryInput.value = r.q;
          if (r.p) priceInput.value = r.p;
          queryInput.focus();
          hiddenCat.value = "";
        });
        row.appendChild(el);
      });

      var clear = document.createElement("span");
      clear.className = "clear-link";
      clear.textContent = "Clear";
      clear.addEventListener("click", function () {
        localStorage.removeItem(STORAGE_KEY);
        renderRecent();
      });
      row.appendChild(clear);
    }

    renderRecent();

    function buildLoadingMessages(query, names, useAll) {
      var messages = [];
      messages.push(query ? 'Launching search for "' + query + '"...' : "Launching search...");
      messages.push("Building the live results view...");
      if (useAll) {
        messages.push(names.length ? "Preparing all " + names.length + " stores..." : "Preparing certified stores...");
      } else if (names.length > 1) {
        messages.push("Preparing " + names.length + " stores...");
      }
      names.slice(0, 4).forEach(function (name) {
        messages.push("Queueing " + name + "...");
      });
      messages.push("Results will appear as each store finishes.");
      return messages;
    }

    function startLoadingMessages(query, names, useAll) {
      if (!overlay || !loadText || !loadSub) return;
      var messages = buildLoadingMessages(query, names, useAll);
      var idx = 0;
      if (loadingTimer) window.clearInterval(loadingTimer);
      loadSub.textContent = "You’ll land on the results page immediately, and it will keep updating while stores finish.";
      loadText.textContent = messages[0];
      loadingTimer = window.setInterval(function () {
        idx = (idx + 1) % messages.length;
        loadText.textContent = messages[idx];
      }, 1800);
    }

    form.addEventListener("submit", function (event) {
      var useAll = allInput && allInput.checked;
      if (!useAll) {
        var anySource = false;
        sourceIdInputs().forEach(function (inp) {
          if (!inp.disabled && inp.checked) anySource = true;
        });
        if (!anySource) {
          event.preventDefault();
          alert("Select at least one store, or turn on All registered stores.");
          return;
        }
      }

      var q = queryInput.value.trim();
      if (q) addRecent(q, priceInput.value.trim());
      if (overlay) overlay.classList.add("active");

      var names = [];
      if (useAll) {
        sourceIdInputs().forEach(function (inp) {
          names.push(inp.getAttribute("data-name"));
        });
      } else {
        sourceIdInputs().forEach(function (inp) {
          if (!inp.disabled && inp.checked) names.push(inp.getAttribute("data-name"));
        });
      }
      startLoadingMessages(q, names, useAll);
    });

    var toggle = document.getElementById("cat-toggle");
    var grid = document.getElementById("cat-grid");
    if (toggle && grid) {
      toggle.addEventListener("click", function () {
        toggle.classList.toggle("open");
        grid.classList.toggle("visible");
      });
    }

    var openParent = null;
    document.querySelectorAll(".cat-card").forEach(function (card) {
      card.addEventListener("click", function () {
        var pid = card.getAttribute("data-parent-id");
        var childPanel = document.querySelector('.cat-children[data-for-parent="' + pid + '"]');

        if (openParent && openParent !== pid) {
          var prev = document.querySelector('.cat-card[data-parent-id="' + openParent + '"]');
          var prevPanel = document.querySelector('.cat-children[data-for-parent="' + openParent + '"]');
          if (prev) prev.classList.remove("active");
          if (prevPanel) prevPanel.classList.remove("visible");
        }

        if (openParent === pid) {
          card.classList.remove("active");
          if (childPanel) childPanel.classList.remove("visible");
          openParent = null;
        } else {
          card.classList.add("active");
          if (childPanel) childPanel.classList.add("visible");
          openParent = pid;
        }
      });
    });

    document.querySelectorAll(".cat-child-chip").forEach(function (chip) {
      chip.addEventListener("click", function (event) {
        event.stopPropagation();
        queryInput.value = chip.getAttribute("data-keywords");
        hiddenCat.value = chip.getAttribute("data-cat-id");
        form.requestSubmit();
      });
    });
  }

  document.addEventListener("DOMContentLoaded", initDiscoverPage);
})();
