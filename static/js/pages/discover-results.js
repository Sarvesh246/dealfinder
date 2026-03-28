(function () {
  function initDiscoveryResults() {
    var shell = document.querySelector("[data-discovery-shell]");
    if (!shell || shell.dataset.bound === "true") return;
    shell.dataset.bound = "true";

    var progressSlot = document.getElementById("discovery-progress-slot");
    var resultsSlot = document.getElementById("discovery-results-slot");
    var statusUrl = shell.getAttribute("data-status-url");
    var pollInterval = parseInt(shell.getAttribute("data-poll-interval") || "1200", 10);
    var searchStatus = shell.getAttribute("data-search-status") || "completed";
    var pollTimer = null;
    var currentSort = null;
    var currentSourceFilter = null;

    function getResultsGrid() {
      return resultsSlot ? resultsSlot.querySelector("#results-grid") : null;
    }

    function getSortSelect() {
      return resultsSlot ? resultsSlot.querySelector("[data-results-sort]") : null;
    }

    function getSourceSelect() {
      return resultsSlot ? resultsSlot.querySelector("[data-results-source]") : null;
    }

    function getSourceEmpty() {
      return resultsSlot ? resultsSlot.querySelector("[data-source-empty]") : null;
    }

    function detectSort() {
      var select = getSortSelect();
      return select ? select.value : currentSort;
    }

    function detectSourceFilter() {
      var select = getSourceSelect();
      return select ? select.value : (currentSourceFilter || "");
    }

    function sortCards(key) {
      var grid = getResultsGrid();
      if (!grid || !key) return;
      currentSort = key;
      var cards = Array.from(grid.children);
      cards.sort(function (a, b) {
        var aVerify = parseFloat(a.dataset.verifyRank) || 2;
        var bVerify = parseFloat(b.dataset.verifyRank) || 2;
        if (aVerify !== bVerify) return aVerify - bVerify;
        var aVal = parseFloat(a.dataset[key]) || 0;
        var bVal = parseFloat(b.dataset[key]) || 0;
        if (key === "price") return aVal - bVal;
        return bVal - aVal;
      });
      cards.forEach(function (card) {
        grid.appendChild(card);
      });
    }

    function applySourceFilter(sourceId) {
      var grid = getResultsGrid();
      if (!grid) return;
      currentSourceFilter = sourceId || "";
      var visibleCount = 0;
      Array.from(grid.children).forEach(function (card) {
        var matches = !currentSourceFilter || card.dataset.sourceId === currentSourceFilter;
        card.hidden = !matches;
        card.classList.toggle("is-hidden-by-filter", !matches);
        if (matches) visibleCount += 1;
      });
      var empty = getSourceEmpty();
      if (empty) {
        empty.hidden = visibleCount !== 0;
      }
    }

    function syncControlValues() {
      var sortSelect = getSortSelect();
      if (sortSelect && currentSort) {
        sortSelect.value = currentSort;
        if (window.PricePulse && typeof window.PricePulse.syncCustomSelect === "function") {
          window.PricePulse.syncCustomSelect(sortSelect);
        }
      }
      var sourceSelect = getSourceSelect();
      if (sourceSelect && currentSourceFilter !== null) {
        sourceSelect.value = currentSourceFilter;
        if (window.PricePulse && typeof window.PricePulse.syncCustomSelect === "function") {
          window.PricePulse.syncCustomSelect(sourceSelect);
        }
      }
    }

    function bindControls() {
      if (!resultsSlot || resultsSlot.dataset.controlsBound === "true") return;
      resultsSlot.dataset.controlsBound = "true";

      resultsSlot.addEventListener("change", function (event) {
        var target = event.target;
        if (!(target instanceof HTMLSelectElement)) return;
        if (target.matches("[data-results-sort]")) {
          sortCards(target.value);
          applySourceFilter(currentSourceFilter !== null ? currentSourceFilter : detectSourceFilter());
          return;
        }
        if (target.matches("[data-results-source]")) {
          applySourceFilter(target.value);
        }
      });

      resultsSlot.addEventListener("submit", function (event) {
        var form = event.target;
        if (!(form instanceof HTMLFormElement) || !form.matches(".track-form")) return;
        stopPolling();
        form.querySelectorAll("button[type='submit']").forEach(function (button) {
          if (!(button instanceof HTMLButtonElement)) return;
          button.disabled = true;
          button.setAttribute("aria-busy", "true");
          button.classList.add("is-pending");
        });
      });
    }

    function applyHtml(payload) {
      if (progressSlot && typeof payload.progress_html === "string") {
        progressSlot.innerHTML = payload.progress_html;
      }
      if (resultsSlot && typeof payload.results_html === "string") {
        resultsSlot.innerHTML = payload.results_html;
      }
      if (window.PricePulse && typeof window.PricePulse.initFragment === "function") {
        window.PricePulse.initFragment(progressSlot || document);
        window.PricePulse.initFragment(resultsSlot || document);
      }
      bindControls();
      syncControlValues();
      var sortKey = currentSort !== null ? currentSort : detectSort();
      if (sortKey) sortCards(sortKey);
      var sourceFilter = currentSourceFilter !== null ? currentSourceFilter : detectSourceFilter();
      applySourceFilter(sourceFilter);
    }

    function stopPolling() {
      if (pollTimer) {
        window.clearTimeout(pollTimer);
        pollTimer = null;
      }
    }

    function scheduleNext(delay) {
      stopPolling();
      pollTimer = window.setTimeout(fetchStatus, delay);
    }

    async function fetchStatus() {
      if (!statusUrl) return;
      try {
        var response = await window.fetch(statusUrl, {
          headers: { "X-Requested-With": "XMLHttpRequest" },
          cache: "no-store",
        });
        if (!response.ok) {
          scheduleNext(Math.max(pollInterval, 2000));
          return;
        }
        var payload = await response.json();
        if (!payload || !payload.status) {
          scheduleNext(Math.max(pollInterval, 2000));
          return;
        }
        searchStatus = payload.status;
        shell.setAttribute("data-search-status", searchStatus);
        applyHtml(payload);
        if (searchStatus === "queued" || searchStatus === "running") {
          scheduleNext(payload.poll_interval_ms || pollInterval);
        } else {
          stopPolling();
        }
      } catch (error) {
        scheduleNext(Math.max(pollInterval, 2500));
      }
    }

    bindControls();
    currentSort = detectSort();
    currentSourceFilter = detectSourceFilter();
    if (currentSort) sortCards(currentSort);
    applySourceFilter(currentSourceFilter);

    window.addEventListener("pagehide", stopPolling, { once: true });

    if (searchStatus === "queued" || searchStatus === "running") {
      scheduleNext(300);
    }
  }

  document.addEventListener("DOMContentLoaded", initDiscoveryResults);
})();
