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

    function getResultsGrid() {
      return resultsSlot ? resultsSlot.querySelector("#results-grid") : null;
    }

    function detectSort() {
      var active = resultsSlot ? resultsSlot.querySelector(".sort-btn.active[data-sort]") : null;
      return active ? active.getAttribute("data-sort") : currentSort;
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
      if (resultsSlot) {
        resultsSlot.querySelectorAll(".sort-btn[data-sort]").forEach(function (btn) {
          btn.classList.toggle("active", btn.getAttribute("data-sort") === key);
        });
      }
    }

    function bindSortButtons() {
      if (!resultsSlot || resultsSlot.dataset.sortBound === "true") return;
      resultsSlot.dataset.sortBound = "true";
      resultsSlot.addEventListener("click", function (event) {
        var button = event.target.closest(".sort-btn[data-sort]");
        if (!button) return;
        sortCards(button.getAttribute("data-sort"));
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
      bindSortButtons();
      var sortKey = currentSort || detectSort();
      if (sortKey) sortCards(sortKey);
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

    bindSortButtons();
    currentSort = detectSort();
    if (currentSort) sortCards(currentSort);

    if (searchStatus === "queued" || searchStatus === "running") {
      scheduleNext(300);
    }
  }

  document.addEventListener("DOMContentLoaded", initDiscoveryResults);
})();
