from bs4 import BeautifulSoup

from scraper import _amazon_item_title


def test_amazon_title_extractor_prefers_product_link_and_keeps_brand():
    html = """
    <div data-component-type="s-search-result">
      <h2 class="a-size-mini s-line-clamp-1">
        <span class="a-size-medium a-color-base">Apple</span>
      </h2>
      <div class="a-section a-spacing-none puis-padding-right-small s-title-instructions-style">
        <a class="a-link-normal s-line-clamp-2 puis-line-clamp-3-for-col-4-and-8 s-link-style a-text-normal"
           href="/dp/example">
          AirPods Pro 3 Wireless Earbuds, Active Noise Cancellation
        </a>
      </div>
    </div>
    """
    item = BeautifulSoup(html, "html.parser").select_one('[data-component-type="s-search-result"]')
    link = item.select_one('a[href="/dp/example"]')
    assert _amazon_item_title(item, link) == (
        "Apple AirPods Pro 3 Wireless Earbuds, Active Noise Cancellation"
    )
