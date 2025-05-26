import os
import asyncio
import time
from datetime import timedelta
from crawlee import ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawlingContext, PlaywrightCrawler
from dotenv import load_dotenv
from utils.google_maps_utils import google_map_consent_check  # Assuming you have this helper
from typing import List

load_dotenv('.env')

# Configuration
FETCHER_MIN_CONCURRENCY = int(os.getenv("FETCHER_MIN_CONCURRENCY", "5"))
OUTPUT_FILE = 'scraped_links.txt'

# Stats tracking
start_time = None
total_links_scraped = 0
lock = asyncio.Lock()  # For thread-safe writes

# Initialize crawler
crawler = PlaywrightCrawler(
    request_handler_timeout=timedelta(minutes=10),
    max_request_retries=2,
    concurrency_settings=ConcurrencySettings(min_concurrency=FETCHER_MIN_CONCURRENCY),
)

async def main():
    global start_time
    start_time = time.time()

    print(f"Starting fetcher with concurrency={FETCHER_MIN_CONCURRENCY}")
    urls = get_test_urls()  # Replace with your input method if needed

    await crawler.run(urls)

    elapsed = time.time() - start_time
    lpm = total_links_scraped / (elapsed / 60) if elapsed else 0
    print(f"\n✅ Total links scraped: {total_links_scraped} | Speed: {lpm:.2f} links/min")

def get_test_urls():
    # Replace these with actual URLs you want to scrape
    return [
        "https://www.google.com/maps/search/restaurant/ @37.4056,-122.0775,15z/",
        "https://www.google.com/maps/search/coffee/ @40.7128,-74.0060,13z/",
    ]

@crawler.router.default_handler
async def request_handler(context: PlaywrightCrawlingContext) -> None:
    url = context.request.url
    try:
        await google_map_consent_check(context)
        links = await scroll_and_extract_links(context)

        async with lock:
            save_links_to_file(links)

        context.log.info(f"Scraped {len(links)} links from {url}")
    finally:
        await context.page.close()  # Close the page explicitly

async def scroll_and_extract_links(context: PlaywrightCrawlingContext) -> List[str]:
    selector = '[role="feed"]'
    scrollable_section = await context.page.query_selector(selector)
    if not scrollable_section:
        raise Exception("Could not find scrollable feed section")

    seen_links = set()
    last_scroll_top = -1
    retry_count = 0

    while retry_count < 5:
        current_scroll_top = await scrollable_section.evaluate('''element => {
            const scrollStep = element.scrollHeight * 0.9;
            element.scrollBy(0, scrollStep);
            return element.scrollTop;
        }''')

        if current_scroll_top == last_scroll_top:
            retry_count += 1
        else:
            retry_count = 0

        last_scroll_top = current_scroll_top

        new_links = await scrollable_section.evaluate('''() => {
            return Array.from(document.querySelectorAll('a[href]')).map(a => a.href);
        }''')

        new_links = [link for link in new_links if "/maps/place" in link]
        seen_links.update(new_links)

        end_signal = await scrollable_section.query_selector("span.HlvSq")
        if end_signal:
            end_text = await scrollable_section.evaluate('''() => document.querySelector("span.HlvSq").innerText''')
            if "You've reached the end" in end_text:
                break

        await asyncio.sleep(1.5)  # Throttle scrolling

    return list(seen_links)

def save_links_to_file(links: List[str]):
    global total_links_scraped
    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        for link in links:
            f.write(link + '\n')
    total_links_scraped += len(links)

# Run the scraper
if __name__ == "__main__":
    asyncio.run(main())