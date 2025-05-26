import os
import re
import json
import time
import requests
import asyncio

from datetime import datetime, timedelta
from urllib.parse import urlparse
from dotenv import load_dotenv

from crawlee import ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

from utils.enums import Status
from utils.google_maps_utils import google_map_consent_check

load_dotenv('.env')

COUNTRY = os.getenv("COUNTRY", "usa_blockdata")
MACHINE_ID = os.getenv("MACHINE_ID", None)
TASK_SPREADER_API_URL = os.getenv("TASK_SPREADER_API_URL")
FETCHER_MIN_CONCURRENCY = os.getenv("FETCHER_MIN_CONCURRENCY", 5)

if not TASK_SPREADER_API_URL:
    raise Exception("TASK_SPREADER_API_URL is not set")

crawler = PlaywrightCrawler(
    request_handler_timeout=timedelta(minutes=10),
    max_request_retries=1,
    concurrency_settings=ConcurrencySettings(min_concurrency=int(FETCHER_MIN_CONCURRENCY)),
)

business_data_batch = []

@crawler.router.default_handler
async def request_handler(context: PlaywrightCrawlingContext):
    url = context.request.url
    await google_map_consent_check(context)

    if "google.com/maps/search" in url:
        links = await scroll_to_bottom_results_section(context)
        print(f"Discovered {len(links)} business links from {url}")
        for link in links:
            await crawler.add_requests([{'url': link}])
    else:
        try:
            data = await process_business(context)
            if data:
                data['status'] = Status.PROCESSED.value
                business_data_batch.append(data)

                # Send immediately to avoid batching indefinitely
                response = requests.post(
                    f"{TASK_SPREADER_API_URL}/queries/results",
                    json=data,
                    timeout=30
                )
                if response.status_code != 200:
                    print(f"Failed to save {data['url']}: {response.text}")
        except Exception as e:
            print(f"Error processing business page {url}: {e}")

def get_query_urls():
    retries = [10, 20, 30]
    url = f"{TASK_SPREADER_API_URL}/queries?country={COUNTRY}&machine_id={MACHINE_ID}"

    for i, delay in enumerate(retries):
        try:
            response = requests.get(url, timeout=60)
            data = response.json()
            return [q['query_url'] for q in data.get('queries', [])]
        except Exception as e:
            print(f"Retrying ({i + 1}) after error: {e}")
            time.sleep(delay)
    return []

async def scroll_to_bottom_results_section(context: PlaywrightCrawlingContext):
    selector = '[role="feed"]'
    scrollable_section = await context.page.query_selector(selector)

    if not scrollable_section:
        context.log.info("Could not find scrollable section with selector '[role=\"feed\"]'")
        return []

    links = []
    seen_links = set()

    for _ in range(50):
        await scrollable_section.evaluate('(el) => el.scrollBy(0, el.scrollHeight)')
        await asyncio.sleep(1)

        new_links = await scrollable_section.evaluate('''() => {
            return Array.from(document.querySelectorAll("a"))
                .map(a => a.href)
                .filter(h => h.startsWith("https://www.google.com/maps/place"));
        }''')

        for link in new_links:
            if link not in seen_links:
                seen_links.add(link)
                links.append(link)

        end_signal = await scrollable_section.query_selector("span.HlvSq")
        if end_signal:
            text = await scrollable_section.evaluate('() => document.querySelector("span.HlvSq").innerText')
            if "reached the end" in text.lower():
                break
    return links

def parse_coordinate_from_map_url(url):
    try:
        url_obj = urlparse(url)
        match = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url_obj.path)
        if match:
            return {'latitude': float(match.group(1)), 'longitude': float(match.group(2))}
    except Exception as e:
        print(f"Coordinate parsing failed: {e}")
    return None

async def process_business(context: PlaywrightCrawlingContext) -> dict:
    page = context.page
    url = context.request.url

    title_el = await page.query_selector("h1")
    title = await title_el.inner_text() if title_el else None

    review_el = await page.query_selector("div.F7nice")
    review_text = await review_el.inner_text() if review_el else ""
    if review_text:
        star = float(review_text.split("(")[0].strip() or 0)
        review_count = int(review_text.split("(")[1].split(")")[0]) if "(" in review_text else 0
    else:
        star = review_count = None

    category_el = await page.query_selector("button.DkEaL")
    category = await category_el.inner_text() if category_el else None

    address_el = await page.query_selector("button[data-item-id='address']")
    address = (await address_el.get_attribute("aria-label")).replace("Address: ", "") if address_el else ""

    open_hours_el = await page.query_selector("div[aria-label*='Sunday']")
    open_hours = await open_hours_el.get_attribute("aria-label") if open_hours_el else None

    website_el = await page.query_selector("a[data-item-id='authority']")
    website = await website_el.get_attribute("href") if website_el else None

    phone_el = await page.query_selector("button[aria-label*='Phone']")
    phone = await phone_el.inner_text() if phone_el else None
    phone = re.sub(r'^[^+]*', '', phone).strip() if phone else None

    pluscode_el = await page.query_selector("button[aria-label*='Plus code']")
    pluscode = await pluscode_el.inner_text() if pluscode_el else None

    coordinates = parse_coordinate_from_map_url(url)

    return {
        'url': url,
        'title': title,
        'star_rating': star,
        'review_count': review_count,
        'category': category,
        'address': address,
        'open_hours': open_hours,
        'phone': phone,
        'website': website,
        'pluscode': pluscode,
        'coordinates': coordinates
    }

async def main():
    print("Continuous business crawler running...")
    while True:
        urls = get_query_urls()
        if not urls:
            print("No new queries. Sleeping 60s...")
            await asyncio.sleep(60)
            continue
        await crawler.run(urls)

if __name__ == "__main__":
    asyncio.run(main())