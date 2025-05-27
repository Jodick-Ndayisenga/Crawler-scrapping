import os
import asyncio
import time
import requests
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
from typing import List, Dict
import gc
import psutil
from crawlee import ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawlingContext, PlaywrightCrawler
from utils.google_maps_utils import google_map_consent_check  # You must implement this

load_dotenv('.env')

# Configuration from environment
FETCHER_MIN_CONCURRENCY = int(os.getenv("FETCHER_MIN_CONCURRENCY", "1"))
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB")
MACHINE_ID = os.getenv("MACHINE_ID", "2")
API_URL = "http://82.112.254.77:8000/queries?country=usa_blockdata&machine_id=2"

# MongoDB setup
mongo_client = MongoClient(
    f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_DB}"
)
mongo_db = mongo_client[MONGO_DB]
collection = mongo_db["business_links"]

# Global storage
query_metadata = {}
place_links_temp: List[str] = []

import shutil
if os.path.exists('./storage'):
    shutil.rmtree('./storage')
# Crawler setup
concurrency = ConcurrencySettings(
    min_concurrency=FETCHER_MIN_CONCURRENCY,
    max_concurrency=FETCHER_MIN_CONCURRENCY + 2,
)

crawler = PlaywrightCrawler(
    concurrency_settings=concurrency,
    request_handler_timeout=timedelta(minutes=10),
    max_request_retries=2,
)

lock = asyncio.Lock()
MAX_PARALLEL_QUERIES = 2  # Limit parallel queries to avoid overloading
async def main():
    print("🚀 Starting Google Maps scraper")
    asyncio.create_task(log_memory_usage())

    while True:
        try:
            queries = await fetch_queries()
            documents = []

            sem = asyncio.Semaphore(MAX_PARALLEL_QUERIES)

            async def handle_query(query):
                async with sem:
                    place_urls = await run_single_query_with_new_crawler(query)
                    if place_urls:
                        documents.append({
                            "query_id": query["id"],
                            "industry": query["industry"],
                            "latitude": query["latitude"],
                            "longitude": query["longitude"],
                            "zoom_level": query["zoom_level"],
                            "machine_id": MACHINE_ID,
                            "source_query_url": query["query_url"],
                            "place_urls": place_urls,
                            "timestamp": datetime.utcnow(),
                            "status": "processed",
                        })

            await asyncio.gather(*(handle_query(q) for q in queries))

            if documents:
                collection.insert_many(documents)
                print(f"✅ Inserted {len(documents)} documents into MongoDB.")
        except Exception as e:
            print(f"❌ Error in main loop: {e}")

        await asyncio.sleep(10)

async def run_single_query_with_new_crawler(query: Dict) -> List[str]:
    links = []
    lock = asyncio.Lock()

    crawler_instance = PlaywrightCrawler(
        concurrency_settings=ConcurrencySettings(min_concurrency=1, max_concurrency=3),
        request_handler_timeout=timedelta(minutes=10),
        max_request_retries=2,
    )

    @crawler_instance.router.default_handler
    async def handle_request(context: PlaywrightCrawlingContext):
        try:
            await google_map_consent_check(context)
            place_urls = await scroll_and_extract_links(context)

            async with lock:
                links.extend(place_urls)

            context.log.info(f"✅ Scraped {len(place_urls)} links from {context.request.url}")
        except Exception as e:
            context.log.error(f"❌ Error scraping {context.request.url}: {e}")
        finally:
            await context.page.close()
            gc.collect()

    await crawler_instance.run([query["query_url"]])
    return links

async def fetch_queries() -> List[Dict]:
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()["queries"]

async def run_single_query(query: Dict) -> List[str]:
    global query_metadata, place_links_temp
    query_metadata = query
    place_links_temp = []
    await crawler.run([query["query_url"]])
    return place_links_temp

@crawler.router.default_handler
async def handle_request(context: PlaywrightCrawlingContext):
    url = context.request.url
    context.log.info(f"Scraping: {url}")

    try:
        await google_map_consent_check(context)
        place_urls = await scroll_and_extract_links(context)

        async with lock:
            place_links_temp.extend(place_urls)

        context.log.info(f"✅ Scraped {len(place_urls)} links from {url}")
    except Exception as e:
        context.log.error(f"❌ Error scraping {url}: {e}")
    finally:
        await context.page.close()
        gc.collect()

async def scroll_and_extract_links(context: PlaywrightCrawlingContext) -> list:
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

        # Extract new links
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

async def log_memory_usage():
    while True:
        mem = psutil.virtual_memory()
        used_gb = mem.used / (1024 ** 3)
        total_gb = mem.total / (1024 ** 3)
        print(f"[Memory] Used: {used_gb:.2f} GB / Total: {total_gb:.2f} GB")
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
