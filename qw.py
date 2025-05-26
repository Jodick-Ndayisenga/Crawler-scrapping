import os
import asyncio
import time
import json
from datetime import timedelta
from crawlee import ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawlingContext, PlaywrightCrawler
from dotenv import load_dotenv
import requests
from typing import List, Dict, Optional
from utils.google_maps_utils import google_map_consent_check

load_dotenv('.env')

# Configuration from .env
COUNTRY = os.getenv("COUNTRY", "usa_blockdata")
MACHINE_ID = os.getenv("MACHINE_ID")
TASK_SPREADER_API_URL = os.getenv("TASK_SPREADER_API_URL")
FETCHER_MIN_CONCURRENCY = int(os.getenv("FETCHER_MIN_CONCURRENCY", "5"))

if not TASK_SPREADER_API_URL:
    raise Exception("TASK_SPREADER_API_URL is not set in environment.")

queries = {"country": COUNTRY, "machine_id": MACHINE_ID, "queries": []}
CACHE_FILE = 'queries_cache.json'

# Initialize crawler with concurrency and timeout settings
crawler = PlaywrightCrawler(
    request_handler_timeout=timedelta(minutes=10),
    max_request_retries=2,
    concurrency_settings=ConcurrencySettings(min_concurrency=FETCHER_MIN_CONCURRENCY),
)

async def main():
    global queries
    print("Fetcher started")

    try:
        while True:
            urls_to_process = get_queries_to_process()
            if urls_to_process:
                await crawler.run(urls_to_process)
                push_results_to_db()
            else:
                print("No queries to process. Waiting before next fetch...")
                time.sleep(30)
    except Exception as e:
        handle_fatal_error(e)

def handle_fatal_error(e):
    """Centralized fatal error handler"""
    error_msg = str(e)
    messages = {
        "READ_TIMEOUT": "Script stopped because it failed to fetch queries after multiple retries (READ_TIMEOUT).",
        "CONNECT_TIMEOUT": "Script stopped because it failed to establish a connection after multiple retries (CONNECT_TIMEOUT).",
        "CONNECTION_ERROR": "Script stopped due to repeated connection errors while pushing results to DB.",
    }
    print(messages.get(error_msg, f"Unexpected error: {error_msg}"))
    raise SystemExit(1)

@crawler.router.default_handler
async def request_handler(context: PlaywrightCrawlingContext) -> None:
    url = context.request.url
    status = "failed"
    try:
        await google_map_consent_check(context)
        links = await scroll_and_extract_links(context)
        save_query_results(url, links)
        status = "processed"
    except Exception as e:
        context.log.error(f"Error processing page {url}: {str(e)}")
    finally:
        update_query_status(url, status)
        context.log.info(f"Finished processing {url} with status: {status}")

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

# ========== QUERY HANDLING ==========

def get_queries_to_process_from_db() -> List[Dict]:
    url = f"{TASK_SPREADER_API_URL}/queries?country={COUNTRY}&machine_id={MACHINE_ID}"
    for delay in [10, 20, 30]:
        try:
            response = requests.get(url, timeout=60)
            data = response.json()
            queries['queries'] = data.get('queries', [])
            cache_queries()
            print(f"Received {len(queries['queries'])} queries from database")
            return [q['query_url'] for q in queries['queries']]
        except requests.ReadTimeout:
            print("[ReadTimeout] Retrying...")
        except requests.ConnectTimeout:
            print("[ConnectTimeout] Retrying...")
        except requests.RequestException as e:
            print(f"[RequestException]: {e}")
        time.sleep(delay)
    return []


def get_queries_to_process() -> List[str]:
    return get_queries_to_process_from_db()

# ========== RESULTS HANDLING ==========

def get_query_from_queries(query_url: str) -> dict:
    for query in queries['queries']:
        base_url = query['query_url'].replace('?hl=en', '').replace('&', '%26')
        if query_url.startswith(base_url):
            return query
    raise Exception(f"Query {query_url} not found in queries")

def update_query_status(query_url: str, status: str):
    try:
        query = get_query_from_queries(query_url)
        query['status'] = status
        cache_queries()
    except Exception as e:
        print(f"Failed to update query status: {e}")

def save_query_results(query_url: str, links: list):
    try:
        query = get_query_from_queries(query_url)
        query['results'] = links
        cache_queries()
    except Exception as e:
        print(f"Failed to save results for {query_url}: {e}")

def count_processed_results() -> int:
    return sum(len(q['results']) for q in queries['queries'] if q.get('status') == "processed")

def push_results_to_db():
    num_results = count_processed_results()
    print(f"Pushing {num_results} results to database...")

    url = f"{TASK_SPREADER_API_URL}/queries/results"
    for delay in [10, 20, 30]:
        try:
            res = requests.post(url, json=queries, timeout=120)
            if res.status_code == 200:
                print("Results pushed successfully.")
                clear_queries()
                return
            else:
                print(f"Server returned code {res.status_code}. Retrying...")
        except requests.ConnectionError:
            print("Connection error. Retrying...")
        time.sleep(delay)
    raise Exception("CONNECTION_ERROR")

def cache_queries():
    try:
        with open(CACHE_FILE + ".tmp", 'w') as f:
            json.dump(queries, f, indent=2, ensure_ascii=False)
        os.replace(CACHE_FILE + ".tmp", CACHE_FILE)
    except Exception as e:
        print(f"Error writing cache: {e}")

def clear_queries():
    queries['queries'] = []
    cache_queries()

# ========== START SCRIPT ==========

if __name__ == "__main__":
    asyncio.run(main())