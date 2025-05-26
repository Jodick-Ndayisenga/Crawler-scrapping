import os
import asyncio
import time
import json

from datetime import timedelta
from crawler import crawler as main_crawler
import requests

from crawlee import ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

from dotenv import load_dotenv

from utils.enums import Status
from utils.google_maps_utils import google_map_consent_check


load_dotenv('.env')


COUNTRY = os.getenv("COUNTRY", "usa_blockdata")
MACHINE_ID = os.getenv("MACHINE_ID", None)
TASK_SPREADER_API_URL = os.getenv("TASK_SPREADER_API_URL")
FETCHER_MIN_CONCURRENCY = os.getenv("FETCHER_MIN_CONCURRENCY", 5) 

if not TASK_SPREADER_API_URL:
  raise Exception("TASK_SPREADER_API_URL is not set")

queries = {"country": COUNTRY, "machine_id": MACHINE_ID, "queries": []}

crawler = PlaywrightCrawler(
  request_handler_timeout=timedelta(minutes=10), 
  max_request_retries=1,
  concurrency_settings=ConcurrencySettings(
    min_concurrency=int(FETCHER_MIN_CONCURRENCY),
  ),
)

async def main():
  global queries
  print("Fetcher started")

  try:
    while True:
      queries_urls = get_queries_to_process()
      if queries_urls:
        await crawler.run(queries_urls)
        push_results_to_db()
  except Exception as e:
    error_msg = str(e)

    if error_msg == "READ_TIMEOUT":
      print("Script stopped because it failed to fetch queries after multiple retries (READ_TIMEOUT).")

    elif error_msg == "CONNECT_TIMEOUT":
      print("Script stopped because it failed to establish a connection after multiple retries (CONNECT_TIMEOUT).")

    elif error_msg.startswith("REQUEST_FAILED"):
      print(f"Script stopped due to repeated request errors: {error_msg}")

    elif error_msg == "CONNECTION_ERROR":
      print("Script stopped due to repeated connection errors while pushing results to DB.")

    else:
      print(f"Script stopped due to an unexpected error: {error_msg}")
      raise

@crawler.router.default_handler
async def request_handler(context: PlaywrightCrawlingContext) -> None:
  status = Status.FAILED.value
  context.log.info(f'Processing Url {context.page.url} ...')

  try:
    await google_map_consent_check(context)
    links = await scroll_to_bottom_results_section(context)
    links = links if links else []
    status = Status.PROCESSED.value
  except Exception as e:
    context.log.info(f"Error processing page: {e}")
    status = Status.FAILED.value
  finally:
    update_query_status(context.page.url, status)
  
  if status == Status.PROCESSED.value:
    save_query_results(context.page.url, links)

async def scroll_to_bottom_results_section(context: PlaywrightCrawlingContext):
  selector = '[role="feed"]'

  try:
    scrollable_section = await context.page.query_selector(selector)
    
    while True:
      await scrollable_section.evaluate('''(element) => {
        const scrollHeight = element.scrollHeight;
        const scrollStep = scrollHeight * 1;
        element.scrollBy(0, scrollStep);
        return element.scrollTop;
      }''')

      end_signal = await scrollable_section.query_selector("span.HlvSq")
      if end_signal:
        button = await scrollable_section.evaluate('() => document.querySelector("span.HlvSq").innerText')
        if "reached the end" in button:
          links = await scrollable_section.evaluate('''() => {
            return Array.from(document.querySelectorAll("a")).map(a => a.href);
          }''')
          # In scroll_to_bottom_results_section()
          links = [element for element in links if element.startswith("https://www.google.com/maps/place")]
          
          # Immediately add to crawler's queue instead of caching
          
          for link in links:
              main_crawler.add_requests([{'url': link}])
          return links
  
  except Exception as e:
    context.log.info(f"Error finding scrollable section: {e}")
    raise e
 
def get_queries_to_process():
  global queries

  queries_urls_to_process = get_queries_to_process_from_cache()
  if not queries_urls_to_process:
    queries_urls_to_process = get_queries_to_process_from_db()

  return queries_urls_to_process

def get_queries_to_process_from_db():
  global queries

  retries = [10, 20, 30]  # wait times in seconds
  url = f"{TASK_SPREADER_API_URL}/queries?country={COUNTRY}&machine_id={MACHINE_ID}"

  for i, delay in enumerate(retries):
    try:
      response = requests.get(url, timeout=60)
      data = response.json()
      queries['queries'] = data['queries']
      cache_queries()
      print(f"Received {len(data['queries'])} queries from database")
      return [query['query_url'] for query in data['queries']]
    
    except requests.ReadTimeout:
      if i == len(retries) - 1:
        raise Exception("READ_TIMEOUT")
      print(f"[ReadTimeout] Retrying in {delay} seconds...")
      time.sleep(delay)

    except requests.ConnectTimeout:
      if i == len(retries) - 1:
        raise Exception("CONNECT_TIMEOUT")
      print(f"[ConnectTimeout] Retrying in {delay} seconds...")
      time.sleep(delay)

    except requests.exceptions.RequestException as e:
      if i == len(retries) - 1:
        raise Exception(f"REQUEST_FAILED: {str(e)}")
      print(f"[RequestException] {e}. Retrying in {delay} seconds...")
      time.sleep(delay)

def get_queries_to_process_from_cache():
  global queries
  
  try:
    with open('queries_cache.json', 'r') as f:
      content = f.read().strip()
      if not content:
        return None  
      try:
          cached_queries = json.loads(content)
      except json.JSONDecodeError as e:
          print(f"Invalid cache format: {e}")
          return None
      # Initialize status for cached queries
      for query in cached_queries['queries']:
          query.setdefault('status', Status.PENDING.value)
  except FileNotFoundError:
    return None

  if not cached_queries or len(cached_queries['queries']) == 0:
    return None
  
  if len(cached_queries['queries']) > 0:
    pending_queries = [
      query for query in cached_queries['queries'] 
      if query['status'] == Status.PENDING.value
    ]

    if len(pending_queries) == 0:
      print(f"All queries {len(cached_queries['queries'])} are processed, pushing results to database")
      queries['queries'] = cached_queries['queries']
      push_results_to_db()
      return None
    else:
      print(f"Found {len(pending_queries)} pending queries in cache")
      queries['queries'] = cached_queries['queries']
      return [query['query_url'] for query in pending_queries]

def get_query_from_queries(query_url):
  global queries

  for query in queries['queries']:
    _query_url = query['query_url'].replace('?hl=en', '')
    _query_url = _query_url.replace('&', '%26')
 
    if query_url.startswith(_query_url):
      return query
    
  raise Exception(f"Query {query_url} not found in queries")

def update_query_status(query_url, status):
  global queries
  query = get_query_from_queries(query_url)
  query['status'] = status
  cache_queries()

def save_query_results(query_url, links):
  query = get_query_from_queries(query_url)
  query['results'] = links
  cache_queries()

def count_queries_results():
  global queries
  total_results = 0
  for query in queries['queries']:
    if query.get('status', None) == Status.PROCESSED.value:
      total_results += len(query['results'])

  return total_results

def push_results_to_db():
  global queries

  num_queries_results = count_queries_results()
  print(f"Pushing {num_queries_results} results to database...")

  retries = [10, 20, 30]
  url = f"{TASK_SPREADER_API_URL}/queries/results"

  for i, delay in enumerate(retries):
    try:
      response = requests.post(url, json=queries, timeout=120)
      if response.status_code != 200:
        raise Exception("Failed to push results to database.")
      else:
        print("Results pushed to database successfully")
        clear_queries()
        return

    except requests.ConnectionError:
      if i == len(retries) - 1:
        raise Exception("CONNECTION_ERROR")
      print(f"Connection error while pushing results. Retrying in {delay} seconds...")
      time.sleep(delay)

def cache_queries():
  global queries
  try:
      with open('queries_cache.json.tmp', 'w') as f:
          json.dump(queries, f, indent=4, ensure_ascii=False)
      os.replace('queries_cache.json.tmp', 'queries_cache.json')
  except Exception as e:
      print(f"Error saving cache: {e}")

def clear_queries():
  global queries
  queries['queries'] = []
  cache_queries()

if __name__ == "__main__":
  asyncio.run(main())
