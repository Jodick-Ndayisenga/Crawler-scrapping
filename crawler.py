import re

from datetime import datetime, timedelta
from urllib.parse import urlparse

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from dotenv import load_dotenv

crawler = PlaywrightCrawler()

from utils.enums import Status
from utils.google_maps_utils import google_map_consent_check

import requests
import os

load_dotenv('.env')

@crawler.router.default_handler
async def request_handler(context: PlaywrightCrawlingContext):
    try:
        data = await process_business(context)
        
        # Database payload with all required fields
        payload = {
            'url': data['url'],
            'title': data['title'],
            'star_rating': data['star_rating'],
            'review_count': data['review_count'],
            'category': data['category'],
            'address': data['address'],
            'coordinates': data['coordinates'],
            'phone': data['phone'],
            'website': data['website'],
            'pluscode': data['pluscode'],
            'open_hours': data['open_hours']
        }

        response = requests.post(
            f"{os.getenv('TASK_SPREADER_API_URL')}/queries/results",
            json=payload,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"Failed to save {data['url']}: {response.text}")

    except Exception as e:
        print(f"Error processing {context.request.url}: {e}")
    update_local_query_status(url, Status.FAILED.value)

async def process_business(context: PlaywrightCrawlingContext) -> dict:
  page = context.page
  url = context.request.url

  # Title
  title_el = await page.query_selector("h1")
  title = await title_el.inner_text() if title_el else None

  # Reviews
  review_el = await page.query_selector("div.F7nice")
  review_text = await review_el.inner_text() if review_el else ""
  if review_text:
    star = float(review_text.split("(")[0].strip() or 0)
    review_text = review_text.replace(',', '').strip() if "," in review_text else review_text
    review_count = int(review_text.split("(")[1].split(")")[0]) if "(" in review_text and ")" in review_text else 0
  else:
    star = None
    review_count = None

  # Headline
  headline_el = await page.query_selector("div[aria-label*='About'] div[jslog*='metadata']")
  headline = await headline_el.inner_text() if headline_el else ""
  
  # Category
  category_el = await page.query_selector("button.DkEaL")
  category = await category_el.inner_text() if category_el else None
  
  # Address
  address_el = await page.query_selector("button[data-item-id='address']")
  address = await address_el.get_attribute("aria-label") if address_el else ""
  address = address.replace("Address: ", "")
  
  # Open Hours
  open_hours_el = await page.query_selector("div[aria-label*='Sunday']")
  open_hours = await open_hours_el.get_attribute("aria-label") if open_hours_el else None
  
  # Check-in Info
  check_in_el = await page.query_selector("div[data-item-id='place-info-links:'] .Io6YTe")
  check_in = await check_in_el.inner_text() if check_in_el else None
  
  # Booking Link
  book_el = await page.query_selector("a.M77dve")
  book = await book_el.get_attribute("href") if book_el else None

  # Website
  website_el = await page.query_selector("a[data-item-id='authority']")
  website = await website_el.get_attribute("href") if website_el else None
  
  # Phone
  phone_el = await page.query_selector("button[aria-label*='Phone']")
  phone = await phone_el.inner_text() if phone_el else None
  phone = re.sub(r'^[^+]*', '', phone).strip() if phone else None
  
  # Pluscode
  pluscode_el = await page.query_selector("button[aria-label*='Plus code']")
  pluscode = await pluscode_el.inner_text() if pluscode_el else None

  # About
  # about = await process_about(page)

  # Reviews
  # reviews = await process_reviews(context)

  # Coordinates
  coordinates = parse_coordinate_from_map_url(url)

  return {
    'url': url,
    'title': title,
    'star_rating': star,
    'review_count': review_count,
    'headline': headline,
    'category': category,
    'address': address,
    'open_hours': open_hours,
    'check_in': check_in,
    'book': book,
    'website': website,
    'phone': phone,
    'pluscode': pluscode,
    # 'reviews': reviews,
    # 'about': about,
    'coordinates': coordinates
  }

async def process_about(page):
  if not (await page.query_selector("button[aria-label*='About']")):
    return None
  await page.click("button[aria-label*='About']")

  if not (await page.query_selector("h2")):
    return None
  await page.wait_for_selector("h2", timeout=1000 * 10)
        
  list_of_el = await page.query_selector_all("div.fontBodyMedium")
  data = {}

  # If there is only one element in the list
  if len(list_of_el) == 1:
    text = await page.evaluate("""
      () => Array.from(document.querySelectorAll("div.P1LL5e")).map(el => el.innerText.trim())
    """)
    
    attrs = await page.evaluate("""
      () => Array.from(document.querySelectorAll("div.WKLD0c .CK16pd")).map(el => el.getAttribute("aria-label"))
    """)
    
    return "\n".join(text) + "\n" + "\n".join(attrs)
  
  # Loop through each item in the list and extract details
  for item in list_of_el:
    h2_el = await item.query_selector("h2")
    if not h2_el:
      continue

    title = await h2_el.inner_text()

    texts = await item.query_selector_all("li")
    items = []

    for t in texts:
      span_el = await t.query_selector("span[aria-label]")
      if span_el:
        text = await span_el.get_attribute("aria-label")
        items.append(text)
    
    data[title] = items
  
  return data

async def process_reviews(context):
  page = context.page
  if not (await page.query_selector("button[aria-label*='Reviews']")):
    return None
  await page.click("button[aria-label*='Reviews']")
  
  # Wait for either button to appear (with a 10-second timeout)
  await page.wait_for_selector("button[aria-label*='relevant'], button[aria-label*='Sort']", timeout=1000 * 10)
  
  # Check if the 'relevant' button exists and click it, otherwise click 'Sort' button
  relevant_button = await page.query_selector("button[aria-label*='relevant']")
  sort_button = await page.query_selector("button[aria-label*='Sort']")
  if relevant_button:
    await relevant_button.click()
  elif sort_button:
    await sort_button.click()
  
  # Wait for the selector to appear with a 10-second timeout
  await page.wait_for_selector("div[id='action-menu'] div[data-index='1']", timeout=1000 * 10)

  # Check if the element exists, if not return None
  menu_item = await page.query_selector("div[id='action-menu'] div[data-index='1']")
  if not menu_item:
    return None
  
  # Click the element if found
  await menu_item.click()

  # Wait for the next selector to appear
  await page.wait_for_selector("div.d4r55", timeout=1000 * 10)
  await scroll_page(context, '.DxyBCb')

  reviews = await page.evaluate("""
    () => Array.from(document.querySelectorAll(".jftiEf")).map(el => {
      const reviews = el.querySelector("div.RfnDt")?.textContent;

      const [isLocalGuide, reviewCount] = (() => {
        if (!reviews) {
          return [false, null];
        }

        const parts = reviews.split("·").map(part => part.trim());
        const localGuide = parts.some(part => part === "Local Guide");
        const reviewMatch = parts.find(part => part.endsWith("review") || part.endsWith("reviews"))?.match(/\\d+/);

        return [
          localGuide, 
          reviewMatch ? parseInt(reviewMatch[0]) : null,
        ];
      })();
                                            
      return {
        user: {
          name: el.querySelector(".d4r55")?.textContent.trim(),
          link: el.querySelector(".al6Kxe")?.getAttribute("data-href"),
          thumbnail: el.querySelector(".NBa7we")?.getAttribute("src"),
          localGuide: isLocalGuide ? true : undefined,
          reviews: reviewCount,
        },
        
        rating: parseFloat(el.querySelector(".kvMYJc")?.getAttribute("aria-label") || parseInt(el.querySelector(".fzvQIb")?.textContent.split("/")[0]) / 5),
        snippet: el.querySelector(".MyEned")?.textContent.trim(),
        date: el.querySelector(".rsqaWe")?.textContent.trim() || el.querySelector(".xRkPPb")?.textContent.trim().split(" on")[0],
      };
    });
  """)
  
  for review in reviews:
    if review.get("date"):
      seconds_ago = parse_text_duration(review["date"])
      review["date"] = (datetime.now() - timedelta(seconds=seconds_ago)).isoformat()

  return reviews

async def scroll_page(context, scroll_container, limit=30):
  count = 0
  page = context.page
  
  while True:
    if count >= limit:
      context.log.info(f"reached scoll page limit ${limit}, url ${page.url}")
      break
  
    # Scroll to the bottom of the scroll container
    await page.evaluate(f'document.querySelector("{scroll_container}").scrollTo(0, document.querySelector("{scroll_container}").scrollHeight)')

    count += 1

    if await page.query_selector("span.xRkPPb"):
      # Evaluate to extract dates from elements
      dates = await page.evaluate("""
        () => Array.from(document.querySelectorAll("span.xRkPPb")).map(el => {
          console.log(`date ${el.innerText.trim().split("on")[0]}`);
          return el.innerText.trim().split("on")[0];
        })
      """)

      # Get the last date from the extracted list
      date = dates[-1] if dates else None

      # Check the date condition
      if date and ("year ago" in date or "years ago" in date):
        break

    if await page.query_selector("span.rsqaWe"):
      # Extract dates using evaluate
      dates = await page.evaluate("""
        () => Array.from(document.querySelectorAll("span.rsqaWe")).map(el => el.innerText.trim())
      """)

      # Get the last date from the extracted list
      date = dates[-1] if dates else None

      # Check the date condition
      if date and ("year ago" in date or "years ago" in date):
        break

def parse_coordinate_from_map_url(url):
  try:
    url_obj = urlparse(url)
    coordinate_match = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url_obj.path)
    
    if coordinate_match:
      latitude = float(coordinate_match.group(1))
      longitude = float(coordinate_match.group(2))
      return {'latitude': latitude, 'longitude': longitude}
    
    return None
  
  except Exception as e:
    print(f"Error parsing URL: {e}")
    return None

def parse_text_duration(duration_text):
  """
  Parses a duration string like "2 days ago", "3 weeks ago", etc., into a duration in seconds.
  """
  match = re.match(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", duration_text.lower())
  if not match:
    return 0
  value, unit = int(match.group(1)), match.group(2)
  multiplier = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
    "month": 2592000,  # Approximate, assumes 30 days per month
    "year": 31536000,  # Approximate, assumes 365 days per year
  }
  return value * multiplier.get(unit, 0)


async def process_business(context: PlaywrightCrawlingContext) -> dict:
  page = context.page
  url = context.request.url

  # Title
  title_el = await page.query_selector("h1")
  title = await title_el.inner_text() if title_el else None

  # Reviews
  review_el = await page.query_selector("div.F7nice")
  review_text = await review_el.inner_text() if review_el else ""
  if review_text:
    star = float(review_text.split("(")[0].strip() or 0)
    review_text = review_text.replace(',', '').strip() if "," in review_text else review_text
    review_count = int(review_text.split("(")[1].split(")")[0]) if "(" in review_text and ")" in review_text else 0
  else:
    star = None
    review_count = None

  # Headline
  headline_el = await page.query_selector("div[aria-label*='About'] div[jslog*='metadata']")
  headline = await headline_el.inner_text() if headline_el else ""
  
  # Category
  category_el = await page.query_selector("button.DkEaL")
  category = await category_el.inner_text() if category_el else None
  
  # Address
  address_el = await page.query_selector("button[data-item-id='address']")
  address = await address_el.get_attribute("aria-label") if address_el else ""
  address = address.replace("Address: ", "")
  
  # Open Hours
  open_hours_el = await page.query_selector("div[aria-label*='Sunday']")
  open_hours = await open_hours_el.get_attribute("aria-label") if open_hours_el else None
  
  # Check-in Info
  check_in_el = await page.query_selector("div[data-item-id='place-info-links:'] .Io6YTe")
  check_in = await check_in_el.inner_text() if check_in_el else None
  
  # Booking Link
  book_el = await page.query_selector("a.M77dve")
  book = await book_el.get_attribute("href") if book_el else None

  # Website
  website_el = await page.query_selector("a[data-item-id='authority']")
  website = await website_el.get_attribute("href") if website_el else None
  
  # Phone
  phone_el = await page.query_selector("button[aria-label*='Phone']")
  phone = await phone_el.inner_text() if phone_el else None
  phone = re.sub(r'^[^+]*', '', phone).strip() if phone else None
  
  # Pluscode
  pluscode_el = await page.query_selector("button[aria-label*='Plus code']")
  pluscode = await pluscode_el.inner_text() if pluscode_el else None

  # About
  # about = await process_about(page)

  # Reviews
  # reviews = await process_reviews(context)

  # Coordinates
  coordinates = parse_coordinate_from_map_url(url)

  return {
    'url': url,
    'title': title,
    'star_rating': star,
    'review_count': review_count,
    'headline': headline,
    'category': category,
    'address': address,
    'open_hours': open_hours,
    'check_in': check_in,
    'book': book,
    'website': website,
    'phone': phone,
    'pluscode': pluscode,
    # 'reviews': reviews,
    # 'about': about,
    'coordinates': coordinates
  }

async def process_about(page):
  if not (await page.query_selector("button[aria-label*='About']")):
    return None
  await page.click("button[aria-label*='About']")

  if not (await page.query_selector("h2")):
    return None
  await page.wait_for_selector("h2", timeout=1000 * 10)
        
  list_of_el = await page.query_selector_all("div.fontBodyMedium")
  data = {}

  # If there is only one element in the list
  if len(list_of_el) == 1:
    text = await page.evaluate("""
      () => Array.from(document.querySelectorAll("div.P1LL5e")).map(el => el.innerText.trim())
    """)
    
    attrs = await page.evaluate("""
      () => Array.from(document.querySelectorAll("div.WKLD0c .CK16pd")).map(el => el.getAttribute("aria-label"))
    """)
    
    return "\n".join(text) + "\n" + "\n".join(attrs)
  
  # Loop through each item in the list and extract details
  for item in list_of_el:
    h2_el = await item.query_selector("h2")
    if not h2_el:
      continue

    title = await h2_el.inner_text()

    texts = await item.query_selector_all("li")
    items = []

    for t in texts:
      span_el = await t.query_selector("span[aria-label]")
      if span_el:
        text = await span_el.get_attribute("aria-label")
        items.append(text)
    
    data[title] = items
  
  return data

async def process_reviews(context):
  page = context.page
  if not (await page.query_selector("button[aria-label*='Reviews']")):
    return None
  await page.click("button[aria-label*='Reviews']")
  
  # Wait for either button to appear (with a 10-second timeout)
  await page.wait_for_selector("button[aria-label*='relevant'], button[aria-label*='Sort']", timeout=1000 * 10)
  
  # Check if the 'relevant' button exists and click it, otherwise click 'Sort' button
  relevant_button = await page.query_selector("button[aria-label*='relevant']")
  sort_button = await page.query_selector("button[aria-label*='Sort']")
  if relevant_button:
    await relevant_button.click()
  elif sort_button:
    await sort_button.click()
  
  # Wait for the selector to appear with a 10-second timeout
  await page.wait_for_selector("div[id='action-menu'] div[data-index='1']", timeout=1000 * 10)

  # Check if the element exists, if not return None
  menu_item = await page.query_selector("div[id='action-menu'] div[data-index='1']")
  if not menu_item:
    return None
  
  # Click the element if found
  await menu_item.click()

  # Wait for the next selector to appear
  await page.wait_for_selector("div.d4r55", timeout=1000 * 10)
  await scroll_page(context, '.DxyBCb')

  reviews = await page.evaluate("""
    () => Array.from(document.querySelectorAll(".jftiEf")).map(el => {
      const reviews = el.querySelector("div.RfnDt")?.textContent;

      const [isLocalGuide, reviewCount] = (() => {
        if (!reviews) {
          return [false, null];
        }

        const parts = reviews.split("·").map(part => part.trim());
        const localGuide = parts.some(part => part === "Local Guide");
        const reviewMatch = parts.find(part => part.endsWith("review") || part.endsWith("reviews"))?.match(/\\d+/);

        return [
          localGuide, 
          reviewMatch ? parseInt(reviewMatch[0]) : null,
        ];
      })();
                                            
      return {
        user: {
          name: el.querySelector(".d4r55")?.textContent.trim(),
          link: el.querySelector(".al6Kxe")?.getAttribute("data-href"),
          thumbnail: el.querySelector(".NBa7we")?.getAttribute("src"),
          localGuide: isLocalGuide ? true : undefined,
          reviews: reviewCount,
        },
        
        rating: parseFloat(el.querySelector(".kvMYJc")?.getAttribute("aria-label") || parseInt(el.querySelector(".fzvQIb")?.textContent.split("/")[0]) / 5),
        snippet: el.querySelector(".MyEned")?.textContent.trim(),
        date: el.querySelector(".rsqaWe")?.textContent.trim() || el.querySelector(".xRkPPb")?.textContent.trim().split(" on")[0],
      };
    });
  """)
  
  for review in reviews:
    if review.get("date"):
      seconds_ago = parse_text_duration(review["date"])
      review["date"] = (datetime.now() - timedelta(seconds=seconds_ago)).isoformat()

  return reviews

async def scroll_page(context, scroll_container, limit=30):
  count = 0
  page = context.page
  
  while True:
    if count >= limit:
      context.log.info(f"reached scoll page limit ${limit}, url ${page.url}")
      break
  
    # Scroll to the bottom of the scroll container
    await page.evaluate(f'document.querySelector("{scroll_container}").scrollTo(0, document.querySelector("{scroll_container}").scrollHeight)')

    count += 1

    if await page.query_selector("span.xRkPPb"):
      # Evaluate to extract dates from elements
      dates = await page.evaluate("""
        () => Array.from(document.querySelectorAll("span.xRkPPb")).map(el => {
          console.log(`date ${el.innerText.trim().split("on")[0]}`);
          return el.innerText.trim().split("on")[0];
        })
      """)

      # Get the last date from the extracted list
      date = dates[-1] if dates else None

      # Check the date condition
      if date and ("year ago" in date or "years ago" in date):
        break

    if await page.query_selector("span.rsqaWe"):
      # Extract dates using evaluate
      dates = await page.evaluate("""
        () => Array.from(document.querySelectorAll("span.rsqaWe")).map(el => el.innerText.trim())
      """)

      # Get the last date from the extracted list
      date = dates[-1] if dates else None

      # Check the date condition
      if date and ("year ago" in date or "years ago" in date):
        break

def parse_coordinate_from_map_url(url):
  try:
    url_obj = urlparse(url)
    coordinate_match = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url_obj.path)
    
    if coordinate_match:
      latitude = float(coordinate_match.group(1))
      longitude = float(coordinate_match.group(2))
      return {'latitude': latitude, 'longitude': longitude}
    
    return None
  
  except Exception as e:
    print(f"Error parsing URL: {e}")
    return None

def parse_text_duration(duration_text):
  """
  Parses a duration string like "2 days ago", "3 weeks ago", etc., into a duration in seconds.
  """
  match = re.match(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", duration_text.lower())
  if not match:
    return 0
  value, unit = int(match.group(1)), match.group(2)
  multiplier = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
    "month": 2592000,  # Approximate, assumes 30 days per month
    "year": 31536000,  # Approximate, assumes 365 days per year
  }
  return value * multiplier.get(unit, 0)
