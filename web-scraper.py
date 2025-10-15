import os
import requests
from bs4 import BeautifulSoup
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.robotparser
import json

# --- Setup logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- User-Agent header ---
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MyScraper/1.0)"}


def can_fetch(url: str) -> bool:
    """Check robots.txt permissions"""
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url("https://relianceretail.com/robots.txt")
    try:
        rp.read()
        return rp.can_fetch(HEADERS["User-Agent"], url)
    except:
        return False


def fetch_page(url: str, retries: int = 3, delay: int = 2) -> str:
    """Fetch a single page with retries"""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                return resp.text
            else:
                logging.warning(f"{url} returned {resp.status_code}")
        except Exception as e:
            logging.warning(f"Error fetching {url}: {e}")
        time.sleep(delay)
    return None


def parse_content(html: str) -> str:
    """Extract clean text from HTML"""
    soup = BeautifulSoup(html, "html.parser")

    # remove scripts & styles
    for tag in soup(["script", "style"]):
        tag.decompose()

    return soup.get_text(separator=" ", strip=True)


def scrape_range(base_url: str, param: str, start: int, end: int, workers: int = 5):
    """Scrape a range of pages and keep only unredeemed coupons"""
    urls = [f"{base_url}?{param}={i}" for i in range(start, end + 1)]

    unredeemed = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_url = {executor.submit(fetch_page, url): url for url in urls if can_fetch(url)}

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                html = future.result()
                if html:
                    text = parse_content(html)

                    # ✅ Skip if coupon is already redeemed
                    if "This coupon has already been redeemed" not in text:
                        unredeemed[url] = {"html": html, "text": text}
                        logging.info(f"✅ Unredeemed coupon found: {url}")
                    else:
                        logging.info(f"❌ Redeemed coupon skipped: {url}")
                else:
                    logging.warning(f"Failed: {url}")
            except Exception as e:
                logging.error(f"Error at {url}: {e}")

    return unredeemed


if __name__ == "__main__":
    base_url = "https://relianceretail.com/JioMart/"
    param = "jiocpn"
    start, end = 1, 500  # <-- set your range here

    data = scrape_range(base_url, param, start, end, workers=5)

    # Save only unredeemed coupons
    with open("unredeemed_coupons.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"✅ Scraping completed. Found {len(data)} unredeemed coupons.")
