import asyncio
import json
import re
from typing import Dict, List
from collections import defaultdict
from urllib.parse import urljoin
import math
import random
import pandas as pd
from tqdm.asyncio import tqdm_asyncio  # Import tqdm async version

import httpx
from parsel import Selector
from typing_extensions import TypedDict

# Add this function to generate random sleep intervals
def random_sleep(min_sleep=1, max_sleep=5):
    return random.uniform(min_sleep, max_sleep)

# Establish persisten HTTPX session with browser-like headers to avoid blocking
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'es-ES,es;q=0.9,en;q=0.8',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
}
session = httpx.AsyncClient(headers=headers, follow_redirects=True)

# type hints fo expected results so we can visualize our scraper easier:
class PropertyResult(TypedDict):
    url: str
    title: str
    location: str
    price: int
    currency: str
    description: str
    updated: str
    features: Dict[str, List[str]]
    images: Dict[str, List[str]]
    plans: List[str]


def parse_property(response: httpx.Response) -> PropertyResult:
    """parse Idealista.com property page"""
    # load response's HTML tree for parsing:
    selector = Selector(text=response.text)
    css = lambda x: selector.css(x).get("").strip()
    css_all = lambda x: selector.css(x).getall()

    data = {}
    # Meta data
    data["url"] = str(response.url)

    # Basic information
    data['title'] = css("h1 .main-info__title-main::text")
    data['location'] = css(".main-info__title-minor::text")
    data['currency'] = css(".info-data-price::text")
    data['price'] = int(css(".info-data-price span::text").replace(".", ""))
    data['description'] = "\n".join(css_all("div.comment ::text")).strip()
    data["updated"] = selector.xpath(
        "//p[@class='stats-text']"
        "[contains(text(),'actualizado el')]/text()"
    ).get("").split(" el ")[-1]

    # Features
    data["features"] = {}
    #  first we extract each feature block like "Basic Features" or "Amenities"
    for feature_block in selector.css(".details-property-h3"):
        # then for each block we extract all bullet points underneath them
        label = feature_block.xpath("text()").get()
        features = feature_block.xpath("following-sibling::div[1]//li")
        data["features"][label] = [
            ''.join(feat.xpath(".//text()").getall()).strip()
            for feat in features
        ]

    # Images
    # the images are tucked away in a javascript variable.
    # We can use regular expressions to find the variable and parse it as a dictionary:
    image_data = re.findall(
        "fullScreenGalleryPics\s*:\s*(\[.+?\]),", 
        response.text
    )[0]
    # we also need to replace unquoted keys to quoted keys (i.e. title -> "title"):
    images = json.loads(re.sub(r'(\w+?):([^/])', r'"\1":\2', image_data))
    data['images'] = defaultdict(list)
    data['plans'] = []
    for image in images:
        url = urljoin(str(response.url), image['imageUrl'])
        if image['isPlan']:
            data['plans'].append(url)
        else:
            data['images'][image['tag']].append(url)
    return data


async def scrape_properties(urls: List[str]) -> List[PropertyResult]:
    """Scrape Idealista.com properties"""
    properties = []
    to_scrape = [session.get(url) for url in urls]
    # tip: asyncio.as_completed allows concurrent scraping - super fast!
    for response in tqdm_asyncio(asyncio.as_completed(to_scrape), total=len(to_scrape), desc='Scraping Properties'):
        response = await response
        if response.status_code != 200:
            print(f"can't scrape property: {response.url}")
            continue
        properties.append(parse_property(response))
        await asyncio.sleep(random_sleep())  # Add a random sleep between requests
    return properties

def parse_search(response: httpx.Response) -> List[str]:
    """Parse search result page for 30 listings"""
    selector = Selector(text=response.text)
    urls = selector.css("article.item .item-link::attr(href)").getall()
    return [urljoin(str(response.url), url) for url in urls]


async def scrape_search(url: str, paginate=True) -> List[str]:
    """
    Scrape search urls like:
    https://www.idealista.com/en/venta-viviendas/marbella-malaga/con-chalets/
    for proprety urls
    """
    first_page = await session.get(url)
    await asyncio.sleep(random_sleep())  # Add a random sleep after the first request
    property_urls = parse_search(first_page)
    if not paginate:
        return property_urls
    selector = Selector(text=first_page.text)
    # Get the number of total results given the search
    expression = selector.css("h1#h1-container").get("")
    total_results = re.search(r'([0-9.,]+)\s*(?:casas|anuncios)', expression).group(1)
    total_pages = math.ceil(int(total_results.replace(".", "").replace(",", "")) / 30)
    if total_pages > 60:
        print(f"search contains more than max page limit ({total_pages}/60)")
        total_pages = 60
    print(f"scraping {total_pages} pages of search results concurrently")
    to_scrape = [
        session.get(str(first_page.url) + f"pagina-{page}.htm")
        for page in range(2, total_pages + 1)
    ]
    for response in tqdm_asyncio(asyncio.as_completed(to_scrape), total=len(to_scrape), desc='Scraping Search Results'):
        property_urls.extend(parse_search(await response))
        await asyncio.sleep(random_sleep())  # Add a random sleep between requests
    return property_urls

# Flatten the nested dictionary
def flatten_dict(d, prefix=''):
    flat_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            flat_dict.update(flatten_dict(v, f"{prefix}{k}_"))
        else:
            flat_dict[f"{prefix}{k}"] = v
    return flat_dict


async def run():
    # scrape properties:
    # urls = ["https://www.idealista.com/inmueble/101108517/"]
    # search properties
    url = "https://www.idealista.com/venta-viviendas/madrid-madrid/con-publicado_ultimas-24-horas/"
    property_urls = await scrape_search(url)
    result_properties = await scrape_properties(property_urls)
    flattened_data = [flatten_dict(item) for item in result_properties]
    # Create pandas DataFrame
    df = pd.DataFrame(flattened_data)
    df.to_csv("properties.csv", index=False, encoding='utf-8')
    print("Successful scraping!")


if __name__ == "__main__":
    asyncio.run(run())