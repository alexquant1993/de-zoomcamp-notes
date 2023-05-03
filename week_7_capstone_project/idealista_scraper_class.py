# Built-in imports
import asyncio
import json
import re
import math
import random
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import defaultdict
from urllib.parse import urljoin
from dataclasses import dataclass

# Import third-party libraries
import httpx
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio

@dataclass
class PropertyResult:
    """A dataclass representing the result of scraping an Idealista.com property page"""
    url: str
    title: str
    location: str
    price: int
    original_price: Optional[int]
    tags: Optional[List[str]]
    currency: str
    description: str
    poster_type: str
    poster_name: str
    features: Dict[str, List[str]]
    images: Dict[str, List[str]]
    plans: List[str]
    updated: str
    time_stamp: str


class IdealistaScraper:
    """
    A class for scraping property data from Idealista.com
    """
    def __init__(self):
        self.MAX_RETRIES = 5
        self.INITIAL_BACKOFF = 1
        self.MAX_BACKOFF = 32
        self.CONCURRENT_REQUESTS_LIMIT = 2
        self.NUM_RESULTS_PAGE = 30
        self.HEADERS = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'es-ES,es;q=0.9,en;q=0.8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
        }
        self.session = None
        self.base_url = "https://www.idealista.com"

    
    async def __aenter__(self):
        self.session = httpx.AsyncClient(headers=self.HEADERS, follow_redirects=True, timeout=60)
        return self
    

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.aclose()
        self.session = None
    
    async def make_request(self, url: str):
        """
        Make an HTTP request to a URL

        Args:
            url: The URL to make a request to

        Returns:
        The response object from the request, or None if the request failed
        """
        for i in range(self.MAX_RETRIES + 1):
            try:
                async with asyncio.Semaphore(self.CONCURRENT_REQUESTS_LIMIT):                        
                    response = await self.session.get(url)
                    if response.status_code != 200:
                        print(f"can't scrape URL: {response.url}")
                    else:
                        return response
            except (httpx.RequestError, asyncio.TimeoutError):
                if i < self.MAX_RETRIES:
                    await asyncio.sleep(self.exponential_backoff_with_jitter(i))
                else:
                    print(f"failed to scrape URL after {self.MAX_RETRIES} retries: {url}")
                    return None
    

    def parse_property(self, response: httpx.Response) -> PropertyResult:
        """
        Parse an Idealista.com property page

        Args:
            response: The HTTP response object from the property page request

        Returns:
            A PropertyResult object representing the parsed data
        """
        # Parse response
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get original price, before discount, if available
        original_price_element = soup.select_one('.pricedown_price span')
        original_price = None
        if original_price_element:
            original_price = int(original_price_element.text.strip().replace(".", "").replace(",", ""))
        
        # Get tags
        tags = None
        if soup.select_one('.detail-info-tags'):
            tags = soup.select_one('.detail-info-tags').text.split()
        
        # Get poster details
        # If the poster is not a particular/professional, then try with a bank class
        check_professional = soup.select_one('.advertiser-name-container .about-advertiser-name')
        if check_professional:
            poster_type = "Profesional"
            poster_name = check_professional.text.strip()
        else:
            poster_type = "Particular"
            poster_name = soup.select_one('.professional-name span').text.strip()

        # Get image data
        image_data = self.get_image_data(soup)

        # Create PropertyResult object
        property_result = PropertyResult(
            url=str(response.url),
            title=soup.select_one('.main-info__title-main').text.strip(),
            location=soup.select_one('.main-info__title-minor').text.strip(),
            currency=soup.select_one('.info-data-price').contents[-1].strip(),
            price=int(soup.select_one('.info-data-price span').text.replace(".", "").replace(",", "")),
            original_price = original_price,
            tags = tags,
            description='\n'.join([p.text.strip() for p in soup.select('div.comment p')]),
            poster_type=poster_type,
            poster_name=poster_name,
            features=self.get_features(soup),
            images=self.get_images(image_data),
            plans=self.get_plans(image_data),
            updated=soup.select_one('p.stats-text:-soup-contains("actualizado el")').text.split(' el ')[-1],
            time_stamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        return property_result
    

    async def scrape_properties(self, urls: List[str]) -> List[PropertyResult]:
        """
        Scrape Idealista.com properties

        Args:
            urls: A list of property URLs to scrape

        Returns:
            A list of PropertyResult objects representing the scraped data
        """
        properties = []
        to_scrape = [self.make_request(url) for url in urls]
        for response in tqdm_asyncio(asyncio.as_completed(to_scrape), total=len(to_scrape), desc='Scraping Properties', ncols=100):
            response = await response
            if response is not None:
                print(response.url)
                properties.append(self.parse_property(response))
                await asyncio.sleep(self.get_random_sleep_interval())

        return properties


    async def scrape_search(self, url: str, paginate=True) -> List[str]:
        """
        Scrape search result pages from Idealista.com for property URLs

        Args:
            url: The search result URL to scrape
            paginate: Whether to scrape all pages of search results (up to a maximum of 60)

        Returns:
            A list of URLs for properties found in the search results
        """
        property_urls = []
        first_page = await self.make_request(url)
        property_urls.extend(self.parse_search(first_page))

        if not paginate:
            return property_urls

        total_pages = self.get_total_pages(first_page)
        if total_pages > 60:
            print(f"search contains more than max page limit ({total_pages}/60)")
            total_pages = 60

        print(f"scraping {total_pages} pages of search results concurrently")

        to_scrape = [
            self.make_request(str(first_page.url) + f"pagina-{page}.htm")
            for page in range(2, total_pages + 1)
        ]

        for response in tqdm_asyncio(asyncio.as_completed(to_scrape), total=len(to_scrape), desc='Scraping Search Results', ncols=100):
            property_urls.extend(self.parse_search(await response)) 

        return property_urls


    def parse_search(self, response: httpx.Response) -> List[str]:
        """
        Parse an Idealista.com search result page for property URLs

        Args:
            response: The HTTP response object from the search result page request

        Returns:
            A list of property URLs found in the search results
        """
        soup = BeautifulSoup(response.text, 'html.parser')
        urls = [urljoin(str(response.url), a['href']) for a in soup.select('article.item .item-link')]
        return urls

    def get_total_pages(self, response: httpx.Response) -> int:
        """
        Get the total number of pages of search results for a given search URL

        Args:
            response: The HTTP response object from the first page of search results

        Returns:
            The total number of pages of search results
        """
        soup = BeautifulSoup(response.text, 'html.parser')
        total_results = soup.select_one('h1#h1-container').text
        total_results = re.search(r'([0-9.,]+)\s*(?:casas|anuncios)', total_results).group(1)
        return math.ceil(int(total_results.replace(".", "").replace(",", "")) / self.NUM_RESULTS_PAGE)


    def get_features(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """
        Extract property features from an Idealista.com property page

        Args:
            soup: The BeautifulSoup object representing the parsed HTML of the property page

        Returns:
            A dictionary of property features, where each key is a feature category and each value is a list of features in that category
        """
        feature_dict = {}
        for feature_block in soup.select('.details-property-h3'):
            feature_name = feature_block.text.strip()
            if feature_name != 'Certificado energético':
                features = [feat.text.strip() for feat in feature_block.find_next('div').select('li')]
            else:
                features = []
                for feat in feature_block.find_next('div').select('li'):
                    feat_props = feat.find_all('span')
                    type_certificate = feat_props[0].text.strip()
                    kwh_m2 = feat_props[1].text.strip()
                    energy_label = feat_props[1]['title'].upper()
                    energy_feat = f"{type_certificate} {kwh_m2} {energy_label}"
                    features.append(energy_feat.strip())
                        
            feature_dict[feature_name] = features
        return feature_dict


    def get_image_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract image data from an Idealista.com property page

        Args:
            soup: The BeautifulSoup object representing the parsed HTML of the property page

        Returns:
            A list of dictionaries representing each image, with keys for the image URL, caption, and other metadata
        """
        script = soup.find('script', string=re.compile('fullScreenGalleryPics'))
        if script is None:
            return []
        match = re.search(r'fullScreenGalleryPics\s*:\s*(\[.+?\]),', script.string)
        if match is None:
            return []
        image_data = json.loads(re.sub(r'(\w+?):([^/])', r'"\1":\2', match.group(1)))
        return image_data


    def get_images(self, image_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Extract image URLs from a list of image data dictionaries

        Args:
            image_data: A list of dictionaries representing each image, with keys for the image URL, caption, and other metadata

        Returns:
            A dictionary of image URLs, where each key is an image category and each value is a list of image URLs in that category
        """
        image_dict = defaultdict(list)
        for image in image_data:
            url = urljoin(self.base_url, image['imageUrl'])
            if image['isPlan']:
                continue
            if image['tag'] is None:
                image_dict['main'].append(url)
            else:
                image_dict[image['tag']].append(url)
        return dict(image_dict)


    def get_plans(self, image_data: List[Dict[str, Any]]) -> List[str]:
        """
        Extract plan image URLs from a list of image data dictionaries

        Args:
            image_data: A list of dictionaries representing each image, with keys for the image URL, caption, and other metadata

        Returns:
            A list of plan image URLs
        """
        plan_urls = [urljoin(self.base_url, image['imageUrl']) for image in image_data if image['isPlan']]
        return plan_urls

    def get_random_sleep_interval(self, min_sleep=1, max_sleep=5):
        """
        Generate a random sleep interval to add between requests

        Args:
            min_sleep: The minimum sleep time in seconds (default 1)
            max_sleep: The maximum sleep time in seconds (default 5)

        Returns:
            A random sleep interval in seconds
        """
        return random.uniform(min_sleep, max_sleep)

    def exponential_backoff_with_jitter(self, retry_count):
    
        return random.uniform(0, min(self.MAX_BACKOFF, self.INITIAL_BACKOFF * (2 ** retry_count)))


    def flatten_dict(self, d: dict, prefix: str = '') -> Dict[str, Any]:
        """
        Flatten a nested dictionary by concatenating keys with underscores

        Args:
            d: The dictionary to flatten
            prefix: A string to prepend to each key (default '')

        Returns:
            A flattened dictionary, where each key is a concatenation of the original keys separated by underscores
        """
        flat_dict = {}
        for k, v in d.items():
            if isinstance(v, dict):
                flat_dict.update(self.flatten_dict(v, f"{prefix}{k}_"))
            else:
                flat_dict[f"{prefix}{k}"] = v
        return flat_dict