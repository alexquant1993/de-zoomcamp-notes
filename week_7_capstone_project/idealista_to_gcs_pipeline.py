# Import custom functions
from clean_scraped_data import *
from upload_to_gcs import *
from idealista_scraper_class import *

# Built-in imports
import asyncio
from typing import Dict, List, Any
from dataclasses import asdict

# Prefect dependencies
from prefect import flow, task


@task(retries=3, log_prints=True)
async def scrape_search_task(scraper: IdealistaScraper, url: str, paginate=True) -> List[str]:
    """Scrape a search page to get property URLs
    Args:
        scraper: An IdealistaScraper instance
        url: The URL of the search page
        paginate: Whether to scrape all pages of the search results (default True)
    Returns:
        A list of property URLs
    """
    return await scraper.scrape_search(url, paginate=paginate)


@task(retries=3, log_prints=True)
async def scrape_properties_task(scraper: IdealistaScraper, property_urls: List[str]) -> List[Dict[str, Any]]:
    """Scrape a list of property pages to get property data
    Args:
        scraper: An IdealistaScraper instance
        property_urls: A list of property URLs
    Returns:
        A list of dictionaries representing each property
    """
    scraped_properties = await scraper.scrape_properties(property_urls)
    flattened_properties = [scraper.flatten_dict(asdict(item)) for item in scraped_properties]
    return flattened_properties


@flow(log_prints=True)
async def idealista_to_gcs_pipeline(url:str, bucket_name:str, to_path:str, credentials_path:str, testing:bool=False):
    """
    Scrape idealista listings given a search URL and upload to GCS
    Args:
        url: The URL of the search page
        bucket_name: The name of the GCS bucket
        to_path: The path to the GCS bucket
        credentials_path: The path to the GCS credentials
        testing: Whether to run the pipeline in testing mode (default False)
    """
    # Scrape idealista listings given a search URL    
    async with IdealistaScraper() as scraper:
        property_urls = await scrape_search_task(scraper, url, paginate=not testing)
        property_data = await scrape_properties_task(scraper, property_urls)

    # Clean up scraped data 
    cleaned_property_data = await clean_scraped_data(property_data)

    # Upload to GCS
    pa_cleaned_property_data = prepare_parquet_file(cleaned_property_data)
    save_and_upload_to_gcs(pa_cleaned_property_data, bucket_name, to_path, credentials_path)

    # Debugging one URL
    # url = ['https://www.idealista.com/inmueble/94481996/']
    # scraper = IdealistaScraper()
    # property_data = await scraper.scrape_properties(url)
    # await scraper.session.aclose()

if __name__ == "__main__":
    url = "https://www.idealista.com/venta-viviendas/madrid-madrid/con-publicado_ultimas-24-horas/"
    bucket_name = 'idealista_data_lake_idealista-scraper-384619'
    to_path = 'production/madrid/venta-viviendas/'
    credentials_path = '~/.gcp/terraform.json'
    asyncio.run(idealista_to_gcs_pipeline(url, bucket_name, to_path, credentials_path, testing=True))
