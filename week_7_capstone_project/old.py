async def run():
    # Scrape properties from a given search URL
    url = "https://www.idealista.com/venta-viviendas/madrid-madrid/con-publicado_ultimas-24-horas/"
    idealista_scraper = IdealistaScraper()
    property_urls = await idealista_scraper.scrape_search(url)
    scraped_properties = await idealista_scraper.scrape_properties(property_urls)
    
    # Debugging one URL
    # idealista_scraper = IdealistaScraper()
    # scraped_properties = await idealista_scraper.scrape_properties(['https://www.idealista.com/inmueble/101190406/'])
    
    # Close session
    await idealista_scraper.session.aclose()

    # Flatten the nested dictionaries for a cleaner DataFrame
    flattened_properties = [idealista_scraper.flatten_dict(asdict(item)) for item in scraped_properties]
    
    # Save the results to a CSV file
    df = pd.DataFrame(flattened_properties)
    df.to_csv("scraped_properties.csv", index=False, encoding='utf-8')
    print("Scraped properties saved to 'scraped_properties.csv'")