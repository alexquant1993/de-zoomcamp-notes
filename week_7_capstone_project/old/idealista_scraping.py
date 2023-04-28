# Load libraries
import requests
from bs4 import BeautifulSoup as bs

# Define headers to mimic a browser
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'es-ES,es;q=0.9,en;q=0.8',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
}

# Scrape a given property
id_inmueble = "101108517"
url = f'https://www.idealista.com/inmueble/{id_inmueble}/'
response = requests.get(url, headers=headers, timeout = 5)
response
soup = bs(response.content, 'html.parser')

# Extract property header details
title = soup.find("span", {"class": "main-info__title-main"}).text.strip()
location = soup.find("span", {"class": "main-info__title-minor"}).text.strip()
info_data = soup.find("div", {"class": "info-data"})
price = info_data.find("span", {"class": "info-data-price"}).text.strip()
try:
    price_original = info_data.find("span", {"class": "pricedown_price"}).text.strip()
except:
    price_original = price

description = soup.find("div", {"class": "comment"}).text.strip()

# Property details
basic_features = soup.find("div", {"class": "details-property-feature-one"})
ls_basic_features = [li.text.strip() for li in basic_features.find_all("li")]

# Extra property details
extra_features = soup.find("div", {"class": "details-property-feature-two"})
# Find the "equipamiento" features
equipment = extra_features.find("h3", string="Equipamiento").find_next("ul")
ls_equip_features = [li.text.strip() for li in equipment.find_all("li")]

# Find the energy certificate details
cee = soup.find('h3', string='Certificado energético').find_next('ul')
try:
    consumo = cee.find('span', string='Consumo: ').find_next('span')
    ce_consumo = consumo['title'].upper()
    ce_consumo_text = consumo.text.strip()
    feature_consumo = f'Consumo: {ce_consumo} {ce_consumo_text}'.strip()
    emisiones = cee.find('span', string='Emisiones: ').find_next('span')
    ce_emisiones = emisiones['title'].upper()
    ce_emisiones_text = emisiones.text.strip()
    feature_emisiones = f'Emisiones: {ce_emisiones} {ce_emisiones_text}'.strip()
    ls_ce_features = [feature_consumo, feature_emisiones]
except:
    ls_ce_features = [cee.find_next('span').text.strip()]

# Scrape listings in a given city
base_url = "https://www.idealista.com/buscar/venta-viviendas/"
city = "madrid-madrid"
filter = "con-publicado_ultimas-24-horas"
zone_search = "salamanca"
# Parameterize zone search
zone_search = zone_search.replace(" ", "_")
# Get full url
url = f'{base_url}{city}/{filter}/{zone_search}/'

# Request the url and parse the response
response = requests.get(url, headers=headers)
response
soup = bs(response.content, 'html.parser')

# Try this URLs
url = "https://www.idealista.com/buscar/venta-viviendas/barrio_de_gracia/"
response = requests.get(url, headers=headers)
response


response = requests.get('https://httpbin.org/ip')
ip_info = response.json()
public_ip = ip_info['origin']

print("Your public IP address is:", public_ip)







