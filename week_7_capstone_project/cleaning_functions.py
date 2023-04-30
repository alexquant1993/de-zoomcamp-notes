# Asynchronous functions for cleaning data
import asyncio
import aiohttp
# Geopy imports
from geopy.adapters import AioHTTPAdapter
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import AsyncRateLimiter
# Progress bar
from tqdm import tqdm
# Built-in imports
import locale
from datetime import datetime
import pandas as pd
import re
from typing import Dict, List, Any


async def get_geocode_details(address1: str, address2: str, geocode) -> pd.Series:
    """
    Get geocode details given two addresses. The first address is the full address
    and the second address is the generic location. A geocode object from geopy.geocoders
    is required as an argument."""
    # If address doesn't have a number, add a 1
    if not re.search(r'\d', address1):
        address1_split = address1.split(',')
        address1_split[0] = address1_split[0] + ' 1'
        address1 = ','.join(address1_split)
    
    # Try with the full address first
    location = await geocode(address1)
    # If that doesn't work, try with the generic location
    if location is None:
        location = await geocode(address2)

    # If that doesn't work, return None for all fields
    if location:
        # Get zip code from the full address
        zip_code = location.raw['display_name'].split(",")[-2].strip()
        if zip_code.isdigit():
            zip_code = zip_code
        else:
            zip_code = None
        return pd.Series({
            'full_address': location.raw['display_name'],
            'postal_code': zip_code,
            'class': location.raw.get('class'),
            'type': location.raw.get('type'),
            'latitude': location.latitude,
            'longitude': location.longitude,
            'importance': location.raw.get('importance'),
            'place_id': location.raw.get('place_id')
        })
    else:
        return pd.Series({
            'full_address': None,
            'postal_code': None,
            'class': None,
            'type': None,
            'latitude': None,
            'longitude': None,
            'importance': None,
            'place_id': None
        })


async def get_geocode_details_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Get geocode details for a dataframe of addresses.
    Args:
        df (pd.DataFrame): Dataframe with two columns: ADDRESS and LOCATION
            - ADDRESS: Full address
            - LOCATION: Generic location
    Returns:
        pd.DataFrame: Dataframe with geocode details
            - FULL_ADDRESS
            - ZIP_CODE
            - CLASS_LOCATION
            - TYPE_LOCATION
            - LATITUDE
            - LONGITUDE
            - IMPORTANCE_LOCATION
            - LOCATION_ID
    """
    async with Nominatim(user_agent="idealista-scraper", adapter_factory=AioHTTPAdapter) as geolocator:
        geocode = AsyncRateLimiter(geolocator.geocode, min_delay_seconds=1)
        tasks = [get_geocode_details(row['ADDRESS'], row['LOCATION'], geocode) for _, row in df.iterrows()]
        results = []
        # Geocode addresses asynchronously
        for task in tqdm(tasks, total=df.shape[0], desc="Geocoding...", ncols=100):
            result = await task
            results.append(result)

    df_out = pd.DataFrame()
    df_out[['FULL_ADDRESS', 'ZIP_CODE', 'CLASS_LOCATION', 'TYPE_LOCATION',
            'LATITUDE', 'LONGITUDE', 'IMPORTANCE_LOCATION', 'LOCATION_ID']] = pd.DataFrame(results)
    
    return df_out

def split_basic_features(features: List[str]) -> Dict[str, str]:
    """Split basic listing features into a dictionary of key-value pairs"""
    dict_out = {}
    copy_features = features.copy()

    for feature in copy_features:
        lower_feature = feature.lower()

        if 'construidos' in lower_feature or 'útiles' in lower_feature:
            if 'construidos' in lower_feature:
                dict_out['BUILT_AREA'] = int(re.findall(r'\d+', lower_feature.split('construidos')[0])[0])
            if 'útiles' in lower_feature:
                dict_out['USEFUL_AREA'] = int(re.search(r'\d+(?= m² útiles)', lower_feature).group())
            features.remove(feature)

        elif 'planta' in lower_feature:
            # Valid for single family homes
            dict_out['NUM_FLOORS'] = int(re.search(r'\d+', lower_feature).group())
            features.remove(feature)
        
        elif 'parcela' in lower_feature:
            # Valid for single family homes
            lot_area = re.search(r'[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+', lower_feature).group()
            dict_out['LOT_AREA'] = int(lot_area.replace('.', ''))
            features.remove(feature)

        elif 'habitaci' in lower_feature:
            if 'sin' in lower_feature:
                dict_out['NUM_BEDROOMS'] = 0
            else:
                dict_out['NUM_BEDROOMS'] = int(re.search(r'\d+(?= habita)', feature).group())
            features.remove(feature)

        elif 'baño' in lower_feature:
            dict_out['NUM_BATHROOMS'] = int(re.search(r'\d+', lower_feature).group())
            features.remove(feature)

        elif 'garaje' in lower_feature:
            dict_out['FLAG_PARKING'] = True
            dict_out['PARKING_INCLUDED'] = 'incluida' in lower_feature

            if not dict_out['PARKING_INCLUDED']:
                parking_price = re.findall(r'[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+', lower_feature)[0]
                dict_out['PARKING_PRICE'] = int(parking_price.replace('.', ''))

            features.remove(feature)

        elif 'promoción' in lower_feature or 'segunda mano' in lower_feature:
            dict_out['CONDITION'] = feature
            features.remove(feature)

        elif 'armario' in lower_feature:
            dict_out['BUILTIN_WARDROBE'] = True
            features.remove(feature)

        elif 'trastero' in lower_feature:
            dict_out['STORAGE_ROOM'] = True
            features.remove(feature)

        elif 'orientación' in lower_feature:
            dict_out['CARDINAL_ORIENTATION'] = feature
            features.remove(feature)

        elif 'calefacción' in lower_feature:
            dict_out['HEATING'] = feature
            features.remove(feature)

        elif 'movilidad reducida' in lower_feature:
            dict_out['ACCESIBILITY_FLAG'] = True
            features.remove(feature)

        elif 'construido en' in lower_feature:
            dict_out['YEAR_BUILT'] = int(re.search(r'\d+', lower_feature).group())
            features.remove(feature)

        elif 'terraza' in lower_feature:
            dict_out['TERRACE'] = True
            features.remove(feature)

        elif 'balcón' in lower_feature:
            dict_out['BALCONY'] = True
            features.remove(feature)
        
    # Set default values for keys that were not found in the features
    dict_out.setdefault('FLAG_PARKING', False)
    dict_out.setdefault('BUILTIN_WARDROBE', False)
    dict_out.setdefault('STORAGE_ROOM', False)
    dict_out.setdefault('ACCESIBILITY_FLAG', False)
    dict_out.setdefault('TERRACE', False)
    dict_out.setdefault('BALCONY', False)

    if features:
        print(f"WARNING: The following features were not parsed: {features}")
        
    return dict_out


def split_building_features(features: List[str]) -> Dict[str, str]:
    """Split building features into a dictionary of key-value pairs"""
    dict_out = {}
    copy_features = features.copy()
    
    for feature in copy_features:
        lower_feature = feature.lower()
        if 'bajo' in lower_feature or 'planta' in lower_feature:
            # Floor number
            if 'bajo' in lower_feature:
                dict_out['FLOOR'] = 0
            elif 'entreplanta' in lower_feature:
                dict_out['FLOOR'] = 0.5
            else:
                dict_out['FLOOR'] = int(re.search(r'\d+', lower_feature).group())
        if 'interior' in lower_feature or 'exterior' in lower_feature:
            if 'interior' in lower_feature:
                dict_out['PROPERTY_ORIENTATION'] = 'Interior'
            elif 'exterior' in lower_feature:
                dict_out['PROPERTY_ORIENTATION'] = 'Exterior'
            features.remove(feature)
        elif 'ascensor' in lower_feature:
            if 'con' in lower_feature:
                dict_out['ELEVATOR'] = True
            elif 'sin' in lower_feature:
                dict_out['ELEVATOR'] = False
            features.remove(feature)
    
    if features:
        print(f"WARNING: The following features were not parsed: {features}")
    
    return dict_out


def split_amenity_features(features: List[str]) -> Dict[str, str]:
    """Split amenity features into a dictionary of key-value pairs"""
    dict_out = {}
    copy_features = features.copy()

    for feature in copy_features:
        lower_feature = feature.lower()
        if 'aire acondicionado' in lower_feature:
            dict_out['AIR_CONDITIONING'] = True
            features.remove(feature)
        elif 'piscina' in lower_feature:
            dict_out['POOL'] = True
            features.remove(feature)
        elif 'zonas verdes' in lower_feature or 'jardín' in lower_feature:
            dict_out['GREEN_AREAS'] = True
            features.remove(feature)
    
    # Set default values for keys that were not found in the features
    dict_out.setdefault('AIR_CONDITIONING', False)
    dict_out.setdefault('POOL', False)
    dict_out.setdefault('GREEN_AREAS', False)

    if features:
        print(f"WARNING: The following features were not parsed: {features}")

    return dict_out


def split_energy_features(features: List[str]) -> Dict[str, str]:
    """Split energy features into a dictionary of key-value pairs"""
    dict_out = {}
    copy_features = features.copy()

    for feature in copy_features:
        lower_feature = feature.lower()
        if 'consumo' not in lower_feature and 'emisiones' not in lower_feature:
            dict_out['STATUS_EPC'] = feature
            features.remove(feature)
            break
        else:
            dict_out['STATUS_EPC'] = 'Disponible'
            if 'consumo' in lower_feature:
                s = lower_feature.split()
                if 'kwh' in lower_feature:
                    dict_out['ENERGY_CONSUMPTION'] = \
                        float(re.search(r'[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+', lower_feature).group().replace(',', '.'))
                if len(s) > 1:
                    if len(s[-1]) == 1 and s[-1].isalpha():
                        dict_out['ENERGY_CONSUMPTION_LABEL'] = s[-1]
            if 'emisiones' in lower_feature:
                s = lower_feature.split()
                if 'kg co2' in lower_feature:
                    dict_out['ENERGY_EMISSIONS'] = \
                        float(re.search(r'[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+', lower_feature).group().replace(',', '.'))
                if len(s) > 1:
                    if len(s[-1]) == 1 and s[-1].isalpha():
                        dict_out['ENERGY_EMISSIONS_LABEL'] = s[-1]     
            features.remove(feature)

    if features:
        print(f"WARNING: The following features were not parsed: {features}")

    return dict_out


def parse_date_with_locale(date_string, format_string, new_locale):
    """Parse a date string with a given format and locale."""
    # Save the current locale
    old_locale = locale.getlocale(locale.LC_TIME)
    
    # Set the new locale
    locale.setlocale(locale.LC_TIME, new_locale)

    # Perform the date parsing
    date_object = datetime.strptime(date_string, format_string)

    # Restore the original locale
    locale.setlocale(locale.LC_TIME, old_locale)

    return date_object


def parse_date_in_column(date_string):
    """Parse a date string given a spanish locale and adding the current year."""
    year = datetime.now().year 
    date_string_with_year = f"{date_string} de {year}"
    return parse_date_with_locale(date_string_with_year, "%d de %B de %Y", 'es_ES.UTF-8')


def get_features_asdf(pds: pd.Series, split_function) -> pd.DataFrame:
    """Split a pandas series of features into a dataframe
    Args:
        pds: A pandas series of features. Each element stores a list of features.
        split_function: A function to split each feature into a dictionary
    Returns:
        A dataframe of features
    """
    # Check if pds is a pandas series
    if not isinstance(pds, pd.Series):
        raise TypeError("pds must be a pandas series")
    # Apply split function to each element of the series
    df_features = []
    for feature in pds:
        if not isinstance(feature, list):
            feature = []
        df_features.append(split_function(feature))
    df_features = pd.DataFrame(df_features)
    return df_features


async def clean_data(property_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Clean the data from the scraped properties."""
    print("Cleaning data...")
    df = pd.DataFrame(property_data)
    
    # Get ID from URL
    df_out = pd.DataFrame()
    df_out['ID_LISTING'] = df['url'].apply(lambda x: re.findall(r"\d+", x)[0])
    df_out['URL'] = df['url']
    # Get type of property and address from title and location
    df_out['TYPE_PROPERTY'] = df['title'].str.split('en venta en').str[0].str.strip()
    df_out['ADDRESS'] = df['title'].str.split('en venta en').str[1].str.strip() + ', ' + df['location']
    df_out['LOCATION'] = df['location']
    # Get ZIP code based on address and location
    df_geocode_details = await get_geocode_details_batch(df_out)
    df_out = pd.concat([df_out, df_geocode_details], axis=1)
    # Get price and currency
    df_out['PRICE'] = df['price']
    df_out['CURRENCY'] = df['currency']
    # Get listing description
    df_out['LISTING_DESCRIPTION'] = df['description']
    # Get poster details
    df_out['POSTER_TYPE'] = df['poster_type']
    df_out['POSTER_NAME'] = df['poster_name']
    # Get basic listing features
    df_basic_features = get_features_asdf(df['features_Características básicas'], split_basic_features)
    # Get building listing features
    df_building_features = get_features_asdf(df['features_Edificio'], split_building_features)
    # Get amenities listing features
    df_amenities_features = get_features_asdf(df['features_Equipamiento'], split_amenity_features)
    # Get energy listing features
    df_energy_features = get_features_asdf(df['features_Certificado energético'], split_energy_features)
    # Concatenate all features
    df_out = \
        pd.concat([df_out, df_basic_features, df_building_features,
                    df_amenities_features, df_energy_features], axis=1)
    # Get last update date and timestamp
    df_out['LAST_UPDATE_DATE'] = df['updated'].apply(parse_date_in_column)
    df_out['TIMESTAMP'] = pd.to_datetime(df['time_stamp'])
    # Get columns related to photos of the listing - might be useful for future analysis
    image_cols = [col for col in df.columns if col.startswith('image')]
    df_out[image_cols] = df[image_cols]
    
    return df_out
