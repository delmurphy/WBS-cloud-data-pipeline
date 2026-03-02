### Functions for the local ETL pipeline to extract geographic, demographic and weather data for cities.

#import packages
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lat_lon_parser import parse
import re
from datetime import datetime

#---------------------------------------------------------------------------------
#Define connection string to sql instance
def connection_string_cloud():
    schema = "gans"
    host = "34.53.199.207"
    user = "root"
    password = os.getenv("GCP_SQL_PASSWORD")
    port = 3306
    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    return connection_string

#---------------------------------------------------------------------------------
#Send cities, populations, airports data to sql (slow changing dimensions)
def create_gans_tables(cities, connection_string):

    #Cities
    #Scrape data
    cities_full_df = get_cities(cities)

    #Format data
    cities_df = cities_full_df[['city','country', 'lat', 'long']].drop_duplicates()

    #Send cities data to sql
    cities_df.to_sql('cities',
                    if_exists='append',
                    con=connection_string,
                    index=False)

    #Retrieve cities table from sql with sql-generated city_id
    cities_from_sql = pd.read_sql("cities", con=connection_string)

    #Populations
    #Format data
    pops_df = cities_full_df.merge(cities_from_sql,
                                    on = "city",
                                    how="left")
    pops_df = pops_df[['city_id', 'population', 'population_date', 'retrieved']]

    #Send to sql
    pops_df.to_sql('populations',
                    if_exists='append',
                    con=connection_string,
                    index=False)
    
    #Airports
    #Scrape data (API)
    airports_df = get_airports(connection_string)

    #Format data
    airport_tosql_df = (cities_from_sql.drop(columns = ['country', 'lat', 'long'])
                        .merge(airports_df,
                                    on = "city",
                                    how="left")
                        .drop(columns=['city', 'iata', 'shortName', 'countryCode', 'timeZone', 'location.lat', 'location.lon'])
    )

    #Send to sql
    airport_tosql_df.to_sql('airports',
                    if_exists='append',
                    con=connection_string,
                    index=False)
    
#---------------------------------------------------------------------------------
# Scrape latitude, longitude and population estimates for cities from Wikipedia
def get_cities(city_list):
    headers = {'User-Agent': 'Chrome/134.0.0.0'}
    today_date = datetime.today().date()
    cities = city_list
    countries = []
    lats = []
    longs = []
    pops = []
    pop_dates = []
    retrievals = []

    for city in cities:
        url = "https://en.wikipedia.org/wiki/" + city
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except:
            print(f"Page does not exist: {url}")
            print(f"Cannot return data for {city}\n")
            cities.remove(city)
            continue
        citysoup = BeautifulSoup(response.content, 'html.parser')
        try:
            countries.append(citysoup.find(class_ = 'infobox').find(string = 'Country').find_next().get_text(strip=True))
        except:
            countries.append(pd.NA)
        lats.append(parse(citysoup.find(class_ = 'latitude').get_text(strip=True)))
        longs.append(parse(citysoup.find(class_ = 'longitude').get_text(strip=True)))
        #format population as numeric:
        pop = citysoup.find(string='Population').find_next('td').get_text(strip=True)
        pop = re.split(r'[\[\(]', pop)[0].strip().replace(',','')  #remove square bracket citations or round brackets and spaces, and commas (e.g. 592,713[6]->592713, 592,713 (6)->592713)
        pop = pd.to_numeric(pop)
        pops.append(pop)
        # some formatting required to extract the year/date of the population estimate (and not all pages have this):
        try:
            raw_text = citysoup.find(string = 'Population').find_next(class_ = 'ib-settlement-fn').get_text(strip=True)
            match = re.search(r'\((.*?)\)', raw_text)
            pop_dates.append(match.group(1) if match else pd.NA)
        except AttributeError:
            pop_dates.append(pd.NA)
        retrievals.append(today_date)

    cities_df = pd.DataFrame({
        'city': cities,
        'country': countries,
        'lat': lats,
        'long': longs,
        'population':pops,
        'population_date':pop_dates,
        'retrieved':retrievals})
    
    return(cities_df)

#---------------------------------------------------------------------------------
#Get a list of airports from city names in sql table 'cities'
def get_airports(connection_string):

  #get lat/longs of each city in cities
  cities_df = pd.read_sql("cities", con=connection_string)
  #get icaos using lat/long
  latitudes = cities_df['lat'].tolist()
  longitudes = cities_df['long'].tolist()

  # API headers
  headers = {
      "X-RapidAPI-Key": os.getenv('RAPID_API_KEY'),
      "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
  }

  querystring = {"withFlightInfoOnly": "true"}

  # DataFrame to store results
  all_airports = []

  for lat, lon in zip(latitudes, longitudes):
    # Construct the URL with the latitude and longitude
    url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{lat}/{lon}/km/50/16"

    # Make the API request
    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
      data = response.json()
      airports = pd.json_normalize(data.get('items', []))
      all_airports.append(airports)

  output_df = pd.concat(all_airports, ignore_index=True)
  output_df = output_df.rename(columns = {'municipalityName':'city'})
  return output_df
