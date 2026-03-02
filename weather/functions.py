import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lat_lon_parser import parse


#-----------------------------------------------------------
#Define connection string to sql instance
def connection_string_cloud():
    schema = "gans"
    host = "34.53.199.207"
    user = "root"
    password = os.environ.get("SQL_PASSWORD")
    port = 3306
    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    return connection_string


#-----------------------------------------------------------
#Retrieve and update weather data
def retrieve_update_weather(connection_string):

    #extract list of cities from sql db
    cities_from_sql = pd.read_sql("cities", con=connection_string)
    cities = cities_from_sql['city'].drop_duplicates()

    #Scrape data
    weather_df = get_weather(cities)

    #Format data
    weather_tosql_df = (cities_from_sql.drop(columns = ['country', 'lat', 'long'])
                        .merge(weather_df,
                                    on = "city",
                                    how="left")
                        .drop(columns=['city', 'location', 'country', 'time'])
    )

    #Send to sql
    weather_tosql_df.to_sql('weathers',
                    if_exists='append',
                    con=connection_string,
                    index=False)


#-----------------------------------------------------------
# Get weather data for cities
def get_weather(city_list):
    weather_df = pd.DataFrame()
    cities = []
    lats = []
    lons = []

    wiki_headers = {'User-Agent': 'Chrome/134.0.0.0'}

    for city in city_list:
        url = "https://en.wikipedia.org/wiki/" + city
        try:
            response = requests.get(url, headers=wiki_headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            #print(f"Page does not exist: {url}")
            #print(f"Cannot return data for {city}\n")
            continue
        cities.append(city)
        citysoup = BeautifulSoup(response.content, 'html.parser')
        lats.append(parse(citysoup.find(class_ = 'latitude').get_text()))
        lons.append(parse(citysoup.find(class_ = 'longitude').get_text()))


    for c in range(len(cities)):
        # Define the parameters to be passed into the url
        url = "https://api.openweathermap.org/data/2.5/forecast"
        weather_headers={"X-Api-Key": os.environ.get("WEATHER_API_KEY")}
        querystring = {"lat" : lats[c], "lon":lons[c], "units":"metric"}

        # Reference the sections in the request.
        weather_request = requests.get(url, headers=weather_headers, params=querystring)
        weather_json = weather_request.json()
        weather_json


        directions=['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        for i in range(len(weather_json['list'])):

            wind_dir_rnd=round(weather_json['list'][i]['wind']['deg']/45) % 8
            dt = pd.to_datetime(weather_json['list'][i]['dt'], unit='s')

            forecast = pd.DataFrame(
                {'date':[dt],
                'day':[weekdays[dt.dayofweek]],
                'time':[dt.time()],
                'city':[city_list[c]],
                'location':[weather_json['city'].get('name', pd.NA)],
                'country':[weather_json['city'].get('country', pd.NA)],
                'description':[weather_json['list'][i]['weather'][0].get('description', pd.NA)],
                'temp_C':[weather_json['list'][i]["main"].get('temp', pd.NA)],
                'feels_like_C':[weather_json['list'][i]["main"].get('feels_like', pd.NA)],
                'precipitation_prob':[weather_json['list'][i].get('pop', pd.NA)],
                'humidity_pct':[weather_json['list'][i]["main"].get('humidity', pd.NA)],
                'cloudiness_pct':[weather_json['list'][i]['clouds'].get('all', pd.NA)],
                'wind_speed_meters_per_s':[weather_json['list'][i]['wind'].get('speed', pd.NA)],
                'wind_direction':[directions[wind_dir_rnd]],}
            )

            weather_df = pd.concat([weather_df, forecast], ignore_index=True)
    
    return(weather_df)