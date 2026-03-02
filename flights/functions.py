import os
import pandas as pd
import requests
import re
from datetime import datetime, timedelta

#--------------------------------------------------------
#Define connection string to sql instance
def connection_string_cloud():
    schema = "gans"
    host = "34.53.199.207"
    user = "root"
    password = os.environ.get("SQL_PASSWORD")
    port = 3306
    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    return connection_string

#--------------------------------------------------------
##Retrieve and update flights data
def retrieve_update_flights(connection_string):


    airports_from_sql = pd.read_sql("airports", con=connection_string)
    icaos = airports_from_sql['icao'].tolist()

    #Scrape data (API)
    arrivals_df = get_arrivals(icaos)

    #format data to send to sql
    flights_tosql_df = (airports_from_sql
                        .merge(arrivals_df,
                                    on = "icao",
                                    how="left")
                        .drop(columns=['city_id', 'name'])
    )

    #remove timezone information (not supported in sql)
    for col in ['scheduled_time', 'revised_time']:
        flights_tosql_df[col] = pd.to_datetime(flights_tosql_df[col], errors='coerce')  # parse normally
        flights_tosql_df[col] = flights_tosql_df[col].dt.tz_localize(None)              # remove timezone info

    #drop duplicates
    flights_tosql_df = flights_tosql_df.drop_duplicates()

    #Send to sql
    flights_tosql_df.to_sql('flights',
                    if_exists='append',
                    con=connection_string,
                    index=False)



#--------------------------------------------------------------
#Use API to get flight data for tomorrow's arrivals
def get_arrivals(icaos):

    #set up storage dataframe for output
    arrivals_df = pd.DataFrame()

    # get start and end times for api call
    now = datetime.now()
    midnight_today = datetime(year = now.year, month = now.month, day = now.day)
    midnight_tonight = midnight_today + timedelta(days=1)

    for icao in icaos:
        start_time = midnight_tonight.strftime("%Y-%m-%dT%H:%M")
        end_time = (midnight_tonight + timedelta(hours = 12)).strftime("%Y-%m-%dT%H:%M")

        # fetch response from API 
        # (note: API response only includes max 12 hours - for 24 hours, call must be made twice)
        response = fetch_flights_api(icao, start_time, end_time)
        if response.status_code == 200:
            arrivals1 = response.json()['arrivals']
            #api call 2 (append response from this call to the response from the first call)
            start_time = end_time
            end_time = (midnight_tonight + timedelta(hours = 24)).strftime("%Y-%m-%dT%H:%M")
            response = fetch_flights_api(icao, start_time, end_time)
            arrivals2 = response.json()['arrivals']
            arrivals = arrivals1 + arrivals2

            #create dataframe
            rows = []
            for arrival in arrivals:
                rows.append({
                    'icao':icao,
                    'status': arrival['status'],
                    'scheduled_time': arrival['movement']['scheduledTime'].get('local'),
                    'revised_time': arrival['movement'].get('revisedTime', {}).get('local'),
                    'origin_icao': arrival['movement']['airport'].get('icao'),
                    'origin_name': arrival['movement']['airport'].get('name'),
                    'origin_tz': arrival['movement']['airport'].get('timeZone')
                })

            df = pd.DataFrame(rows)
            
            arrivals_df = pd.concat([arrivals_df, df], ignore_index=True) 
        
    
    return arrivals_df

#-------------------------------------------------------------------------
def fetch_flights_api(icao, start_time, end_time):
        url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{start_time}/{end_time}"
        querystring = {"withLeg":"false","direction":"Arrival","withCancelled":"false","withCodeshared":"true","withCargo":"false","withPrivate":"false","withLocation":"false"}
        headers = {"x-rapidapi-key": os.environ.get('RAPID_API_KEY'),"x-rapidapi-host": "aerodatabox.p.rapidapi.com"}
        response = requests.get(url, headers=headers, params=querystring)
        return response