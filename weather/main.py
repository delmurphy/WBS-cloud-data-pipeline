import pysqlite3
import sys
sys.modules["sqlite3"] = pysqlite3


import functions_framework
from functions import connection_string_cloud, retrieve_update_weather

@functions_framework.http
def weather(request):

    connection_string = connection_string_cloud()
    retrieve_update_weather(connection_string)

    return 'Succesfully sent weather data'