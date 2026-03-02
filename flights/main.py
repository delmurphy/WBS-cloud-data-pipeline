import pysqlite3
import sys
sys.modules["sqlite3"] = pysqlite3

import functions_framework
from functions import connection_string_cloud, retrieve_update_flights

@functions_framework.http
def flights(request):

    connection_string = connection_string_cloud()
    retrieve_update_flights(connection_string)

    return 'Succesfully sent flights data'
