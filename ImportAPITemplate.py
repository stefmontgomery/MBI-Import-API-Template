import requests
import json
import zipfile
from datetime import datetime
import time
import pandas as pd

i_apikey = '' # Insert Import API key here
clientid = '' # Insert Client ID here

tablename = 'iapi_import_table_name' # This is the name of the table that will be created/updated in the MBI data warehouse

# Generating a fake data frame here - replace this with data to import to MBI
data = pd.DataFrame({'pk': [1, 2, 3], 'data1': ['red', 'white', 'blue'], 'data2': [10.0, 20.0, 30.0]})
pk_columns = ["pk"] # Replace with the PKs in your dataset

arr = pd.DataFrame.to_json(data, orient = 'records')
arr = json.loads(arr)

posturl = 'https://connect.rjmetrics.com/v2/client/' + clientid + '/table/' + tablename + '/data?apikey=' + i_apikey
h = {'Content-type': 'application/json'}

for i in arr:
    i.update({"keys": pk_columns}) 

group = [arr[i:i+100] for i in range(0, len(arr), 100)] # Batch data in sets of 100

for i in group: # Send POST request in batches
    response = requests.post(posturl, headers = h, json=i)
    print(response.content,"@",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
