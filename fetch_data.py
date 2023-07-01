import requests
import json
from bs4 import BeautifulSoup
import pandas
import os


if not os.path.exists('data'):
    os.makedirs('data')

api_key = '**'

headers = {
    'Authorization': 'Bearer %s' % api_key,
}

url = 'https://api.yelp.com/v3/businesses/search'
cities = ['New York', 'Los Angeles']
term = 'restaurants'
limit = 50

for city in cities:
    offset = 0
    iterations = 0
    max_iterations = 10 #  set max iterations per city to avoid infinite loop
    while iterations < max_iterations:
        params = {
            'location': city,
            'term': term,
            "limit": limit,
            'offset': offset
        } 

        response = requests.get(url, headers=headers, params = params)
        if response.status_code == 200: #  successful fetch
            data = response.json()
            
             # Store the data
            with open(f'data/{city}_{offset}.json', 'w') as f:
                json.dump(data, f, indent=4)

            if data['businesses']: # If there are still businesses in the response
                offset += limit # Prepare the offset for the next set of results
                iterations += 1 #  autoincrement number of iterations
            else:
                break
            
        else:
            print(f'Request failed with status code {response.status_code}')
            break

#  script fetches restaurant data for both New York and Los Angeles, 
#  making multiple requests per city to fetch more than just the first 50 results.
