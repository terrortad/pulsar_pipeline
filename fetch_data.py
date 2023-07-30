import time
import logging
import json
import os
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv()

Path('data').mkdir(parents=True, exist_ok=True)

# Manually set the arguments
cities = ["New York", "San Francisco"]
term = "coffee"
limit = 50


def search_businesses(cities, term, limit):
    api_key = os.getenv('YELP_API_KEY')
    headers = {
        'Authorization': f'Bearer {api_key}',
    }
    url = 'https://api.yelp.com/v3/businesses/search'
    for city in cities:
        offset = 0
        iterations = 0
        max_iterations = 10  # set max iterations per city to avoid infinite loop
        for iteration in range(max_iterations):
            params = {
                'location': city,
                'term': term,
                "limit": limit,
                'offset': offset
            }

            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 429: # retry header, too many requests
                time.sleep(60)
                response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:  # successful fetch
                data = response.json()

                # Store the data
                try:
                    with open(f'data/{city}_{offset}.json', 'w') as f:
                        json.dump(data, f, indent=4)
                except Exception as e:
                    logging.error(f'Error writing to file: {e}')

                if 'businesses' in data and data['businesses']:  # if there are still businesses in the response
                    offset += limit  # prepare the offset for the next set of results
                    iterations += 1  # autoincrement number of iterations
                else:
                    break

            else:
                raise requests.exceptions.HTTPError(f'Request failed with status code {response.status_code}')



search_businesses(cities, term, limit)