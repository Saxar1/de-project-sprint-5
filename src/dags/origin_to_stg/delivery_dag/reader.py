from datetime import datetime, timedelta
from typing import Dict, List
import requests
import json


class CourierReader:
    def get_couriers(self):
        url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers"
        headers = headers = {
                                "X-Nickname": "saxarok",
                                "X-Cohort": "11",
                                "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
                            }
        sort_field = '_id'
        sort_direction = 'asc'
        limit = 50
        offset = 0
        
        data_list = []

        while True:
            params = {
                'sort_field': sort_field,
                'sort_direction': sort_direction,
                'limit': limit,
                'offset': offset
            }
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                data = json.loads(response.text)
                if not data:
                    break
                data_list.extend(data) 
                offset += limit
            else:
                raise ValueError(f'Request failed with status code {response.status_code}')
        return data_list

class DeliveryReader:
    def get_deliveries(self):
        url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries"
        headers = headers = {
                                "X-Nickname": "saxarok",
                                "X-Cohort": "11",
                                "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
                            }
        restaurant_id = ''
        limit = 50
        sort_field = 'date'
        sort_direction = 'asc'
        end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_date = (datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').replace(hour=0, minute=0, second=0) - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        offset = 0
        
        data_list = []

        while True:
            params = {
                'restaurant_id': restaurant_id,
                'from': start_date,
                'to': end_date,
                'sort_field': sort_field,
                'sort_direction': sort_direction,
                'limit': limit,
                'offset': offset
            }
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                data = json.loads(response.text)
                if not data:
                    break
                data_list.extend(data) 
                offset += limit
            else:
                raise ValueError(f'Request failed with status code {response.status_code}')
        return data_list