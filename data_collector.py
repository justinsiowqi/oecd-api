import json
import requests
import pandas as pd
import time
import datetime
import re
import os

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}

class DataCollector:
    """ Class fetches and loads the json data. Next, it extracts the important information and saves it into a csv file.

    Methods:
        fetch_api: Fetches and loads the data as a json file.
        convert_timestamp: Formats the timestamp in the pandas dataframe.
        extract_data: Extracts the data from the json file and saves it as a csv file.
        process_data: Loops through the path list and calls the two class methods above.

    """
    def fetch_api(self, path_list):
        """ Class method fetches and loads the data as a json file. 
        
        Note:
            Only handles a maximum of 120 API requests. If requests exceed, sleep for 60s.

        Args:
            url: A list of urls from the updated_macro_path_list.

        Returns:
            Data from the json file.

        """
        data = {}
        for i in range(0, len(path_list), 120):
            batch_paths = path_list[i:i + 120]
            for url in batch_paths:
                res = requests.get(url, headers=HEADERS)
                endpoint_name = re.search(r'data/(.*?)(/AUS|.AUS)', url).group(1).replace('/', '_')
                try:
                    json_data = json.loads(res.text)
                    data[endpoint_name] = json_data
                except json.JSONDecodeError:
                    print(f'Error decoding JSON response from {url}')
            if i + 120 < len(path_list):
                time.sleep(60)
        return data
    
    def convert_timestamp(self, df):
        """ Class method formats the timestamp in the pandas dataframe.
        
        Note:
            For quarterly timestamps, use the last month of the quarter and last day of the month (eg. Q4-2022 -> 2022-Dec-31).
            For monthly timestamps, use the last day of the month (eg. Dec-2022 -> 2022-Dec-31).

        Args:
            df: Pandas dataframe.

        Returns:
            Pandas dataframe with a formatted timestamp.

        """
        def convert_quarter(timestamp):
            quarter, year = timestamp.split('-')
            timestamp = f'{year}-{quarter}'
            return pd.to_datetime(pd.Series(timestamp)) + pd.offsets.QuarterEnd(0)

        if any(df.iloc[:, 0].str.startswith('Q')):
            df['timestamp'] = df['timestamp'].apply(convert_quarter)
        else:
            month_days = {"Jan": 31, "Feb": 28, "Mar": 31, "Apr": 30, "May": 31, "Jun": 30, "Jul": 31, "Aug": 31, "Sep": 30, "Oct": 31, "Nov": 30, "Dec": 31}
            df['timestamp'] = df['timestamp'].apply(lambda x: datetime.datetime.strptime(x, "%b-%Y").date().replace(day=month_days[x[:3]]))
        return df

    def extract_data(self, json_data, endpoint_name):
        """ Class method extracts the data from the json file and saves it as a csv file.

        Note:
            Only handles json data with 3 or 4 key positions: subject/variable, location, measure and frequency.
            Assumption that time_period is the last item in the struct_obs list.

        Args:
            json_data: Data from the json file.
            endpoint_name: Name of the CSV file.

        Returns:
            A pandas dataframe with 3 rows: timestamp, country and subject values.

        """
        data_obs = json_data['dataSets'][0]['observations']
        struct_obs = json_data['structure']['dimensions']['observation']

        key_positions = {item.get('id'): item.get('keyPosition') for item in struct_obs}
        location_list = struct_obs[key_positions['LOCATION']].get('values')
        location = [item['name'] for item in location_list]
        time_period_list = struct_obs[-1]['values']
        time_period = [item['name'] for item in time_period_list]

        try: 
            subject_list = struct_obs[key_positions['SUBJECT']].get('values')
            subject = subject_list[0]['name']
        except KeyError:
            subject_list = struct_obs[key_positions['VAR']].get('values')
            subject = subject_list[0]['name']
        
        obs_list = []
        for data_obs_key, data_obs_value in data_obs.items():
            indices = data_obs_key.split(":")
            country = location[int(indices[key_positions['LOCATION']])]
            date = time_period[int(indices[-1])]
            value = data_obs_value[0]
            obs = [date, country, value]
            obs_list.append(obs)
    
        df = pd.DataFrame(obs_list, columns=['timestamp', 'country', subject.lower()])
        df = self.convert_timestamp(df) 
        os.makedirs('macro-indicators', exist_ok=True)
        df.to_csv(f'macro-indicators/{endpoint_name}.csv', index=False)
        return df
    
    def process_data(self, url_list):
        """ Class method loops through the path list and calls the two class methods above.

        Args:
            url_list: A list of urls from the updated_macro_path_list.

        """
        data = self.fetch_api(url_list)
        for endpoint_name, json_data in data.items():
            self.extract_data(json_data, endpoint_name)