import json
import requests
import pandas as pd
import time
import datetime
import re
import os

class URLUpdater:
    """ Class updates the edition and end time in the macro urls.

    Methods:
        update_edition_number: Updates the current year and month if edition is present in the url.
        update_end_time: Updates the current end time in the url.

    """
    def __init__(self):
        pass

    def update_edition_number(self, url):
        """ Class method updates the current year and month if edition is present in the url.

        Args:
            url: A single url from the macro_path_list.

        Returns:
            An updated url with the current year and month as the edition.

        """
        now = datetime.datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        edition_number = f'{year}{month}'

        edition = re.search(r'\.(\d+)\.Q/', url).group(1)

        if edition == edition_number:
            updated_url = url
        else:
            updated_url = re.sub(r'\.\d+\.Q/', f'.{edition_number}.Q/', url)
        
        return updated_url

    def update_end_quarter(self, url):
        """ Class method updates the current end quarter in the url.

        Args:
            url: A single url from the macro_path_list.

        Returns:
            An updated url with the current year and quarter in the end time.

        """
        now = datetime.datetime.now()

        if now.month <= 3:
            year_quarter =  f'{now.year}-Q1'
        elif now.month <= 6:
            year_quarter = f'{now.year}-Q2'
        elif now.month <= 9:
            year_quarter = f'{now.year}-Q3'
        else:
            year_quarter = f'{now.year}-Q4'

        updated_url = re.sub(r'endTime=\d+-Q\d+', f'endTime={year_quarter}', url)
        return updated_url
    
    def update_end_month(self, url):
        """ Class method updates the current end month in the url.

        Args:
            url: A single url from the macro_path_list.

        Returns:
            An updated url with the current year and month in the end time.

        """
        now = datetime.datetime.now()

        updated_url = re.sub(r'endTime=\d+-\d+', f'endTime={now.year}-{now.strftime("%m")}', url)
        return updated_url

    def update_data(self, path_list):
        """ Class method loops through the path list and calls the two class methods above.

        Args:
            url: A list of urls from the macro_path_list.

        Returns:
            An updated list of urls with the current edition and end time.

        """
        updated_urls = []
        
        for url in path_list:
            if re.search(r'\.(\d+)\.Q/', url):
                updated_url = self.update_edition_number(url)
                updated_url = self.update_end_quarter(updated_url)
            else:
                updated_url = self.update_end_month(url)
            updated_urls.append(updated_url)
        
        return updated_urls