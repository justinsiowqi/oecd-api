import json
import requests
import pandas as pd
import time
import datetime
import re
import os

class FileCombiner:
    """ Class splits each macro dataframe according to its country before combining everything in a left-join.

    Methods:
        get_csv: Loops through the csv files in a folder and returns a list of dataframes.
        create_timestamp_df: Creates an empty dataframe with timestamp as the index. It ranges from 2009-01-01 till present day.
        split_csv: Splits the dataframe according to the country and performs a left outer-join.
        join_csv: Calls get_csv on a list of dataframes and performs a left outer-join.

    """
    def get_csv(self, folder):
        """ Class method loops through the csv files in a folder and returns a list of dataframes.

        Note:
            Uses the folder directory that is created in MacroDataCollector.

        Args:
            folder: Name of the folder containing macro csv files.

        Returns:
            A list of dataframes.

        """
        df_list = []
        for filename in os.listdir(folder):
            if filename.endswith('.csv'):
                df = pd.read_csv(os.path.join(folder, filename), parse_dates=['timestamp'])
                df_list.append(df)
        return df_list

    def create_timestamp_df(self):
        """ Class method creates an empty dataframe with timestamp as the index. It ranges from 2008-12-31 till present day.

        Returns:
            An empty dataframe with timestamp as the index column.

        """
        start_date = pd.Timestamp('2008-12-31')
        end_date = pd.Timestamp.now()
        timestamp_df = pd.DataFrame({'timestamp': pd.date_range(start_date, end_date)}).set_index('timestamp')
        return timestamp_df

    def split_csv(self, df):
        """ Class method splits the dataframe according to the country and performs a left outer-join.

        Note:
            Splits the dataframe according to its unique countries. Performs an outer-join with timestamp_df.
            Timestamp is set as the index and each country column is removed.

        Args:
            df: Macro dataframe. 

        Returns:
            A dataframe containing the timestamp_df index column and the value from each country.

        """
        country_list = df['country'].unique()

        new_df_list = []
        for country in country_list:
            new_df = df[df['country'] == country].set_index('timestamp')
            new_df.index = pd.to_datetime(new_df.index, format='%Y-%m-%d')
            new_df.columns = [f'{col}: {country}' for col in new_df.columns]
            new_df_list.append(new_df)

        timestamp_df = self.create_timestamp_df()
        final_df = timestamp_df.join(new_df_list, how='outer')
        final_df = final_df.filter(regex='^(?!country)')

        return final_df

    def join_csv(self, folder, delete_null=False):
        """ Class method calls get_csv on a list of dataframes and performs a left outer-join.

        Note:
            Loop through and splits each dataframe according to its unique countries. Performs an outer-join with timestamp_df.
            To remove rows that contain only null values, pass delete_null=True as an argument.

        Args:
            folder: Name of the folder containing macro csv files.
            delete_null: Removes rows that contains only null values (set to false by default).

        Returns:
            A dataframe with all the dataframes combined column-wise.

        """
        df_list = self.get_csv(folder)

        split_df_list = []
        for df in df_list:
            split_df = self.split_csv(df)
            split_df_list.append(split_df)
        
        timestamp_df = self.create_timestamp_df()
        join_df = timestamp_df.join(split_df_list, how='outer')

        if delete_null:
            join_df.dropna(how='all', inplace=True)
        
        join_df.to_csv('macro-indicators-concatenated.csv')

        return join_df