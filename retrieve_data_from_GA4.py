import pandas as pd
import os
from collections import defaultdict
from datetime import datetime
import uuid
from typing import List, Dict, Any
# import backoff

# GA4 package: https://pypi.org/project/google-analytics-data/
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric
from google.analytics.data_v1beta.types import RunReportRequest, BatchRunReportsRequest


class AuthorizationData:
    def __init__(self, service_account: str):
        """
        Initializes the AuthorizationData with the provided service account credentials.

        Args:
            service_account (str): Path to the service account JSON file.
        """
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account
        self.client = BetaAnalyticsDataClient()


class GetGA4Data():
    def __init__(self, start_date=None, end_date=None):
        """
        Initializes the GetGA4Data with optional start and end dates.

        Args:
            start_date (str, optional): The start date for the data range. Defaults to '2024-04-01'.
            end_date (str, optional): The end date for the data range. Defaults to '2024-04-30'.
        """
        self.start_date = start_date if start_date else '2024-04-01'
        self.end_date = end_date if end_date else '2024-04-30'

    def dimension_parameters(self, dimension_lst: List[str]) -> List[str]:
        """
        Sets the dimension parameters for the report request.

        Args:
            dimension_lst (List[str]): A list of dimension names.

        Returns:
            List[str]: The list of dimension names.
        """
        self.dimension_lst = dimension_lst
        return self.dimension_lst

    def metric_parameters(self, metric_lst: List[str]) -> List[str]:
        """
        Sets the metric parameters for the report request.

        Args:
            metric_lst (List[str]): A list of metric names.

        Returns:
            List[str]: The list of metric names.
        """
        self.metric_lst = metric_lst
        return self.metric_lst

    def reset_parameters(self):
        """
        Resets the dimension and metric parameters.
        """
        self.dimension_lst = []
        self.metric_lst = []

    def request_parameters(self, dimension_lst: List[str], metric_lst: List[str]) -> RunReportRequest:
        """
        Creates a RunReportRequest object with the given dimension and metric lists.

        Args:
            dimension_lst (List[str]): A list of dimension names.
            metric_lst (List[str]): A list of metric names.

        Returns:
            RunReportRequest: The RunReportRequest object.
        """
        request = RunReportRequest(
            dimensions=[Dimension(name=dim)
                        for dim in self.dimension_parameters(dimension_lst)],
            metrics=[Metric(name=metric)
                     for metric in self.metric_parameters(metric_lst)],
            date_ranges=[
                DateRange(start_date=self.start_date, end_date=self.end_date)],
            limit=100000,
        )
        return request

    def parse_input(self, config: Dict[str, Any], name: str, params: str) -> List[str]:
        """
        Parses the input configuration to retrieve the specified parameters.

        Args:
            config (Dict[str, Any]): The configuration dictionary.
            name (str): The name of the configuration.
            params (str): The parameter type to retrieve ('dimension' or 'metric').
            e.g. parse_input(config, "active_usr", "dimension")

        Returns:
            List[str]: The list of parameters. e.g. Output: ['date', 'userGender', 'userAgeBracket']        
        """
        if name in config:
            value = [data[params]
                     for data in config[name] if params in data.keys()]
        return value[0]

    def run_report_batch(self, client: BetaAnalyticsDataClient, property_id: int, config: Dict[str, Any]) -> BatchRunReportsRequest:
        """
        Runs a batch report request.

        Args:
            client (BetaAnalyticsDataClient): The Analytics Data API client.
            property_id (int): The GA4 property ID.
            config (Dict[str, Any]): The configuration dictionary.

        Returns:
            BatchRunReportsRequest: The response from the batch run reports request.
        """
        request_lst = []
        for name in list(config.keys()):
            request_lst.append(RunReportRequest(self.request_parameters(
                dimension_lst=self.parse_input(config, name, "dimension"),
                metric_lst=self.parse_input(config, name, "metric")
            )))
            self.reset_parameters()

        requests = BatchRunReportsRequest(
            property=f"properties/{property_id}",
            requests=request_lst
        )
        response = client.batch_run_reports(requests)
        return response

    def generate_batch_report(self, client: BetaAnalyticsDataClient, property_id: int, config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        Generates a batch report.

        Args:
            client (BetaAnalyticsDataClient): The Analytics Data API client.
            property_id (int): The GA4 property ID.
            config (Dict[str, Any]): The configuration dictionary.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary of DataFrames with the report data.
        """
        # for better dealing with dict type
        report_data = defaultdict(list)
        response = self.run_report_batch(client, property_id, config)
        # list config name
        config_name_lst = list(config.keys())
        # retrieve config name by order
        config_count = 0
        # final output
        result_dict = {}

        for report in response.reports:
            # get all batch report
            for row in report.rows:
                # config name: config_name_lst[config_count]
                dimensions = self.parse_input(
                    config, config_name_lst[config_count], "dimension")
                metrics = self.parse_input(
                    config, config_name_lst[config_count], "metric")

                # dimensions
                for idx, key in enumerate(dimensions):
                    report_data[key].append(row.dimension_values[idx].value)
                # metrics
                for idx, key in enumerate(metrics):
                    report_data[key].append(row.metric_values[idx].value)

            result_dict[config_name_lst[config_count]
                        ] = self.create_dataframe(report_data)
            # reset value of report_data
            report_data = defaultdict(list)
            # next config name
            config_count += 1
        return result_dict

    def add_insert_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds UUID and timestamp columns to the DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with added UUID and timestamp columns.
        """
        df['uuid'] = [uuid.uuid4().hex for _ in range(len(df.index))]
        df['emitted_at'] = datetime.now()
        return df

    def change_col_type(self, df: pd.DataFrame) -> None:
        """
        Changes the column types of the DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame.
        """
        # Dictionary mapping column names to their desired types
        column_types = {
            'activeUsers': 'int64',
            'active1DayUsers': 'int64',
            'active7DayUsers': 'int64',
            'active28DayUsers': 'int64',
            'userEngagementDuration': 'float64',
            'engagedSessions': 'int64',
            'sessions': 'int64'
        }

        # Loop through the dictionary and change column types if the column exists in the DataFrame
        for column, dtype in column_types.items():
            if column in df.columns:
                df[column] = df[column].astype(dtype)

    def change_date_type(self, df: pd.DataFrame) -> None:
        """
        Changes the date column type of the DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame.
        """
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].apply(
            lambda x: datetime(x.year, x.month, x.day))

    def create_dataframe(self, data: Dict[str, List[str]]) -> pd.DataFrame:
        """
        Creates a DataFrame from the given data and applies necessary transformations.

        Args:
            data (Dict[str, List[str]]): The input data dictionary.

        Returns:
            pd.DataFrame: The resulting DataFrame.
        """
        df = pd.DataFrame(data)
        self.change_col_type(df)
        self.change_date_type(df)
        df = self.add_insert_info(df)
        return df


if __name__ == '__main__':
    config = {"signal_data": [
        {"dimension": ['date', 'userGender', 'userAgeBracket']},
        {"metric": ['activeUsers']}
    ],
        "active_usr": [
            {"dimension": ['date']},
            {"metric": ['active1DayUsers',
                        'active7DayUsers', 'active28DayUsers']}
    ]
    }
    auth_data = AuthorizationData("YOUR_SERVICE_ACCOUNT.json")
    ga4 = GetGA4Data()
    data = ga4.generate_batch_report(auth_data.client, YOUR_PROPERTY_ID, config)
