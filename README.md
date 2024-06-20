# GA4 Data Extraction Tool

This Python script extracts data from Google Analytics 4 (GA4) using the Google Analytics Data API. It allows for batch report generation with customizable dimensions and metrics. The extracted data is then transformed and output as Pandas DataFrames.

## Setup

### link google service account
* You can follow the instructions on [this page](https://www.contentful.com/help/google-analytics-service-account-setup/) to link your Google service account to Google Analytics 4.

## Usage

### Initialization

1. **Service Account**: 
   Place your service account JSON file in the project directory.
   
2. **Update the service account path** in the script:
    ```python
    auth_data = AuthorizationData("YOUR_SERVICE_ACCOUNT.json")
    ```

3. **Set your GA4 property ID**:
    ```python
    data = ga4.generate_batch_report(auth_data.client, YOUR_PROPERTY_ID, config)
    ```

### Configuration

Define the configuration dictionary to specify the dimensions and metrics for the reports. Here is an example configuration:

```python
config = {
    "signal_data": [
        {"dimension": ['date', 'userGender', 'userAgeBracket']},
        {"metric": ['activeUsers']}
    ],
    "active_usr": [
        {"dimension": ['date']},
        {"metric": ['active1DayUsers', 'active7DayUsers', 'active28DayUsers']}
    ]
}
```
