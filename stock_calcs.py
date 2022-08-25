from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ta.volume import VolumeWeightedAveragePrice
import yfinance as yf 
import pandas as pd

# Global variable
TMP_FILE_LOC = '/tmp/stock-calcs'

# File path to categorize by day
exec_date = '{{ ds }}'
end_date = '{{ next_ds }}'
final_loc = f'/Users/adcv/VSCode/projects/airflow-miniproject-1/stock-calcs/data/{exec_date}'

# Tickers to download from yfinance
tickers = ["AAPL","TSLA","GOOGL","AMD","PG","BHP"]


def get_data(start_date:str, end_date:str, tickers:list):
    """ get_data downloads the data for the ticker symbols passed to this method

    Using the yfinance library, which gets stock data straight from
    Yahoo! Finance's API, this method downloads the ticker symbols passed
    as an argument. It downloads the current day's data and saves
    it as a csv in the temporary location created in the task: t0.

    Arguments:
        tickers(list) - List of strings for tickers to download
        start_date(str) - when to begin download (exec_date for that day)
        end_date(str) - when to end download
    """
    for ticker in tickers:
        cur_tic = str(ticker)
        cur_df = yf.download(f'{cur_tic}', start=start_date, end=end_date, interval='1m')
        cur_df.to_csv(f'{TMP_FILE_LOC}/{cur_tic}_data.csv')


def preview_data(tickers:list, location:str):
    """query_data queries the data found in the location passed in as an argument

    This method displays a preview of the data present in the location provided
    using head(). Printing helps with logs in Airflow.

    Arguments:
        tickers(list) - List of strings for tickers to be previewed
        location(str) - File path where the .csv files are located 
    """
    for ticker in tickers:
        cur_tic = str(ticker)
        cur_df = pd.read_csv(f'{location}/{cur_tic}_data.csv')
        print(f'{cur_tic} data for {exec_date}')
        print(cur_df.head())


def get_vwap(location:str, tickers:list):
    """get_vwap gets the Volume Weighted Average Price for tickers on a given day.

    "The volume-weighted average price (VWAP) is a measurement that shows the average price
    of a security, adjusted for its volume" - Investopedia

    VWAP is a commonly used trading statistic to determine a stock's price based
    on the average volume for the day. Because stock prices move significantly in times
    of high liquidity, it's easy to lose track of what a stock should cost and focus on
    the direction the price is going instead. VWAP acts as a guide to ground expectations
    for what a stock should cost based on that day's statistics.

    get_vwap uses the imported ta (technical analysis) library to simplify the calculation.

    Arguments:
        tickers(list) - List of strings for tickers to look for
        location(str) - File path to locate .csv files
    """
    for ticker in tickers:
        cur_tic = str(ticker)
        cur_df = pd.read_csv(f'{location}/{cur_tic}_data.csv')
    
        # Imports ta library to use the VWAP function
        cur_df['VWAP']=VolumeWeightedAveragePrice(high=cur_df['High'],
                                                   low=cur_df['Low'],
                                                   close=cur_df["Adj Close"],
                                                   volume=cur_df['Volume'],
                                                   window=3,
                                                   fillna=True).volume_weighted_average_price()

        cur_df.to_csv(f'{location}/{cur_tic}_vwap.csv', index=False)

        # Displays preview for log
        print(cur_df.head())


# arguments for DAGs
default_args = {
    'owner': 'ko-allan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay':timedelta(minutes = 2)
}

# DAG configuration
dag_conf = DAG(
    dag_id = 'stock-calcs',
    default_args = default_args,
    description = 'DAG for stock-calcs',
    start_date = datetime(2022, 8, 1),
    catchup = True,
    schedule_interval = '0 14 * * 1-5'
)

t0 = BashOperator(
    task_id = 'make_directory',
    bash_command = f'mkdir -p {final_loc}/ | mkdir -p {TMP_FILE_LOC}/',
    dag = dag_conf
)

t1 = PythonOperator(
    task_id = 'get_ticker_data',
    python_callable = get_data,
    op_args = [exec_date, end_date, tickers],
    dag = dag_conf
)

t2 = BashOperator(
    task_id = 'move_ticker_data',
    bash_command = f'mv {TMP_FILE_LOC}/*_data.csv {final_loc}',
    dag = dag_conf
)

t3 = PythonOperator(
    task_id = 'get_vwap',
    python_callable = get_vwap,
    op_args = [final_loc, tickers],
    dag = dag_conf
)

t0 >> t1 >> t2 >> t3