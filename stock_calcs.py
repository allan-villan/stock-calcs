from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ta.volume import VolumeWeightedAveragePrice
import yfinance as yf 
import pandas as pd

# Global variable
TMP_FILE_LOC = '/tmp'

# File path to categorize by day
exec_date = '{{ ds }}'
final_loc = f'/Users/adcv/VSCode/projects/airflow-miniproject-1/stock-calcs/data/{exec_date}'


def get_data(ticker:str, start_date:str, end_date:str):
    """ get_data downloads the data for the ticker symbol passed to this method

    Using the yfinance library, this method downloads the ticker symbol passed
    as an argument to this method. It downloads the current day's data and saves
    it as a csv in the temporary location created in the task: t0.
    """

    ticker_df = yf.download(f'{ticker}', start=start_date, end=end_date, interval='1m')
    ticker_df.to_csv(f'{TMP_FILE_LOC}/{ticker}_data.csv')


def preview_data(location: str):
    """query_data queries the data found in the location passed in as an argument

    This method displays a preview of the data present in the location provided.
    """

    aapl_df = pd.read_csv(f'{location}/AAPL_data.csv')
    print(f'AAPL data for {exec_date}')
    print(aapl_df.head())

    tsla_df = pd.read_csv(f'{location}/TSLA_data.csv')
    print(f'TSLA data for {exec_date}')
    print(tsla_df.head())


def get_vwap(location: str):
    """get_vwap gets the Volume Weighted Average Price for Apple and Tesla on a given day.

    "The volume-weighted average price (VWAP) is a measurement that shows the average price
    of a security, adjusted for its volume" - Investopedia

    VWAP is a commonly used trading statistic to determine a stock's price based
    on the average volume for the day. Because stock prices move significantly in times
    of high liquidity, it's easy to lose track of what a stock should cost and focus on
    the direction the price is going. VWAP acts as a guide to ground expectations for what
    a stock should cost based on that day's statistics.

    get_vwap uses the imported ta (technical analysis) library to simplify the calculation.

    Arguments:
        Location - File path to locate .csv files
    """
    aapl_df = pd.read_csv(f'{location}/AAPL_data.csv')
    tsla_df = pd.read_csv(f'{location}/TSLA_data.csv')

    # Imports ta library to use the VWAP function
    aapl_df['VWAP']=VolumeWeightedAveragePrice(high=aapl_df['High'],
                                               low=aapl_df['Low'],
                                               close=aapl_df["Adj Close"],
                                               volume=aapl_df['Volume'],
                                               window=3,
                                               fillna=True).volume_weighted_average_price()

    tsla_df['VWAP']=VolumeWeightedAveragePrice(high=tsla_df['High'],
                                               low=tsla_df['Low'],
                                               close=tsla_df["Adj Close"],
                                               volume=tsla_df['Volume'],
                                               window=3,
                                               fillna=True).volume_weighted_average_price()

    aapl_df.to_csv(f'{location}/AAPL_vwap.csv', index=False)
    tsla_df.to_csv(f'{location}/TSLA_vwap.csv', index=False)
    
    # Displays preview for log
    print(aapl_df.head())
    print(tsla_df.head())


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
    bash_command = f'mkdir -p {final_loc}/',
    dag = dag_conf
)

t1 = PythonOperator(
    task_id = 'get_aapl_data',
    python_callable = get_data,
    op_kwargs = {'ticker': 'AAPL', 
                 'start_date': '{{ ds }}',
                 'end_date': '{{ next_ds }}'},
    dag = dag_conf
)

t2 = PythonOperator(
    task_id = 'get_tsla_data',
    python_callable = get_data,
    op_kwargs = {'ticker': 'TSLA',
                 'start_date': '{{ ds }}',
                 'end_date': '{{ next_ds }}',
                },
    dag = dag_conf
)

t3 = BashOperator(
    task_id = 'move_aapl_data',
    bash_command = f'mv {TMP_FILE_LOC}/AAPL_data.csv {final_loc}/AAPL_data.csv',
    dag = dag_conf
)

t4 = BashOperator(
    task_id = 'move_tsla_data',
    bash_command = f'mv {TMP_FILE_LOC}/TSLA_data.csv {final_loc}/TSLA_data.csv',
    dag = dag_conf
)

t5 = PythonOperator(
    task_id = 'get_vwap',
    python_callable = get_vwap,
    op_kwargs = {'location': final_loc},
    dag = dag_conf
)

t0 >> [t1,t2]
t1 >> t3
t2 >> t4
[t3,t4] >> t5