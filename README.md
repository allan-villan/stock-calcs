# stock-calcs
> Downloads stock data using the yfinance library and performs calculations on it, using Apache Airflow to manage workflows set to download at 2PM PST every weekday.

## Requirements:
- Python version: 3.10.4

- Airflow version: 2.3.3

- ta version: 0.10.2

### DAG graph:
<img width="549" alt="marketvol DAG graph" src="https://user-images.githubusercontent.com/84660320/185814119-e2d0e3a5-8213-4282-843d-d7cc673dc413.png">

### Grid graph:
<img width="440" alt="Screen Shot 2022-08-23 at 2 45 01 PM" src="https://user-images.githubusercontent.com/84660320/186272496-9990ad86-76af-4637-9584-fe8742d1f770.png">

### 2022 Calendar (Only August):
<img width="434" alt="Screen Shot 2022-08-23 at 3 13 49 PM" src="https://user-images.githubusercontent.com/84660320/186275599-d496afc5-cde1-4730-94ed-407e0d2a9c5f.png">

### Sample data from yfinance:
<img width="556" alt="Screen Shot 2022-08-23 at 3 10 46 PM" src="https://user-images.githubusercontent.com/84660320/186275235-2963dbb7-2e52-4d01-b31b-a255b5fea15a.png">

### Sample data after applying calculations:
<img width="630" alt="Screen Shot 2022-08-23 at 3 08 10 PM" src="https://user-images.githubusercontent.com/84660320/186274999-43e77c4b-2501-464d-b24f-b07453c74ec8.png">