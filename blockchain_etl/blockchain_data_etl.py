import requests
import pandas as pd

# API Endpoint from Coingecko to get market data
url = 'https://api.coingecko.com/api/v3/coins/markets'
params = {
    'vs_currency': 'usd',
    'ids': 'bitcoin,ethereum,cardano,binancecoin'
}

# Calling the API to fetch data
try:
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        print('Blockchain data fetched successfully.')
    else:
        print('Failed to retrive data.')
except Exception as e:
    print(f'Error occured while calling the endpoint to fetch data: {e}')

# Convert data into a dataframe
df = pd.DataFrame(data)
print(df.head())