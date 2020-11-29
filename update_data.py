"""
Imports
"""
import pandas as pd
import io
import json
import requests


"""
Data Scraping
"""
end_date = pd.Timestamp.now().strftime('%Y-%m-%d')
    
def query_stlouisfed(stream='GDPDEF', end_date:str=end_date, start_date='1999-01-01'):
    stlouisfed_url = 'https://fred.stlouisfed.org/graph/fredgraph.csv'

    params = {
        'id': stream,
        'cosd': start_date,
        'coed': end_date,
    }

    headers = {
        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    }

    r = requests.get(stlouisfed_url, params=params, headers=headers)

    s = (pd.read_csv(io.StringIO(r.content.decode('utf-8')))
         .sort_values('DATE', ascending=False)
         .set_index('DATE')
         .pipe(lambda df: df[df.columns[-1]])
        )
    
    s.index.name = 'date'
    
    return s

query_FX = lambda currency_from='US', currency_to='UK', end_date=end_date, start_date='1999-01-01': query_stlouisfed(f'EX{currency_from}{currency_to}', end_date, start_date)
query_GDP = lambda end_date=end_date, start_date='1999-01-01': query_stlouisfed('GDPDEF', end_date, start_date)

def query_alberta_hub_price():
    alberta_url = 'https://www.alberta.ca/alberta-natural-gas-reference-price.aspx'
    r = requests.get(alberta_url)
    
    df = (pd
          .read_html(r.text)
          [1]
          .set_index('Unnamed: 0')
         )

    df = df.unstack().reset_index()
    df.columns = ['month', 'year', 'price']
    df.index = pd.to_datetime(df['year'].astype(str) + ' ' + df['month'].astype(str), format='%Y %B')

    s_price = df.sort_index(ascending=False)['price'].dropna()
    s_price.name = 'alberta_hub_price'
    s_price.index.name = 'date'
    s_price.index = s_price.index.astype(str)

    return s_price

def query_victoria_hub_price():
    csv_url = 'http://www.nemweb.com.au/REPORTS/CURRENT/VicGas/INT310_V4_PRICE_AND_WITHDRAWALS_1.CSV'

    s_price = (pd
               .read_csv(csv_url)
               .pipe(lambda df: df.groupby(pd.to_datetime(df['gas_date'])))
               ['price_value']
               .mean()
               .resample('m')
               .mean()
               .round(2)
               .sort_index(ascending=False)
              )
    
    s_price.name = 'victoria_hub_price'
    s_price.index.name = 'date'
    s_price.index = s_price.index.astype(str)

    return s_price


"""
Data Processing
"""
def query_data(data_queries):
    series_name_to_data = dict()

    for series_name, data_query in data_queries.items():
        s = data_query['query_func'](**data_query['kwargs'])
        series_name_to_data[series_name] = {'values': s.to_list(), 'dates': s.index.to_list()}
        
    return series_name_to_data

get_series = lambda series, series_name_to_data: pd.Series(series_name_to_data[series]['values'], 
                                                           index=pd.to_datetime(series_name_to_data[series]['dates']), 
                                                           name=series)

def save_data(series_name_to_data, data_dir='data'):
    with open(f'{data_dir}/indexes.json', 'w') as fp:
        json.dump(series_name_to_data, fp)
        
    return 

def get_most_recent_values(series_name_to_data):
    series_names = []
    dates = []
    values = []

    for series_name, data in series_name_to_data.items():
        date_to_most_recent_value = (pd.Series(data['values'], 
                                               index=data['dates'])
                                     .sort_index(ascending=False)
                                     .head(1)
                                     .to_dict()
                                    )

        series_names += [series_name]
        dates += list(date_to_most_recent_value.keys())
        values += list(date_to_most_recent_value.values())

    df_most_recent_values = pd.DataFrame({'Date': dates, 'Value': values}, index=series_names)
    df_most_recent_values.index.name = 'Variable'
    
    return df_most_recent_values

def update_readme_time(readme_fp, 
                       df_most_recent_values, 
                       splitter='<br>'):
    
    with open(readme_fp, 'r') as readme:
        txt = readme.read()
    
    start, end = txt.split(splitter)
    table = df_most_recent_values.to_markdown()
    
    new_txt = start + splitter + '\n\n' + table
    
    with open(readme_fp, 'w') as readme:
        readme.write(new_txt)
        
    return



"""
Task
"""
data_queries = {
    'FX US->EU': {
        'query_func': query_FX,
        'kwargs': {
            'currency_from': 'US',
            'currency_to': 'EU'
        }
    },
    'FX US->UK': {
        'query_func': query_FX,
        'kwargs': {
            'currency_from': 'US',
            'currency_to': 'UK'
        }
    },
    'FX CA->US': {
        'query_func': query_FX,
        'kwargs': {
            'currency_from': 'CA',
            'currency_to': 'US'
        }
    },
    'FX US->AL': {
        'query_func': query_FX,
        'kwargs': {
            'currency_from': 'US',
            'currency_to': 'AL'
        }
    },
    'GDP US': {
        'query_func': query_GDP,
        'kwargs': {}
    },
    'Alberta Hub': {
        'query_func': query_alberta_hub_price,
        'kwargs': {}
    },
    'Victoria Hub': {
        'query_func': query_victoria_hub_price,
        'kwargs': {}
    },
}

series_name_to_data = query_data(data_queries)
df_most_recent_values = get_most_recent_values(series_name_to_data)
update_readme_time('README.md', df_most_recent_values)