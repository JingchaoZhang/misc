from unittest import skip
#from dask.multiprocessing import get
from dask.distributed import Client, progress
import yfinance as yahooFinance
import bs4 as bs
import pickle
import requests
import pandas as pd
import dask.dataframe as dd

if __name__ == '__main__':

    client = Client(threads_per_worker=1, n_workers=8)

    def save_sp500_tickers():
        resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        soup = bs.BeautifulSoup(resp.text, 'lxml')
        table = soup.find('table', {'class': 'wikitable sortable'})
        tickers = []
        for row in table.findAll('tr')[1:]:
            ticker = row.findAll('td')[0].text.strip ()
            tickers.append(ticker)
            
        with open("sp500tickers.pickle","wb") as f:
            pickle.dump(tickers,f)
            
        return tickers

    sp500 = save_sp500_tickers()
    df = pd.Series(sp500, name='ticker')

    def div(ticker):
        GetInfo = yahooFinance.Ticker(ticker)
        try:
            div = GetInfo.info['dividendYield']
            #print(f'Prcessing {ticker}')
            return div
        except Exception:
            skip
            #print(f'KeyError for {ticker}')

    ddata = dd.from_pandas(df, npartitions=8)

    res = ddata.map_partitions(lambda df: df.apply(div)).compute() 
    res.name = 'div'
    
    all_div = pd.concat([df, res], axis=1).sort_values(by='div', ascending=False)
    all_div.to_csv('all_div.csv', sep=',', index=False)

    print(all_div.head(30))
