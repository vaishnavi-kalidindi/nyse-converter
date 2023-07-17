 
import pandas as pd
from dask import dataframe as dd




def main():
    print('File Format Conversion Started')
    dn=dd.read_csv('data/nyse_all/nyse_data/NYSE*.txt.gz',
        
        blocksize=None,
        names=['ticker', 'trade_date', 'open_price', 'low_price', 'high_price', 'close_price', 'volume']
    )
    print('DataFrame is created and written in JSON Format')
    dn.to_json( 
           f'data/nyse_all/nyse_json/part-*.json.gz',
           orient='records',
           lines=True,
           compression='gzip',
            
           name_function=lambda i: '%05d' % i
           )
    print('File Format Conversion Completed')
    
     


if __name__ == '__main__':
    main()
