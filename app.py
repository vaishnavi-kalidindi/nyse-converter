 
import pandas as pd
from dask import dataframe as dd
import os

#'data/nyse_all/nyse_data/NYSE*.txt.gz'
#'data/nyse_all/nyse_json/part-*.json.gz'


def main():
    src_base_dir=os.environ['SRC_BASE_DIR']
    tgt_base_dir=os.environ['TGT_BASE_DIR']
    print('File Format Conversion Started')
    dn=dd.read_csv(f'{src_base_dir}/NYSE*.txt.gz',
        
        blocksize=None,
        names=['ticker', 'trade_date', 'open_price', 'low_price', 'high_price', 'close_price', 'volume']
    )
    print('DataFrame is created and written in JSON Format')
    dn.to_json( 
           f'{tgt_base_dir}/part-*.json.gz',
           orient='records',
           lines=True,
           compression='gzip',
            
           name_function=lambda i: '%05d' % i
           )
    print('File Format Conversion Completed')
    
     


if __name__ == '__main__':
    main()
