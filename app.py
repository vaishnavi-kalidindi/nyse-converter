 
import pandas as pd
from dask import dataframe as dd
import os
import glob

#'data/nyse_all/nyse_data/NYSE*.txt.gz'
#'data/nyse_all/nyse_json/part-*.json.gz'
 # tgt_dir=os.environ['TGT_DIR']
 

def main():
    src_dir=os.environ['SRC_DIR']
     
    src_file_names=sorted(glob.glob(f'{src_dir}/NYSE*.txt.gz'))
    tgt_file_names=[file.replace('txt','json').replace('nyse_data','nyse_json')
                    for file in src_file_names]
    print('File Format Conversion Started')
    dn=dd.read_csv(
         src_file_names,
         blocksize=None,
         names=['ticker', 'trade_date', 'open_price', 'low_price', 'high_price', 'close_price', 'volume']
    )
    print('DataFrame is created and written in JSON Format')
    dn.to_json( 
            tgt_file_names,
           orient='records',
           lines=True,
           compression='gzip'
            
            
           )
    print('File Format Conversion Completed')
     


     
    
     


if __name__ == '__main__':
    main()
