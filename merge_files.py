import pandas as pd 
import glob 
import os 

def joined_files():
    joined_files = os.path.join(r"<insert location where files are stored>", "insert file naming convention name*.csv")
    joined_list = glob.glob(joined_files)
    df = pd.concat(map(pd.read_csv,joined_list), ignore_index=True)
    print('Files are joining')
    df.to_csv('test.csv')
joined_files()