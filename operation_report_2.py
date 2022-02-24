import requests
from bs4 import BeautifulSoup
import warnings 
warnings.filterwarnings('ignore')
import pandas as pd 
import numpy as np
from pandas.io import  gbq
from datetime import datetime 
import os
warnings.filterwarnings('ignore')

URL = "insert URL or API"
r = requests.get(URL)
soup = BeautifulSoup(r.content, 'html5lib')

class get_refresh_report(): 
    def request_API(): 
        r = requests.get(URL)
        if r.status_code == '404': 
                raise ValueError('cannot connect to web server')
        else: 
            print('202 status - connected')

    def generate_report(): 
        table = soup.find_all('table')[0]
        trs = table.findAll('tr')
        truetable = trs[4:]
        df = pd.read_html('<table><tbody>'+str(truetable)+'</table></tbody>',header=1)
        df1 = df[0]
        #df.columns = df.droplevel(1) #change df.columns.droplevel(2), df.droplevel(1)
        df2 = df1.rename(columns={'Unnamed: 1_level_0': 'Period_from','Unnamed: 2_level_0': 'Period_To', 'Unnamed: 3_level_0': 'Phase_name', 'Unnamed: 4_level_0': 'Run_status','Unnamed: 5_level_0': 'Start_date', 'Unnamed: 6_level_0': 'start_time','Unnamed: 7_level_0':'end_date','Unnamed: 8_level_0' : 'End_time','Unnamed: 9_level_0': 'ADP_CONFIGS_FREEZED_FLAG' })
        df2 = df2.iloc[3:]
        timestr = datetime.now().strftime("%Y-%m-%d")
        path = r'C:\Users\a151968\Desktop\Operations/'
        new_update = os.path.join(path, 'lovevery_report_'+timestr+'.csv')
        output_file = df2.to_csv(new_update,header=True)
        return output_file


#bigquery tables
    #def create_table_gcp(): 
        #timestr = datetime.now().strftime("%Y-%m-%d")
        #table = soup.find_all('table')[0]
        #df = pd.read_html(str(table))[0]
        #df.columns = df.columns.droplevel(1)
        #df2 = df.rename(columns={'Unnamed: 1_level_0': 'Period_from','Unnamed: 2_level_0': 'Period_To', 'Unnamed: 3_level_0': 'Phase_name', 'Unnamed: 4_level_0': 'Run_status','Unnamed: 5_level_0': 'Start_date', 'Unnamed: 6_level_0': 'start_time','Unnamed: 7_level_0':'end_date','Unnamed: 8_level_0' : 'End_time','Unnamed: 9_level_0': 'ADP_CONFIGS_FREEZED_FLAG' })
        #df2 = df2.iloc[3:]
        #lovevery_status = df2[df2['Wed Jan 19 11:00:26 PST 2022'] == 'gcp_client']
        #df2.to_gbq(destination_table='', project_id='', if_exists='append')
        #print('Data has been uploaded on {}'.format(timestr))

if __name__ == "__main__": 
    run_refresh = get_refresh_report()
    run_refresh.request_API()
    run_refresh.generate_report()

        