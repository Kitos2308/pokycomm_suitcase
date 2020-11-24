
from im_lib import *
from event import *

class Alarm(Event):
   
    def __init__(self, start_date):
        super().__init__(start_date)
    
    def check_alarm(self, df, number_row):
        
        start_date=df.at[number_row,"dateini"]
        end_date=df.at[number_row,"date"]
        
        conn=psycopg2.connect("dbname='unpack' user='postgres' password='root' host='localhost' port='5432' ") 
        #mysql_statement= """SELECT * FROM polycommalarm WHERE total = %(total)s"""   
        data2=pd.read_sql_query("SELECT * FROM polycommalarm WHERE date BETWEEN  %(dstart)s AND %(dfinish)s ",con=conn, params={"dstart":start_date, "dfinish":end_date })
        if data2.empty : 
            conn.commit()
            conn.close() 
           
            return 0
        else: 
            
            conn.commit()
            conn.close()
            return 1
    
    def take_data_alarm(self):
        
        conn=psycopg2.connect("dbname='unpack' user='postgres' password='root' host='localhost' port='5432' ") 
        data_alarm=pd.read_sql_query("SELECT * FROM polycommalarm WHERE localdate >%(date)s order by localdate limit 1000" ,con=conn, params={"date":self.date_df}) 
        conn.commit()
        conn.close()
        return data_alarm
    
    def take_certain_data_df(self, df,device_):
        
        
        
        device_out=df[df['device']==int(device_)]
       
        _device_=(df.mask(df['device']==int(device_)))
        mask_device=(df['device']!=_device_['device'])
       
        # if _device_.empty:
        #     print("nothing droping")
        # else:   
        #     df.drop(index=df.loc[mask_device].index,inplace=True)
        
        df._clear_item_cache
        device_out._clear_item_cache
        mask_device._clear_item_cache
        return device_out
    


