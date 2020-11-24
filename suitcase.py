
from im_lib import *
from event import *

class Suitcase(Event):
    
    def __init__(self, start_date):
        super().__init__(start_date)
    
    def check_suitcase(self, df, number_row):
        receiptid_=df.at[number_row,"receiptid"] 
        if receiptid_==None:
            return print("are you serious it is null????")
        if receiptid_ == '-1':
            return print("receiptid problem, may be create new table?")
        else: find_receiptid= receiptid_[receiptid_.find('/')+1:len(receiptid_)]
        
        conn=psycopg2.connect("dbname='unpack' user='postgres' password='root' host='localhost' port='5432' ") 
        data2=pd.read_sql_query("SELECT * FROM receipts WHERE receiptid = %(receiptid)s", con=conn, params={"receiptid":find_receiptid})
        conn.commit()
        conn.close()
        flag=0
        number=0
        compare_data=df.at[number_row,"local_date"]
        while flag==0:
            
            tmp_data=data2.at[number, "dateopen"] 
            if tmp_data.date()==compare_data.date():
                flag=1
            else: 
                number =number+1 
           
        flag=0

        quantitypackageone=data2.at[number,"quantitypackageone"]
        quantitypackagedouble=data2.at[number,"quantitypackagedouble"]
        package_type_final=df.at[number_row,"package_type_final"]

        if quantitypackageone == package_type_final:
            return 0
        elif quantitypackagedouble==package_type_final:
            return 0
        else: 
            return 1