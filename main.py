from im_lib import *
from alarm import *
from issue import *
from receipt import *
from suitcase import *
from event import *

# df_output.to_csv('out.csv')

df_output=pd.DataFrame(columns=['number_of_group','type_occasion','start_date','end_date','internal_occasion','device','devicecode','qauntity_package_one','quantity_package_double','>420','r-a'])
df_output.to_csv('out.csv')



conn=psycopg2.connect("dbname='unpack' user='postgres' password='root' host='localhost' port='5432' ")
date_=pd.read_sql_query("select local_date from polycomm_suitcase  order by random() limit 1",con=conn)

conn.commit()
conn.close()
p1=Event(date_.iat[0,0])
p2=Issue(date_.iat[0,0])
p3=Alarm(date_.iat[0,0])
p4=Receipt(date_.iat[0,0])
p5=Suitcase(date_.iat[0,0])


df=p1.period_time()

data_issue=p2.take_data_issue()


data_alarm=p3.take_data_alarm()

data_receipt=p4.take_data_receipt()


length=len(p1.find_device(df).index)
devices=p1.find_device(df)

devices_list=[]

for i in devices:
    devices_list.append(i)

DeviceCode=[]
cnt_code=0

while length>cnt_code:
    tmp=devices_list[cnt_code]
    item=p1.find_devicecode(tmp)
    DeviceCode.append(item.iat[0,0])
    cnt_code=cnt_code+1

print(len(devices_list))
print(len(DeviceCode))
all_cnt=len(devices_list)
cnt_=0
for i in devices_list:
    
    certain_data_issue=p2.take_certain_data_df(data_issue,i)
    certain_data_df=p1.take_certain_data_df(df,i)
    certain_data_alarm=p3.take_certain_data_df(data_alarm,i)
    certain_data_receipt=p4.take_certain_data_receipt(DeviceCode[cnt_],data_receipt)

    df_output.at[0, "device"]=devices_list[cnt_]
    df_output.at[0,"devicecode"]=DeviceCode[cnt_]

    flag_receipt=0

    # print("len befor issue", len(data_issue.index))
        # print("len certain_data_issue",len(certain_data_receipt.index))
        # print("len certain_data_issue",len(certain_data_receipt.index))
        # print("len certain_data_issue",len(certain_data_receipt.index))
        # print("len certain_data_issue",len(certain_data_receipt.index))
    # data_issue.drop(data_issue.index[certain_data_issue], inplace=True)
    # data_alarm.drop(index=certain_data_alarm.index, inplace=True)
    # data_receipt.drop(index=certain_data_receipt.index, inplace=True)  

        # print("len after issue", len(data_receipt.index))
        # print("len after issue", len(data_receipt.index))
        # print("len after issue", len(data_receipt.index))
        # print("len after issue", len(data_receipt.index))
        # print("len after issue", len(data_receipt.index))
    col=len(certain_data_df.index)
    # # p1.do_suitcase(data_alarm, df,1)
    cnt=0
    TT=[]
    while col-1>cnt:
        TT.append(p1.do_suitcase(certain_data_alarm,certain_data_issue,certain_data_receipt, certain_data_df,cnt,p4,df_output))
        cnt=cnt+1

    # print(TT)
   
    p4.do_dataframe_csv(TT, df_output)

    # df=pd.read_csv('out.csv')

    last=len(df.index)

    end=df.iat[last-1, 4]
    start=df.iat[0,3]

    # if p4.filter_by_date(start,end, certain_data_receipt)==0:
    #     print("all receipts found their packages")
    # else:
    #     print(p4.filter_by_date(start,end, certain_data_receipt))
    TT.clear()
    certain_data_issue._clear_item_cache
    certain_data_df._clear_item_cache
    certain_data_alarm._clear_item_cache
    certain_data_receipt._clear_item_cache

    col=0
    cnt_=cnt_+1
    if cnt_==len(devices_list):
        break



for i, row in p4.data_analyz.iterrows():
    
    
    type_occasion=row['type_occasion']
    internal_occasion=row['internal_occasion']


    if (type_occasion=="alarm") or (type_occasion=="issue"):
        end_time_occasion=row['start_date']
    else:
        end_time_occasion=row['end_date']

    
    
    if (type_occasion=="alarm") & (internal_occasion==0):
        print("found alarm beyound package")
        p4.data_analyz.iat[i,11]="task_alarm"
        
    if (type_occasion=="alarm") & (internal_occasion==1):
        print("alarm within package")
        
    if i==len(p4.data_analyz.index)-1:
        start_time_occasion_another=p4.data_analyz.iat[i,2]
    else:
        start_time_occasion_another=p4.data_analyz.iat[i+1,2]
    print(start_time_occasion_another)
    print(end_time_occasion)
    if start_time_occasion_another>end_time_occasion:
        time=timedelta(hours=start_time_occasion_another.hour,minutes=start_time_occasion_another.minute, seconds=start_time_occasion_another.second)-timedelta(hours=end_time_occasion.hour,minutes=end_time_occasion.minute,seconds=end_time_occasion.second)
    else:
        time=timedelta(hours=end_time_occasion.hour,minutes=end_time_occasion.minute,seconds=end_time_occasion.second)-timedelta(hours=start_time_occasion_another.hour,minutes=start_time_occasion_another.minute, seconds=start_time_occasion_another.second)
    
    print(time.seconds)
    if i==len(p4.data_analyz.index)-2:
        if (time.seconds>420) & (p4.data_analyz.iat[i,0]==p4.data_analyz.iat[i+1,0])  :
            #data_analyz=pd.DataFrame(columns=['number_of_group','type_occasion','start_date','end_date','internal_occasion','device','devicecode','qauntity_package','>420','r-a','task_alarm','mix_task','KPU/KnPU','receipt_without_pack','receipt_without_pack_and_alarm'])
            
            if i==len(p4.data_analyz.index)-1:
                pass
            
            else:
                if p4.data_analyz.iat[i,5]==p4.data_analyz.iat[i+1,5]:
                    
                    # p4.data_analyz.iat[i+1,0]="more than 420" !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    tmp=p4.data_analyz.iat[i+1,0]
                else:
                    pass
                    # p4.data_analyz.iat[i,0]=p4.data_analyz.iat[i,0]+1
                # p4.data_analyz.iat[i-1,0]=p4.data_analyz.iat[i-1,0]+1
                row['>420']=">420"


    type_occasion=p4.data_analyz.iat[i,1]
    print(type_occasion)
    if i==len(p4.data_analyz.index)-1:
        type_occasion_next=p4.data_analyz.iat[i,1]
    else:
        type_occasion_next=p4.data_analyz.iat[i+1,1]
        print(type_occasion_next)

    if (type_occasion=="receipt") & (type_occasion_next=="alarm"):
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        if i==len(p4.data_analyz.index)-1:
            pass
        else:
            if p4.data_analyz.iat[i,5]==p4.data_analyz.iat[i+1,5]: 
                # p4.data_analyz.iat[i+1,0]="receipt->alarm" !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                tmp=p4.data_analyz.iat[i+1,0]
                # p4.data_analyz.iat[i+1,0]=p4.data_analyz.iat[i,0]+1
            else:
                pass
            row['r-a']="R-A"
        
    
    # elif i==len(p4.data_analyz.index)-1:
    #     pass
    # else:
    #     p4.data_analyz.iat[i+1,0]=tmp
    
    # if i==len(p4.data_analyz.index):
    #     tmp=None



p4.do_task(p4.data_analyz)
        
p4.data_analyz.to_csv('out1.csv', mode='a', header=False)

 
    







    


   

    


