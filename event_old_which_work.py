from im_lib import *

class Event():

    def __init__(self, start_date):
        self.date_df=start_date
       
        
        
        

    
    
    def do_suitcase(self, df_alarm, df_issue, df_receipt, df, num_suitcase,Receipt):
        PT=[]
        

        alarm_time=df_alarm["localdate"]
        issue_time=df_issue["localdate"]
        receipt_open=df_receipt["dateopen"]
        receipt_close=df_receipt["dateclose"]
        suitcase_time_start_of_one_package= df.iat[num_suitcase,15] - timedelta(hours=0, minutes=0, seconds=int(df.iat[num_suitcase,13])) 
        suitcase_time_end_of_one_package=df.iat[num_suitcase,15]
        suitcase_time_start_of_another_package=df.iat[num_suitcase+1,15]-timedelta(hours=0, minutes=0, seconds=int(df.iat[num_suitcase+1,13]))
        suitcase_time_end_of_another_package=df.iat[num_suitcase+1,15]
        receiptid_suitcase=df.iat[num_suitcase,21]
        mask_alarm_package=(alarm_time>suitcase_time_start_of_one_package) & (alarm_time<=suitcase_time_start_of_another_package)
        mask_issue_package=(issue_time>suitcase_time_start_of_one_package) & (issue_time<=suitcase_time_start_of_another_package)
        mask_receipt_package=(receipt_close>suitcase_time_start_of_one_package) & (receipt_close<=suitcase_time_start_of_another_package)
        mask_receipt_package_next=(receipt_close>suitcase_time_start_of_another_package) & (receipt_close<=suitcase_time_end_of_another_package)

        
        
        df_receipt_next=df_receipt.loc[mask_receipt_package_next]
        df_receipt_=df_receipt.loc[mask_receipt_package]
        df_alarm_=df_alarm.loc[mask_alarm_package]
        df_issue_=df_issue.loc[mask_issue_package]
        
        # df_receipt_.index=np.arange(1,len(df_receipt_)+1)
        # df_alarm_.index=np.arange(1,len(df_alarm_)+1)
        # df_issue_.index=np.arange(1,len(df_issue_)+1)

        cnt_alarm=len(df_alarm_.index)
        cnt_issue=len(df_issue_.index)
        cnt_receipt=len(df_receipt_.index)
       
        

        # print("=======================DF_receiptdf_receipt==========================", df_receipt_)
        # print("=======================DF_alarm=======================================", df_alarm_)
        # print("=======================DF_issue=======================================", df_issue_)
        
        logging.info("===============Info about suitcase=================")
        logging.info("package: %s", str(num_suitcase))
        logging.info("cnt_alarm %s",str(cnt_alarm))
        logging.info("cnt_issue %s", str(cnt_issue))
        logging.info("cnt_receipt %s", str(cnt_receipt))
        logging.info("+++++++++++++++++++++++++++++++++++++++++++++++++++")
        logging.info(" DF  %s", df.iat[num_suitcase,15])
        logging.info("df_receipt %s" ,df_receipt_.loc[:,"dateopen"])
        
        cnt_main_issue=0
        cnt_main_receipt=0
        cnt_main_alarm=0
        flag=0
        cnt_error_check=0
       
        PT.append((df.iloc[num_suitcase,:].to_frame()).transpose())
        
        state=[df_alarm_.empty,df_issue_.empty, df_receipt_.empty]  #state for checking empty


        # if df_receipt_.empty:
        #     cnt_error_check=cnt_error_check+1


        # if not df_receipt_next.empty and df_receipt_.empty:
            
        #     Receipt.check_quantity(cnt_error_check, df_receipt_next)
        #     cnt_error_check=0


        # if (suitcase_time_start_of_another_package - suitcase_time_end_of_one_package).seconds >420:
            
        #     logging.info("++++++++++++++++++++++++++DIVIDE_GROUP BY DURATION MORE THAN 420 SECOND+++++++++++++++++++++++++++++++++++++++++++++++")
        #     logging.info("++++++++++++++++++++++++++DIVIDE_GROUP BY DURATION MORE THAN 420 SECOND+++++++++++++++++++++++++++++++++++++++++++++++")



        if state==[True, True, True]:
            PT.append("receipt doesn't exist")
            
        elif state==[True, True, False]:
            if len(df_receipt_.index)==1:
                # print(df_receipt_)
                PT.append(df_receipt_)
            else:
                cnt=0
                len_num=len(df_receipt_.index)
                while True:
                                     
                    PT.append(df_receipt_.iloc[cnt,:])
                    cnt=cnt+1
                    if cnt==len_num:
                        break
            
                

            logging.info("==========Info about receipt======")
            logging.info("receipt: %s",Receipt.check_receipt(df_receipt_,suitcase_time_end_of_one_package,receiptid_suitcase))
            
        elif state==[True,False, True]:
            PT.append("receipt doesn't exist")
            if len(df_issue_.index)==1:
                # print(df_receipt_)
                PT.append(df_issue_)
            else:
                cnt=0
                len_num=len(df_issue_.index)
                while True:
                   
                                
                    PT.append(df_issue_.iloc[cnt,:])
                    cnt=cnt+1
                    if cnt==len_num:
                        break

            
           

        elif state==[True,False,False]: #this is work
            flag=0
            logging.info("==========Info about receipt======")
            logging.info("receipt: %s",Receipt.check_receipt(df_receipt_,suitcase_time_end_of_one_package,receiptid_suitcase))
            while True:       #need to define alarm and issue in package with time
                
               
                if cnt_issue==cnt_main_issue:
                    
                    while True:
                        if cnt_main_receipt!=cnt_receipt:
                            PT.append(df_receipt_.iloc[cnt_main_receipt, :])
                            cnt_main_receipt=cnt_main_receipt+1
                          
                        else:
                            
                            break    

                if cnt_receipt==cnt_main_receipt:
                    
                    while True:
                        if cnt_main_issue!=cnt_issue:
                            PT.append(df_issue_.iloc[cnt_main_issue,:])
                            cnt_main_issue=cnt_main_issue+1
                         
                        else:
                            flag=1
                            break

                if flag==1:
                    
                    break

                if df_receipt_.iat[cnt_main_receipt,5]<=df_issue_.iat[cnt_main_issue,3]: #5
                    PT.append(df_receipt_.iloc[cnt_main_receipt, :])                    
                    cnt_main_receipt=cnt_main_receipt+1
                else:
                    PT.append(df_issue_.iloc[cnt_main_issue,:])
                    cnt_main_issue=cnt_main_issue+1
                    
                
                
                
                   
                
               
                
               
        elif state==[False,True,True]:
            PT.append("receipt doesn't exist")
            if len(df_alarm_.index)==1:
                # print(df_receipt_)
                PT.append(df_alarm_)
            else:
                cnt=0
                len_num=len(df_alarm_.index)
                while True:
                                                  
                    PT.append(df_alarm_.iloc[cnt,:])
                    cnt=cnt+1
                    if cnt==len_num:
                        break
            
        
        elif state==[False,True,False]:
            flag=0
            logging.info("==========Info about receipt======")
            logging.info("receipt: %s",Receipt.check_receipt(df_receipt_,suitcase_time_end_of_one_package,receiptid_suitcase))
            while True:       #need to define alarm and issue in package with time
                

               
                if cnt_alarm==cnt_main_alarm:
                   
                    while True:
                        if cnt_main_receipt!=cnt_receipt:
                            PT.append(df_receipt_.iloc[cnt_main_receipt, :])
                            cnt_main_receipt=cnt_main_receipt+1
                           
                        else:
                            flag=1
                            break    

                if cnt_receipt==cnt_main_receipt:
                   
                    while True:
                        if cnt_main_alarm!=cnt_alarm:
                            PT.append(df_alarm_.iloc[cnt_main_alarm,:])
                            cnt_main_alarm=cnt_main_alarm+1
                          
                        else:
                            flag=1
                            break

                if flag==1:
                    
                    break

                if df_receipt_.iat[cnt_main_receipt,5]<=df_alarm_.iat[cnt_main_alarm,18]: # 4
                    PT.append(df_receipt_.iloc[cnt_main_receipt, :])                    
                    cnt_main_receipt=cnt_main_receipt+1
                else:
                    PT.append(df_alarm_.iloc[cnt_main_alarm,:])
                    cnt_main_alarm=cnt_main_alarm+1
                   
                
                
                
                   
                   
            
           
                
                
                 
        elif state==[False,False,True]:
            flag=0
            PT.append("receipt doesn't exist")
            print("cnt_alarm",cnt_alarm)
            print("cnt_issue",cnt_issue)
            while True:       
                
                print("step one while true")


                if cnt_alarm==cnt_main_alarm:

                    print("cnt_alarm==cnt_main_alarm")
                   
                    while True:
                        if cnt_main_issue!=cnt_issue:
                            print("cnt_main_issue", cnt_main_issue)
                            PT.append(df_issue_.iloc[cnt_main_issue, :])
                            cnt_main_issue=cnt_main_issue+1
                          
                        else:
                            print("flag", flag)
                            flag=1
                            break    

                if cnt_issue==cnt_main_issue:
                    
                    print("cnt_issue==cnt_main_issue")

                    while True:

                        if cnt_main_alarm!=cnt_alarm:
                            print("cnt_main_alarm",cnt_main_alarm)
                            PT.append(df_alarm_.iloc[cnt_main_alarm,:])
                            cnt_main_alarm=cnt_main_alarm+1
                         
                        else:
                            print("flag",flag)
                            flag=1
                            break

                if flag==1:
                    
                    break

                if df_issue_.iat[cnt_main_issue,3]<=df_alarm_.iat[cnt_main_alarm,18]:
                    print("step_two df_issue<", cnt_main_issue)
                    PT.append(df_issue_.iloc[cnt_main_issue, :])                    
                    cnt_main_issue=cnt_main_issue+1
                else:
                    print("step_two df_alarm<", cnt_main_alarm)
                    PT.append(df_alarm_.iloc[cnt_main_alarm,:])
                    cnt_main_alarm=cnt_main_alarm+1

                
                
                # if df_alarm_.iat[cnt_main_alarm,18]<=df_issue_.iat[cnt_main_issue,3]:
                #     print("step_two df_alarm<", cnt_main_alarm)
                #     PT.append(df_alarm_.iloc[cnt_main_alarm,:])
                #     cnt_main_alarm=cnt_main_alarm+1
                
            
                           
               
                      
        elif state==[False,False,False]:
            logging.info("==========Info about receipt======")
            logging.info("receipt: %s",Receipt.check_receipt(df_receipt_,suitcase_time_end_of_one_package,receiptid_suitcase))  
            
            flag_alarm=0
            flag_issue=0
            flag_receipt=0        
            while True:
                print("while step first")
                if(cnt_main_alarm==cnt_alarm) and (cnt_main_issue==cnt_issue)and (cnt_main_receipt==cnt_receipt):
                    break
                else:
                    pass
                if (cnt_main_alarm==cnt_alarm) and (cnt_main_issue==cnt_issue):
                    print("cnt_main_alarm==cnt_alarm) and (cnt_main_issue==cnt_issue")
                    while True:
                        if cnt_main_receipt!=cnt_receipt:
                            PT.append(df_receipt_.iloc[cnt_main_receipt, :])
                            cnt_main_receipt=cnt_main_receipt+1
                           
                        else:
                            flag=1
                            break 

                if (cnt_main_receipt==cnt_receipt) and (cnt_main_issue==cnt_issue):
                    print("cnt_main_receipt==cnt_receipt) and (cnt_main_issue==cnt_issue")
                    while True:
                        if cnt_main_alarm!=cnt_alarm:
                            PT.append(df_alarm_.iloc[cnt_main_alarm,:])
                            cnt_main_alarm=cnt_main_alarm+1
                          
                        else:
                            flag=1
                            break
                

                if (cnt_main_alarm==cnt_alarm) and (cnt_main_receipt==cnt_receipt):
                    print("cnt_main_alarm==cnt_alarm) and (cnt_main_receipt==cnt_receipt")
                    while True:
                        if cnt_main_issue!=cnt_issue:
                            PT.append(df_issue_.iloc[cnt_main_issue, :])
                            cnt_main_issue=cnt_main_issue+1
                           
                        else:
                            flag=1
                            break    
                                

                while True:
                    print("midle step")

                    print("cnt_alarm",cnt_alarm)
                    print("cnt_issue",cnt_issue)
                    print("cnt_receipt",cnt_receipt)

                    
                    print("cnt_main_alarm middle",cnt_main_alarm)
                    print("cnt_main_issue middle",cnt_main_issue)
                    print("cnt_main_receipt middle",cnt_main_receipt)
                    
                    if cnt_main_alarm==cnt_alarm:

                        if (cnt_main_receipt==cnt_receipt) or (cnt_main_issue==cnt_issue):
                            break
                        if df_receipt_.iat[cnt_main_receipt,5]<=df_issue_.iat[cnt_main_issue,3]: #4
                            PT.append(df_receipt_.iloc[cnt_main_receipt, :])                    
                            cnt_main_receipt=cnt_main_receipt+1
                          
                        else:
                            PT.append(df_issue_.iloc[cnt_main_issue, :])                    
                            cnt_main_issue=cnt_main_issue+1
                        
                        print("cnt_main_receipt==cnt_receipt) or (cnt_main_issue==cnt_issue)")
                        print("cnt_main_alarm middle",cnt_main_alarm)
                        print("cnt_main_issue middle",cnt_main_issue)
                        print("cnt_main_receipt middle",cnt_main_receipt)
                          

                        

                    if cnt_main_issue==cnt_issue:
                        if (cnt_main_alarm==cnt_alarm) or (cnt_main_receipt==cnt_receipt):
                            break
                        if df_alarm_.iat[cnt_main_alarm,18]<=df_receipt_.iat[cnt_main_receipt,5]: #4
                            PT.append(df_alarm_.iloc[cnt_main_alarm,:])
                            cnt_main_alarm=cnt_main_alarm+1
                           
                        else:
                            PT.append(df_receipt_.iloc[cnt_main_receipt, :])                    
                            cnt_main_receipt=cnt_main_receipt+1
                        
                        print("(cnt_main_alarm==cnt_alarm) or (cnt_main_receipt==cnt_receipt)")
                        print("cnt_main_alarm middle",cnt_main_alarm)
                        print("cnt_main_issue middle",cnt_main_issue)
                        print("cnt_main_receipt middle",cnt_main_receipt)
                           



                    if cnt_main_receipt==cnt_receipt:
                        if (cnt_main_alarm==cnt_alarm) or (cnt_main_issue==cnt_issue):
                            break
                        if df_alarm_.iat[cnt_main_alarm,18]<=df_issue_.iat[cnt_main_issue,3]:
                            PT.append(df_alarm_.iloc[cnt_main_alarm,:])
                            cnt_main_alarm=cnt_main_alarm+1
                           
                        else:
                            PT.append(df_issue_.iloc[cnt_main_issue, :])                    
                            cnt_main_issue=cnt_main_issue+1
                        
                        print("cnt_main_alarm==cnt_alarm) or (cnt_main_issue==cnt_issue")
                        print("cnt_main_alarm middle",cnt_main_alarm)
                        print("cnt_main_issue middle",cnt_main_issue)
                        
                            

                
                    
                       
                    while True:

                        if cnt_main_alarm==cnt_alarm:
                            break

                        if cnt_main_receipt==cnt_receipt:
                            break

                        if cnt_main_issue==cnt_issue:
                            break
                    
                        print("cnt_alarm",cnt_alarm)
                        print("cnt_issue",cnt_issue)
                        print("cnt_receipt",cnt_receipt)

                        print("inner step")
                        print("cnt_main_alarm",cnt_main_alarm)
                        print("cnt_main_issue",cnt_main_issue)
                        print("cnt_main_receipt",cnt_main_receipt)

                        if (df_alarm_.iat[cnt_main_alarm,18]<=df_issue_.iat[cnt_main_issue,3]) and (df_alarm_.iat[cnt_main_alarm,18]<=df_receipt_.iat[cnt_main_receipt,5]): #4
                            PT.append(df_alarm_.iloc[cnt_main_alarm, :])
                            if cnt_main_alarm!=cnt_alarm:
                                flag_alarm=1
                                
                                
                            
                        
                    
                        if (df_issue_.iat[cnt_main_issue,3]<=df_alarm_.iat[cnt_main_alarm,18]) and (df_issue_.iat[cnt_main_issue,3]<=df_receipt_.iat[cnt_main_receipt,5]): #4
                            PT.append(df_issue_.iloc[cnt_main_issue,:])
                            if cnt_main_issue!=cnt_issue:
                                flag_issue=1
                                
                                
                            
                    
                        
                        
                        if  (df_receipt_.iat[cnt_main_receipt,5]<=df_issue_.iat[cnt_main_issue,3]) and (df_receipt_.iat[cnt_main_receipt,5]<=df_alarm_.iat[cnt_main_alarm,18]): #4
                            PT.append(df_receipt_.iloc[cnt_main_receipt,:])
                            if cnt_main_receipt!=cnt_receipt:
                                flag_receipt=1
                                
                                
                            
                        if flag_alarm==1:
                            cnt_main_alarm=cnt_main_alarm+1
                            flag_alarm=0
                            print("cnt_main_alarm",cnt_main_alarm)
                        
                        if flag_issue==1:
                            cnt_main_issue=cnt_main_issue+1
                            flag_issue=0
                            print("cnt_main_issue",cnt_main_issue)
                        
                        if flag_receipt==1:
                            cnt_main_receipt=cnt_main_receipt+1
                            flag_receipt=0
                            print("cnt_main_receipt",cnt_main_receipt)



                        if(cnt_main_alarm==cnt_alarm) or (cnt_main_issue==cnt_issue) or (cnt_main_receipt==cnt_receipt):
                    
                            break
                
                   
        else:
            PT.append("receipt doesn't exist")
       

       
        # df_issue.drop(index=df_issue.loc[mask_issue_package].index, inplace=True)
        # df_alarm.drop(index=df_alarm.loc[mask_alarm_package].index, inplace=True)
        # df_receipt.drop(index=df_receipt.loc[mask_receipt_package].index, inplace=True)
        df_receipt_._clear_item_cache
        df_alarm_._clear_item_cache
        df_issue_._clear_item_cache

       
        return PT 

    
    
    def period_time(self): #it will be beter use SQL query 'BETWEEN' and then use only one dataframe?????
        
        conn=psycopg2.connect("dbname='unpack' user='postgres' password='root' host='localhost' port='5432' ")
        data_analyz=pd.read_sql_query("SELECT * FROM polycomm_suitcase WHERE DATE(local_date) > %(date)s order by local_date limit 10000" ,con=conn, params={"date": self.date_df})
        conn.commit()
        conn.close()
        return data_analyz

    def take_certain_data_df(self, df,device_):
        device_out=df[df['device']==str(device_)]
       
        _device_=(df.mask(df['device']==str(device_)))
        mask_device=(df['device']!=_device_['device'])
       
        if _device_.empty:
            print("nothing droping")
        else:   
            df.drop(index=df.loc[mask_device].index,inplace=True)
       
        df._clear_item_cache
        device_out._clear_item_cache
        mask_device._clear_item_cache
        return device_out.sort_values(by=['local_date'])


    def find_device(self, df):
        return df.drop_duplicates(subset=['device']).iloc[:,3]

    def find_devicecode(self, device):
        conn=psycopg2.connect("dbname='unpack' user='postgres' password='root' host='localhost' port='5432' ")
        devicecode=pd.read_sql_query("SELECT code FROM polycomm_device WHERE id=%(device)s GROUP BY  code" ,con=conn, params={"device": device})
        devicecode.drop_duplicates(subset=['code'])
        conn.commit()
        conn.close()
        return devicecode

    def make_group(self, TT):
        
        
        
        
        
        
        
        # df_output.to_csv('out.csv', mode='a', header=False)
        # df_output.drop([0],axis=0, inplace=True)
        return 0



           
           
           
    



