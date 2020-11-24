from im_lib import *

from event import *



class Receipt(Event):
    
    def __init__(self, start_date):
        super().__init__(start_date)
       
    data_analyz=pd.DataFrame(columns=['number_of_group','type_occasion','start_date','end_date','internal_occasion','device','devicecode','qauntity_package_one','quantity_package_double','>420','r-a','task_alarm','mix_task','KPU/KnPU','receipt_without_pack','receipt_without_pack_and_alarm','solved_task'])
    data_analyz.to_csv('out1.csv')
    cnt=0
    cnt_frame=0
    cnt_dataframe=0
    cnt_receipt=0
    flag_receipt=0
    flag_pack=0
    flag_issue=0
    flag_alarm=0

    def unpack_receipts(self, df_receipts):
        number=len(df_receipts.index)
        receipt_list=[]
        n=0
        receipt_list.append(number)
        while number>n:
            receipt_list.append(df_receipts.iloc[n,:])
            n=n+1
        
        return receipt_list
            
    
    def check_receipt(self,df_receipt_, end_time_suitcase,receipt_suitcase):
        if len(df_receipt_.index)>1:
            return "it will be work later"
        if df_receipt_.empty:
            return "there is no receipt in this package"
        
        receiptid_=df_receipt_.iat[0,1]   
        open_receipt=df_receipt_.iat[0,4]             

        if receiptid_==None:
            return "receipt doesn't exist in database"
        if receiptid_ == '-1':
            return "receipt exist but it is equal -1"
        
        if receiptid_==receipt_suitcase:
            if (receiptid_==receipt_suitcase) and (open_receipt<end_time_suitcase):
                df_receipt_.iat[0,4]=end_time_suitcase
                return "receipt id is equal and correction time have changed successfully"
            else:
                return "receipts_id is equal in suitcase"
    
       
    def find_devicode(self,df,num):
       
        receiptid_=df.iat[num,1]
        print(receiptid_)
        while True:
            if receiptid_==None:
                num=num+1
                receiptid_=df.iat[num,1]
            elif receiptid_=='-1':
                num=num+1
                receiptid_=df.iat[num,1]
            else:
                break
                
        
        return df.iat[num,3], df.iat[num,1]


    def take_certain_data_receipt(self,devicode,df_receipt):
        device_out=df_receipt[df_receipt['devicecode']==str(devicode)]
        _device_=(df_receipt.mask(df_receipt['devicecode']==str(devicode)))
        mask_device=(df_receipt['devicecode']!=_device_['devicecode'])
        
        # if _device_.empty:
        #     print("nothing droping")
        # else:   
        #     df_receipt.drop(index=df_receipt.loc[mask_device].index,inplace=True)
       
        df_receipt._clear_item_cache
        device_out._clear_item_cache
        mask_device._clear_item_cache
        return device_out.sort_values(by=['dateclose']) # dateopen
   
    def take_data_receipt(self):
        
        conn=psycopg2.connect("dbname='unpack' user='postgres' password='root' host='localhost' port='5432' ") 
        data_receipt=pd.read_sql_query("SELECT * FROM receipts WHERE DATE(dateclose) > %(date)s order by dateclose limit 1000" ,con=conn, params={"date":self.date_df}) #dateopen
        conn.commit()
        conn.close()
        return data_receipt
    
    def check_quantity(self,cnt_error_check, df_receipt_):

        if df_receipt_.iat[0,6]==cnt_error_check:
            return None
        else:
            logging.info("++++++++++++++++++++++++++DIVIDE_GROUP BY RECEIPT+++++++++++++++++++++++++++++++++++++++++++++++")
            logging.info("++++++++++++++++++++++++++DIVIDE_GROUP BY RECEIPT+++++++++++++++++++++++++++++++++++++++++++++++")

    
    def check_range_of_receipt(self, df_receipt, start_suitcase, end_suitcase, start_suitcase_next,end_suitcase_next):
        # length_receipt=len(df_receipt.index)
        # if length_receipt>1:
        #     # do something

        # else:
        #     start_receipt=df_receipt.iat[0,4]
        #     end_receipt=df_receipt.iat[0,5]
        return 0
    
    def do_dataframe_csv(self, TT, df_output):

        len_TT=len(TT)
        print("len_tt", len_TT)
        cnt_quantity=0
        cnt_TT=-1
        cnt_PT=0
        cnt_group=0
        flag_receipt=0
        df_output.at[0,"number_of_group"]=cnt_group
        cnt_PT=0
        while len_TT-1>=cnt_TT:
            if len_TT-1==cnt_TT:
                break
            print(cnt_TT)
            cnt_TT=cnt_TT+1
            len_PT=len(TT[cnt_TT])
        
            print("lenTT",len_TT)
            flag=0
            
            print("=================================================================================================================================================================================")
            print("len_PT", len_PT) 
            print(TT[cnt_TT])
            cnt_PT=0
            while len_PT-1>=cnt_PT:

                if len_PT==cnt_PT:
                
                    break

                
            
            
                


                if isinstance((TT[cnt_TT][cnt_PT]), pd.Series):
                    # TT[cnt_TT][cnt_PT].to_frame()
                    print(TT[cnt_TT][cnt_PT].index[0])
                    
                    

                    if  (TT[cnt_TT][cnt_PT]).index[0]=="polycom_id":
                        print("polycomm_id")
                        df_output.at[0,"type_occasion"]='pack'
                        df_output.at[0,"start_date"]=(TT[cnt_TT][cnt_PT]).at['local_date'] - timedelta(hours=0, minutes=0, seconds=int((TT[cnt_TT][cnt_PT]).at['duration']))
                        df_output.at[0,"end_date"]=(TT[cnt_TT][cnt_PT]).at['local_date']
                        df_output.at[0,"internal_occasion"]=1
                        
                        end_pack=(TT[cnt_TT][cnt_PT]).at['local_date']
                        start_pack=(TT[cnt_TT][cnt_PT]).at['local_date'] - timedelta(hours=0, minutes=0, seconds=int((TT[cnt_TT][cnt_PT]).at['duration']))
                        # start_pack_next=(TT[cnt_TT+1][cnt_PT]).at[0,15] - timedelta(hours=0, minutes=0, seconds=int((TT[cnt_TT+1][cnt_PT]).at[0,13]))
                        # if not len_TT-3==cnt_TT:  
                        #     if start_pack_next-end_pack>timedelta(hours=0,minutes=0, seconds=420):
                        #         cnt_group=cnt_group+1
                        #         df_output.at[0,"number_of_group"]=cnt_group
                        
                    
                        if  (type(TT[cnt_TT][cnt_PT+1])==str) & (flag_receipt==0):
                            flag_receipt=1
                            cnt_group=cnt_group+1
                            cnt_quantity=cnt_quantity+1
                            df_output.at[0,"number_of_group"]=cnt_group
                            Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                            
                            df_output.to_csv('out.csv', mode='a', header=False)
                            
                            cnt_PT=cnt_PT+1

                        elif (type(TT[cnt_TT][cnt_PT+1])==str) & (flag_receipt==1):
                            cnt_quantity=cnt_quantity+1
                            Receipt.data_analyz=Receipt.data_analyz.append(df_output,ignore_index=True)
                            
                            df_output.to_csv('out.csv', mode='a', header=False)
                            
                            cnt_PT=cnt_PT+1
                        
                        elif flag_receipt==0 & (not type(TT[cnt_TT][cnt_PT+1])==str):
                            cnt_group=cnt_group+1
                            df_output.at[0,"number_of_group"]=cnt_group
                            Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                            print(Receipt.data_analyz)
                            df_output.to_csv('out.csv', mode='a', header=False)
                            
                            # cnt_PT=cnt_PT+1
                        
                        else:
                            Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                            
                            df_output.to_csv('out.csv', mode='a', header=False)
                            

                        
                        cnt_PT=cnt_PT+1



                        #   if not len_PT==cnt_PT and not len_PT==1:
                        #     if  type(TT[cnt_TT][cnt_PT+1])==str:
                        #         pass
                        #     else:
                        #         df_output.to_csv('out.csv', mode='a', header=False)
                        # else:
                        # else:
                        #     df_output.to_csv('out.csv', mode='a', header=False)
                        # print("cnt_PT from polycom",cnt_PT)
                        
                    
                    elif TT[cnt_TT][cnt_PT].index[0]=="polycommalarm_id":
                        print("alarm")
                        df_output.at[0,"type_occasion"]='alarm'
                        df_output.at[0,"start_date"]=TT[cnt_TT][cnt_PT].at['localdate']
                        df_output.at[0,"end_date"]='None'
                        if TT[cnt_TT][cnt_PT].at['localdate']>end_pack:
                            df_output.at[0,"internal_occasion"]=0
                        else:
                            df_output.at[0,"internal_occasion"]=1 
                        Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                        
                        df_output.to_csv('out.csv', mode='a', header=False)
                        
                        print("cnt_PT from alarm",cnt_PT)
                        cnt_PT=cnt_PT+1
                    
                    elif TT[cnt_TT][cnt_PT].index[0]=="polycommissue_id":
                        print("issue")
                        df_output.at[0,"type_occasion"]='issue'
                        df_output.at[0,"start_date"]=TT[cnt_TT][cnt_PT].at['localdate']
                        df_output.at[0,"end_date"]='None'
                        if TT[cnt_TT][cnt_PT].at['localdate']>end_pack:
                            df_output.at[0,"internal_occasion"]=0
                        else:
                            df_output.at[0,"internal_occasion"]=1
                        Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                        df_output.to_csv('out.csv', mode='a', header=False)
                        
                        print("cnt_PT from issue",cnt_PT)
                        cnt_PT=cnt_PT+1

                    elif (TT[cnt_TT][cnt_PT]).index[0]=="receipt_id":
                        print("receipt")
                        if flag_receipt==1:
                            flag_receipt=0

                            # if (TT[cnt_TT][cnt_PT]).at['quantitypackageone']==cnt_quantity:
                            #     logging.info("EQUAL QUANTITY PACKAGE ONE")
                            #     #df_output.at[0,"qauntity_package"]="qauntity_package_one =="
                
                            # elif (TT[cnt_TT][cnt_PT]).at['quantitypackagedouble']==cnt_quantity:   
                            #     logging.info("EQUAL QUANTITY PACKAGE DOUBLE")
                            #     #df_output.at[0,"qauntity_package"]="qauntity_package_double =="
                
                            # else:
                            #     logging.info("NOT EQUAL QUANTITY PACKAGE")
                            #     #df_output.at[0,"qauntity_package"]="qauntity_package_not equal =="
                                
                        df_output.at[0,"type_occasion"]='receipt'
                        df_output.at[0,"start_date"]=(TT[cnt_TT][cnt_PT]).at['dateopen'] 
                        df_output.at[0,"end_date"]=(TT[cnt_TT][cnt_PT]).at['dateclose']
                        if TT[cnt_TT][cnt_PT].at['dateopen']>=end_pack:
                            df_output.at[0,"internal_occasion"]=0
                        else: 
                            df_output.at[0,"internal_occasion"]=1


                        if (TT[cnt_TT][cnt_PT]).at['quantitypackageone']==0:
                            df_output.iat[0,8]=(TT[cnt_TT][cnt_PT]).at['quantitypackagedouble']
                            df_output.iat[0,7]=0
                        else:
                            df_output.iat[0,7]=(TT[cnt_TT][cnt_PT]).at['quantitypackageone']
                            df_output.iat[0,8]=0

                       

                        Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                        
                        df_output.to_csv('out.csv', mode='a', header=False)
                        df_output.iat[0,7]=None
                        df_output.iat[0,8]=None
                        print("cnt_PT from receipt",cnt_PT)
                        cnt_PT=cnt_PT+1

            
                    
                    
                # elif  type(TT[cnt_TT][cnt_PT])==str:
                #     inner_TT=cnt_TT
                #     inner_PT=0
                    
                #     cnt_check_error=0
                    
                #     while flag<=0:
                #         if flag==1:
                #             break
                #         inner_TT=inner_TT+1
                #         num_PT=len(TT[inner_TT])
                #         inner_PT=0
                #         print("num_PT",num_PT)
                #         print("step_flag_while")
                #         print("flag",flag)
                #         cnt_check_error=cnt_check_error+1
                #         while num_PT>=inner_PT:
                #             print("inner_pt", inner_PT)
                #             if inner_PT==num_PT:
                #                 print("inner_break")
                #                 break
                #             elif isinstance((TT[inner_TT][inner_PT]), pd.Series):
                #                 print("i am series")
                #                 if (TT[inner_TT][inner_PT]).index[0]=="receipt_id":
                #                     print("i am series, found receipt")
                #                     cnt_group=cnt_group+1
                #                     df_output.at[0,"number_of_group"]=cnt_group
                #                     df_output.to_csv('out.csv', mode='a', header=False)
                #                     flag=1
                #                     cnt_PT=cnt_PT+1
                #                     break

                #             elif isinstance((TT[inner_TT][inner_PT]), pd.DataFrame):
                #                 print("i am dataframe")
                #                 if (TT[inner_TT][inner_PT]).columns[0]=="receipt_id":
                #                     print("i am dataframe, found receipt")
                #                     cnt_group=cnt_group+1
                #                     df_output.at[0,"number_of_group"]=cnt_group
                #                     df_output.to_csv('out.csv', mode='a', header=False)
                #                     print("i am bad receipt")
                #                     flag=1
                #                     cnt_PT=cnt_PT+1
                #                     break
                            
                            
                #             print("flag_inner", flag)
                #             inner_PT=inner_PT+1
                        

            
                
                # if TT[cnt_TT][cnt_PT].columns[0]=="polycom_id":
                #     end_pack=(TT[cnt_TT][cnt_PT]).iat[0,15]
                #     start_pack=(TT[cnt_TT][cnt_PT]).iat[0,15] - timedelta(hours=0, minutes=0, seconds=int((TT[cnt_TT][cnt_PT]).iat[0,13]))
                        # if (TT[cnt_TT+1][cnt_PT]).columns[0]=="polycom_id":
                        #     start_pack_next=(TT[cnt_TT+1][cnt_PT]).iat[0,15] - timedelta(hours=0, minutes=0, seconds=int((TT[cnt_TT+1][cnt_PT]).iat[0,13]))

                
                
                
                elif (TT[cnt_TT][cnt_PT]).columns[0]=="polycom_id":
                    print("polycomm_id")
                    df_output.at[0,"type_occasion"]='pack'
                    df_output.at[0,"start_date"]=(TT[cnt_TT][cnt_PT]).iat[0,15] - timedelta(hours=0, minutes=0, seconds=int((TT[cnt_TT][cnt_PT]).iat[0,13]))
                    df_output.at[0,"end_date"]=(TT[cnt_TT][cnt_PT]).iat[0,15]
                    df_output.at[0,"internal_occasion"]=1
                    
                    end_pack=(TT[cnt_TT][cnt_PT]).iat[0,15]
                    start_pack=(TT[cnt_TT][cnt_PT]).iat[0,15] - timedelta(hours=0, minutes=0, seconds=int((TT[cnt_TT][cnt_PT]).iat[0,13]))
                    # start_pack_next=(TT[cnt_TT+1][cnt_PT]).iat[0,15] - timedelta(hours=0, minutes=0, seconds=int((TT[cnt_TT+1][cnt_PT]).iat[0,13]))
                    # if not len_TT-3==cnt_TT:  
                    #     if start_pack_next-end_pack>timedelta(hours=0,minutes=0, seconds=420):
                    #         cnt_group=cnt_group+1
                    #         df_output.at[0,"number_of_group"]=cnt_group

                
                    if  ((type(TT[cnt_TT][cnt_PT+1])==str) & (flag_receipt==0)):
                        flag_receipt=1
                        print("flag up", flag)
                        cnt_group=cnt_group+1
                        cnt_quantity=cnt_quantity+1
                        df_output.at[0,"number_of_group"]=cnt_group
                        Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                        
                        df_output.to_csv('out.csv', mode='a', header=False)
                        
                        cnt_PT=cnt_PT+1
                    

                    elif ((type(TT[cnt_TT][cnt_PT+1])==str) & (flag_receipt==1)):
                        print("flag_middle", flag)
                        cnt_quantity=cnt_quantity+1
                        Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                       
                        df_output.to_csv('out.csv', mode='a', header=False)
                        
                        cnt_PT=cnt_PT+1
                        

                    elif (flag_receipt==0 & (not type(TT[cnt_TT][cnt_PT+1])==str)):
                        cnt_group=cnt_group+1
                        df_output.at[0,"number_of_group"]=cnt_group
                        Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                        
                        df_output.to_csv('out.csv', mode='a', header=False)
                        
                        # cnt_PT=cnt_PT+1

                    else:
                        Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                        
                        df_output.to_csv('out.csv', mode='a', header=False)
                        
                       
                
                    

                    print("cnt_PT from polycom",cnt_PT)
                    cnt_PT=cnt_PT+1
                
                
                
                elif TT[cnt_TT][cnt_PT].columns[0]=="polycommalarm_id":
                    print("alarm")
                    df_output.at[0,"type_occasion"]='alarm'
                    df_output.at[0,"start_date"]=TT[cnt_TT][cnt_PT].iat[0,18]
                    df_output.at[0,"end_date"]='None'
                    if TT[cnt_TT][cnt_PT].iat[0,18]>end_pack:
                        df_output.at[0,"internal_occasion"]=0
                    else:
                        df_output.at[0,"internal_occasion"]=1 
                    
                    Receipt.data_analyz=Receipt.data_analyz.append([df_output], ignore_index=True)
                    
                    df_output.to_csv('out.csv', mode='a', header=False)
                    
                    print("cnt_PT from alarm",cnt_PT)
                    cnt_PT=cnt_PT+1
                
                
                
                elif TT[cnt_TT][cnt_PT].columns[0]=="polycommissue_id":
                    print("issue")
                    df_output.at[0,"type_occasion"]='issue'
                    df_output.at[0,"start_date"]=TT[cnt_TT][cnt_PT].iat[0,3]
                    df_output.at[0,"end_date"]='None'
                    if TT[cnt_TT][cnt_PT].iat[0,3]>end_pack:
                        df_output.at[0,"internal_occasion"]=0
                    else:
                        df_output.at[0,"internal_occasion"]=1
                    Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                    
                    df_output.to_csv('out.csv', mode='a', header=False)
                    
                    print("cnt_PT from issue",cnt_PT)
                    cnt_PT=cnt_PT+1
                
                
                
                elif (TT[cnt_TT][cnt_PT]).columns[0]=="receipt_id":
                    if flag_receipt==1:
                        flag_receipt=0
                        # if (TT[cnt_TT][cnt_PT]).iat[0,6]==cnt_quantity:
                        #     logging.info("EQUAL QUANTITY PACKAGE ONE")
                        #     #df_output.at[0,"qauntity_package"]="qauntity_package_one =="
                
                        # elif (TT[cnt_TT][cnt_PT]).iat[0,7]==cnt_quantity:   
                        #     logging.info("EQUAL QUANTITY PACKAGE DOUBLE")
                        #     #df_output.at[0,"qauntity_package"]="qauntity_package_double =="
                
                        # else:
                        #     logging.info("NOT EQUAL QUANTITY PACKAGE")
                        #     #df_output.at[0,"qauntity_package"]="qauntity_package_not equal =="
                    print("receipt")
                    df_output.at[0,"type_occasion"]='receipt'
                    df_output.at[0,"start_date"]=(TT[cnt_TT][cnt_PT]).iat[0,4] 
                    df_output.at[0,"end_date"]=(TT[cnt_TT][cnt_PT]).iat[0,5]
                    if TT[cnt_TT][cnt_PT].iat[0,4]>=end_pack:
                        df_output.at[0,"internal_occasion"]=0
                    else: 
                        df_output.at[0,"internal_occasion"]=1

                    if (TT[cnt_TT][cnt_PT]).iat[0,6]==0:
                        df_output.iat[0,7]=(TT[cnt_TT][cnt_PT]).iat[0,7]
                        df_output.iat[0,8]=0
                    else:
                        df_output.iat[0,8]=(TT[cnt_TT][cnt_PT]).iat[0,6]
                        df_output.iat[0,7]=0

                    Receipt.data_analyz=Receipt.data_analyz.append(df_output, ignore_index=True)
                    
                    df_output.to_csv('out.csv', mode='a', header=False)

                    df_output.iat[0,7]=None
                    df_output.iat[0,8]=None
                    
                    print("cnt_PT from receipt",cnt_PT)
                    cnt_PT=cnt_PT+1
                
                
                
                # cnt_PT=cnt_PT+1

                

        cnt_PT=0
        cnt_TT=0
        inner_TT=0
        inner_PT=0
        
        

    


    def filter_by_date(self, start_date, end_date, df_receipt):
        
        receipt_close=df_receipt["dateclose"]
        mask_receipt_package=(receipt_close>=start_date) & (receipt_close<=end_date)  # choose start from first package and end in the last package in df_output
        df_receipt_=df_receipt.loc[mask_receipt_package]
        if df_receipt_.empty :
            return 0
        else:
            return df_receipt_

    def check_receipt_period(self,df_receipt, df,df_output):
        
        receipt_open=df_receipt.iat[0,4]
        receipt_close=df_receipt.iat[0,5]
        df_time_close=df["local_date"]
        df_time_open=df["dateini_local"]

        mask_df= (receipt_open<=df_time_close) & (receipt_close>=df_time_open)

        mask_df1= (receipt_open<=df_time_open) & (receipt_close>=df_time_open) & (df_time_close<=receipt_close)
        
        df_mask=df.loc[mask_df]

        df_mask1=df.loc[mask_df1]

        
       

        if len(df_mask.index)>=2:
            
            print(df_mask)
            
            print(df_receipt)
            print("quantity",df_receipt.iat[0,6])
            print("df_close",df_mask["local_date"])
            if df_receipt.iat[0,6]==len(df_mask.index):
                logging.info("EQUAL QUANTITY PACKAGE ONE")
                # df_output.at[0,"qauntity_package"]="qauntity_package_one =="
                
            elif 2*df_receipt.iat[0,7]==len(df_mask.index):   
                logging.info("EQUAL QUANTITY PACKAGE DOUBLE")
                # df_output.at[0,"qauntity_package"]="qauntity_package_double =="
                
            else:
                logging.info("NOT EQUAL QUANTITY PACKAGE")
                # df_output.at[0,"qauntity_package"]="qauntity_package_not equal =="
        
        # if not df_mask1.empty:
        #     logging.info("yes it df_mask1")
        # else:
        #     logging.info("no mask_df1")
                

    def do_task(self, data_analyz):
        
        
        while True:
            #cnt frame it is number of pack in one group
            if len(data_analyz.index)-2==Receipt.cnt_dataframe: # need to fix becouse of Receipt.cnt +1 in line 581, in this reason -2
                break
            else:
                Receipt.cnt_dataframe=Receipt.cnt_dataframe+1


                
                
            if data_analyz.iat[Receipt.cnt,5]==data_analyz.iat[Receipt.cnt+1,5]:
                pass
            else:
                Receipt.cnt=0
                Receipt.cnt_frame=0
                Receipt.cnt_receipt=0




            if data_analyz.iat[Receipt.cnt,0]==data_analyz.iat[Receipt.cnt+1,0] :             
                
                Receipt.cnt=Receipt.cnt_dataframe+1
                
               
                
            else:
                state=[Receipt.flag_pack,Receipt.flag_receipt,Receipt.flag_issue,Receipt.flag_alarm]


                if state==[1,1,0,1]:
                    data_analyz.iat[Receipt.cnt, 12]="mix_task"  # 12 is mix task
                    Receipt.flag_pack=0
                    Receipt.flag_receipt=0
                    Receipt.flag_alarm=0


                elif state==[1,1,1,1]:
                    data_analyz.iat[Receipt.cnt, 12]="mix_task"
                    Receipt.flag_pack=0
                    Receipt.flag_receipt=0
                    Receipt.flag_alarm=0
                    Receipt.flag_issue=0

                elif state==[0,1,0,0]:
                    data_analyz.iat[Receipt.cnt, 14]="receipt_without_pack"
                    Receipt.flag_receipt=0

                elif state==[0,1,0,1]:
                    data_analyz.iat[Receipt.cnt, 15]="receipt_without_pack_and_alarm"
                    Receipt.flag_receipt=0
                    Receipt.flag_alarm=0
                
                Receipt.cnt=0
                Receipt.cnt_frame=0
                Receipt.cnt_receipt=0
            
           

                
        
        
            

            if data_analyz.iat[Receipt.cnt, 1]=='pack':
                Receipt.flag_pack=1
                Receipt.cnt_frame=Receipt.cnt_frame+1

            elif data_analyz.iat[Receipt.cnt, 1]=='receipt':
                Receipt.flag_receipt=1
                Receipt.cnt_receipt=Receipt.cnt_receipt+1



                if data_analyz.iat[Receipt.cnt, 7]==0:
                    if Receipt.cnt_frame==data_analyz.iat[Receipt.cnt, 8]:
                        data_analyz.iat[Receipt.cnt, 13]="KPU"
                        data_analyz.iat[Receipt.cnt, 16]="task_solved"
                    
                    elif Receipt.cnt_frame==2*data_analyz.iat[Receipt.cnt, 8]:
                        data_analyz.iat[Receipt.cnt, 13]="KPU"
                        data_analyz.iat[Receipt.cnt, 16]="task_solved"
                    
                    elif 2*Receipt.cnt_frame==data_analyz.iat[Receipt.cnt, 8]:
                        data_analyz.iat[Receipt.cnt, 13]="KPU"
                        data_analyz.iat[Receipt.cnt, 16]="task_solved"

                    else:
                        if data_analyz.iat[Receipt.cnt+1, 1]=='receipt':
                            pass
                        else:
                            data_analyz.iat[Receipt.cnt, 13]="KnPU"
                            data_analyz.iat[Receipt.cnt, 16]="task_doesn't_solved"
                
                elif Receipt.cnt_receipt==Receipt.cnt_frame:
                    data_analyz.iat[Receipt.cnt, 13]="KPU"
                    data_analyz.iat[Receipt.cnt, 16]="task_solved"



                else:
                    if Receipt.cnt_frame==data_analyz.iat[Receipt.cnt, 7]:
                        data_analyz.iat[Receipt.cnt, 13]="KPU"
                        data_analyz.iat[Receipt.cnt, 16]="task_solved"
                    else:
                        if data_analyz.iat[Receipt.cnt+1, 1]=='receipt':
                            pass
                        else:
                            data_analyz.iat[Receipt.cnt, 13]="KnPU"
                            data_analyz.iat[Receipt.cnt, 16]="task_doesn't_solved"


            
            elif data_analyz.iat[Receipt.cnt,1]=='issue':
                Receipt.flag_issue=1
            
            elif data_analyz.iat[Receipt.cnt,1]=='alarm':
                Receipt.flag_alarm=1
            

            # data_analyz=pd.DataFrame(columns=['number_of_group','type_occasion','start_date','end_date','internal_occasion','device','devicecode','qauntity_package_one','quantity_package_double','>420','r-a','task_alarm','mix_task','KPU/KnPU','receipt_without_pack','receipt_without_pack_and_alarm','solved_task'])
        
            

            

    





            


        







        


            
            
    
   
       
    
    