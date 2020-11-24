elif  type(TT[cnt_TT][cnt_PT])==str:
            inner_TT=cnt_TT
            inner_PT=0
            
            cnt_check_error=0
            
            while flag<=0:
                if flag==1:
                    break
                inner_TT=inner_TT+1
                num_PT=len(TT[inner_TT])
                inner_PT=0
                print("num_PT",num_PT)
                print("step_flag_while")
                print("flag",flag)
                cnt_check_error=cnt_check_error+1
                while num_PT>=inner_PT:
                    print("inner_pt", inner_PT)
                    if inner_PT==num_PT:
                        print("inner_break")
                        break
                    elif isinstance((TT[inner_TT][inner_PT]), pd.Series):
                        print("i am series")
                        if (TT[inner_TT][inner_PT]).index[0]=="receipt_id":
                            print("i am series, found receipt")
                            if cnt_check_error==(TT[inner_TT][inner_PT]).at['quantitypackageone']:
                                df_output.at[0,"number_of_group"]=cnt_group
                                df_output.to_csv('out.csv', mode='a', header=False)
                                flag=1
                                cnt_PT=cnt_PT+1
                                break
                            else:
                                cnt_group=cnt_group+1
                                df_output.at[0,"number_of_group"]=cnt_group
                                df_output.to_csv('out.csv', mode='a', header=False)
                                flag=1
                                cnt_PT=cnt_PT+1
                                break

                    elif isinstance((TT[inner_TT][inner_PT]), pd.DataFrame):
                        print("i am dataframe")
                        if (TT[inner_TT][inner_PT]).columns[0]=="receipt_id":
                            print("i am dataframe, found receipt")
                            if cnt_check_error==(TT[inner_TT][inner_PT]).iat[0,6]:
                                print("i am good receipt")
                                df_output.at[0,"number_of_group"]=cnt_group
                                df_output.to_csv('out.csv', mode='a', header=False)
                                flag=1
                                cnt_PT=cnt_PT+1
                                break
                            else:
                                cnt_group=cnt_group+1
                                df_output.at[0,"number_of_group"]=cnt_group
                                df_output.to_csv('out.csv', mode='a', header=False)
                                print("i am bad receipt")
                                flag=1
                                cnt_PT=cnt_PT+1
                                break
                    
                    
                    print("flag_inner", flag)
                    inner_PT=inner_PT+1