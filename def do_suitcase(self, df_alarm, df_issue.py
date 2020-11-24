def do_suitcase(self, df_alarm, df_issue, df, num_suitcase):
    PT=[]
    
    alarm_time=df_alarm["date"]
    issue_time=df_issue["date"]
    suitcase_time_start_of_one_package= df.at[num_suitcase,"dateini"]
    suitcase_time_start_of_another_package=df.at[num_suitcase+1,"dateini"]
    mask_alarm_package=(alarm_time>suitcase_time_start_of_one_package) & (alarm_time<suitcase_time_start_of_another_package)
    mask_issue_package=(issue_time>suitcase_time_start_of_one_package) & (issue_time<suitcase_time_start_of_another_package)
    df_alarm_=df_alarm.loc[mask_alarm_package]
    df_issue_=df_issue.loc[mask_issue_package]

    state=[df_alarm_.empty,df_issue_.empty]  #state for checking empty

    if state==[True, True]:
        pass
    elif state==[True, False]:
        PT.append(df_issue_)
    elif state==[False, True]:
        PT.append(df_alarm_)
    elif state==[False,False]:
        cnt_alarm=len(df_alarm_.index)
        cnt_issue=len(df_issue_.index)
        cnt_main_issue=0
        cnt_main_alarm=0
        while cnt_alarm>cnt_main_alarm or cnt_issue>cnt_main_issue:
            if df_alarm_.at[cnt_main_alarm, "date"]<df_issue_.at[cnt_main_issue,"date"]:
                PT.append(df_alarm_.loc[cnt_main_alarm, :])
                cnt_main_alarm=cnt_main_alarm+1
            else:
                PT.append(df_issue_.loc[cnt_main_issue,:])
                cnt_main_issue=cnt_main_issue+1
        
        if cnt_alarm>cnt_issue:
            new_cnt= cnt_alarm-cnt_issue
            while new_cnt>cnt_main_alarm:
                PT.append(df_alarm_.loc[cnt_main_alarm,:])
                cnt_main_alarm=cnt_main_alarm+1
        else:
            new_cnt=cnt_issue-cnt_alarm
            while new_cnt>cnt_main_issue:
                PT.append(df_alarm_.loc[cnt_main_issue,:])
                cnt_main_issue=cnt_main_issue+1
    
    df_issue.drop(index=df_issue.loc[mask_issue_package].index, inplace=True)
    df_alarm.drop(index=df_alarm.loc[mask_alarm_package].index, inplace=True)
    df_alarm._clear_item_cache
    df_alarm_._clear_item_cache

    return PT
               