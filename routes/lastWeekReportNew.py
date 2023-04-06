import pytz
from datetime import datetime, timedelta, timezone

import os
import csv
from DB_Schema.DB_Connect import session, exc, text
from routes.helperFunctions import getDayofWeek, getWeeklyBusinessHours, getAllStoresPolls, mergeIntervals, getUpDownTime, getStoreTimeZone, convertToLocal, convertToLocal_T, convertToLocal_withFloat, getStoresPolls, getPollsWeek, getIntervalsWeekNew, listOflistsMergeIntervals

def getTimeObj(dateTimeString):
    return datetime.strptime(dateTimeString, '%Y-%m-%d %H:%M:%S.%f').time()

def getTimeObj_HHMMSS(timeString):
    return datetime.strptime(timeString, '%H:%M:%S').time()

def inNotin(interval,start,end,date):
    # print("----> inNotin : ",interval,start,end)
    start_status = False
    end_status = False
    
    interval_start = datetime.strptime(date+" "+interval[0], '%Y-%m-%d %H:%M:%S')
    interval_end = datetime.strptime(date+" "+interval[1], '%Y-%m-%d %H:%M:%S')

    if interval_start <= start <= interval_end:
        start_status = True
    if interval_start <= end <= interval_end:
        end_status = True
    
    return start_status, end_status

def sendTIMEGetDATETIME(dateString,timeString):
    return datetime.strptime(dateString+' '+timeString, '%Y-%m-%d %H:%M:%S')

def calc_upDown_Time(uptime_lastday_curStore, downtime_lastday_curStore, mergedIntervals, local_hr_start, local_hr_end, polls,cur_date):
    
    for interval in mergedIntervals:
        # print("_inter = ",interval,flush=True)
        
        start_status, end_status = inNotin(interval, local_hr_start, local_hr_end,cur_date)
        # print(f"\n__start_s = {start_status} end_s = {end_status}",flush=True)
        if start_status==False and end_status==False:
            up, down = getUpDownTime(local_hr_start, local_hr_end, polls)
            # print(f"___up : {up} down : {down}",flush=True)
            uptime_lastday_curStore+=up
            downtime_lastday_curStore+=down
        elif start_status==True and end_status==True:
            # print(local_hr_start,local_hr_end)
            up, down = getUpDownTime(local_hr_start, local_hr_end, polls)
            # print(f"___up : {up} down : {down}",flush=True)
            uptime_lastday_curStore+=up
            downtime_lastday_curStore+=down
            # break
        elif start_status==True and end_status==False:
            up, down = getUpDownTime(local_hr_start, sendTIMEGetDATETIME(cur_date,interval[1]), polls)
            # print(f"___up : {up} down : {down}",flush=True)
            uptime_lastday_curStore+=up
            downtime_lastday_curStore+=down                                            # Don't break in this case
        elif start_status==False and end_status==True:
            up, down = getUpDownTime(sendTIMEGetDATETIME(cur_date,interval[0]), local_hr_end, polls)
            # print(f"___up : {up} down : {down}",flush=True)
            uptime_lastday_curStore+=up
            downtime_lastday_curStore+=down
            # break
    return uptime_lastday_curStore, downtime_lastday_curStore


def lastWeekReportNew(report_id):
    # print("Function called")
    try:

        db=session()
        
        # IF MENU HOURS NULL FOR SOME STORE ID OPEN 24x7
        default_hrs = {
            "0": [["00:00:00","23:59:59"]],
            "1": [["00:00:00","23:59:59"]],
            "2": [["00:00:00","23:59:59"]],
            "3": [["00:00:00","23:59:59"]],
            "4": [["00:00:00","23:59:59"]],
            "5": [["00:00:00","23:59:59"]],
            "6": [["00:00:00","23:59:59"]]
        }
        
        targethour = [
            (datetime.strptime('2023-01-25 17:13:22.47922', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-25 18:13:22.47922', '%Y-%m-%d %H:%M:%S.%f'))
        ]
        
        targetWeek = [
            (datetime.strptime('2023-01-19 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-19 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-20 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-20 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-21 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-21 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-22 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-22 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-23 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-23 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-24 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-24 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-25 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-25 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f'))
        ]
        
        allPolls = getPollsWeek(db)     # list of tuples
        
        # print("-------> allPolls = ", allPolls, flush=True)
        
        getAllStoresIntervalsWeekNew = getIntervalsWeekNew(db)
        
        intervalDict = dict()
        
        for store in getAllStoresIntervalsWeekNew:
            intervalDict[store[0]] = store[1]
        
        
        # for run in ['day', 'week']:
        for curStore in allPolls:
            
            storeid = curStore[0]
            store_zone = curStore[1]
            pollsDictKeyDate = curStore[2]
            storeOpenIntervals_weekDict = intervalDict.get(storeid)   

            if pollsDictKeyDate == None:
                continue
            
            # Open 24x7
            if storeOpenIntervals_weekDict == None:
                storeOpenIntervals_weekDict = default_hrs

            uptime_last_hour = 0
            downtime_last_hour = 0
            
            uptime_last_day = 0
            downtime_last_day = 0
            
            uptime_last_week = 0
            downtime_last_week = 0

            for run in ['hour','day', 'week']:
                if run=='hour':
                    target = targethour
                elif run=='day':
                    target = [targetWeek[-1]]
                elif run=='week':
                    target = targetWeek
                
                uptime_lastday_curStore = 0
                downtime_lastday_curStore = 0
                
                for curDay in target:
                    cur_date = curDay[0].strftime('%Y-%m-%d')
                    
                    # print("\n\n",cur_date, flush=True)
                    
                    polls_orig = pollsDictKeyDate.get(cur_date)      # gives list of dicts, each dict (p1:status, p2:timestamp)

                    # print(polls_orig)

                    if polls_orig == None or len(polls_orig)==0:
                        continue
                    
                    polls=[]
                    for dictObj in polls_orig:
                        polls.append({'p1':dictObj.get('p1'),'p2':convertToLocal_T(dictObj.get('p2'), store_zone)})

                    # print(polls)
                    # day = getDayofWeek(curDay[0].strftime('%Y-%m-%d %H:%M:%S.%f'))     # datetime object to string format with no float                
                    
                    day = curDay[0].replace(tzinfo=timezone.utc).weekday()
                    curdayIntervalList = storeOpenIntervals_weekDict.get(str(day))
                    
                    if curdayIntervalList==None or len(curdayIntervalList)==0:        # No intervals for this day
                        # print("storeid: ",storePoll[0]," up = ", uptime_lastday_curStore/60," down = ", downtime_lastday_curStore/60,'\n',flush=True)
                        continue
                    
                    
                    mergedIntervals = listOflistsMergeIntervals(curdayIntervalList)
                    
                    # print('\n\n',mergedIntervals)
                    
                    # print("mergedIntervals = ",mergedIntervals ,flush=True)
                    
                    # dateString = curDay[0].strftime('%Y-%m-%d')
                    if run=='hour':
                        local_hr_start = datetime.strptime(convertToLocal_withFloat('2023-01-25 17:13:22.479',store_zone), '%Y-%m-%d %H:%M:%S')
                        local_hr_end = datetime.strptime(convertToLocal_withFloat('2023-01-25 18:13:22.479',store_zone), '%Y-%m-%d %H:%M:%S')
                    else:
                        local_hr_start = datetime.strptime(curDay[0].strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
                        local_hr_end = datetime.strptime(curDay[1].strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
                        
                    up_t, down_t = calc_upDown_Time(uptime_lastday_curStore,downtime_lastday_curStore,mergedIntervals,local_hr_start,local_hr_end,polls,cur_date)
                    
                    uptime_lastday_curStore = up_t
                    downtime_lastday_curStore = down_t
                
                if run=='hour':
                    uptime_last_hour = uptime_lastday_curStore
                    downtime_last_hour = downtime_lastday_curStore
                elif run=='day':
                    uptime_last_day = uptime_lastday_curStore
                    downtime_last_day = downtime_lastday_curStore
                elif run=='week':
                    uptime_last_week = uptime_lastday_curStore
                    downtime_last_week = downtime_lastday_curStore

                if run == 'hour':
                    print(f"{run} storeid: ",storeid," up = ", '{:.2f}'.format(uptime_lastday_curStore)," down = ", '{:.2f}'.format(downtime_lastday_curStore),flush=True)
                else:
                    print(f"{run} storeid: ",storeid," up = ", '{:.2f}'.format(uptime_lastday_curStore/60)," down = ", '{:.2f}'.format(downtime_lastday_curStore/60),flush=True)
                    
                    
            # DB Query
            insert_query = text("INSERT INTO reports \
            ( \
            report_id, \
            store_id, \
            uptime_last_hour, \
            downtime_last_hour, \
            uptime_last_day, \
            downtime_last_day, \
            uptime_last_week, \
            downtime_last_week \
            ) \
            VALUES \
            ( \
            :report_id, \
            :store_id, \
            :uptime_last_hour, \
            :downtime_last_hour, \
            :uptime_last_day, \
            :downtime_last_day, \
            :uptime_last_week, \
            :downtime_last_week \
            ) \
            ON CONFLICT (report_id, store_id) \
            DO UPDATE SET \
            uptime_last_hour = EXCLUDED.uptime_last_hour, \
            downtime_last_hour = EXCLUDED.downtime_last_hour, \
            uptime_last_day = EXCLUDED.uptime_last_day, \
            downtime_last_day = EXCLUDED.downtime_last_day, \
            uptime_last_week = EXCLUDED.uptime_last_week, \
            downtime_last_week = EXCLUDED.downtime_last_week \
            ")
                
            params = {
                "report_id": report_id,
                "store_id": storeid,
                "uptime_last_hour": '{:.2f}'.format(uptime_last_hour),
                "downtime_last_hour": '{:.2f}'.format(downtime_last_hour),
                "uptime_last_day": '{:.2f}'.format(uptime_last_day/60),
                "downtime_last_day": '{:.2f}'.format(downtime_last_day/60),
                "uptime_last_week": '{:.2f}'.format(uptime_last_week/60),
                "downtime_last_week": '{:.2f}'.format(downtime_last_week/60)
            }
            
            # print(params)
            
            db.execute(insert_query, params)
            db.commit()
    except exc.SQLAlchemyError as e:
        return "ERROR"
    finally:
        
        # Execute the SQL query to retrieve the rows with the WHERE condition
        sql_query = text("SELECT report_id, \
            store_id, \
            uptime_last_hour, uptime_last_day, uptime_last_week, \
            downtime_last_hour, downtime_last_day, downtime_last_week \
            FROM reports WHERE report_id = :report_id")
        params = {
            "report_id": report_id
        }
        rows = db.execute(sql_query,params).fetchall()

        # If the file doesn't exist, create it
        if not os.path.exists(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\report\report.csv'):
            with open(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\report\report.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["report_id", "store_id", "uptime_last_hour", "uptime_last_day", "uptime_last_week", "downtime_last_hour", "downtime_last_day", "downtime_last_week"])

        # Write the rows to the CSV file
        with open(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\report\report.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            for row in rows:
                writer.writerow(row)
        print("<Exit>", flush=True)  
        db.close()