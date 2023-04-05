import pytz
from datetime import datetime, timedelta, timezone

import os
import csv
from DB_Schema.DB_Connect import session, exc, text
from routes.helperFunctions import getDayofWeek, getWeeklyBusinessHours, getAllStoresPolls, mergeIntervals, getUpDownTime, getStoreTimeZone, convertToLocal, convertToLocal_T, convertToLocal_withFloat, getStoresPolls

def getTimeObj(dateTimeString):
    return datetime.strptime(dateTimeString, '%Y-%m-%d %H:%M:%S.%f').time()

def getTimeObj_HHMMSS(timeString):
    return datetime.strptime(timeString, '%H:%M:%S').time()

def inNotin(interval,start,end):
    # print("----> inNotin : ",interval,start,end)
    start_status = False
    end_status = False
    
    interval_start = datetime.strptime('2023-01-24 '+interval.get('p2'), '%Y-%m-%d %H:%M:%S')
    interval_end = datetime.strptime('2023-01-24 '+interval.get('p3'), '%Y-%m-%d %H:%M:%S')

    if interval_start <= start <= interval_end:
        start_status = True
    if interval_start <= end <= interval_end:
        end_status = True
    
    return start_status, end_status

def sendTIMEGetDATETIME(dateString,timeString):
    return datetime.strptime(dateString+' '+timeString, '%Y-%m-%d %H:%M:%S')

def lastWeekReport(report_id):
    # print("Function called")
    try:
        db=session()
        # oneDayBackDateTime = datetime.strptime('2023-01-24 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f')     # Max time in store_status polls
        # current_datetime= datetime.strptime('2023-01-24 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f') 
        
        # Format [ (start, end), ... ]
        targetWeek = [
            (datetime.strptime('2023-01-24 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-24 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-23 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-23 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-22 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-22 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-21 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-21 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-20 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-20 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-19 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-19 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-18 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-18 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f'))
        ]
    

        # day = getDayofWeek(curDay[0].strftime('%Y-%m-%d %H:%M:%S.%f'))     # datetime object to string format with no float
        
        # print(targetWeek)
        
        store_ids = db.execute(text("SELECT DISTINCT(store_id) \
            FROM store_status \
            ORDER BY store_id")).fetchall()
        
        # print(store_ids)
        
        db.commit()
        for sid in store_ids:
            storeid = sid[0]
            store_zone = getStoreTimeZone(db,storeid)

            uptime_lastday_curStore = 0
            downtime_lastday_curStore = 0
            
            storeOpenIntervals_weekDict = getWeeklyBusinessHours(db, storeid)     # [{"p1": 0, "p2": "00:00:00", "p3": "00:10:00"}, {"p1": 0, "p2": "11:00:00", "p3": "23:59:00"}]
            
            for curDay in targetWeek:
                polls = getStoresPolls(db, curDay[0], curDay[1], storeid)

                if len(polls)==0:
                    continue
                
                # print("------------> ",polls)
                
                for dictObj in polls[0][1]:
                    dictObj['p2']=convertToLocal_T(dictObj.get('p2'), store_zone)

                local_hr_start = datetime.strptime(curDay[0].strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
                local_hr_end = datetime.strptime(curDay[1].strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')

                day = getDayofWeek(curDay[0].strftime('%Y-%m-%d %H:%M:%S.%f'))     # datetime object to string format with no float                
                curdayInterval = storeOpenIntervals_weekDict.get(day)
                
                if curdayInterval==None:        # No intervals for this day
                    # print("storeid: ",storePoll[0]," up = ", uptime_lastday_curStore/60," down = ", downtime_lastday_curStore/60,'\n',flush=True)
                    continue
                
                mergedIntervals = mergeIntervals(curdayInterval)
                
                # print("mergedIntervals = ",mergedIntervals ,flush=True)
                
                dateString = curDay[0].strftime('%Y-%m-%d')
                
                for interval in mergedIntervals:
                    # print("_inter = ",interval,flush=True)
                    
                    start_status, end_status = inNotin(interval, local_hr_start, local_hr_end)
                    # print(f"__start_s = {start_status} end_s = {end_status}",flush=True)
                    if start_status==False and end_status==False:
                        up, down = getUpDownTime(local_hr_start, local_hr_end, polls[0][1])
                        # print(f"___up : {up} down : {down}",flush=True)
                        uptime_lastday_curStore+=up
                        downtime_lastday_curStore+=down
                    elif start_status==True and end_status==True:
                        up, down = getUpDownTime(local_hr_start, local_hr_end, polls[0][1])
                        # print(f"___up : {up} down : {down}",flush=True)
                        uptime_lastday_curStore+=up
                        downtime_lastday_curStore+=down
                        # break
                    elif start_status==True and end_status==False:
                        up, down = getUpDownTime(local_hr_start, sendTIMEGetDATETIME(dateString,interval.get('p3')), polls[0][1])
                        # print(f"___up : {up} down : {down}",flush=True)
                        uptime_lastday_curStore+=up
                        downtime_lastday_curStore+=down                                            # Don't break in this case
                    elif start_status==False and end_status==True:
                        up, down = getUpDownTime(sendTIMEGetDATETIME(dateString,interval.get('p2')), local_hr_end, polls[0][1])
                        # print(f"___up : {up} down : {down}",flush=True)
                        uptime_lastday_curStore+=up
                        downtime_lastday_curStore+=down
                        # break
            print("week storeid: ",storeid," up = ", '{:.2f}'.format(uptime_lastday_curStore/60)," down = ", '{:.2f}'.format(downtime_lastday_curStore/60),'\n',flush=True)
            insert_query = text('INSERT INTO reports \
                ( \
                report_id, \
                store_id, \
                uptime_last_week, \
                downtime_last_week \
                ) \
                VALUES \
                ( \
                :report_id, \
                :store_id, \
                :uptime_last_week, \
                :downtime_last_week \
                ) \
                ON CONFLICT (report_id, store_id) \
                DO UPDATE SET \
                uptime_last_week = EXCLUDED.uptime_last_week, \
                downtime_last_week = EXCLUDED.downtime_last_week \
                ')
                
            params = {
                "report_id": report_id,
                "store_id": storeid,
                "uptime_last_week": '{:.2f}'.format(uptime_lastday_curStore/60),
                "downtime_last_week": '{:.2f}'.format(downtime_lastday_curStore/60)
            }
            
            db.execute(insert_query, params)
            db.commit()
    except exc.SQLAlchemyError as e:
        return "ERROR"
    finally:
        
        # Execute the SQL query to retrieve the rows with the WHERE condition
        sql_query = f"SELECT report_id, store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week FROM reports WHERE report_id = '{report_id}'"
        rows = db.execute(sql_query).fetchall()

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
                
        db.close()
        
        