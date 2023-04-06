import os
import csv
import time
from datetime import datetime, timezone

from DB_Schema.DB_Connect import session, exc, text
from routes.helperFunctions import getUpDownTime, convertToLocal_T, convertToLocal_withFloat, getPollsWeek, getIntervalsWeekNew, listOflistsMergeIntervals

# OPEN 24x7: Use this global variable if business hours data is missing
default_hrs = {
            "0": [["00:00:00","23:59:59"]],
            "1": [["00:00:00","23:59:59"]],
            "2": [["00:00:00","23:59:59"]],
            "3": [["00:00:00","23:59:59"]],
            "4": [["00:00:00","23:59:59"]],
            "5": [["00:00:00","23:59:59"]],
            "6": [["00:00:00","23:59:59"]]
        }

# This can be generated dynamically according to current_max_time
targethour = [
        (datetime.strptime('2023-01-25 17:13:22.47922', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-25 18:13:22.47922', '%Y-%m-%d %H:%M:%S.%f'))
]
        
# This can be generated dynamically for given range of weeks
targetWeek = [
    (datetime.strptime('2023-01-19 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-19 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
    (datetime.strptime('2023-01-20 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-20 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
    (datetime.strptime('2023-01-21 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-21 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
    (datetime.strptime('2023-01-22 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-22 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
    (datetime.strptime('2023-01-23 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-23 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
    (datetime.strptime('2023-01-24 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-24 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
    (datetime.strptime('2023-01-25 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-25 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f'))
]

# Check if start and end time lies in business hr intervals
def inNotin(interval,start,end,date):
    start_status = False
    end_status = False
    
    interval_start = datetime.strptime(date+" "+interval[0], '%Y-%m-%d %H:%M:%S')
    interval_end = datetime.strptime(date+" "+interval[1], '%Y-%m-%d %H:%M:%S')

    if interval_start <= start <= interval_end:
        start_status = True
    if interval_start <= end <= interval_end:
        end_status = True
    
    return start_status, end_status

# Convert open and close times of store business intervals to datetime.datetime objects
def sendTIMEGetDATETIME(dateString,timeString):
    return datetime.strptime(dateString+' '+timeString, '%Y-%m-%d %H:%M:%S')

# Accumulate the uptime and downtime for current given interval
def calc_upDown_Time(uptime_lastday_curStore, downtime_lastday_curStore, mergedIntervals, local_hr_start, local_hr_end, polls,cur_date):
    for interval in mergedIntervals:
        start_status, end_status = inNotin(interval, local_hr_start, local_hr_end,cur_date)
        if start_status==False and end_status==False:
            up, down = getUpDownTime(local_hr_start, local_hr_end, polls)
            uptime_lastday_curStore+=up
            downtime_lastday_curStore+=down
        elif start_status==True and end_status==True:
            up, down = getUpDownTime(local_hr_start, local_hr_end, polls)
            uptime_lastday_curStore+=up
            downtime_lastday_curStore+=down
        elif start_status==True and end_status==False:
            up, down = getUpDownTime(local_hr_start, sendTIMEGetDATETIME(cur_date,interval[1]), polls)
            uptime_lastday_curStore+=up
            downtime_lastday_curStore+=down                                           
        elif start_status==False and end_status==True:
            up, down = getUpDownTime(sendTIMEGetDATETIME(cur_date,interval[0]), local_hr_end, polls)
            uptime_lastday_curStore+=up
            downtime_lastday_curStore+=down
    return uptime_lastday_curStore, downtime_lastday_curStore

# To calculate EXECUTION TIME for report generation
start = 0
end = 0

# IMP -> Initiate report generation
def generateReport(report_id):
    global default_hrs, targethour, targetWeek, start, end
    start = time.time()

    try:
        db=session()
    
        # Fetch all data at once
        allPolls = getPollsWeek(db)
        getAllStoresIntervalsWeekNew = getIntervalsWeekNew(db)
        
        intervalDict = dict()
        for store in getAllStoresIntervalsWeekNew:
            intervalDict[store[0]] = store[1]
        
        # Calculate up, down time for each store_id
        for curStore in allPolls:
            storeid = curStore[0]
            store_zone = curStore[1]
            pollsDictKeyDate = curStore[2]
            storeOpenIntervals_weekDict = intervalDict.get(storeid)   

            # Skip if no polls
            if pollsDictKeyDate == None:
                continue
            
            # If menu_hours not in data, consider Open for 24x7
            if storeOpenIntervals_weekDict == None:
                storeOpenIntervals_weekDict = default_hrs

            uptime_last_hour = 0
            downtime_last_hour = 0
            
            uptime_last_day = 0
            downtime_last_day = 0
            
            uptime_last_week = 0
            downtime_last_week = 0

            # Fetch data once and calculate all requirements
            for run in ['hour','day', 'week']:
                if run=='hour':
                    target = targethour
                elif run=='day':
                    target = [targetWeek[-1]]
                elif run=='week':
                    target = targetWeek
                
                uptime_lastday_curStore = 0
                downtime_lastday_curStore = 0
                
                # Runs on target hour, day, week
                for curDay in target:
                    cur_date = curDay[0].strftime('%Y-%m-%d')
                    polls_orig = pollsDictKeyDate.get(cur_date)      # gives list of dicts, each dict (p1:status, p2:timestamp)

                    if polls_orig == None or len(polls_orig)==0:
                        continue
                    
                    polls=[]
                    for dictObj in polls_orig:
                        polls.append({'p1':dictObj.get('p1'),'p2':convertToLocal_T(dictObj.get('p2'), store_zone)})

                    day = curDay[0].replace(tzinfo=timezone.utc).weekday()
                    curdayIntervalList = storeOpenIntervals_weekDict.get(str(day))
                    
                    # Skip is not open
                    if curdayIntervalList==None or len(curdayIntervalList)==0:        # No intervals for this day
                        continue
                    
                    # Remove overlapping intervals (if any)
                    mergedIntervals = listOflistsMergeIntervals(curdayIntervalList)

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
                    
                    
            # DB Query - save in Reports table in Postgres DB
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
        
        end = time.time()
        run_time = end - start
        run_time_mins = run_time/60 
        print("<EXIT> Time taken: {:.2f} minutes".format(run_time_mins))
        db.close()