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

def lastWeekReportNew(report_id):
    # print("Function called")
    try:
        # 257406274356679	
        # America/New_York	
        
        # {"2023-01-18": [{"p1": "active", "p2": "2023-01-18T13:31:37.537599"}, {"p1": "active", "p2": "2023-01-18T14:10:34.598488"}, {"p1": "active", "p2": "2023-01-18T15:36:51.841037"}, {"p1": "active", "p2": "2023-01-18T16:06:11.775275"}, {"p1": "active", "p2": "2023-01-18T17:25:19.066889"}, {"p1": "active", "p2": "2023-01-18T18:14:55.690059"}, {"p1": "active", "p2": "2023-01-18T19:32:57.438104"}, {"p1": "active", "p2": "2023-01-18T20:08:28.59866"}, {"p1": "active", "p2": "2023-01-18T21:06:28.438466"}, {"p1": "active", "p2": "2023-01-18T23:17:34.272136"}], 
        # "2023-01-19": [{"p1": "active", "p2": "2023-01-19T00:21:29.790672"}, {"p1": "active", "p2": "2023-01-19T01:46:56.615808"}, {"p1": "active", "p2": "2023-01-19T02:06:54.02339"}, {"p1": "active", "p2": "2023-01-19T03:21:59.583591"}, {"p1": "active", "p2": "2023-01-19T04:09:14.585774"}, {"p1": "active", "p2": "2023-01-19T05:06:53.729209"}, {"p1": "active", "p2": "2023-01-19T06:08:56.223562"}, {"p1": "active", "p2": "2023-01-19T07:10:47.717242"}, {"p1": "active", "p2": "2023-01-19T08:10:27.777183"}, {"p1": "active", "p2": "2023-01-19T09:19:40.659915"}, {"p1": "active", "p2": "2023-01-19T09:20:35.095145"}, {"p1": "active", "p2": "2023-01-19T10:20:31.774683"}, {"p1": "active", "p2": "2023-01-19T11:06:14.21261"}, {"p1": "active", "p2": "2023-01-19T12:13:47.909752"}, {"p1": "active", "p2": "2023-01-19T13:38:26.322669"}, {"p1": "active", "p2": "2023-01-19T13:57:30.70324"}, {"p1": "active", "p2": "2023-01-19T14:59:40.863988"}, {"p1": "active", "p2": "2023-01-19T15:09:43.353002"}, {"p1": "active", "p2": "2023-01-19T15:22:06.807332"}, {"p1": "active", "p2": "2023-01-19T16:16:36.268276"}, {"p1": "active", "p2": "2023-01-19T17:43:34.76841"}, {"p1": "active", "p2": "2023-01-19T18:44:26.564571"}, {"p1": "active", "p2": "2023-01-19T19:40:24.809892"}], 
        # }
        
        # {"0": {"dayofweek": 0, "end_local": "23:59:00", "start_local": "00:00:00"},
        # "1": {"dayofweek": 1, "end_local": "23:59:00", "start_local": "00:00:00"},
        # "2": {"dayofweek": 2, "end_local": "23:59:00", "start_local": "00:00:00"},
        # "3": {"dayofweek": 3, "end_local": "23:59:00", "start_local": "00:00:00"},
        # "4": {"dayofweek": 4, "end_local": "23:59:00", "start_local": "00:00:00"},
        # "5": {"dayofweek": 5, "end_local": "23:59:00", "start_local": "00:00:00"},
        # "6": {"dayofweek": 6, "end_local": "23:59:00", "start_local": "00:00:00"}}
        
        
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
        
        targetWeek = [
            (datetime.strptime('2023-01-18 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-18 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-19 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-19 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-20 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-20 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-21 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-21 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-22 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-22 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-23 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-23 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f')),
            (datetime.strptime('2023-01-24 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'),datetime.strptime('2023-01-24 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f'))
        ]
        
        
        db=session()

        allPolls = getPollsWeek(db)     # list of tuples
        
        print("-------> allPolls = ",allPolls,flush=True)
        
        getAllStoresIntervalsWeekNew = getIntervalsWeekNew(db)
        
        intervalDict = dict()
        
        for store in getAllStoresIntervalsWeekNew:
            intervalDict[store[0]] = store[1]
        
        
        
        for curStore in allPolls:
            uptime_lastday_curStore = 0
            downtime_lastday_curStore = 0
            
            storeid = curStore[0]
            store_zone = curStore[1]
            pollsDictKeyDate = curStore[2]
            storeOpenIntervals_weekDict = intervalDict.get(storeid)   
            
            
            print(storeid, flush=True)
            print(store_zone , flush=True)
            print(pollsDictKeyDate['2023-01-18'], flush=True)
            print(storeOpenIntervals_weekDict, flush=True)
            
            if pollsDictKeyDate == None:
                continue
            
            # Open 24x7
            if storeOpenIntervals_weekDict == None:
                storeOpenIntervals_weekDict = default_hrs

            
            for curDay in targetWeek:
                
                
                cur_date = curDay[0].strftime('%Y-%m-%d')
                
                print("\n\n",cur_date, flush=True)
                
                polls = pollsDictKeyDate.get(cur_date)      # gives list of dicts, each dict (p1:status, p2:timestamp)

                if polls == None or len(polls)==0:
                    continue
                
                
                for dictObj in polls:
                    dictObj['p2']=convertToLocal_T(dictObj.get('p2'), store_zone)

                    print("local polls ===> ",dictObj,flush=True)

                # day = getDayofWeek(curDay[0].strftime('%Y-%m-%d %H:%M:%S.%f'))     # datetime object to string format with no float                
                
                day = curDay[0].replace(tzinfo=timezone.utc).weekday()
                curdayIntervalList = storeOpenIntervals_weekDict.get(str(day))
                
                if curdayIntervalList==None or len(curdayIntervalList)==0:        # No intervals for this day
                    # print("storeid: ",storePoll[0]," up = ", uptime_lastday_curStore/60," down = ", downtime_lastday_curStore/60,'\n',flush=True)
                    continue
                
                
                
                mergedIntervals = listOflistsMergeIntervals(curdayIntervalList)
                
                # print("mergedIntervals = ",mergedIntervals ,flush=True)
                
                # dateString = curDay[0].strftime('%Y-%m-%d')
                
                local_hr_start = datetime.strptime(curDay[0].strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
                local_hr_end = datetime.strptime(curDay[1].strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
                
                for interval in mergedIntervals:
                    print("_inter = ",interval,flush=True)
                    
                    start_status, end_status = inNotin(interval, local_hr_start, local_hr_end,cur_date)
                    print(f"\n__start_s = {start_status} end_s = {end_status}",flush=True)
                    if start_status==False and end_status==False:
                        up, down = getUpDownTime(local_hr_start, local_hr_end, polls)
                        print(f"___up : {up} down : {down}",flush=True)
                        uptime_lastday_curStore+=up
                        downtime_lastday_curStore+=down
                    elif start_status==True and end_status==True:
                        up, down = getUpDownTime(local_hr_start, local_hr_end, polls)
                        print(f"___up : {up} down : {down}",flush=True)
                        uptime_lastday_curStore+=up
                        downtime_lastday_curStore+=down
                        # break
                    elif start_status==True and end_status==False:
                        up, down = getUpDownTime(local_hr_start, sendTIMEGetDATETIME(cur_date,interval[1]), polls)
                        print(f"___up : {up} down : {down}",flush=True)
                        uptime_lastday_curStore+=up
                        downtime_lastday_curStore+=down                                            # Don't break in this case
                    elif start_status==False and end_status==True:
                        up, down = getUpDownTime(sendTIMEGetDATETIME(cur_date,interval[0]), local_hr_end, polls)
                        print(f"___up : {up} down : {down}",flush=True)
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
        print("Success!", flush=True)  
        db.close()
        
        