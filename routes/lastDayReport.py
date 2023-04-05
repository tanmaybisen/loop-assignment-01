import pytz
from datetime import datetime, timedelta, timezone

from DB_Schema.DB_Connect import session, exc, text
from routes.helperFunctions import getDayofWeek, getWeeklyBusinessHours, getAllStoresPolls, mergeIntervals, getUpDownTime, getStoreTimeZone, convertToLocal, convertToLocal_T, convertToLocal_withFloat

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

def sendTIMEGetDATETIME(timeString):
    return datetime.strptime('2023-01-24 '+timeString, '%Y-%m-%d %H:%M:%S')

def lastdayReport(report_id):

    oneDayBackDateTime = datetime.strptime('2023-01-24 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f')     # Max time in store_status polls
    current_datetime= datetime.strptime('2023-01-24 23:59:59.000', '%Y-%m-%d %H:%M:%S.%f') 
    
    # American Samoa/Niue are the furthest behind places at UTC-11 [So the date will remain same for all timezone converted to UTC]
    day = getDayofWeek('2023-01-24 18:13:22.479')
    
    try:
        db=session()
        
        # Polls will be in universal time
        allPolls = getAllStoresPolls(db, oneDayBackDateTime, current_datetime)   # 257406274356679 : [{"p1": "active", "p2": "2023-01-25T17:15:00.905937"}, {"p1": "active", "p2": "2023-01-25T18:03:20.095218"}]
        for storePoll in allPolls:
            
            store_zone = getStoreTimeZone(db,storePoll[0])
            
            for dictObj in storePoll[1]:
                dictObj['p2']=convertToLocal_T(dictObj.get('p2'),store_zone)
                # print("poll = ",dictObj)
            
            local_hr_start = datetime.strptime('2023-01-24 00:00:00', '%Y-%m-%d %H:%M:%S')
            local_hr_end = datetime.strptime('2023-01-24 23:59:59', '%Y-%m-%d %H:%M:%S')
            
            # print("STARTED FOR STORE ID = ", storePoll[0],flush=True)
            # print(f"oneDayBackDateTime = {oneDayBackDateTime}, current_datetime = {current_datetime}",flush=True)
            
            uptime_lastday_curStore = 0
            downtime_lastday_curStore = 0
            
            storeOpenIntervals = getWeeklyBusinessHours(db, storePoll[0]).get(day)     # [{"p1": 0, "p2": "00:00:00", "p3": "00:10:00"}, {"p1": 0, "p2": "11:00:00", "p3": "23:59:00"}]
            
            # print("storeOpenIntervals = ", storeOpenIntervals ,flush=True)
            
            if storeOpenIntervals==None:        # No intervals for this day
                # print("storeid: ",storePoll[0]," up = ", uptime_lastday_curStore/60," down = ", downtime_lastday_curStore/60,'\n',flush=True)
                continue
            
            mergedIntervals = mergeIntervals(storeOpenIntervals)
            
            # print("mergedIntervals = ",mergedIntervals ,flush=True)
            
            
            for interval in mergedIntervals:
                # print("_inter = ",interval,flush=True)
                
                start_status, end_status = inNotin(interval, local_hr_start, local_hr_end)
                # print(f"__start_s = {start_status} end_s = {end_status}",flush=True)
                if start_status==False and end_status==False:
                    up, down = getUpDownTime(local_hr_start, local_hr_end, storePoll[1])
                    # print(f"___up : {up} down : {down}",flush=True)
                    uptime_lastday_curStore+=up
                    downtime_lastday_curStore+=down
                elif start_status==True and end_status==True:
                    up, down = getUpDownTime(local_hr_start, local_hr_end, storePoll[1])
                    # print(f"___up : {up} down : {down}",flush=True)
                    uptime_lastday_curStore+=up
                    downtime_lastday_curStore+=down
                    break
                elif start_status==True and end_status==False:
                    up, down = getUpDownTime(local_hr_start, sendTIMEGetDATETIME(interval.get('p3')), storePoll[1])
                    # print(f"___up : {up} down : {down}",flush=True)
                    uptime_lastday_curStore+=up
                    downtime_lastday_curStore+=down                                            # Don't break in this case
                elif start_status==False and end_status==True:
                    up, down = getUpDownTime(sendTIMEGetDATETIME(interval.get('p2')), local_hr_end, storePoll[1])
                    # print(f"___up : {up} down : {down}",flush=True)
                    uptime_lastday_curStore+=up
                    downtime_lastday_curStore+=down
                    break
            
            
            
            print("day storeid: ",storePoll[0]," up = ", '{:.2f}'.format(uptime_lastday_curStore/60)," down = ", '{:.2f}'.format(downtime_lastday_curStore/60),flush=True)
            insert_query = text('INSERT INTO reports \
                ( \
                report_id, \
                store_id, \
                uptime_last_day, \
                downtime_last_day \
                ) \
                VALUES \
                ( \
                :report_id, \
                :store_id, \
                :uptime_last_day, \
                :downtime_last_day \
                ) \
                ON CONFLICT (report_id, store_id) \
                DO UPDATE SET \
                uptime_last_day = EXCLUDED.uptime_last_day, \
                downtime_last_day = EXCLUDED.downtime_last_day \
                ')
                
            params = {
                "report_id": report_id,
                "store_id": storePoll[0],
                "uptime_last_day": '{:.2f}'.format(uptime_lastday_curStore/60),
                "downtime_last_day": '{:.2f}'.format(downtime_lastday_curStore/60)
            }
            
            db.execute(insert_query, params)
            db.commit()
            # break
    except exc.SQLAlchemyError as e:
        return "ERROR"
    finally:
        db.close()