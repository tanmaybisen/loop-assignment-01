import pytz
from datetime import datetime, timedelta, timezone
from DB_Schema.DB_Connect import session, exc, text

def convertToLocal_withFloat(time_string, zone):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=pytz.utc).astimezone(pytz.timezone(zone)).strftime('%Y-%m-%d %H:%M:%S')

def convertToLocal(time_string, zone):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc).astimezone(pytz.timezone(zone)).strftime('%Y-%m-%d %H:%M:%S')

def convertToLocal_T(time_string, zone):
    return datetime.strptime(time_string, '%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=pytz.utc).astimezone(pytz.timezone(zone)).strftime('%Y-%m-%d %H:%M:%S')

def convertToUTC(time, zone):
    dt = datetime.strptime('2023-01-25' + ' ' + time, '%Y-%m-%d %H:%M:%S')
    tz = pytz.timezone(zone)
    dt_localized = tz.localize(dt)
    dt_utc = dt_localized.astimezone(pytz.utc)
    return dt_utc.strftime('%H:%M:%S')
 
# Takes DateTimeStr (str) Returns weekDay (int)
def getDayofWeek(date_string_utc):
    date_utc = datetime.strptime(date_string_utc, '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
    return date_utc.weekday()


# Takes two times and returns difference in minutes
def timeDiffMinutes(time1_obj, time2_obj):
    diff = time2_obj - time1_obj
    diff_minutes = diff.total_seconds() / 60.0
    return diff_minutes


# Takes store_id (int) Returns TimeZone (str)
def getStoreTimeZone(db,store_id):
    try:
        query1 = text("SELECT timezone_str \
            FROM store_timezone \
            WHERE store_id=:store_id")
            
        params1 = {
            "store_id" : store_id
        }
        
        response1 = db.execute(query1, params1).fetchall()      
        db.commit()
        
        if len(response1)==0:
            zone_str = 'America/Chicago'
        else:
            zone_str = response1[0][0]
            
        return zone_str
    except exc.SQLAlchemyError as e:
        return e


# Takes store_id (int) Returns WeeklyBusinessHours (dict (int: List(Dict)))
def getWeeklyBusinessHours(db,store_id):
    
    try:    
        # Gets all 7 days business hours details, All intervals of a day are grouped in list.
        query2 = text("SELECT p1, \
            jsonb_agg(json_build_object('p1', p1, 'p2', p2 ,'p3' ,p3)order by p2) as day_of_week \
            FROM (  \
                SELECT ss.store_id,  \
                dayofweek as p1, \
                start_time_local AS p2,  \
                end_time_local AS p3  \
                FROM menu_hours ss  \
                WHERE store_id = :store_id \
                WINDOW w AS (PARTITION BY ss.dayofweek)  \
            ) AS t  \
            GROUP BY p1  \
            ORDER BY p1;")
        
        params2 = {
            "store_id" : store_id
        }
        
        # [(0,	[{"p1": 0, "p2": "00:00:00", "p3": "00:10:00"}, {"p1": 0, "p2": "11:00:00", "p3": "23:59:00"}]),...]
        response2 = db.execute(query2, params2).fetchall()    # List of Tuples 
        db.commit()
        
        Open24x7={
            0:[{"p1": 0, "p2": "00:00:00", "p3": "23:59:59"}],
            1:[{"p1": 1, "p2": "00:00:00", "p3": "23:59:59"}],
            2:[{"p1": 2, "p2": "00:00:00", "p3": "23:59:59"}],
            3:[{"p1": 3, "p2": "00:00:00", "p3": "23:59:59"}],
            4:[{"p1": 4, "p2": "00:00:00", "p3": "23:59:59"}],
            5:[{"p1": 5, "p2": "00:00:00", "p3": "23:59:59"}],
            6:[{"p1": 6, "p2": "00:00:00", "p3": "23:59:59"}]
        }
        
        
        
        if len(response2)==0:
            return Open24x7
        
        OpenDict = dict()
        for day in response2:
            OpenDict[int(day[0])]=day[1]
        
        
        return OpenDict
    except exc.SQLAlchemyError as e:
        return e


# Takes start, end time and Returns polls-grouped-by-store_id [ (store_id, [{}, {}, ...] ) ]
def getAllStoresPolls(db, start_time, end_time):
    
    try:
        query = text("SELECT t.store_id, \
                jsonb_agg(json_build_object('p1', p1, 'p2', p2) ORDER BY p2) as status_timestamps \
                FROM (  \
                    SELECT ss.store_id,  \
                    status AS p1,  \
                    timestamp_utc AS p2  \
                    FROM store_status ss  \
                    WHERE timestamp_utc >= :start_time and timestamp_utc <= :end_time \
                    WINDOW w AS (PARTITION BY ss.store_id)  \
                ) AS t  \
                GROUP BY t.store_id  \
                ORDER BY t.store_id  \
            ")
        
        params = {
            "start_time" : start_time,
            "end_time" : end_time
        }
        
        response = db.execute(query, params).fetchall()        
        db.commit()

        return response
    except exc.SQLAlchemyError as e:
        return e

# Merge overlapping intervals
def mergeIntervals(intervals):
    
    # Initialize the merged list with the first interval
    merged = [intervals[0]]
    
    for interval in intervals[1:]:
        # Check if the current interval overlaps with the previous interval
        start_time = datetime.strptime(interval["p2"], "%H:%M:%S")
        end_time = datetime.strptime(interval["p3"], "%H:%M:%S")
        prev_end_time = datetime.strptime(merged[-1]["p3"], "%H:%M:%S")
        
        if start_time <= prev_end_time:
            # If the current interval overlaps with the previous one, merge them
            merged[-1]["p3"] = max(end_time, prev_end_time).strftime("%H:%M:%S")
        else:
            # If the current interval does not overlap, add it to the merged list
            merged.append(interval)
    return merged


def parse(time_str):
    # print("REQUEST RECEIVED FOR = ", time_str,flush=True)
    res = time_str.split('.')
    if len(res)==2:
        lres=len(res[1])
        if lres<6:
            for i in range(6-lres):
                res[1]+='0'
        time_str=res[0]+'.'+res[1]
            
    datetime_obj = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    return datetime_obj

# I will get all polls for last hour, Only using polls between (start , end)
# polls is list of dictionaries [(store_id, polls),]
# polls -> [{"p1":active,"p2":time},]

def getUpDownTime(start, end, polls):
     
    status = [None]
    times = [start]
    
    filtered_listOfDicts = []
    for poll in polls:
        parsedTime = parse(poll['p2'])
        if parsedTime>=start and parsedTime<=end:
            filtered_listOfDicts.append((poll['p1'],parsedTime))
    
    for poll in filtered_listOfDicts:
        status.append(poll[0])
        times.append(poll[1])
        
    status.append(None)
    times.append(end)
    
    print("############# GET UP DOWN TIME = local hr start = ",start,flush=True)
    print("############# GET UP DOWN TIME = local hr end = ",end,flush=True)
    
    print("############# GET UP DOWN TIME = status = ",status,flush=True)
    print("############# GET UP DOWN TIME = times = ",times,flush=True)
    
    # Interpolation
    uptime = 0
    downtime = 0
    for i in range(1,len(status)):
        diffInMins=timeDiffMinutes(times[i-1],times[i])
        if status[i-1]=='active':
            uptime+=diffInMins
        elif status[i-1]=='inactive':
            downtime+=diffInMins
        elif status[i-1]==None:
            pass
            # if status[i]=='active':
            #     uptime+=diffInMins
            # elif status[i]=='inactive':
            #     downtime+=diffInMins
            # elif status[i]==None:
            #     pass                        # Ignoring these cases for now.
    
    return uptime, downtime




def getStoresPolls(db, start_time, end_time, store_id):
    
    try:
        query = text("SELECT t.store_id, \
                jsonb_agg(json_build_object('p1', p1, 'p2', p2) ORDER BY p2) as status_timestamps \
                FROM (  \
                    SELECT ss.store_id,  \
                    status AS p1,  \
                    timestamp_utc AS p2  \
                    FROM store_status ss  \
                    WHERE timestamp_utc >= :start_time and timestamp_utc <= :end_time and store_id = :store_id \
                    WINDOW w AS (PARTITION BY ss.store_id)  \
                ) AS t  \
                GROUP BY t.store_id  \
                ORDER BY t.store_id  \
            ")
        
        params = {
            "start_time" : start_time,
            "end_time" : end_time,
            "store_id" : store_id
        }
        
        response = db.execute(query, params).fetchall()        
        db.commit()

        return response
    except exc.SQLAlchemyError as e:
        return e
    
    


def getPollsWeek(db):
    
    try:
        query = text("SELECT t.store_id, \
                coalesce(store_timezone.timezone_str,'America/Chicago'), \
                jsonb_build_object( \
                    '2023-01-18', jsonb_agg( \
                        json_build_object('p1', p1, 'p2', p2) ORDER BY p2 \
                    ) FILTER (WHERE DATE(p2) = '2023-01-18'), \
                    '2023-01-19', jsonb_agg( \
                        json_build_object('p1', p1, 'p2', p2) ORDER BY p2 \
                    ) FILTER (WHERE DATE(p2) = '2023-01-19'), \
                    '2023-01-20', jsonb_agg( \
                        json_build_object('p1', p1, 'p2', p2) ORDER BY p2 \
                    ) FILTER (WHERE DATE(p2) = '2023-01-20'), \
                    '2023-01-21', jsonb_agg( \
                        json_build_object('p1', p1, 'p2', p2) ORDER BY p2 \
                    ) FILTER (WHERE DATE(p2) = '2023-01-21'), \
                    '2023-01-22', jsonb_agg( \
                        json_build_object('p1', p1, 'p2', p2) ORDER BY p2 \
                    ) FILTER (WHERE DATE(p2) = '2023-01-22'), \
                    '2023-01-23', jsonb_agg( \
                        json_build_object('p1', p1, 'p2', p2) ORDER BY p2 \
                    ) FILTER (WHERE DATE(p2) = '2023-01-23'), \
                    '2023-01-24', jsonb_agg( \
                        json_build_object('p1', p1, 'p2', p2) ORDER BY p2 \
                    ) FILTER (WHERE DATE(p2) = '2023-01-24') \
                ) as status_timestamps \
            FROM ( \
                SELECT ss.store_id, \
                    status AS p1, \
                    timestamp_utc AS p2 \
                FROM store_status ss \
                WHERE timestamp_utc >= '2023-01-18 00:00:00' AND timestamp_utc <= '2023-01-24 23:59:59' \
                and (store_id in (1481966498820158979)) \
                WINDOW w AS (PARTITION BY ss.store_id) \
            ) AS t \
            JOIN store_timezone ON t.store_id = store_timezone.store_id \
            GROUP BY t.store_id, store_timezone.timezone_str \
            ORDER BY t.store_id")
        
        response = db.execute(query).fetchall()        
        db.commit()

        return response
    except exc.SQLAlchemyError as e:
        return e
    
    
def getIntervalsWeekNew(db):
    
    try:
        query = text("SELECT store_id, \
            json_object_agg(CAST(dayofweek AS text), agg_array ORDER BY dayofweek) AS agg_object \
            FROM ( \
                SELECT store_id, dayofweek, ARRAY_AGG(ARRAY[start_time_local, end_time_local] ORDER BY start_time_local) AS agg_array \
                FROM menu_hours \
                GROUP BY store_id, dayofweek \
            ) subquery \
            GROUP BY store_id \
            ORDER BY store_id")
        
        response = db.execute(query).fetchall()        
        db.commit()

        return response
    except exc.SQLAlchemyError as e:
        return e
    
def listOflistsMergeIntervals(intervals):

    # Convert time strings to datetime objects for easier comparison
    for interval in intervals:
        interval[0] = datetime.strptime(interval[0], "%H:%M:%S")
        interval[1] = datetime.strptime(interval[1], "%H:%M:%S")

    # Sort the intervals by the start time
    # intervals.sort(key=lambda interval: interval[0])

    merged_intervals = []
    for interval in intervals:
        if not merged_intervals or merged_intervals[-1][1] < interval[0]:
            # If the current interval doesn't overlap with the previous one, add it to the list
            merged_intervals.append(interval)
        else:
            # If the current interval overlaps with the previous one, merge them
            merged_intervals[-1][1] = max(merged_intervals[-1][1], interval[1])

    # Convert datetime objects back to time strings
    for interval in merged_intervals:
        interval[0] = interval[0].strftime("%H:%M:%S")
        interval[1] = interval[1].strftime("%H:%M:%S")

    return merged_intervals
