import pytz
from datetime import datetime, timedelta, timezone
from DB_Schema.DB_Connect import session, exc, text


# Piece-wise constant interpolation method (take previous poll status)

# a ,b ,c ,d    (polls)
# + ,- ,+ ,+    (active +, inactive -)

# a -> b +
# b -> c -
# c -> d +

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
    
    return uptime, downtime

# Takes two times and returns difference in minutes
def timeDiffMinutes(time1_obj, time2_obj):
    diff = time2_obj - time1_obj
    diff_minutes = diff.total_seconds() / 60.0
    return diff_minutes

# Handles different datetime formats
def parse(time_str):
    res = time_str.split('.')
    if len(res)==2:
        lres=len(res[1])
        if lres<6:
            for i in range(6-lres):
                res[1]+='0'
        time_str=res[0]+'.'+res[1]
            
    datetime_obj = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    return datetime_obj

# Get all data at once - storeid, timezone, polls-aggregate-object [Grouped by storeid]
def getPollsWeek(db):
    try:
        query = text("SELECT t.store_id, \
                coalesce(store_timezone.timezone_str,'America/Chicago'), \
                jsonb_build_object( \
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
                    ) FILTER (WHERE DATE(p2) = '2023-01-24'), \
                    '2023-01-25', jsonb_agg( \
                        json_build_object('p1', p1, 'p2', p2) ORDER BY p2 \
                    ) FILTER (WHERE DATE(p2) = '2023-01-25') \
                ) as status_timestamps \
            FROM ( \
                SELECT ss.store_id, \
                    status AS p1, \
                    timestamp_utc AS p2 \
                FROM store_status ss \
                WHERE timestamp_utc >= '2023-01-18 00:00:00' AND timestamp_utc <= '2023-01-25 23:59:59' \
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
      
# Get all data at once - storeid, aggregate-Menu_hours-object
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

# Merge overlapping intervals
def listOflistsMergeIntervals(intervals):
    # Convert time strings to datetime objects for easier comparison
    for interval in intervals:
        if type(interval[0]) == str:
            interval[0] = datetime.strptime(interval[0], "%H:%M:%S")
        if type(interval[1]) == str:
            interval[1] = datetime.strptime(interval[1], "%H:%M:%S")

    # Sort the intervals by the start time [Already sorted from database]
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

# Type casting utilities
def convertToLocal_withFloat(time_string, zone):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=pytz.utc).astimezone(pytz.timezone(zone)).strftime('%Y-%m-%d %H:%M:%S')

def convertToLocal(time_string, zone):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc).astimezone(pytz.timezone(zone)).strftime('%Y-%m-%d %H:%M:%S')

def convertToLocal_T(time_string, zone):
    meta = time_string.split('.')
    if len(meta)<=1:
        return datetime.strptime(time_string, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.utc).astimezone(pytz.timezone(zone)).strftime('%Y-%m-%d %H:%M:%S')
    return datetime.strptime(time_string, '%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=pytz.utc).astimezone(pytz.timezone(zone)).strftime('%Y-%m-%d %H:%M:%S')
