from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse, Response
import pytz
from datetime import datetime, timedelta, timezone

from DB_Schema.DB_Connect import session, exc, text

from routes.lastHourReport import lastHourReport
from routes.lastDayReport import lastdayReport
from routes.lastWeekReport import lastWeekReport
from routes.lastWeekReportNew import lastWeekReportNew

import os
import random
import string

triggerReportRouter = APIRouter()

# def compute_values(reportID):
#     lastHourReport(reportID)
#     lastdayReport(reportID)
#     lastWeekReport(reportID)
#     pass

@triggerReportRouter.get("/trigger_report", tags=['Report'])
async def get_values(background_tasks: BackgroundTasks):
    # reportID = generateReportId()
    # background_tasks.add_task(compute_values , reportID)
    # background_tasks.add_task(lastHourReport , '8JUk')
    # background_tasks.add_task(lastdayReport , '8JUk')
    background_tasks.add_task(lastWeekReportNew , '8JUk')        # In finally block on WeekReport generate combined csv
    return {"report_id": '8JUk'}

@triggerReportRouter.get("/get_report", tags=['Report'])
async def get_status():
    if os.path.exists(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\report\report.csv'):
        with open(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\report\report.csv', "rb") as csv_file:
            contents = csv_file.read()
        return Response(contents, media_type="text/csv")
    else:
        return {"status": "running"}
    
# generate random report_id
# def generateReportId():
#     alphanumeric = string.ascii_letters + string.digits
#     return ''.join(random.choice(alphanumeric) for i in range(4))