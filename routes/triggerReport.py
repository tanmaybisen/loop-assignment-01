import os
from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import Response, StreamingResponse

from routes.lastWeekReportNew import lastWeekReportNew

triggerReportRouter = APIRouter()

@triggerReportRouter.get("/trigger_report", tags=['Report'])
async def get_values(background_tasks: BackgroundTasks):
    background_tasks.add_task(lastWeekReportNew , '8JUk')
    return {"report_id": '8JUk'}

@triggerReportRouter.get("/get_report", tags=['Report'])
async def get_status():
    if os.path.exists(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\report\report.csv'):
        with open(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\report\report.csv', "rb") as csv_file:
            contents = csv_file.read()
        return Response(contents, media_type="text/csv")
    else:
        return {"status": "Running"}