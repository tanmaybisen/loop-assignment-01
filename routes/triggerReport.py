import os
from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import Response
from routes.reportGenerator import generateReport

triggerReportRouter = APIRouter()

report_id = 'report'

# Trigger report generation - return ReportID (Random for now)
@triggerReportRouter.get("/trigger_report", tags=['Report'])
async def get_values(background_tasks: BackgroundTasks):
    global report_id
    background_tasks.add_task(generateReport , report_id)
    return {"report_id": report_id}

# Get report status as Running if process not completed and return CSV file is completed
@triggerReportRouter.get("/get_report/{id}", tags=['Report'])
async def get_status(id:str):
    global report_id
    if os.path.exists(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\report\\'+id+'.csv'):
        with open(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\report\\'+id+'.csv', "rb") as csv_file:
            contents = csv_file.read()
        return Response(contents, media_type="text/csv")
    else:
        return {"status": "Running"}