from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes.triggerReport import triggerReportRouter

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/',tags=['Check'])
def root_route():
    return {'Success':'UP & RUNNING'}

@app.get('/check',tags=['Check'])
def check_route():
    return {'Success':'OK'}

app.include_router(triggerReportRouter)
