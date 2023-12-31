from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.api_v1.api import api_router

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(api_router)
