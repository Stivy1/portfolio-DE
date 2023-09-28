from fastapi import APIRouter

from api.api_v1.endpoints import gtins, datadog


api_router = APIRouter()

api_router.include_router(
    datadog.app,
    prefix="/datadog",
    tags=["health"]
)

api_router.include_router(
    gtins.app,
    prefix="/gtins",
    tags=["products"])