from fastapi import APIRouter

app = APIRouter()


@app.get("/health")
async def health():
    return {
        "status": 200,
        "message": "API sku activa"
    }