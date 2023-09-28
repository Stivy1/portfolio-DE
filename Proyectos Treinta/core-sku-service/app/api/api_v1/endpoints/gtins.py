from fastapi import APIRouter, FastAPI, HTTPException, UploadFile, File, Body, Depends, Request
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from fastapi_csrf_protect import CsrfProtect
from schemas.gtins import UpdateProduct, CreateProduct, CreateProducts
from crud.crud_gtins import crud_gtins
from models.gtins import CreateOut, UpdateOut, DeleteOut, ProductOut, ProductsOut
from models.csrf import CsrfSettings


app = APIRouter()


@CsrfProtect.load_config
def get_csrf_config():
    return CsrfSettings()


@app.get("/csrftoken")
async def get_csrf_token(csrf_protect: CsrfProtect = Depends()):
    csrf_token = csrf_protect.generate_csrf()
    response = {
        "csrf_token": csrf_token
    }
    return response


@app.post("/create_products_by_file", response_model=CreateOut, response_class=JSONResponse)
async def create_commons_sku_by_file(request: Request, file: UploadFile = File(...), csrf_protect: CsrfProtect = Depends()):
    try:
        csrf_token = csrf_protect.get_csrf_from_headers(request.headers)
        csrf_protect.validate_csrf(csrf_token)
    except:
        raise HTTPException (
            status_code=400,
            detail="Error validación CSRF"
        )
    if file.content_type != "text/csv":
        raise HTTPException (
            status_code=400,
            detail="El formato del archivo debe ser csv"
        )
    response_sku = await crud_gtins.get_sku_by_file(file)
    if response_sku["status"] == 200:
        sku = response_sku["data"]
    else:
        raise HTTPException(
            status_code=400,
            detail=response_sku["message"]
        )

    response_token = await crud_gtins.get_token()
    if response_token["status"] == 200:
        token = response_token["data"]
    else:
        raise HTTPException(
            status_code=400,
            detail=response_token["message"]
        )

    response_commons_sku = await crud_gtins.get_commons_sku(token, sku)
    if response_commons_sku["status"] == 200:
        commons_sku = response_commons_sku["data"]
    else:
        raise HTTPException(
            status_code=400,
            detail=response_commons_sku["message"]
        )

    response_rename = await crud_gtins.rename_fields(commons_sku)
    if response_rename["status"] == 200:
        product_mapping_df = response_rename["data"]["df"]
        product_mapping = response_rename["data"]["object"]
    else:
        raise HTTPException(
            status_code=400,
            detail=response_rename["message"]
        )

    response_mongo = await crud_gtins.save_mongo(product_mapping)
    if response_mongo["status"] == 200:
        if "data" in response_mongo:
            data = crud_gtins.json_to_df(response_mongo["data"])
            response_save_bq = await crud_gtins.save_bq(data)
            if response_save_bq["status"] != 200:
                raise HTTPException(
                    status_code=400,
                    detail=response_save_bq["message"]
                )
        else:
            return {
                "status": 200,
                "message": "Los productos ya existen en la base de datos"
            }
    else:
        raise HTTPException(
            status_code=400,
            detail=response_mongo["message"]
        )
    return {
        "status": 200,
        "message": "Productos creados correctamente"
    }


@app.post("/create_products", response_model=CreateOut, response_class=JSONResponse)
async def create_products_by_body(request: Request, products: CreateProducts = Body(...), csrf_protect: CsrfProtect = Depends()):
    try:
        csrf_token = csrf_protect.get_csrf_from_headers(request.headers)
        csrf_protect.validate_csrf(csrf_token)
    except:
        raise HTTPException (
            status_code=400,
            detail="Error validación CSRF"
        )
    response = await crud_gtins.create_products(products)
    if response["status"] ==400:
        raise HTTPException (
            status_code=400,
            detail=response["message"]
        )
    return response
    

@app.put("/product/{sku}", response_model=UpdateOut)
async def update_product(request: Request, sku: int, req: UpdateProduct = Body(...), csrf_protect: CsrfProtect = Depends()):
    try:
        csrf_token = csrf_protect.get_csrf_from_headers(request.headers)
        csrf_protect.validate_csrf(csrf_token)
    except:
        raise HTTPException (
            status_code=400,
            detail="Error validación CSRF"
        )
    req = {k: v for k, v in req.dict().items() if v is not None}
    response_update_mongo = await crud_gtins.update_mongo(sku, req)
    if response_update_mongo["status"] == 400:
        raise HTTPException (
            status_code=400,
            detail=response_update_mongo["message"]
        )
    response_update_bq = crud_gtins.update_bq(sku, req)
    if response_update_mongo["status"] == 200 and response_update_bq["status"] == 200:
        return {
            "status": 200,
            "message": "¡Producto actualizado correctamente!"
        }
    else:
        raise HTTPException(
            status_code=400,
            detail=response_update_mongo["message"]
        )


@app.delete("/product/{sku}", response_model=DeleteOut)
async def delete_product(sku: int):
    response_delete_mongo = await crud_gtins.delete_mongo(sku)
    response_delete_bq = crud_gtins.delete_bq(sku)
    if response_delete_mongo["status"] == 200 and response_delete_bq["status"] == 200:
        return {
            "status": 200,
            "message": "¡Producto eliminado correctamente!"
        }
    else:
        print(response_delete_bq)
        raise HTTPException(
            status_code=400,
            detail=response_delete_bq["message"]
        )


@app.get("/products", response_model=ProductsOut)
async def get_all_products(page, size):
    response = await crud_gtins.list_products(page, size)
    if response["status"] == 400:
        raise HTTPException(
            status_code=400,
            detail=response["message"]
        )
    return response


@app.get("/product/{sku}")
async def get_product(sku: int):
    response = await crud_gtins.get_product_by_sku(sku)
    if response["status"] == 400:
        response_token = await crud_gtins.get_token()
        if response_token["status"] == 200:
            token = response_token["data"]
        else:
            raise HTTPException(
                status_code=400,
                detail=response_token["message"]
            )
        skus = [str(sku)]
        response_commons_sku = await crud_gtins.get_commons_sku(token, skus)
        if response_commons_sku["status"] == 200:
            commons_sku = response_commons_sku["data"]
            response_rename = await crud_gtins.rename_fields(commons_sku)
            if response_rename["status"] == 200:
                product_mapping_df = response_rename["data"]["df"]
                product_mapping = response_rename["data"]["object"]
            else:
                raise HTTPException(
                    status_code=400,
                    detail=response_rename["message"]
                )

            response_mongo = await crud_gtins.save_mongo(product_mapping)
            if response_mongo["status"] == 200:
                
                data = crud_gtins.json_to_df(response_mongo["data"])
                response_save_bq = await crud_gtins.save_bq(data)
                if response_save_bq["status"] != 200:
                    raise HTTPException(
                        status_code=400,
                        detail=response_save_bq["message"]
                    )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=response_mongo["message"]
                )
            return {
                "status": 200,
                "message": "El producto se encontró en el API de LOGYCA y se guardó en la base de datos de Treinta",
            }

        else:
            raise HTTPException(
                status_code=400,
                detail=response_commons_sku["message"]
            )
    return response


add_pagination(app)
