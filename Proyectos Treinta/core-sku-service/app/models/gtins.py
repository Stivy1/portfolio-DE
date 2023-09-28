from pydantic import BaseModel, Field
from bson import ObjectId
from typing import Optional, List
from datetime import datetime

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid Objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")

class BaseProduct(BaseModel):
    sku: int
    category: str
    brand: str
    name: str 
    description: str
    unit_name: Optional[str]
    unit_quatity: Optional[float]
    sale_country: Optional[str]
    language: Optional[str]
    is_cached: bool
    update_date: datetime
    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "sku": 7791290790728,
                "category": "Detergentes para el lavado de ropa",
                "brand": "ALA",
                "name": "ALA JABON EN POLVO LAV MANO CORE MAX 24X400G 400,00 GRM",
                "description": "ALA JABON EN POLVO LAV MANO CORE MAX 24X400G 400,00 GRM",
                "unit_name": "Gramos",
                "unit_quantity": 400.00,
                "sale_country": "AR",
                "language": "es-AR",
                "is_cached": False,
                "update_date": "2022-06-03T18:13:22.829068"
            }
        }

class Product(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    sku: int
    category: str
    brand: str
    name: str 
    description: str
    unit_name: Optional[str]
    unit_quatity: Optional[float]
    sale_country: Optional[str]
    language: Optional[str]
    is_cached: bool
    update_date: datetime
    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "sku": 7791290790728,
                "category": "Detergentes para el lavado de ropa",
                "brand": "ALA",
                "name": "ALA JABON EN POLVO LAV MANO CORE MAX 24X400G 400,00 GRM",
                "description": "ALA JABON EN POLVO LAV MANO CORE MAX 24X400G 400,00 GRM",
                "unit_name": "Gramos",
                "unit_quantity": 400.00,
                "sale_country": "AR",
                "language": "es-AR",
                "is_cached": False,
                "update_date": "2022-06-03T18:13:22.829068",
                "_id": "629a95964930d1a130e6ffd2"
            }
        }

class CreateOut(BaseModel):
    status: str
    message: str
    data: list

    class Config:
        schema_extra = {
            "example": {
                "status": 200,
                "message": "¡Productos guardados exitosamente!",
                "data": [
                    {
                        "sku": 7791290790728,
                        "category": "Detergentes para el lavado de ropa",
                        "brand": "ALA",
                        "name": "ALA JABON EN POLVO LAV MANO CORE MAX 24X400G 400,00 GRM",
                        "description": "ALA JABON EN POLVO LAV MANO CORE MAX 24X400G 400,00 GRM",
                        "unit_name": "Gramos",
                        "unit_quantity": 400.00,
                        "sale_country": "AR",
                        "language": "es-AR",
                        "is_cached": False,
                        "update_date": "2022-06-03T18:13:22.829068",
                        "_id": "629a95964930d1a130e6ffd2"
                    }
                ]
            }
        }

class UpdateOut(BaseModel):
    status: str
    message: str
    class Config:
        schema_extra = {
            "example": {
                "status": 200,
                "message": "¡Producto actualizado correctamente!"
            }
        }

class DeleteOut(BaseModel):
    status: str
    message: str
    class Config:
        schema_extra = {
            "example": {
                "status": 200,
                "message": "¡Producto eliminado correctamente!"
            }
        }

class ProductOut(BaseModel):
    status: str
    message: str
    data: dict

    class Config:
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "status": 200,
                "message": "Producto",
                "data": {
                    "sku": 7702090029871,
                    "category": "Jugo de frutas listo para beber (no perecedero)",
                    "brand": "HIT",
                    "name": "NARANJA PIÑA HIT 500 ML PET X12",
                    "description": "NARANJA PIÑA HIT 500 ML PET X12",
                    "is_cached": False,
                    "update_date": "2022-06-03T18:13:22.826000",
                    "unit_name": "Centimetros cubicos",
                    "unit_quantity": 12,
                    "sale_country": "CO",
                    "language": "es-CO"
                }
            }
        }

class ProductsOut(BaseModel):
    status: str
    data: List[Product]

    class Config:
        schema_extra = {
            "example": {
                "status": 200,
                "data": [
                    {
                        "sku": 7702090029871,
                        "category": "Jugo de frutas listo para beber (no perecedero)",
                        "brand": "HIT",
                        "name": "NARANJA PIÑA HIT 500 ML PET X12",
                        "description": "NARANJA PIÑA HIT 500 ML PET X12",
                        "is_cached": False,
                        "update_date": "2022-06-03T18:13:22.826000",
                        "unit_name": "Centimetros cubicos",
                        "unit_quantity": 12,
                        "sale_country": "CO",
                        "language": "es-CO"
                    },
                    {
                        "sku": 7702090016390,
                        "category": "Bebidas saborizadas listas para beber",
                        "brand": "PEPSI",
                        "name": "PEPSICOLA 2.500 ML PET X8",
                        "description": "PEPSICOLA 2.500 ML PET X8",
                        "is_cached": False,
                        "update_date": "2022-06-03T18:13:22.826000",
                        "unit_name": "Centimetros cubicos",
                        "unit_quantity": 8,
                        "sale_country": "CO",
                        "language": "es-CO"
                    },
                    {
                        "sku": 7702090051995,
                        "category": "Bebidas espirituosas",
                        "brand": "HEINEKEN",
                        "name": "Cerveza HEINEKEN champions x250MLT",
                        "description": "Cerveza HEINEKEN champions x250MLT",
                        "is_cached": False,
                        "update_date": "2022-06-03T18:13:22.826000",
                        "unit_name": "Centimetros cubicos",
                        "unit_quantity": 250,
                        "sale_country": "CO",
                        "language": "es-CO"
                    }
                ]
            }
        }