from datetime import datetime
from pydantic import BaseModel, Field
from bson import ObjectId
from typing import Optional, List

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Objectid inválido")
        return ObjectId(v)
    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")

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
                "update_date": "2022-06-03T18:13:22.829068"
            }
        }

class CreateProduct(Product):

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

class CreateProducts(List[Product]):

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example":[{
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
            },
            {
                "sku": 7791290797778,
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
            }]
        }

class UpdateProduct(BaseModel):
    category: Optional[str]
    brand: Optional[str]
    name: Optional[str] 
    description: Optional[str]
    unit_name: Optional[str]
    unit_quatity: Optional[float]
    sale_country: Optional[str]
    language: Optional[str]
    is_cached: Optional[bool]
    update_date: Optional[datetime]

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
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