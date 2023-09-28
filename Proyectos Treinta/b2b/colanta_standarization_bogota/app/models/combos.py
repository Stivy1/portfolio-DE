from typing import OrderedDict
from pydantic import BaseModel, confloat, constr, conint

# int values are filled in with zero values at the beginning and str with
# spaces at the end. Order is important because is used for building the output string
FIELDS = OrderedDict({
    # "F_NUMERO_REG":  {"type": int, "output_length": 7, "default": None},
    "F_TIPO_REG": {"type": int, "output_length": 4, "default": 431},
    "F_SUBTIPO_REG": {"type": int, "output_length": 2, "default": 0},
    "F_VERSION_REG":  {"type": int, "output_length": 2, "default": 3},
    "F_CIA": {"type": int, "output_length": 3, "default": 1},
    "F_431_ID_CO": {"type": str, "output_length": 3, "default": "416"},
    "F_431_ID_TIPO_DOCTO": {"type": str, "output_length": 3, "default": "PTR"},
    "F_431_ID_CONSEC_DOCTO": {"type": int, "output_length": 8, "default": None},
    "F_431_ID_ITEM": {"type": int, "output_length": 7, "default": 47219},
    "F_431_REFERENCIA_ITEM": {"type": str, "output_length": 50, "default": ""}, # Empty string
    "F_431_CODIGO_BARRAS": {"type": str, "output_length": 20, "default": ""},
    "F_431_CODIGO_PAQUETE": {"type": str, "output_length": 20, "default": None},
    "F_431_ID_BODEGA": {"type": str, "output_length": 5, "default": "41600"},
    "F_431_ID_CONCEPTO": {"type": int, "output_length": 3, "default": 501},
    "F_431_ID_MOTIVO": {"type": str, "output_length": 2, "default": "01"},
    "F_431_ID_CO_MOVTO": {"type": str, "output_length": 3, "default": "416"},
    "F_431_ID_UN_MOVTO": {"type": str, "output_length": 20, "default": "99"},
    "F_431_ID_CCOSTO_MOVTO": {"type": str, "output_length": 15, "default": ""},
    "F_431_ID_PROYECTO": {"type": str, "output_length": 15, "default": ""},
    "F_431_FECHA_ENTREGA": {"type": int, "output_length": 8, "default": None},
    "F_431_NUM_DIAS_ENTREGA": {"type": int, "output_length": 3, "default": None }, #diferencia docto y entrega
    "F_431_ID_LISTA_PRECIO": {"type": str, "output_length": 3, "default": "002"},
    "F_431_CANT_PAQUETE": {"type": float, "output_length": 20, "default": None},
    "F_431_NOTAS": {"type": str, "output_length": 255, "default": ""},
    "F_431_DETALLE": {"type": str, "output_length": 2000, "default": ""},
    "F_431_IND_BACKORDER": {"type": int, "output_length": 1, "default": 1}
    })
NUMBER_PRECISION = 4


class TropiCombos(BaseModel):
    # F_NUMERO_REG: conint(
    #    gt=0, lt=pow(10, FIELDS["F_NUMERO_REG"]['output_length']))
    F_TIPO_REG: conint(
        gt=0, lt=pow(10, FIELDS["F_TIPO_REG"]['output_length'])) = FIELDS["F_TIPO_REG"]['default']
    F_SUBTIPO_REG: conint(
        gt=0, lt=pow(10, FIELDS["F_SUBTIPO_REG"]['output_length'])) = FIELDS["F_SUBTIPO_REG"]["default"]                    
    F_VERSION_REG: conint(
        gt=0, lt=pow(10, FIELDS["F_VERSION_REG"]["output_length"])) = FIELDS["F_VERSION_REG"]["default"]
    F_CIA: conint(
        gt=0, lt=pow(10, FIELDS["F_CIA"]['output_length'])) = FIELDS["F_CIA"]["default"]
    F_431_ID_CO: constr(
        max_length=FIELDS["F_431_ID_CO"]['output_length']) = FIELDS["F_431_ID_CO"]["default"]
    F_431_ID_TIPO_DOCTO: constr(
        max_length=FIELDS["F_431_ID_TIPO_DOCTO"]['output_length']) = FIELDS["F_431_ID_TIPO_DOCTO"]["default"]  
    F_431_ID_CONSEC_DOCTO: conint(
        gt = 0, lt=pow(10, FIELDS["F_431_ID_CONSEC_DOCTO"]['output_length'])) #variable
    F_431_ID_ITEM: conint(
        gt=0, lt=pow(10, FIELDS["F_431_ID_ITEM"]["output_length"])) = FIELDS["F_431_ID_ITEM"]["default"]
    F_431_REFERENCIA_ITEM: constr(
        max_length=FIELDS["F_431_REFERENCIA_ITEM"]['output_length']) = FIELDS["F_431_REFERENCIA_ITEM"]["default"]
    F_431_CODIGO_BARRAS: constr(
        max_length=FIELDS["F_431_CODIGO_BARRAS"]['output_length']) = FIELDS["F_431_CODIGO_BARRAS"]["default"]
    F_431_CODIGO_PAQUETE: constr(
        max_length=FIELDS["F_431_CODIGO_PAQUETE"]['output_length']) # Variable ID item
    F_431_ID_BODEGA: constr(
        max_length=FIELDS["F_431_ID_BODEGA"]['output_length']) = FIELDS["F_431_ID_BODEGA"]["default"]
    F_431_ID_CONCEPTO: conint(
        gt=0, lt=pow(10, FIELDS["F_431_ID_CONCEPTO"]['output_length'])) = FIELDS["F_431_ID_CONCEPTO"]["default"]
    F_431_ID_MOTIVO: constr(
        max_length=FIELDS["F_431_ID_MOTIVO"]['output_length']) = FIELDS["F_431_ID_MOTIVO"]["default"]
    F_431_ID_CO_MOVTO: constr(
        max_length=FIELDS["F_431_ID_CO_MOVTO"]['output_length']) = FIELDS["F_431_ID_CO_MOVTO"]["default"]
    F_431_ID_UN_MOVTO: constr(
        max_length=FIELDS["F_431_ID_UN_MOVTO"]['output_length']) = FIELDS["F_431_ID_UN_MOVTO"]["default"]
    F_431_ID_CCOSTO_MOVTO: constr(
        max_length=FIELDS["F_431_ID_CCOSTO_MOVTO"]['output_length']) = FIELDS["F_431_ID_CCOSTO_MOVTO"]["default"]
    F_431_ID_PROYECTO: constr(
        max_length=FIELDS["F_431_ID_PROYECTO"]['output_length']) = FIELDS["F_431_ID_PROYECTO"]["default"]
    F_431_FECHA_ENTREGA: conint(
        gt =0, lt=pow(10, FIELDS["F_431_FECHA_ENTREGA"]['output_length'])) 
    F_431_NUM_DIAS_ENTREGA: conint(
        gt =0, lt=pow(10, FIELDS["F_431_NUM_DIAS_ENTREGA"]['output_length'])) 
    F_431_ID_LISTA_PRECIO: constr(
        max_length=FIELDS["F_431_ID_LISTA_PRECIO"]['output_length']) = FIELDS["F_431_ID_LISTA_PRECIO"]["default"]
    F_431_CANT_PAQUETE: confloat(
        gt=0, lt=pow(10, FIELDS["F_431_CANT_PAQUETE"]['output_length'] - NUMBER_PRECISION - 1))
    F_431_NOTAS: constr(
        max_length=FIELDS["F_431_NOTAS"]['output_length']) = FIELDS["F_431_NOTAS"]["default"]
    F_431_DETALLE: constr(
        max_length=FIELDS["F_431_DETALLE"]['output_length']) = FIELDS["F_431_DETALLE"]["default"]
    F_431_IND_BACKORDER: conint(
        gt=0, lt=pow(10, FIELDS["F_431_IND_BACKORDER"]['output_length'])) = FIELDS["F_431_IND_BACKORDER"]["default"]
   
    def __str__(self):
        data = self.__dict__.copy()

        output_fields = []
        for fname, fval in FIELDS.items():
            dato = data[fname]
            output_length = fval['output_length']

            if fval['type'] == int:
                output_fields.append(str(dato).zfill(output_length))
            elif fval['type'] == str:
                output_fields.append(str(dato).ljust(output_length))
            elif fval['type'] == float:
                cutted_number: str = f'{dato:0{output_length}.4f}'
                output_fields.append(cutted_number)

        product_string = "".join(output_fields)
        return product_string
