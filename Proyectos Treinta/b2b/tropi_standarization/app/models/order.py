# from calendar import c
# from operator import gt
from typing import OrderedDict, List
from pydantic import BaseModel, confloat, constr, conint
from models.product import TropiProduct
from models.combos import TropiCombos

# int values are filled in with zero values at the beginning and str with
# spaces at the end. Order is important because is used for building the output string
FIELDS = OrderedDict({
    # "F_NUMERO_REG":  {"type": int, "output_length": 7, "default": None},
    "F_TIPO_REG": {"type": int, "output_length": 4, "default": 430},
    "F_SUBTIPO_REG": {"type": int, "output_length": 2, "default": 0},
    "F_VERSION_REG":  {"type": int, "output_length": 2, "default": 2},
    "F_CIA": {"type": int, "output_length": 3, "default": 46},
    "F_LIQUIDA_IMPUESTO": {"type": int, "output_length": 1, "default": 1},
    "F_CONSEC_AUTO_REG": {"type": int, "output_length": 1, "default": 0},
    "F_IND_CONTACTO": {"type": int, "output_length": 1, "default": 1},
    "F_430_ID_CO": {"type": str, "output_length": 3, "default": "802"},
    "F_430_ID_TIPO_DOCTO": {"type": str, "output_length": 3, "default": "PTR"},
    "F_430_CONSEC_DOCTO": {"type": int, "output_length": 8, "default": None},
    "F_430_ID_FECHA": {"type": int, "output_length": 8, "default": None},
    "F_430_ID_CLASE_DOCTO": {"type": int, "output_length": 3, "default": 502},
    "F_430_IND_ESTADO": {"type": int, "output_length": 1, "default": 2},
    "F_430_IND_BACKORDER": {"type": int, "output_length": 1, "default": 1},
    "F_430_ID_TERCERO_FACT": {"type": str, "output_length": 15, "default": None},
    "F_430_ID_SUCURSAL_FACT": {"type": str, "output_length": 3, "default": None},
    "F_430_ID_TERCERO_REM": {"type": str, "output_length": 15, "default": None},
    "F_430_ID_SUCURSAL_REM": {"type": str, "output_length": 3, "default": None},
    "F_430_ID_TIPO_CLI_FACT": {"type": str, "output_length": 4, "default": ""},
    "F_430_ID_CO_FACT": {"type": str, "output_length": 3, "default": "802"},
    "F_430_FECHA_ENTREGA": {"type": int, "output_length": 8, "default": None},
    "F_430_NUM_DIAS_ENTREGA": {"type": int, "output_length": 3, "default": 0},
    "F_430_NUM_DOCTO_REFERENCIA": {"type": str, "output_length": 15, "default": ""},
    "F_430_REFERENCIA": {"type": str, "output_length": 10, "default": None},
    "F_430_ID_CARGUE": {"type": str, "output_length": 10, "default": "TEMPORAL"},
    "F_430_ID_MONEDA_DOCTO": {"type": str, "output_length": 3, "default": "COP"},
    "F_430_ID_MONEDA_CONV": {"type": int, "output_length": 3, "default": "COP"},
    "F_430_TASA_CONV": {"type": float, "output_length": 13, "default": 1},
    "F_430_ID_MONEDA_LOCAL": {"type": str, "output_length": 3, "default": "COP"},
    "F_430_TASA_LOCAL": {"type": float, "output_length": 13, "default": 1},
    "F_430_ID_COND_PAGO": {"type": str, "output_length": 3, "default": "01E"},
    "F_430_IND_IMPRESION": {"type": int, "output_length": 1, "default": 0},
    "F_430_NOTAS": {"type": str, "output_length": 2000, "default": None},
    "F_430_ID_CLI_CONTADO": {"type": str, "output_length": 15, "default": ""},
    "F_430_ID_PUNTO_ENVIO": {"type": int, "output_length": 3, "default": 0},
    "F_430_ID_TERCERO_VENDEDOR": {"type": str, "output_length": 15, "default": None}, #vendedor
    "F_419_CONTACTO": {"type": str, "output_length": 50, "default": ""},
    "F_419_DIRECCION1": {"type": str, "output_length": 40, "default": ""},
    "F_419_DIRECCION2": {"type": str, "output_length": 40, "default": ""},
    "F_419_DIRECCION3": {"type": str, "output_length": 40, "default": ""},
    "F_419_ID_PAIS": {"type": str, "output_length": 3, "default": ""},
    "F_419_ID_DEPTO": {"type": str, "output_length": 2, "default": ""},
    "F_419_ID_CIUDAD": {"type": str, "output_length": 3, "default": ""},
    "F_419_ID_BARRIO": {"type": str, "output_length": 40, "default": ""},
    "F_419_TELEFONO": {"type": str, "output_length": 20, "default": ""},
    "F_419_FAX": {"type": str, "output_length": 20, "default": ""},
    "F_419_COD_POSTAL": {"type": str, "output_length": 10, "default": ""},
    "F_419_EMAIL": {"type": str, "output_length": 50, "default": ""},
    "F_419_IND_DESCUENTO": {"type": int, "output_length": 1, "default": 2}
    })
NUMBER_PRECISION = 4


class TropiOrder(BaseModel):
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
    F_LIQUIDA_IMPUESTO: conint(
        gt=0, lt=pow(10, FIELDS["F_LIQUIDA_IMPUESTO"]['output_length'])) = FIELDS["F_LIQUIDA_IMPUESTO"]['default']
    F_CONSEC_AUTO_REG: conint(
        gt=0, lt=pow(10, FIELDS["F_CONSEC_AUTO_REG"]['output_length'])) = FIELDS["F_CONSEC_AUTO_REG"]['default']
    F_IND_CONTACTO: conint(
        gt=0, lt=pow(10, FIELDS["F_IND_CONTACTO"]['output_length'])) = FIELDS["F_IND_CONTACTO"]['default']
    F_430_ID_CO: constr(
        max_length=FIELDS["F_430_ID_CO"]['output_length']) = FIELDS["F_430_ID_CO"]["default"]
    F_430_ID_TIPO_DOCTO: constr(
        max_length=FIELDS["F_430_ID_TIPO_DOCTO"]['output_length']) = FIELDS["F_430_ID_TIPO_DOCTO"]["default"]
    F_430_CONSEC_DOCTO: conint(
        gt=0, lt=pow(10, FIELDS["F_430_CONSEC_DOCTO"]['output_length'])) # Variable
    F_430_ID_FECHA: conint(
        gt=0, lt=pow(10, FIELDS["F_430_ID_FECHA"]['output_length'])) # Variable
    F_430_ID_CLASE_DOCTO: conint(
        gt=0, lt=pow(10, FIELDS["F_430_ID_CLASE_DOCTO"]['output_length'])) = FIELDS["F_430_ID_CLASE_DOCTO"]["default"] 
    F_430_IND_ESTADO: conint(
        gt=0, lt=pow(10, FIELDS["F_430_IND_ESTADO"]['output_length'])) = FIELDS["F_430_IND_ESTADO"]["default"] 
    F_430_IND_BACKORDER: conint(
        gt=0, lt=pow(10, FIELDS["F_430_IND_BACKORDER"]['output_length'])) = FIELDS["F_430_IND_BACKORDER"]["default"]
    F_430_ID_TERCERO_FACT: constr(
        max_length=FIELDS["F_430_ID_TERCERO_FACT"]['output_length']) # Variable
    F_430_ID_SUCURSAL_FACT: constr(
        max_length=FIELDS["F_430_ID_SUCURSAL_FACT"]['output_length'])# Variable
    F_430_ID_TERCERO_REM: constr(
        max_length=FIELDS["F_430_ID_TERCERO_REM"]['output_length']) # Variable
    F_430_ID_SUCURSAL_REM: constr(
        max_length=FIELDS["F_430_ID_SUCURSAL_REM"]['output_length']) # Variable
    F_430_ID_TIPO_CLI_FACT: constr(
        max_length=FIELDS["F_430_ID_TIPO_CLI_FACT"]['output_length']) = FIELDS["F_430_ID_TIPO_CLI_FACT"]["default"]
    F_430_ID_CO_FACT: constr(
        max_length=FIELDS["F_430_ID_CO_FACT"]['output_length']) = FIELDS["F_430_ID_CO_FACT"]["default"]
    F_430_FECHA_ENTREGA: conint(
        gt=0, lt=pow(10, FIELDS["F_430_FECHA_ENTREGA"]['output_length'])) # Variable
    F_430_NUM_DIAS_ENTREGA: conint(
        gt=0, lt=pow(10, FIELDS["F_430_NUM_DIAS_ENTREGA"]['output_length'])) = FIELDS["F_430_NUM_DIAS_ENTREGA"]["default"]
    F_430_NUM_DOCTO_REFERENCIA: constr(
        max_length=FIELDS["F_430_NUM_DOCTO_REFERENCIA"]['output_length']) = FIELDS["F_430_NUM_DOCTO_REFERENCIA"]["default"]
    F_430_REFERENCIA: constr(
        max_length=FIELDS["F_430_REFERENCIA"]['output_length']) # Variable
    F_430_ID_CARGUE: constr(
        max_length=FIELDS["F_430_ID_CARGUE"]['output_length']) = FIELDS["F_430_ID_CARGUE"]["default"]
    F_430_ID_MONEDA_DOCTO: constr(
        max_length=FIELDS["F_430_ID_MONEDA_DOCTO"]['output_length']) = FIELDS["F_430_ID_MONEDA_DOCTO"]["default"]
    F_430_ID_MONEDA_CONV: constr(
        max_length=FIELDS["F_430_ID_MONEDA_CONV"]['output_length']) = FIELDS["F_430_ID_MONEDA_CONV"]["default"]
    F_430_TASA_CONV: confloat(
        gt=0, lt=pow(10, FIELDS["F_430_TASA_CONV"]['output_length'] - NUMBER_PRECISION - 1)) = FIELDS["F_430_TASA_CONV"]["default"]
    F_430_ID_MONEDA_LOCAL: constr(
        max_length=FIELDS["F_430_ID_MONEDA_LOCAL"]['output_length']) = FIELDS["F_430_ID_MONEDA_LOCAL"]["default"] 
    F_430_TASA_LOCAL: confloat(
        gt=0, lt=pow(10, FIELDS["F_430_TASA_LOCAL"]['output_length'] - NUMBER_PRECISION - 1)) = FIELDS["F_430_TASA_LOCAL"]["default"]
    F_430_ID_COND_PAGO: constr(
        max_length=FIELDS["F_430_ID_COND_PAGO"]['output_length']) = FIELDS["F_430_ID_COND_PAGO"]["default"]
    F_430_IND_IMPRESION: conint(
        gt=0, lt=pow(10, FIELDS["F_430_IND_IMPRESION"]['output_length'])) = FIELDS["F_430_IND_IMPRESION"]["default"]
    F_430_NOTAS: constr(
        max_length=FIELDS["F_430_NOTAS"]['output_length']) #Variable
    F_430_ID_CLI_CONTADO: constr(
        max_length=FIELDS["F_430_ID_CLI_CONTADO"]['output_length']) = FIELDS["F_430_ID_CLI_CONTADO"]["default"]
    F_430_ID_PUNTO_ENVIO: constr(
        max_length=FIELDS["F_430_ID_PUNTO_ENVIO"]['output_length']) = FIELDS["F_430_ID_PUNTO_ENVIO"]["default"]
    F_430_ID_TERCERO_VENDEDOR: constr(
        max_length=FIELDS["F_430_ID_TERCERO_VENDEDOR"]['output_length'])
    F_419_CONTACTO: constr(
        max_length=FIELDS["F_419_CONTACTO"]['output_length']) = FIELDS["F_419_CONTACTO"]["default"]
    F_419_DIRECCION1: constr(
        max_length=FIELDS["F_419_DIRECCION1"]['output_length']) = FIELDS["F_419_DIRECCION1"]["default"]
    F_419_DIRECCION2: constr(
        max_length=FIELDS["F_419_DIRECCION2"]['output_length']) = FIELDS["F_419_DIRECCION2"]["default"]
    F_419_DIRECCION3: constr(
        max_length=FIELDS["F_419_DIRECCION3"]['output_length']) = FIELDS["F_419_DIRECCION3"]["default"]
    F_419_ID_PAIS: constr(
        max_length=FIELDS["F_419_ID_PAIS"]['output_length']) = FIELDS["F_419_ID_PAIS"]["default"]
    F_419_ID_DEPTO: constr(
        max_length=FIELDS["F_419_ID_DEPTO"]['output_length']) = FIELDS["F_419_ID_DEPTO"]["default"]
    F_419_ID_CIUDAD: constr(
        max_length=FIELDS["F_419_ID_CIUDAD"]['output_length']) = FIELDS["F_419_ID_CIUDAD"]["default"]
    F_419_ID_BARRIO: constr(
        max_length=FIELDS["F_419_ID_BARRIO"]['output_length']) = FIELDS["F_419_ID_BARRIO"]["default"]
    F_419_TELEFONO: constr(
        max_length=FIELDS["F_419_TELEFONO"]['output_length']) = FIELDS["F_419_TELEFONO"]["default"]
    F_419_FAX: constr(
        max_length=FIELDS["F_419_FAX"]['output_length']) = FIELDS["F_419_FAX"]["default"]
    F_419_COD_POSTAL: constr(
        max_length=FIELDS["F_419_COD_POSTAL"]['output_length']) = FIELDS["F_419_COD_POSTAL"]["default"]
    F_419_EMAIL: constr(
        max_length=FIELDS["F_419_EMAIL"]['output_length']) = FIELDS["F_419_EMAIL"]["default"]
    F_419_IND_DESCUENTO: conint(
        gt =0, lt=pow(10, FIELDS["F_419_IND_DESCUENTO"]['output_length'])) = FIELDS["F_419_IND_DESCUENTO"]["default"]
    TROPI_PRODUCTS: List[TropiProduct]
    TROPI_COMBOS: List[TropiCombos]
    
         
    def get_lines(self) -> List[str]:
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

        order_lines = ["".join(output_fields)] # ["".join(output_fields)]
        product_lines = [str(product) for product in data['TROPI_PRODUCTS']]
        combos_lines = [str(product) for product in data['TROPI_COMBOS']]
        # total_order = "\n".join(order_lines +  product_lines)
        return order_lines +  product_lines + combos_lines
