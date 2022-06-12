
### Defininicion de funciones a usar en los diferentes scripts
import pandas as pd
import numpy as np
import os
from datetime import datetime

# Funcion para cargar todos los xlsx de una carpeta a un dataframe
def leer_xlsx(ruta):
    
    #print("Inicio carga: " + datetime.now().strftime('%H:%M:%S'))
    contenido = os.listdir(ruta)

    # Dataframe a generar:
    df = pd.DataFrame()

    archivos = []
    for fichero in contenido:
        if os.path.isfile(os.path.join(ruta, fichero)) and fichero.endswith('.xlsx'):
            df_temp = pd.read_excel(ruta+fichero)
            df = pd.concat([df, df_temp], axis=0)
            #print("Se cargó el archivo: ", fichero, "a las: " + datetime.now().strftime('%H:%M:%S'))
    
    df_temp = []
    #print("Fin de la carga de archivos")
    return df


# Renombrar columnas de un df:
def renombrar_columnas(df):
    df.rename( columns = { 'Periodo ID': 'Periodo',
                           'Persona ID': 'Persona',
                           'Persona Fecha Nacimiento': 'Fecha_nac',
                           'Discapacitado ID': 'Discapacidad',
                           'Marca Persona con Diabetes DESC': 'Diabetes',
                           'Marca Persona con Hepatitis DESC': 'Hepatitis',
                           'Marca Persona con PMI DESC': 'PMI',
                           'Marca Persona con Tratamiento Cronico DESC': 'Cronico',
                           'Marca Persona con Tratamiento Oncológico DESC':  'Oncologico',
                           'Sexo ID': 'Sexo',
                           'Plan Sin Segmento ID': 'Plan',
                           'Posicion Asociado DESC': 'Posicion Asociado',
                           'Zona de Promocion DESC': 'Zona de Promocion',
                           'Segmento Agrupado DESC': 'Segmento',
                           'Plan Agrupado Sin Segmento DESC': 'Plan Agrupado',
                           'Stock Asociados Salud': 'Stock',
                           'Edad Periodo ID': 'Edad',
                           'ACE Stock Asociados Salud': 'ACE', 
                           'Consumos Costo con Subsidios SSS': 'Consumo3',
                           'Recaudacion Costo con Otros Subsidios': 'Recaudacion3',
                           'Precio Sugerido Medicamento': "Precio_sug",
                           'Periodo Prestacion ID':'Periodo_Prest',
                           'Provision Acreedor DESC':"Prov_acr_de",
                           'Porcentaje Cobertura Medicamento ID': "Cobertura_DW",
                           'Prestacion ID': 'Prestacion',
                           'Consumos Costo con Otros Subsidios': 'Consumo',
                           'Cantidad Practicas Pagadas': 'Cantidad',
                           'Plan Agrupado Con Segmento DESC':'Plan Agrupado'
                           }, inplace=True)


# La siguiente función genera las columnas: Cantidad, Consumo y Prestaciones distintas
# dado un dataframe de entrada.
# Finalmente, como se genera "multi-index" se cambia el nombre de las columnas para que queden consolidados
def td_consumo(df, columna, segmentacion, metricas):
    
    # Genero la td
    td = pd.pivot_table(df, 
                        index = segmentacion, 
                        columns = columna, 
                        aggfunc = metricas, 
                        fill_value = 0)
    
    # Cambio los nombres de las columnas
    col_names = []
    for i in td.columns:
        col_names.append((i[0] + " " + i[2])) # se debe ver en funcion del multiindex
    
    td.columns = col_names
    
    # Eliminamos las columnas "no considerar"
    col_drop = td.columns.str.contains('no considerar', case=False)
    td = td.drop(td.columns[col_drop],1)
    
    return td





