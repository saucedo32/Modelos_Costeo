# Script para generar varias zonas

def generador_zona(ruta_seteo):

    import funciones
    import os
    import proceso
    import pandas as pd
    import numpy as np


    # Ruta donde se encuentran los seteos:
    ruta_seteos = ruta_seteo

    # leemos las variables de los diferentes archivos txt.
    variables = {}   # Diccionario que contendra los datos leidos.
    with open(ruta_seteos, "r") as datos:
        for linea in datos:
            nombre, valor = linea.strip().split("=", maxsplit=1)
            variables[nombre] = valor

    # Guardamos las variables en variables globales:
    ruta_aux = variables['ruta_aux']
    ruta_stock = variables['ruta_stock']
    ruta_consumo = variables['ruta_consumo']
    ruta_int = variables['ruta_int']
    ruta_diag = variables['ruta_diag']
    medicamentos_especiales = variables['medicamentos_especiales']
    cobertura_medicamentos = variables['cobertura_medicamentos']
    #archivo_exportacion = variables['archivo_exportacion']
    resultado_exportacion = variables['resultado_exportacion']


    #######################################################
    # Importacion de consumo y cambio de nombre columnas ##
    #######################################################

    dfc = funciones.leer_xlsx(ruta_consumo)
    funciones.renombrar_columnas(dfc)

    # Aperturas realizadas al consumo:
    dfc = proceso.apertura_pmo(dfc, ruta_aux)
    dfc = proceso.apertura_tipo_internacion(dfc, ruta_int)
    proceso.apertura_total(dfc)
    proceso.apertura_rubros_amb(dfc)
    proceso.apertura_origen_medicamentos(dfc)
    proceso.apertura_via_medicamentos(dfc)
    dfc = proceso.apertura_cobertura_medicamentos(dfc, medicamentos_especiales, cobertura_medicamentos)

    # Segmentacion y metricas para td consumo:
    segmentacion = (['Periodo_Prest', 'Persona'])
    metricas = ({'Consumo':[np.sum], 'Prestacion':pd.Series.nunique,'Cantidad':[np.sum]})

    # Armado de tds consolidadas de consumos:
    td1 = pd.pivot_table(dfc, index = segmentacion, aggfunc = metricas, fill_value = 0)

    # Nombres de columnas:
    col_td1 = [
        "Cantidad Total",
        "Consumo Total",
        "Prest. Dist. Total"  ]

    # Renombro las columnas:
    td1.columns = col_td1

    td2 = funciones.td_consumo(df = dfc, columna = "Apertura Total", segmentacion = segmentacion, metricas = metricas)
    td3 = funciones.td_consumo(df = dfc, columna = "Amb por Rubros", segmentacion = segmentacion, metricas = metricas)
    td4 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Origen", segmentacion = segmentacion, metricas = metricas)
    td5= funciones.td_consumo(df = dfc, columna = "Medicamentos por Via", segmentacion = segmentacion, metricas = metricas)
    td6 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Cobertura", segmentacion = segmentacion, metricas = metricas)
    td7 = funciones.td_consumo(df = dfc, columna = "Tipo Int. ID", segmentacion = segmentacion, metricas = metricas)
    td8 = funciones.td_consumo(df = dfc, columna = "Marca PMO", segmentacion = segmentacion, metricas = metricas)


    # Agrupamos las tablas de consumos:
    tdt = pd.concat([td1, td2, td3, td4, td5, td6, td7, td8], axis=1)
    tdt = tdt.reset_index()


    #######################################################
    # Importacion de stock y cambio de nombre columnas ####
    #######################################################

    # Cargamos el stock y cambiamos el nombre de las columnas:
    dfs = funciones.leer_xlsx(ruta = ruta_stock)
    funciones.renombrar_columnas(dfs)

    # Agregamos aperturas al stock:
    dfs = proceso.marca_rango_edad(dfs, ruta_aux)
    dfs = proceso.marca_cartilla_base(dfs, ruta_aux)
    dfs = proceso.marca_cobertura_med(dfs, ruta_aux)
    dfs = proceso.marca_lista_copagos(dfs, ruta_aux)
    dfs = proceso.marca_diagnistico(dfs, ruta_diag)
    dfs = proceso.marca_parentesco(dfs)


    ##########################################
    ## CREAMOS EL ARCHIVO DE SALIDA ##########
    ##########################################

    dff = pd.merge(left = dfs, right = tdt, 
                    left_on=("Persona","Periodo"), right_on=("Persona","Periodo_Prest"), how = "left")
    dff = dff.fillna(0)

    dff.to_csv(resultado_exportacion, encoding = 'latin1', index = False, decimal=',')

















