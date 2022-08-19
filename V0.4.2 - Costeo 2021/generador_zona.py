# Script para generar varias zonas

def generador_zona(ruta_seteo):

    import funciones
    import os
    import proceso
    import pandas as pd
    import numpy as np
    import warnings
    warnings.filterwarnings('ignore')


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

    # Aperturas propias del pmo
    dfc = proceso.apertura_tipo_internacion_pmo(dfc, ruta_int)
    proceso.apertura_total_pmo(dfc)
    proceso.apertura_rubros_amb_pmo(dfc)
    proceso.apertura_origen_medicamentos_pmo(dfc)
    proceso.apertura_via_medicamentos_pmo(dfc)


    
    # Segmentacion y metricas para td consumo:
    segmentacion = (['Periodo_Prest', 'Persona'])
    # metricas = ({'Consumo':[np.sum], 'Prestacion':pd.Series.nunique,'Cantidad':[np.sum]})
    metricas = ({'Consumo':[np.sum], 'Cantidad':[np.sum]}) # Quite la prestacion    



    # Armado de tds consolidadas de consumos:
    td1 = pd.pivot_table(dfc, index = segmentacion, aggfunc = metricas, fill_value = 0)

    # Nombres de columnas:
    #col_td1 = [
    #    "Cantidad Total",
    #    "Consumo Total",
    #    "Prest. Dist. Total"  ]

    col_td1 = [
        "Cantidad Total",
        "Consumo Total" ]


    # Renombro las columnas:
    td1.columns = col_td1

    td2 = funciones.td_consumo(df = dfc, columna = "Apertura Total", segmentacion = segmentacion, metricas = metricas)
    td3 = funciones.td_consumo(df = dfc, columna = "Amb por Rubros", segmentacion = segmentacion, metricas = metricas)
    td4 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Origen", segmentacion = segmentacion, metricas = metricas)
    td5= funciones.td_consumo(df = dfc, columna = "Medicamentos por Via", segmentacion = segmentacion, metricas = metricas)
    td6 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Cobertura", segmentacion = segmentacion, metricas = metricas)
    td7 = funciones.td_consumo(df = dfc, columna = "Tipo Int. ID", segmentacion = segmentacion, metricas = metricas)
    td8 = funciones.td_consumo(df = dfc, columna = "Marca PMO", segmentacion = segmentacion, metricas = metricas)

    # Tds propias de PMO:
    td9 = funciones.td_consumo(df = dfc, columna = "Amb por Rubros PMO", segmentacion = segmentacion, metricas = metricas)
    td10 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Origen PMO", segmentacion = segmentacion, metricas = metricas)
    td11 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Via PMO", segmentacion = segmentacion, metricas = metricas)
    td12 = funciones.td_consumo(df = dfc, columna = "Tipo Int. ID PMO", segmentacion = segmentacion, metricas = metricas)
    td13 = funciones.td_consumo(df = dfc, columna = "Apertura Total PMO", segmentacion = segmentacion, metricas = metricas)



    # Agrupamos las tablas de consumos:
    tdt = pd.concat([td1, td2, td3, td4, td5, td6, td7, td8, td9, td10, td11, td12, td13], axis=1)
    tdt = tdt.reset_index()


    #######################################################
    # Importacion de stock y cambio de nombre columnas ####
    #######################################################

    # Cargamos el stock y cambiamos el nombre de las columnas:
    dfs = funciones.leer_xlsx(ruta = ruta_stock)
    funciones.renombrar_columnas(dfs)
    dfs["Stock"] = 1 # modificamos la cantidad de stock a "1" en cada persona.

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




def generador_zona_df(ruta_seteo):

    import funciones
    import os
    import proceso
    import pandas as pd
    import numpy as np
    import warnings
    warnings.filterwarnings('ignore')


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

    # Aperturas propias del pmo
    dfc = proceso.apertura_tipo_internacion_pmo(dfc, ruta_int)
    proceso.apertura_total_pmo(dfc)
    proceso.apertura_rubros_amb_pmo(dfc)
    proceso.apertura_origen_medicamentos_pmo(dfc)
    proceso.apertura_via_medicamentos_pmo(dfc)


    
    # Segmentacion y metricas para td consumo:
    segmentacion = (['Periodo_Prest', 'Persona'])
    # metricas = ({'Consumo':[np.sum], 'Prestacion':pd.Series.nunique,'Cantidad':[np.sum]})
    metricas = ({'Consumo':[np.sum], 'Cantidad':[np.sum]}) # Quite la prestacion    



    # Armado de tds consolidadas de consumos:
    td1 = pd.pivot_table(dfc, index = segmentacion, aggfunc = metricas, fill_value = 0)

    # Nombres de columnas:
    #col_td1 = [
    #    "Cantidad Total",
    #    "Consumo Total",
    #    "Prest. Dist. Total"  ]

    col_td1 = [
        "Cantidad Total",
        "Consumo Total" ]


    # Renombro las columnas:
    td1.columns = col_td1

    td2 = funciones.td_consumo(df = dfc, columna = "Apertura Total", segmentacion = segmentacion, metricas = metricas)
    td3 = funciones.td_consumo(df = dfc, columna = "Amb por Rubros", segmentacion = segmentacion, metricas = metricas)
    td4 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Origen", segmentacion = segmentacion, metricas = metricas)
    td5= funciones.td_consumo(df = dfc, columna = "Medicamentos por Via", segmentacion = segmentacion, metricas = metricas)
    td6 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Cobertura", segmentacion = segmentacion, metricas = metricas)
    td7 = funciones.td_consumo(df = dfc, columna = "Tipo Int. ID", segmentacion = segmentacion, metricas = metricas)
    td8 = funciones.td_consumo(df = dfc, columna = "Marca PMO", segmentacion = segmentacion, metricas = metricas)

    # Tds propias de PMO:
    td9 = funciones.td_consumo(df = dfc, columna = "Amb por Rubros PMO", segmentacion = segmentacion, metricas = metricas)
    td10 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Origen PMO", segmentacion = segmentacion, metricas = metricas)
    td11 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Via PMO", segmentacion = segmentacion, metricas = metricas)
    td12 = funciones.td_consumo(df = dfc, columna = "Tipo Int. ID PMO", segmentacion = segmentacion, metricas = metricas)
    td13 = funciones.td_consumo(df = dfc, columna = "Apertura Total PMO", segmentacion = segmentacion, metricas = metricas)



    # Agrupamos las tablas de consumos:
    tdt = pd.concat([td1, td2, td3, td4, td5, td6, td7, td8, td9, td10, td11, td12, td13], axis=1)
    tdt = tdt.reset_index()


    #######################################################
    # Importacion de stock y cambio de nombre columnas ####
    #######################################################

    # Cargamos el stock y cambiamos el nombre de las columnas:
    dfs = funciones.leer_xlsx(ruta = ruta_stock)
    funciones.renombrar_columnas(dfs)
    dfs["Stock"] = 1 # modificamos la cantidad de stock a "1" en cada persona.

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


    # Atributos para generar la td
    Atributos = [   "Discapacidad", "Posicion Asociado ID", "Segmento", "Plan",
                    "Plan Agrupado", "Tipo de Venta DESC", "Diabetes", "Hepatitis",
                    "PMI", "Cronico", "Oncologico", "Rango_Edad", "Rango_Edad id", "Modelo_Cartilla",
                    "Cobertura_med_id","Cobertura_desc", "Lista_PrecioDesc", "GC", "Rango_Cons",
                    "algun_diag", "Marca_Diagnostico", "Marca Hijo" ]


    td_cubo = pd.pivot_table( dff, index=Atributos, aggfunc=np.sum).reset_index()
    td_cubo.to_csv(resultado_exportacion, encoding = 'latin1', index = False, decimal=',')




    


def generador_zona_df_csv(ruta_seteo):

    import funciones
    import os
    import proceso
    import pandas as pd
    import numpy as np
    import warnings
    warnings.filterwarnings('ignore')


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
    ruta_aumentoSSS = variables['ruta_aumentoSSS']


    #######################################################
    # Importacion de consumo y cambio de nombre columnas ##
    #######################################################

    #dfc = funciones.leer_xlsx(ruta_consumo)
    #funciones.renombrar_columnas(dfc)

    # dfc = pd.read_csv(ruta_consumo, sep=';', header= None)
    dfc = funciones.leer_csv_2(ruta_consumo)
    dfc.columns = ["Periodo ID","Persona ID","Persona Fecha Nacimiento","Sexo ID","Discapacitado ID",
    "Localidad Asociado ID","Provincia Asociado ID","Posicion Asociado ID","Zona de Promocion ID",
    "Segmento Agrupado DESC","Plan Sin Segmento ID","Tipo de Venta DESC","Tipo de Venta ID","Subtipo de Venta DESC",
    "Subtipo de Venta ID","Periodo Prestacion ID","Edad Orden ID","Origen Prestacion ID","Origen Facturacion ID",
    "Prestacion ID","Rubro Indicadores Consumo ID","Prestacion Acreedor ID","Nomenclador ID","Nomenclador DESC","Acreedor ID",
    "Provision Acreedor ID","Posicion Acreedor ID","Subcategoria Acreedor DESC","Tipo Orden ID","Nro Orden ID",
    "Tipo Orden Relacionada ID","Orden Relacionada ID","Porcentaje Cobertura Medicamento ID","Cantidad",
    "Consumo","Precio Sugerido Medicamento"]



    
    print(dfc.Consumo.dtype)
    funciones.renombrar_columnas(dfc)
    print(dfc.Consumo.dtype)
    # Aperturas realizadas al consumo:
    dfc = proceso.apertura_pmo(dfc, ruta_aux)
    print("ok, apertura pmo")
    print(dfc.Consumo.dtype)
    dfc = proceso.apertura_tipo_internacion(dfc, ruta_int)
    print("ok, internaciones")
    proceso.apertura_total(dfc)
    print("ok, apertura total")
    proceso.apertura_rubros_amb(dfc)
    print("ok, apertura rubros")
    print(dfc.Consumo.dtype)
    proceso.apertura_origen_medicamentos(dfc)
    print("ok, apertura medicamentos")
    proceso.apertura_via_medicamentos(dfc)
    print("ok, apertura via medicamentos")
    print(dfc.Consumo.dtype)
    dfc = proceso.apertura_cobertura_medicamentos(dfc, medicamentos_especiales, cobertura_medicamentos)
    print("ok, apertura medicamentos por cobertura")

    # Aperturas propias del pmo
    dfc = proceso.apertura_tipo_internacion_pmo(dfc, ruta_int)
    proceso.apertura_total_pmo(dfc)
    proceso.apertura_rubros_amb_pmo(dfc)
    proceso.apertura_origen_medicamentos_pmo(dfc)
    proceso.apertura_via_medicamentos_pmo(dfc)






    ### Actualización de importes por inflación SSS ###
    # leo el archivo de aumento de SSS:
    dfa = pd.read_excel(ruta_aumentoSSS)
    # unimos con el consumo para actualizar

    dfc = pd.merge(left = dfc, right = dfa, left_on = "Periodo", right_on = "Periodo", how = "left")
    dfc.Consumo = dfc.Consumo * dfc.Ajuste_SSS
    dfa = []




    
    # Segmentacion y metricas para td consumo:
    segmentacion = (['Periodo_Prest', 'Persona'])
    # metricas = ({'Consumo':[np.sum], 'Prestacion':pd.Series.nunique,'Cantidad':[np.sum]})
    metricas = ({'Consumo':[np.sum], 'Cantidad':[np.sum]}) # Quite la prestacion    



    # Armado de tds consolidadas de consumos:
    td1 = pd.pivot_table(dfc, index = segmentacion, aggfunc = metricas, fill_value = 0)

    # Nombres de columnas:
    #col_td1 = [
    #    "Cantidad Total",
    #    "Consumo Total",
    #    "Prest. Dist. Total"  ]

    col_td1 = [
        "Cantidad Total",
        "Consumo Total" ]
    
    print("inicio tds")


    # Renombro las columnas:
    td1.columns = col_td1

    td2 = funciones.td_consumo(df = dfc, columna = "Apertura Total", segmentacion = segmentacion, metricas = metricas)
    td3 = funciones.td_consumo(df = dfc, columna = "Amb por Rubros", segmentacion = segmentacion, metricas = metricas)
    td4 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Origen", segmentacion = segmentacion, metricas = metricas)
    td5= funciones.td_consumo(df = dfc, columna = "Medicamentos por Via", segmentacion = segmentacion, metricas = metricas)
    td6 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Cobertura", segmentacion = segmentacion, metricas = metricas)
    td7 = funciones.td_consumo(df = dfc, columna = "Tipo Int. ID", segmentacion = segmentacion, metricas = metricas)
    td8 = funciones.td_consumo(df = dfc, columna = "Marca PMO", segmentacion = segmentacion, metricas = metricas)

    print("vamos por la td8 finalizada")

    # Tds propias de PMO:
    td9 = funciones.td_consumo(df = dfc, columna = "Amb por Rubros PMO", segmentacion = segmentacion, metricas = metricas)
    td10 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Origen PMO", segmentacion = segmentacion, metricas = metricas)
    td11 = funciones.td_consumo(df = dfc, columna = "Medicamentos por Via PMO", segmentacion = segmentacion, metricas = metricas)
    td12 = funciones.td_consumo(df = dfc, columna = "Tipo Int. ID PMO", segmentacion = segmentacion, metricas = metricas)
    td13 = funciones.td_consumo(df = dfc, columna = "Apertura Total PMO", segmentacion = segmentacion, metricas = metricas)


    print("todas las tds finalizadas, vamos a concatenarlas")

    # Agrupamos las tablas de consumos:
    tdt = pd.concat([td1, td2, td3, td4, td5, td6, td7, td8, td9, td10, td11, td12, td13], axis=1)
    tdt = tdt.reset_index()

    print("todas las tds concatenadas")

    #######################################################
    # Importacion de stock y cambio de nombre columnas ####
    #######################################################

    # Cargamos el stock y cambiamos el nombre de las columnas:
    #dfs = funciones.leer_xlsx(ruta = ruta_stock)
    #funciones.renombrar_columnas(dfs)
    #dfs["Stock"] = 1 # modificamos la cantidad de stock a "1" en cada persona.


    # leer csv separado con ; con pandas
    #dfs = pd.read_csv(ruta_stock, sep=';', header= None)
    dfs = funciones.leer_csv_2(ruta_stock)
    dfs.columns = ["Periodo ID","Persona ID","Persona Fecha Nacimiento","Parentesco IdParentesco","Edad Periodo ID","Sexo ID","Discapacitado ID","Localidad Asociado ID",
    "Provincia Asociado ID","Posicion Asociado ID","Zona de Promocion ID","Segmento Agrupado ID","Segmento Agrupado DESC","Plan Sin Segmento ID",
    "Plan Agrupado Sin Segmento DESC","Tipo de Venta DESC","Tipo de Venta ID","Subtipo de Venta DESC","Subtipo de Venta ID","Marca Persona con Diabetes DESC",
    "Marca Persona con Hepatitis DESC","Marca Persona con PMI DESC","Marca Persona con Tratamiento Cronico DESC","Marca Persona con Tratamiento Oncológico DESC",
    "Stock Asociados Salud","ACE Stock Asociados Salud"]
    funciones.renombrar_columnas(dfs)
    dfs["Stock"] = 1 # modificamos la cantidad de stock a "1" en cada persona.


    # Agregamos aperturas al stock:
    dfs = proceso.marca_rango_edad(dfs, ruta_aux)
    print("Pasamos rango edad")
    dfs = proceso.marca_cartilla_base(dfs, ruta_aux)
    print("Pasamos cartilla base")
    dfs = proceso.marca_cobertura_med(dfs, ruta_aux)
    print("Pasamos marca cobertura med")
    dfs = proceso.marca_lista_copagos(dfs, ruta_aux)
    print("Pasamos lista copagos")
    dfs = proceso.marca_diagnistico(dfs, ruta_diag)
    print("Pasamos marca diag")
    dfs = proceso.marca_parentesco(dfs)
    print("Pasamos parentesco")


    ##########################################
    ## CREAMOS EL ARCHIVO DE SALIDA ##########
    ##########################################

    print("Unimos por periodo persona el consumo para generar el archivo de salida")

    dff = pd.merge(left = dfs, right = tdt, 
                    left_on=("Persona","Periodo"), right_on=("Persona","Periodo_Prest"), how = "left")
    dff = dff.fillna(0)


    # Atributos para generar la td
    Atributos = [   "Discapacidad", "Posicion Asociado ID", "Provincia Asociado ID","Segmento", "Plan",
                    "Plan Agrupado", "Tipo de Venta DESC", "Diabetes", "Hepatitis",
                    "PMI", "Cronico", "Oncologico", "Rango_Edad", "Rango_Edad id", "Modelo_Cartilla",
                    "Cobertura_med_id","Cobertura_desc", "Lista_PrecioDesc", "GC", "Rango_Cons",
                    "algun_diag", "Marca_Diagnostico", "Marca Hijo" ]


    print("exportando archivo de salida:")
    
    td_cubo = pd.pivot_table( dff, index=Atributos, aggfunc=np.sum).reset_index()
    td_cubo.to_csv(resultado_exportacion, encoding = 'latin1', index = False, decimal=',')















