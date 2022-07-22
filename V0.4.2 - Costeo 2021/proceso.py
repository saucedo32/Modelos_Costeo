# Librerías usadas:
import pandas as pd
import numpy as np
import os
from datetime import datetime
import funciones

######################################################
###### Procedimientos que se emplean en CONSUMOS: ####
######################################################

def apertura_pmo(dfc, ruta_aux):

    # CREAMOS LA COLUMNA: "Marca PMO"
    # Levanto el excel con marca de prestaciones pmo
    dfpmo = pd.read_excel(ruta_aux + 'Aux_pmo.xlsx')
    # Quito los espacios a las prestaciones:
    dfc["Prest_sin_esp"] = dfc.Prestacion.str.strip()
    # Unimos la marca al dfc
    dfc = pd.merge( left = dfc, right = dfpmo, left_on='Prest_sin_esp', 
                right_on='Prest sin espacios', how = "left")
    dfpmo = [] # Libero memoria:
    # Prestaciones detectadas como faltantes:
    #print('Las prestaciones que no se pudieron catalogar fueron:')
    #print(dfc[dfc['Marca PMO'].isna()]["Prestacion"].unique())
    # Reemplazamos los nan de marca pmo por "NO PMO"
    dfc['Marca PMO'] = dfc['Marca PMO'].replace(np.nan,"NO PMO")

    return dfc


def apertura_tipo_internacion(dfc, ruta_int):
    
    ########################################################
    ###### AGREGAMOS MARCA TIPO INTERNACION
    ########################################################

    # CREAMOS LA COLUMNA: "Tipo Int. ID"
    # Levanto las internaciones históricas para cruzar:
    dfint = pd.read_excel(ruta_int)
    # Agregamos info a dfc
    dfc = pd.merge( left = dfc, right = dfint[['Paciente ID','Orden Int. ID','Tipo Int. ID']], 
                right_on=['Paciente ID','Orden Int. ID'], 
                left_on=['Persona','Orden Relacionada ID'], how = "left")
    # Libero memoria:
    dfint = []

    # Reemplazamos los falta dato por "otras Int"
    dfc['Tipo Int. ID'] = dfc['Tipo Int. ID'].replace(np.nan,"Otras Int")
    # Armado de la columna calculada:
    condiciones_dfint = [
        (dfc['Origen Prestacion ID'] == 'I')]
    # Lista de resultados en función de las selecciones
    valores_dfint = [dfc['Tipo Int. ID']]   
    # Generación de la columna calculada
    dfc['Tipo Int. ID'] = np.select(condiciones_dfint, valores_dfint, default = 'No Considerar')

    return dfc


def apertura_tipo_internacion_pmo(dfc, ruta_int):
    
    ########################################################
    ###### AGREGAMOS MARCA TIPO INTERNACION _ PMO
    ########################################################

    # CREAMOS LA COLUMNA: "Tipo Int. ID"
    # Levanto las internaciones históricas para cruzar:
    dfint = pd.read_excel(ruta_int) # no es necesario si antes se genero
    
    ## Agregamos info a dfc ---- NO SE REALIZA CUANDO YA REALIZAMOS LA COLUMNA PARA EL TOTAL
    #dfc = pd.merge( left = dfc, right = dfint[['Paciente ID','Orden Int. ID','Tipo Int. ID']], 
    #            right_on=['Paciente ID','Orden Int. ID'], 
    #            left_on=['Persona','Orden Relacionada ID'], how = "left")
    
    # Libero memoria:
    dfint = []  # no es necesario si antes se genero

    ## Reemplazamos los falta dato por "otras Int"
    #dfc['Tipo Int. ID'] = dfc['Tipo Int. ID'].replace(np.nan,"Otras Int")



    # Armado de la columna calculada:
    condiciones_dfint = [
        (dfc['Origen Prestacion ID'] == 'I') & (dfc['Marca PMO'] == 'PMO')]
    # Lista de resultados en función de las selecciones
    valores_dfint = [dfc['Tipo Int. ID']+"_pmo"]   
    # Generación de la columna calculada
    dfc['Tipo Int. ID PMO'] = np.select(condiciones_dfint, valores_dfint, default = 'No Considerar')

    return dfc


def apertura_total(dfc):

    ########################################################
    ###### CREAMOS LA COLUMNA APERTURA TOTAL
    ########################################################
    
    condiciones2 = [
        (dfc['Nomenclador ID'] == 'ME') ,
        (dfc['Nomenclador ID'] == 'M1') ,
        (dfc['Origen Facturacion ID'] == 'A') ,
        (dfc['Origen Facturacion ID'] == 'C') ,
        (dfc['Origen Prestacion ID'] == 'I')]

    # Lista de resultados en función de las selecciones
    valores_condic2 = ['MEd', 'MEd', 'Naj', 'NC', 'Internacion sin A,C,Med']     

    # Generación de la columna calculada
    dfc['Apertura Total'] = np.select(condiciones2, valores_condic2, default = 'Ambulatorio sin A,C,Med')


def apertura_total_pmo(dfc):

    ########################################################
    ###### CREAMOS LA COLUMNA APERTURA TOTAL SOLAMENTE DE PRESTACIONES MARCADAS PMO
    ########################################################
    
    condiciones2 = [
        (dfc['Nomenclador ID'] == 'ME') & (dfc['Marca PMO'] == 'PMO') ,
        (dfc['Nomenclador ID'] == 'M1') & (dfc['Marca PMO'] == 'PMO') ,
        (dfc['Origen Facturacion ID'] == 'A') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Facturacion ID'] == 'C') & (dfc['Marca PMO'] == 'PMO') ,
        (dfc['Origen Prestacion ID'] == 'I') & (dfc['Marca PMO'] == 'PMO'),
        # COLUMNA SOLAMENTE VALIDA PARA CASOS DE PMO (NO TENER EN CUENTA EN CLASIFICACIONES DE TOTALES)
        (dfc['Marca PMO'] == 'PMO')]

    # Lista de resultados en función de las selecciones
    valores_condic2 = ['MEd_pmo', 'MEd_pmo', 'Naj_pmo', 'NC_total_pmo', 'Internacion_sin_med_pmo','Amb_sin_med_pmo']

    # Generación de la columna calculada
    dfc['Apertura Total PMO'] = np.select(condiciones2, valores_condic2, default = 'No Considerar')


def apertura_rubros_amb(dfc):

    ########################################################
    ###### CREAMOS LA COLUMNA: APERTURA AMB POR RUBRO
    ########################################################
    
    # Creamos la columna: Amb por Rubros
    # Apertura para consumo: Agrupador consumos 3
    condiciones3 = [
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'A'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'I'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'C'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'D'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'E'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'F'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'G'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'N'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'M'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'J'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'R'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'S'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'T'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'U'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'V'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'W'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'X'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'ME'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'M1'),

        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'NP'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'DI'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'NC'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'NA'),
        
        (dfc['Origen Prestacion ID'] == 'A') ]

    # Lista de resultados en función de las selecciones
    valores_condic3 = ['Consulta', 'Laboratorio', 'Imagenes', 'Imagenes', 'Imagenes', 
                    'Imagenes', 'Imagenes', 'Fisiokinesio', 'Salud Mental', 
                    'Odontologia', 'Odontologia', 'Odontologia', 'Odontologia', 
                    'Odontologia', 'Odontologia', 'Odontologia', 'Odontologia',
                    'Amb ME','Amb ME','Amb NP','Amb DI', 'Amb NC',
                    'Amb NA','Resto Amb']   

    # Generación de la columna calculada
    dfc['Amb por Rubros'] = np.select(condiciones3, valores_condic3, default = 'No Considerar')


def apertura_rubros_amb_pmo(dfc):

    ########################################################
    ###### CREAMOS LA COLUMNA: APERTURA AMB POR RUBRO PARA PMO
    ########################################################
    
    # Creamos la columna: Amb por Rubros
    # Apertura para consumo: Agrupador consumos 3
    condiciones3 = [
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'A') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'I') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'C') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'D') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'E') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'F') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'G') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'N') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'M') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'J') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'R') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'S') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'T') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'U') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'V') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'W') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Rubro Indicadores Consumo ID'] == 'X') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'ME') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'M1') & (dfc['Marca PMO'] == 'PMO'),

        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'NP') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'DI') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'NC') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') & (dfc['Nomenclador ID'] == 'NA') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Origen Prestacion ID'] == 'A') ]

    # Lista de resultados en función de las selecciones
    valores_condic3 = ['Consulta_pmo', 'Laboratorio_pmo', 'Imagenes_pmo', 'Imagenes_pmo', 'Imagenes_pmo', 
                    'Imagenes_pmo', 'Imagenes_pmo', 'Fisiokinesio_pmo', 'Salud Mental_pmo', 
                    'Odontologia_pmo', 'Odontologia_pmo', 'Odontologia_pmo', 'Odontologia_pmo', 
                    'Odontologia_pmo', 'Odontologia_pmo', 'Odontologia_pmo', 'Odontologia_pmo',
                    'Amb ME_pmo','Amb ME_pmo','Amb NP_pmo','Amb DI_pmo', 'Amb NC_pmo',
                    'Amb NA_pmo','Resto Amb_pmo']   

    # Generación de la columna calculada
    dfc['Amb por Rubros PMO'] = np.select(condiciones3, valores_condic3, default = 'No Considerar')


def apertura_origen_medicamentos(dfc):

    ############################################################
    ###### AGREGAMOS COLUMNA MEDICAMENTOS POR ORIGEN PRESTACION
    ############################################################

    # Apertura para consumo: Medicamentos por Origen
    condiciones4 = [
        
        # Condiciones para marcar medicamentos en internacion:
        (dfc['Origen Prestacion ID'] == 'I') & (dfc['Nomenclador ID'] == 'ME'),
        (dfc['Origen Prestacion ID'] == 'I') & (dfc['Nomenclador ID'] == 'M1'),
        
        # Condiciones para marcar medicamentos en amb -> no se filtra origen (por defecto mandar a AMB)
        (dfc['Nomenclador ID'] == 'ME'),
        (dfc['Nomenclador ID'] == 'M1') ]

    # Lista de resultados en función de las selecciones
    valores_condic4 = ['Med. Int', 'Med. Int', 'Med. Amb', 'Med. Amb']     
    # Generación de la columna calculada
    dfc['Medicamentos por Origen'] = np.select(condiciones4, valores_condic4, default = 'No Considerar')


def apertura_origen_medicamentos_pmo(dfc):

    ############################################################
    ###### AGREGAMOS COLUMNA MEDICAMENTOS POR ORIGEN PRESTACION DE PRESTACIONES MARCADAS COMO PMO
    ############################################################

    # Apertura para consumo: Medicamentos por Origen
    condiciones4 = [
        
        # Condiciones para marcar medicamentos en internacion:
        (dfc['Origen Prestacion ID'] == 'I') & (dfc['Nomenclador ID'] == 'ME') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Origen Prestacion ID'] == 'I') & (dfc['Nomenclador ID'] == 'M1') & (dfc['Marca PMO'] == 'PMO'),
        
        # Condiciones para marcar medicamentos en amb -> no se filtra origen (por defecto mandar a AMB)
        (dfc['Nomenclador ID'] == 'ME') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Nomenclador ID'] == 'M1') & (dfc['Marca PMO'] == 'PMO') ]

    # Lista de resultados en función de las selecciones
    valores_condic4 = ['Med. Int_pmo', 'Med. Int_pmo', 'Med. Amb_pmo', 'Med. Amb_pmo']     
    # Generación de la columna calculada
    dfc['Medicamentos por Origen PMO'] = np.select(condiciones4, valores_condic4, default = 'No Considerar')


def apertura_via_medicamentos(dfc):

    ########################################################
    ###### AGREGAMOS MARCA MEDICAMENTOS POR VIA
    ########################################################

    # Apertura para consumo: Medicamentos por via
    condiciones5 = [
        
        # Condiciones para marcar medicamentos en internacion:
        (dfc['Provision Acreedor ID'] == 6) & (dfc['Nomenclador ID'] == 'ME'),
        (dfc['Provision Acreedor ID'] == 6) & (dfc['Nomenclador ID'] == 'M1'),

        (dfc['Provision Acreedor ID'] == 66) & (dfc['Nomenclador ID'] == 'ME'),
        (dfc['Provision Acreedor ID'] == 66) & (dfc['Nomenclador ID'] == 'M1'),
        
        (dfc['Nomenclador ID'] == 'ME'),
        (dfc['Nomenclador ID'] == 'M1') ]

    # Lista de resultados en función de las selecciones
    valores_condic5 = ['Farmacia', 'Farmacia', 'Provision', 'Provision', 'Otras vias', 'Otras vias']     

    # Generación de la columna calculada
    dfc['Medicamentos por Via'] = np.select(condiciones5, valores_condic5, default = 'No Considerar')


def apertura_via_medicamentos_pmo(dfc):

    ########################################################
    ###### AGREGAMOS MARCA MEDICAMENTOS POR VIA DE PRESTACIONES MARCADAS COMO PMO
    ########################################################

    # Apertura para consumo: Medicamentos por via
    condiciones5 = [
        
        # Condiciones para marcar medicamentos en internacion:
        (dfc['Provision Acreedor ID'] == 6) & (dfc['Nomenclador ID'] == 'ME') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Provision Acreedor ID'] == 6) & (dfc['Nomenclador ID'] == 'M1') & (dfc['Marca PMO'] == 'PMO'),

        (dfc['Provision Acreedor ID'] == 66) & (dfc['Nomenclador ID'] == 'ME') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Provision Acreedor ID'] == 66) & (dfc['Nomenclador ID'] == 'M1') & (dfc['Marca PMO'] == 'PMO'),
        
        (dfc['Nomenclador ID'] == 'ME') & (dfc['Marca PMO'] == 'PMO'),
        (dfc['Nomenclador ID'] == 'M1') & (dfc['Marca PMO'] == 'PMO')]

    # Lista de resultados en función de las selecciones
    valores_condic5 = ['Farmacia_pmo', 'Farmacia_pmo', 'Provision_pmo', 'Provision_pmo', 'Otras vias_pmo', 'Otras vias_pmo']     

    # Generación de la columna calculada
    dfc['Medicamentos por Via PMO'] = np.select(condiciones5, valores_condic5, default = 'No Considerar')


def apertura_cobertura_medicamentos(dfc, medicamentos_especiales, cobertura_medicamentos):
    ########################################################
    ###### AGREGAMOS COLUMNA MEDICAMENTOS POR COBERTURA
    ########################################################
    # Creamos la columna adicional: medicamentos por cobertura:

    dfmed = pd.read_excel(medicamentos_especiales)

    print("inicia la apertura medicamentos por cobertura")

    # Cruzamos con el dfc:
    dfc = pd.merge( left = dfc, right = dfmed[["Prestacion","Tipo_med"]], 
                left_on='Prestacion', right_on='Prestacion', how = "left")

    # Libero memoria:
    dfmed = []
    print(dfc.Consumo.dtype)
    print("división de valores iniciando")

    # Cálculos auxiliares para definir % cobertura:
    # Creo el cálculo auxiliar con *10 para quedarnos con la decena de la cobertura
    dfc['Cob_prec_sug'] = (dfc.Consumo / dfc.Precio_sug)*10

    print("división de valores finalizada")

    # Reemplazo valores +inf y -inf por 0
    dfc['Cob_prec_sug'] = dfc['Cob_prec_sug'].replace([np.inf, -np.inf], 0)


    print("redondear")

    # Redondeo a 0 y multiplico por 10 para quedarme con la decena
    dfc['Cob_prec_sug'] = round(dfc['Cob_prec_sug'],0)*10



    # Coberturas
    dfmed2 = pd.read_excel(cobertura_medicamentos)

    print("realizamos el cruce")
    
    # Cruzamos con el dfc:
    dfc = pd.merge( left = dfc, right = dfmed2[["Plan","Cobertura_med_id", "Cobertura_desc"]], 
                left_on='Plan', right_on='Plan', how = "left")

    # Libero memoria:
    dfmed2 = []


    # MARCA COBERTURA BASE

    # Transformo las provisiones en función de su descripción
    condiciones_c = [
        (dfc.Cobertura_med_id == 301) & (dfc.Cobertura_DW == 40) & (dfc.Cob_prec_sug == 40),
        (dfc.Cobertura_med_id == 302) & (dfc.Cobertura_DW == 40) & (dfc.Cob_prec_sug == 40),
        (dfc.Cobertura_med_id == 302) & (dfc.Cobertura_DW == 50) & (dfc.Cob_prec_sug == 50),    
        (dfc.Cobertura_med_id == 303) & (dfc.Cobertura_DW == 50) & (dfc.Cob_prec_sug == 50),
        (dfc.Cobertura_med_id == 304) & (dfc.Cobertura_DW == 60) & (dfc.Cob_prec_sug == 60),
        (dfc.Cobertura_med_id == 305) & (dfc.Cobertura_DW == 70) & (dfc.Cob_prec_sug == 70),
        (dfc.Cobertura_med_id == 306) & (dfc.Cobertura_DW == 80) & (dfc.Cob_prec_sug == 80),
        (dfc.Cobertura_med_id == 307) & (dfc.Cobertura_DW == 40) & (dfc.Cob_prec_sug == 40),
        (dfc.Cobertura_med_id == 307) & (dfc.Cobertura_DW == 60) & (dfc.Cob_prec_sug == 60),
        (dfc.Cobertura_med_id == 308) & (dfc.Cobertura_DW == 40) & (dfc.Cob_prec_sug == 40),
        (dfc.Cobertura_med_id == 310) & (dfc.Cobertura_DW == 40) & (dfc.Cob_prec_sug == 40),
        (dfc.Cobertura_med_id == 312) & (dfc.Cobertura_DW == 40) & (dfc.Cob_prec_sug == 40),
        (dfc.Cobertura_med_id == 312) & (dfc.Cobertura_DW == 60) & (dfc.Cob_prec_sug == 60),
        (dfc.Cobertura_med_id == 313) & (dfc.Cobertura_DW == 40) & (dfc.Cob_prec_sug == 40),
        (dfc.Cobertura_med_id == 313) & (dfc.Cobertura_DW == 40) & (dfc.Cob_prec_sug == 40),
        (dfc.Cobertura_med_id == 315) & (dfc.Cobertura_DW == 40) & (dfc.Cob_prec_sug == 40) 
    ]

    # Lista de resultados en función de las selecciones
    valores_condic_c = ['Cob. Base 40%', 
                    'Cob. Base 40%',
                    'Cob. Base 50%',
                    'Cob. Base 50%',
                    'Cob. Base 60%',
                    'Cob. Base 70%',
                    'Cob. Base 80%',
                    'Cob. Base 40%',
                    'Cob. Base 60%',
                    'Cob. Base 40%',
                    'Cob. Base 40%',
                    'Cob. Base 40%',
                    'Cob. Base 60%',
                    'Cob. Base 40%',
                    'Cob. Base 40%',
                    'Cob. Base 40%']     

    # Generación de la columna calculada
    dfc['Marca_Cob_Base'] = np.select(condiciones_c, valores_condic_c, default = 'No Cob. Base')


    # MARCA COBERTURA 100%

    condiciones = [(dfc.Cobertura_DW == 100) & (dfc.Cob_prec_sug == 100)]

    # Lista de resultados en función de las selecciones
    valores_condic = ['Cobertura 100%']  

    # Generación de la columna calculada
    dfc['Marca_Cobertura_100'] = np.select(condiciones, valores_condic, default = 'No Cobertura 100%')



    # Genero la agrupación final med
    condiciones = [
        # Tipo medicamento es MACBI, ME6000 o M1 o R310.
        dfc.Tipo_med == 'ME6000000 y ME1600085',
        dfc.Tipo_med.isnull() == 0,
        dfc.Marca_Cob_Base != 'No Cob. Base',
        dfc.Marca_Cobertura_100 == "Cobertura 100%",
        (dfc["Tipo Orden ID"] == 4) & (dfc['Nomenclador ID'] == 'ME') & (dfc['Nomenclador ID'] == 'M1'),
        dfc["Nomenclador ID"] == 'ME',
        dfc["Nomenclador ID"] == 'M1']
        
    # Lista de resultados en función de las selecciones
    valores_condic = ["Prest Genéricas", dfc.Tipo_med, dfc.Marca_Cob_Base, dfc.Marca_Cobertura_100, "Form. 4", "Otros", "Otros"]  

    # Generación de la columna calculada
    dfc['Medicamentos por Cobertura'] = np.select(condiciones, valores_condic, default = 'No Considerar')

    return dfc


######################################################
###### Procedimientos que se emplean en ASOCIADOS: ###
######################################################

def marca_rango_edad(dfs, ruta_aux):

    ########################################################
    ###### AGREGAMOS RANGO EDAD
    ########################################################

    # Levanto la auxiliar:
    Aux_RangoEdad = pd.read_excel(ruta_aux + "Aux_RangoEdad.xlsx")
    # Pego la informacion al df stock:
    dfs = pd.merge(left = dfs, right = Aux_RangoEdad[["Edad","Rango_Edad","Rango_Edad id"]], left_on='Edad', right_on='Edad', how = "left")
    # Elimino la columna adicional -- no hay

    # Elimino Aux_RangoEdad de memoria
    Aux_RangoEdad = []

    return dfs


def marca_cartilla_base(dfs, ruta_aux):

    ########################################################
    ###### AGREGAMOS CARTILLA BASE
    ########################################################

    # Levanto la auxiliar:
    Aux_CartillaBase = pd.read_excel(ruta_aux+"Aux_CartillaBase.xlsx")
    # Pego la informacion al df stock:
    dfs = pd.merge(left = dfs, right = Aux_CartillaBase[["Plan","Modelo_Cartilla"]], left_on='Plan', right_on='Plan', how = "left")
    # Elimino la columna adicional -- no hay

    # Elimino Aux_CartillaBase de memoria
    Aux_CartillaBase = []

    return dfs


def marca_cobertura_med(dfs, ruta_aux):

    ########################################################
    ###### AGREGAMOS COBERTURA MED DEL PLAN
    ########################################################

    # Levanto la auxiliar:
    Aux_CobertMedicamentosPlan = pd.read_excel(ruta_aux+"Aux_CobertMedicamentosPlan.xlsx")
    # Pego la informacion al df stock:
    dfs = pd.merge(left = dfs, right = Aux_CobertMedicamentosPlan[["Plan","Cobertura_med_id", "Cobertura_desc"]], left_on='Plan', right_on='Plan', how = "left")
    # Elimino la columna adicional -- no hay

    # Elimino Aux_CartillaBase de memoria
    Aux_CobertMedicamentosPlan = []

    return dfs


def marca_lista_copagos(dfs, ruta_aux):

    ########################################################
    ###### AGREGAMOS LISTA COPAGOS DEL PLAN
    ########################################################

    # Levanto la auxiliar:
    Aux_ListaCopagos = pd.read_excel(ruta_aux+"Aux_ListaCopagos.xlsx")
    # Pego la informacion al df stock:
    dfs = pd.merge(left = dfs, right = Aux_ListaCopagos[["Plan","Lista_PrecioDesc"]], left_on='Plan', right_on='Plan', how = "left")
    # Elimino la columna adicional -- no hay

    # Elimino Aux_CartillaBase de memoria
    Aux_ListaCopagos = []

    return dfs


def marca_diagnistico(dfs, ruta_diag):

    ########################################################
    ###### AGREGAMOS DIAGNOSTICOS DE LAS PERSONAS
    ########################################################

    dfdiag = pd.read_csv(ruta_diag)

    # Pego la informacion al df stock:
    dfs = pd.merge(left = dfs, right = dfdiag[["Persona",'GC', 'Rango_Cons', 
                'algun_diag', 'prop_diag', 'Marca_Diagnostico']], left_on='Persona', right_on='Persona', how = "left")
    # Elimino la columna adicional -- no hay

    # Elimino Aux_CartillaBase de memoria
    dfdiag = []


    # Reemplazo valores de filas con replace:
    dfs['Diabetes'] = dfs['Diabetes'].replace(['No Diabético', 'Diabético'], [0, 1])
    dfs['Hepatitis'] = dfs['Hepatitis'].replace(['Sin Hepatitis', 'Con Hepatitis'], [0, 1])
    dfs['PMI'] = dfs['PMI'].replace(['NO', 'SI'], [0, 1])
    dfs['Cronico'] = dfs['Cronico'].replace(['NO', 'SI'], [0, 1])
    dfs['Oncologico'] = dfs['Oncologico'].replace(['Sin Marca', 'Paciente Oncológico'], [0, 1])
    dfs['algun_diag'] = dfs['Marca_Diagnostico'].replace(['Sin Diagnostico', 'Con Diagnostico'], [0, 1])


    # TRANSFORMACIONES NECESARIAS

    # Reemplazo los datos faltantes:
    dfs["GC"].fillna(0, inplace = True)
    dfs["Rango_Cons"].fillna("Muy Bajo", inplace = True)
    dfs["algun_diag"].fillna(0, inplace = True)
    dfs["prop_diag"].fillna(0, inplace = True)
    dfs["Marca_Diagnostico"].fillna("Sin Diagnostico", inplace = True)

    # Reemplazo los valores mayores a 1 por 1 para que quede marca de gc 1 o 0
    #en lugar de cantidad de veces siendo GC..
    # Es un reemplazar que funciona al revez: Reemplaza todos los valores que NO CUMPLEN la condición
    dfs['GC'] = dfs['GC'].where(dfs['GC'] == 0 , 1)

    # Reemplazo valores de filas con replace:
    dfs['Sexo'] = dfs['Sexo'].replace(['M', 'F'], [1, 0])
    dfs['Discapacidad'] = dfs['Discapacidad'].replace(['N', 'S'], [0, 1])

    return dfs


def marca_parentesco(dfs):

    # Agregamos los parentescos de los hijos:
    # Método np.select()

    condiciones = [
        (dfs['Parentesco IdParentesco'] == 3),
        (dfs['Parentesco IdParentesco'] == 4),
        (dfs['Parentesco IdParentesco'] == 5),
        (dfs['Parentesco IdParentesco'] == 6),
        (dfs['Parentesco IdParentesco'] == 7)]

    # Lista de resultados en función de las selecciones
    valores_condic = [1,1,1,1,1]     

    # Generación de la columna calculada
    dfs['Marca Hijo'] = np.select(condiciones, valores_condic, default=0) # se analizaron y surgen de consumo 3 == 0

    return dfs



########################################################
###### LECTURA DE VARIABLES CON SETEOS: 
########################################################

# Ruta donde se encuentran los seteos:
#ruta_seteos = txt_seteos































