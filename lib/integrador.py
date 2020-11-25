""" 
    EL INTEGRADOR CONTIENE LAS LOGICAS DE SQL Y BASE DE DATOS 
"""
import datetime
import os
import time
import logging
import pandas as pd
import pyodbc

def conexSQL(planta,cfg_database):
    """
        METODO REALIZA CONEXION A BD
    """
    try:
        server = cfg_database['server']
        databaseName = planta['db']
        username =  cfg_database['username']
        password = cfg_database['password']
        driver= cfg_database['driver']
        cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+databaseName+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        query = """SELECT MAX((CAST(A.[FECHA] as DATETIME) + CAST(A.[HORA] as DATETIME))) FROM [dbo].[HIST_BLANQUEO] A;"""
        cursor.execute(query)
        logging.info("Comunicacion con base de datos ok")
        return cnxn,cursor
    except Exception as e:
        logging.critical("%s",e)

def updateMatriz(cnxn,cursor,data):
    """ 
        METODO PROCESA LAS DIFERENTES CONFIGURACIONES DE PCKLS DEL MODELO 
    """
    try:
        fechaActual = datetime.datetime.now()
        cont = 0
        query = """ SELECT * FROM [dbo].[PKL_RECOMENDACION_BLANQUEO] """
        cursor.execute(query)
        dataDB = cursor.fetchall()
        cnxn.commit()

        query = """ update [dbo].[PKL_RECOMENDACION_BLANQUEO] set [FECHA_FIN] = ? where [ID] = ? """
        for e in dataDB:
            if(str(e[7]) == "9999-12-31 00:00:00"):
                cursor.execute(query,(fechaActual,e[0]))
        
        cnxn.commit()

        query = """ INSERT INTO [dbo].[PKL_RECOMENDACION_BLANQUEO]
            ([TAG],
            [CLUSTER],
            [MINIMO],
            [CENTRO],
            [MAXIMO],
            [FECHA_INICIO],
            [FECHA_FIN] ) values (?,?,?,?,?,?,?) """
        for index, row in data.iterrows():
            cont = cont +1
            cursor.execute(query, (row['TAG'], int(row['CLUSTER']), float(row['MINIMO']), float(row['CENTRO']),float(row['MAXIMO']),fechaActual,"9999-12-31 00:00:00.000"))

        cnxn.commit()
        logging.info("Matriz de pckls cargada")
    except Exception as e:
        logging.error("%s",e)
def updateCostos(cnxn,cursor,costos):
    """ 
        METODO PROCESA LAS DIFERENTES CONFIGURACIONES DE PCKLS DEL MODELO 
    """
    try:
        fechaActual = datetime.datetime.now()
        cont = 0
        query = """ SELECT * FROM [dbo].[PKL_COSTO_REC_BLANQUEO] """
        cursor.execute(query)
        dataDB = cursor.fetchall()
        cnxn.commit()

        query = """ update [dbo].[PKL_COSTO_REC_BLANQUEO] set [FECHA_FIN] = ? where [ID] = ? """
        for e in dataDB:
            if(str(e[5]) == "9999-12-31 00:00:00"):
                cursor.execute(query,(fechaActual,e[0]))
        
        cnxn.commit()

        query = """ INSERT INTO [dbo].[PKL_COSTO_REC_BLANQUEO]
            ([TAG],
            [CLUSTER],
            [VALOR],
            [FECHA_INICIO],
            [FECHA_FIN] ) values (?,?,?,?,?) """
        for index, row in costos.iterrows():
            cont = cont + 1
            cursor.execute(query, (row['TAG'], int(row['CLUSTER']), float(row['COSTO']),fechaActual,"9999-12-31 00:00:00.000"))

        cnxn.commit()
        logging.info("Costos cargados")
    except Exception as e:
        logging.error("%s",e)

def updateTargets(cnxn,cursor,targets):
    """ 
        METODO PROCESA LAS DIFERENTES CONFIGURACIONES DE PCKLS DEL MODELO 
    """
    try:
        fechaActual = datetime.datetime.now()
        cont = 0
        query = """ SELECT * FROM [dbo].[PKL_TARGET_BLANQUEO] """
        cursor.execute(query)
        dataDB = cursor.fetchall()
        cnxn.commit()

        query = """ update [dbo].[PKL_TARGET_BLANQUEO] set [FECHA_FIN] = ? where [ID] = ? """
        for e in dataDB:
            if(str(e[5]) == "9999-12-31 00:00:00"):
                cursor.execute(query,(fechaActual,e[0]))
        
        cnxn.commit()

        query = """ INSERT INTO [dbo].[PKL_TARGET_BLANQUEO]
            ([TAG],
            [CLUSTER],
            [VALOR],
            [FECHA_INICIO],
            [FECHA_FIN] ) values (?,?,?,?,?) """
        for index, row in targets.iterrows():
            cont = cont + 1
            cursor.execute(query, (row['TAG'], int(row['CLUSTER']), float(row['TARGET']),fechaActual,"9999-12-31 00:00:00.000"))

        cnxn.commit()
        logging.info("Targets cargados")
    except Exception as e:
        logging.error("%s",e)

def updateTime(cnxn,cursor,timegaps):
    """ 
        METODO PROCESA LAS DIFERENTES CONFIGURACIONES DE PCKLS DEL MODELO 
    """
    try:
        fechaActual = datetime.datetime.now()
        cont = 0
        query = """ SELECT * FROM [dbo].[PKL_TIMEGAP_BLANQUEO] """
        cursor.execute(query)
        dataDB = cursor.fetchall()
        cnxn.commit()

        query = """ update [dbo].[PKL_TIMEGAP_BLANQUEO] set [FECHA_ACTUALIZACION] = ? where [ID] = ? """
        for e in dataDB:
            if(str(e[4]) == "9999-12-31 00:00:00"):
                cursor.execute(query,(fechaActual,e[0]))
        
        cnxn.commit()

        query = """ INSERT INTO [dbo].[PKL_TIMEGAP_BLANQUEO]
        ([TAG]
        ,[TIEMPO]
        ,[FECHA_INGRESO]
        ,[FECHA_ACTUALIZACION] ) values (?,?,?,?) """
        for index, row in timegaps.iterrows():
            cont = cont + 1
            cursor.execute(query, (row['TAG'], int(row['TIEMPO']),fechaActual,"9999-12-31 00:00:00.000"))

        cnxn.commit()
        logging.info("Timegaps cargados")
    except Exception as e:
        logging.error("%s",e)