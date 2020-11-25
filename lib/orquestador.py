import os
import logging
from .integrador import conexSQL,updateMatriz,updateCostos,updateTargets,updateTime
from .generador import generateMatrix,generarCostos,generarTargets,generartiemposRecom

def orchestrator(planta,config):
    """Metodo que gestiona la aplicacion"""
    logging.debug("Ejecucion")
    cnxn,cursor = conexSQL(planta,config)

    matriz = generateMatrix(planta)
    updateMatriz(cnxn,cursor,matriz)

    costos = generarCostos(planta)
    updateCostos(cnxn,cursor,costos)

    targets = generarTargets(planta)
    updateTargets(cnxn,cursor,targets)

    timeGap = generartiemposRecom(planta)
    updateTime(cnxn,cursor,timeGap)

    cnxn.close()
    logging.debug("Programa finalizado")