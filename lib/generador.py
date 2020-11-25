import os
import time
import logging
import pandas as pd

def generarMatriz(planta):
    """ Metodo que crea un dataframe con varios archivos pckls """
    try:
        cluster = []
        minimos = []
        centros = []
        maximos = []
        tags = []
        elementos = planta['modelos']
        for indice in elementos:
            centros_rec = pd.read_pickle(indice.get('ruta') + '\centros_blanqueoC.pkl')
            centros_resp = pd.read_pickle(indice.get('ruta') + '\centros_blanqueoR.pkl')
            centros_all = pd.read_pickle(indice.get('ruta') + '\centros_blanqueoA.pkl')
            max_rec = pd.read_pickle(indice.get('ruta') + '\max_rec.pkl')
            min_rec = pd.read_pickle(indice.get('ruta') + '\min_rec.pkl')
            lista_ordenana = []

            aux = min_rec.get(0)
            for key,valores in aux.items():
                lista_ordenana.append(key)
            
            for key,valores in min_rec.items():
                for indice in lista_ordenana:
                    for k,v in valores.items():
                        if(indice == k):
                            tags.append(k)
                            minimos.append(v)
                            cluster.append(key)

            for key,valores in max_rec.items():
                for indice in lista_ordenana:
                    for k,v in valores.items():
                        if(indice == k):
                            maximos.append(v)
            
            for key,valores in centros_rec.items():
                for indice in lista_ordenana:
                    for k,v in valores.items():
                        if(indice == k):
                            centros.append(v)

        data = pd.DataFrame({'TAG': tags, 'CLUSTER': cluster, 
                                'MINIMO':minimos, 'CENTRO':centros, 
                                'MAXIMO':maximos})
        return data
    except Exception as e:
        logging.error("%s",e)

def generarCostos(planta):
    """ Metodo que crea un dataframe de costos con los archivos pckls """
    try:
        costos = []
        tags = []
        cluster = []
        elementos = planta['modelos']

        for indice in elementos:
            centros_all = pd.read_pickle(indice.get('ruta') + '\centros_blanqueoA.pkl')
            aux= str(centros_all[1])
            aux = aux.split()

            for e, element in centros_all.items():
                for k,v in element.items():
                    if(k.find("cost") != -1):
                        aux = str(k)
                        costos.append(v)
                        tags.append(aux)
                        cluster.append(e)
                
        data = pd.DataFrame({'TAG': tags,'CLUSTER': cluster,'COSTO': costos})

        return data
    except Exception as e:
        logging.error("%s",e)

def generarTargets(planta):
    """ Metodo que crea un dataframe de targets con los archivos pckls """
    try:
        targets = []
        tags = []
        cluster = []
        aux = ""
        elementos = planta['modelos']

        for indice in elementos:
            centros_resp = pd.read_pickle(indice.get('ruta') + '\centros_blanqueoR.pkl')
            for e, element in centros_resp.items():
                for k,v in element.items():
                    targets.append(v)
                    tags.append(k)
                    cluster.append(e)

        data = pd.DataFrame({'TAG': tags,'CLUSTER': cluster,'TARGET': targets})

        return data
    except Exception as e:
        logging.error("%s",e)


def generartiemposRecom(planta):
    """ Metodo que crea un dataframe de timegaps con los archivos pckls """
    try:
        tiempos = []
        tags = []
        elementos = planta['modelos']

        for indice in elementos:
            timegap = pd.read_pickle(indice.get('ruta') + '\\tiempos_rec.pkl')

            for e, element in timegap.items():
                tags.append(e)
                tiempos.append(element)
        
        data = pd.DataFrame({'TAG': tags,'TIEMPO': tiempos})

        return data
    except Exception as e:
        logging.error("%s",e)