"""UPDATER"""
import time
import logging
import json
from lib.orquestador import orchestrator
import sys
import os
import requests

if __name__ == '__main__':
    
    with open('config.json') as config_file:
        config = json.load(config_file)
    
    format = "%(levelname)s: %(asctime)s | %(module)s - %(funcName)s [%(lineno)d] | %(message)s"
    
    logging.basicConfig(filename='app.log',format=format,level='DEBUG')

    try:
        arg = str(sys.argv[1])
    except:
        arg = None

    print(arg)

    if(arg != None):
        if(arg == "pacifico"):
            orchestrator(config['pacifico'],config)
        if(arg == "santafe"):
            orchestrator(config['santafe'],config)
        if(arg == "laja"):
            orchestrator(config['laja'],config)
    else:
        print("""Debe escribir el nombre de la planta que va a actualizar\n""")
        print(""" Ejemplo: "python app.py santafe" \n""")
        os._exit(-1)