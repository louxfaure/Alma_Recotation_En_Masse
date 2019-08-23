#!/usr/bin/python3
# -*- coding: utf-8 -*-
#Modules externes
import os
import re
import logging
import csv
import xml.etree.ElementTree as ET

#Modules maison
from Abes_Apis_Interface.AbesXml import AbesXml
from Alma_Apis_Interface import Alma_Apis_Records
from Alma_Apis_Interface import Alma_Apis
from logs import logs

SERVICE = "Recotation_en_masse"

LOGS_LEVEL = 'INFO'
LOGS_DIR = os.getenv('LOGS_PATH')

LIBRARY_CODE = 1601900000

REGION = 'EU'
INSTITUTION = 'ub'
API_KEY = os.getenv('TEST_UB_API')

# IN_FILE = '/home/loux/Téléchargements/Fichier_de_test.csv'
IN_FILE = 'Echantillon.csv'
OUT_FILE = 'Rapport_traitement.csv'

def item_change_location(item,location,call):
    """Change location and remove holdinds infos
    
    Arguments:
        item {str} -- xml response of get item ws
        location {str} -- new location_code
        call {str} -- new call

    Returns:
        [str] -- mms_id, holding_id, pid
    """
    mms_id, holding_id, pid = item.find(".//mms_id").text, item.find(".//holding_id").text, item.find(".//pid").text
    item.find(".//item_data/location").text = location
    item.find(".//item_data/alternative_call_number").text = ''
    item.find(".//item_data/alternative_call_number_type").text = ''
    # item.find(".//item_data/location").text = location
    holding_data = item.find(".//holding_data")
    item.remove(holding_data)
    if mms_id in processed_record_dict:
            if location_code in processed_record_dict[mms_id]:
                item.find(".//item_data/alternative_call_number").text = call
    return mms_id, holding_id, pid

def update_holding_data(holding,new_call):
    """Change call (852$$h) and reset call type (852 fiest indicator)
    
    Arguments:
        holding {str} -- response of get holding ws 
        new_call {str} -- new value for call subfield
    
    Returns:
        str -- changed data
    """
    holding_data = ET.fromstring(holding)
    location_field =holding_data.find(".//datafield[@tag='852']")
    location_field.set('ind1', ' ')
    call_subfield = holding_data.find(".//datafield[@tag='852']/subfield[@code='h']")
    call_subfield.text = new_call
    return ET.tostring(holding_data)

#Init logger
logs.init_logs(LOGS_DIR,SERVICE,LOGS_LEVEL)
log_module = logging.getLogger(SERVICE)


conf = Alma_Apis.Alma(apikey=API_KEY, region='EU', service=SERVICE)
alma_api = Alma_Apis_Records.AlmaRecords(apikey=API_KEY, region=REGION, service=SERVICE)

#We get all the locations for the library in a dictionnary
locations_dict = conf.get_locations(LIBRARY_CODE)
log_module.info("Liste des localisation chargée pour la bibliothèque {} :: Main :: Début du traitement".format(LIBRARY_CODE))

report = open(OUT_FILE, "w")
report.write("Code-barres\t")


# Case 00720135 L'apis PUT ITEM ne fonctionne pas correctement.
#Si plusieurs exemplaires d'un même titre sont relocalisés vers la même localisation
#Elle créé une holding par exemplaire
#Je met donc provisoirement en place un rapport pour identifie ces cas
processed_record_dict = {}
multivol = open('doublons_holdings.csv', "w")

with open(IN_FILE, newline='') as f:
    reader = csv.reader(f, delimiter=';')
    headers = next(reader)
    # We read the file
    for row in reader:
        barcode = row[0]
        # Test if new call is defined
        if row[1] is None or row[1] == '':
            log_module.error("{} :: Echec :: pas de cote fournie".format(barcode))
            report.write("{}\tErreur Fichier\tPas de cote fournie\n".format(barcode))
            continue
        call = row[1]
        # Test if new localisation is defined
        if row[3] is None or row[3] == '':
            log_module.error("{} :: Echec :: pas de localisation fournie".format(barcode))
            report.write("{}\tErreur Fichier\tPas de localisation fournie\n".format(barcode))
            continue
            # log_module.info("{} :: Main :: Début du traitement".format(barcode))
        # Transform location label in  location code
        if row[3] not in locations_dict:
            log_module.error("{} :: Echec :: La localisation {} est inconnue dans Alma".format(barcode,row[3]))
            report.write("{}\tErreur Fichier\tLa localisation '{}' est inconnue dans Alma\n".format(barcode,row[3]))
            continue
        location_code = locations_dict[row[3]]
        log_module.debug("{} :: Succes :: A affecter dans la localisation {}".format(barcode,location_code))
        
        ###Update item sequence
        # ###################### 
        # Get datas item with barcode
        status, response = alma_api.get_item_with_barcode(barcode)
        if status == 'Error':
            log_module.error("{} :: Echec :: {}".format(barcode,response))
            report.write("{}\tErreur Retrouve Exemplaire\t{}\n".format(barcode,response))
            continue
        # Change location and remove holdinds infos
        item = ET.fromstring(response)
        mms_id, old_holding_id,item_id = item_change_location(item,location_code, call)
        # log_module.debug("{} :: {} - {} - {}".format(barcode,mms_id,old_holding_id,item_id))
        # Upadte item in Alma
        set_status, set_response = alma_api.set_item(mms_id, old_holding_id,item_id,ET.tostring(item))
        log_module.debug(set_response)
        if set_status == 'Error':
            log_module.error("{} :: Echec :: {}".format(barcode,set_response))
            report.write("{}\tErreur Mise à jour Exemplaire\t{}\n".format(barcode,set_response))
            continue
        changed_item = ET.fromstring(set_response)
        new_holding_id = changed_item.find(".//holding_id").text
        log_module.debug("{} :: Succes :: L'exemplaire est maintenant rattaché à la Holding {}".format(barcode,new_holding_id))

        ###Update holding sequence
        ##########################
        # Get new holding
        get_holding_status, get_holding_response = alma_api.get_holding(mms_id, new_holding_id)
        if get_holding_status == 'Error':
            log_module.error("{} :: Echec :: {}".format(barcode,get_holding_response))
            report.write("{}\tErreur Retrouve Holding\t{}\n".format(barcode,get_holding_response))
            continue
        changed_holding = update_holding_data(get_holding_response,call)
        #Update new Holding in Alma
        set_holding_status, set_holding_response = alma_api.set_holding(mms_id, new_holding_id,changed_holding)
        if set_holding_status == 'Error':
            log_module.error("{} :: Echec :: {}".format(barcode,set_holding_response))
            report.write("{}\tErreur Retrouve Exemplaire\t{}\n".format(barcode,set_holding_response))
            continue
        log_module.debug(set_holding_response)
        #Case 00720135
        if mms_id in processed_record_dict:
            if location_code in processed_record_dict[mms_id]:
                multivol.write("{}\n".format(mms_id))
            else:
                processed_record_dict[mms_id] = {
                    location_code: {
                        'call' : call
                    }
                }
        else:
            processed_record_dict[mms_id] = {
                location_code: {
                    'call' : call
                }
            }
        log_module.info("{} :: Succes :: La Holding {} a été mis à jour".format(barcode,new_holding_id))
        report.write("{}\tSucces\tExemplaire relocalisé\n".format(barcode))
report.close
#Case 00720135
multivol.close
log_module.info("FIN DU TRAITEMENT")

                    