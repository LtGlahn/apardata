import requests 
import json
import sys
from datetime import datetime, timedelta

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkt 

import STARTHER
# if not [ k for k in sys.path if 'nvdbapi' in k]:
#     print( "Adding NVDB api library to python search path")
#     sys.path.append( '/mnt/c/data/leveranser/nvdbapi-V4' )
import nvdbapiv3 

with open( 'SECRET.json' ) as f: 
    secret = json.load( f )

myKey = secret['myAutopassAPARKey']
url = 'https://apar.autopassops.no/api' 

def hentFeltPunkt( stedfesting ): 
    """
    Henter kjørefelt for stedfesting som kommaseparert tekst 
    """
    pos, vid = stedfesting.split( '@' )
    pos = float( pos )
    
    r = requests.get( 'https://nvdbapiles.atlas.vegvesen.no/vegnett/veglenkesekvenser/segmentert/' + \
                    vid  )
    feltoversikt = ''
    if r.ok: 
        mydf = pd.DataFrame( r.json())
        mydf2 = mydf[ (mydf['startposisjon'] <= pos) & (mydf['sluttposisjon'] > pos) ]
        assert len( mydf2 ) == 1, "Skal ha kun ett svar på dette filteret"
        if isinstance( mydf2.iloc[0]['feltoversikt'] , list ): 
            feltoversikt = ','.join( mydf2.iloc[0]['feltoversikt'] ) 
    
    return feltoversikt 


if __name__ == '__main__': 
    headers = { 'Accept' : 'application/json', 
            'Authorization': myKey  }
    
    mappe = '/var/www/html/apardata/'
    # r = requests.get( url + '/operators/100120/tollstations', headers=headers)
    # print("Operatør:", r.status_code, r.text )
    
    
    # params =  { "DFrom" : "2023-01-01T09:00:00Z" } 
    # r2 = requests.get( url + '/tollstations', params=params, headers=headers)
    # print( "Endret etter:", r2.status_code, r2.text )
    
    # Finner alle NVDB bomstasjoner
    nvdbBomst = pd.DataFrame( nvdbapiv3.nvdbFagdata(45).to_records() )
    
    nvdbBomst['stedfest'] = nvdbBomst['relativPosisjon'].astype(str) + '@' + nvdbBomst['veglenkesekvensid'].astype(str)
    nvdbBomst['tilgjengeligeKjfelt'] = nvdbBomst['stedfest'].apply( hentFeltPunkt )
        
    # alle operatør ID 
    operatorId = list( nvdbBomst[  ~nvdbBomst['Operatør_Id'].isnull() ]['Operatør_Id'].unique() )
    operatorId = [ int(x ) for x in operatorId ]

    data = []
    for operator in operatorId: 
        print( f"henter operatør {operator}")
        r = requests.get( url + '/operators/' + str(operator) + '/tollstations', headers=headers)
        if r.ok: 
            data.extend( r.json())
        else: 
            print( f"Fant ingen data for operatørID {operator}: {r.status_code} {r.text} ")
    
    with open( mappe+'apardump.json', 'w') as f:
        json.dump( data, f, ensure_ascii=False, indent=4 )
    
    # # Henter endringer 
    # r = requests.get( url + '/tollstations', headers=headers, params={'DFrom' : '2023-01-01T09:00:00Z' } )
    # if r.ok: 
    #     endret_alle = r.json()
    #     with open( 'endret_bomstasjoner_alle.json', 'w' ) as f: 
    #         json.dump( mappe+endret_alle, f, indent=4, ensure_ascii=False )

    # # Henter endringer etter 1. mars
    # r = requests.get( url + '/tollstations', headers=headers, params={'DFrom' : '2023-03-01T00:00:00Z' } )
    # if r.ok: 
    #     endret_mars = r.json()
    #     with open( 'endret_etter20230301.json', 'w' ) as f: 
    #         json.dump( endret_mars, f, indent=4, ensure_ascii=False )

    # Henter ferske endringer 
    now = datetime.now()
    to_uker_siden = now - timedelta( weeks=2 )
    to_uker_siden = to_uker_siden.replace( hour=0, minute=0, second=0, microsecond=0 )
    r = requests.get( url + '/tollstations', headers=headers, params={'DFrom' : to_uker_siden.isoformat() + 'Z' } )
    if r.ok: 
        endret_sisteuker = r.json()
        with open( mappe + 'endret_bomstasjoner_sisteuker.json', 'w' ) as f: 
            json.dump( endret_sisteuker, f, indent=4, ensure_ascii=False )
    
     