"""
Henter generert endringssett for bomstasjoner og skriver til NVDB
"""

import json
import STARTHER
import skrivnvdb
import requests

if __name__ == '__main__': 
    data = requests.get( 'https://langbein.npra.io/apardata/bomstasjon_endringssett.json' ).json()
    assert 'delvisOppdater' in data, "Dette er ikke endringssett for delvisOppdater"
    assert 'vegobjekter' in  data['delvisOppdater'], "Dette er ikke endringssett for delvisOppdater - mangler vegobjekter"
    assert isinstance(  data['delvisOppdater']['vegobjekter'], list  ), "vegobjekter-elementet må være en liste"
    if len( data['delvisOppdater']['vegobjekter'] ) > 0: 

        print( f"{len( data['delvisOppdater']['vegobjekter']) } bomstasjoner har fått nye takster, lagrer til NVDB")

        endr = skrivnvdb.endringssett( data )
        endr.forbindelse.login( miljo='prodskriv' )
        endr.registrer()
        endr.startskriving()

    else: 
        print( f"Ingen takstoppdatering i dag")