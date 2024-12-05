"""
Henter generert endringssett for bomstasjoner og skriver til NVDB
"""

import json
import STARTHER
import skrivnvdb
import requests

if __name__ == '__main__': 
    data = requests.get( 'https://langbein.npra.io/apardata/bomstasjon_endringssett.json' ).json()
    endr = skrivnvdb.endringssett( data )
    endr.forbindelse.login( miljo='prodskriv' )
    endr.registrer()
    endr.startskriving()