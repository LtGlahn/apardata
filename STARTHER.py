# -*- coding: utf-8 -*-
"""
Setter opp søkestien slik at du finner NVDB-api funksjonene 
Last ned dette reposet
https://github.com/LtGlahn/nvdbapi-V4
og hardkod inn plasseringen 

"""

import sys
import os 

if not [ k for k in sys.path if 'nvdbapi' in k]: 
    print( "Legger NVDB api til søkestien")
    sys.path.append( '/mnt/c/data/leveranser/nvdbapiV4' )
    # sys.path.append( '/home/jajens/produksjon/nvdbapiV4' )

if not [ k for k in sys.path if 'ruteplan' in k]: 
    print( "Legger ruteplan api til søkestien")
    sys.path.append( '/mnt/c/data/leveranser/ruteplan' )
    # sys.path.append( '/home/jajens/produksjon/ruteplan' )


if not [ k for k in sys.path if '/mnt/c/data/leveranser/datafangst' in k]: 
    print( "Legger datafangst api til søkestien")
    sys.path.append( '/mnt/c/data/leveranser/datafangst' )
    # sys.path.append( '/home/jajens/produksjon/datafangst' )
