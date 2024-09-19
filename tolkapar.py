import requests 
import json
import sys
from datetime import datetime

import numpy as np 
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkt 
from pyproj import Transformer
from shapely import wkb, wkt

if not [ k for k in sys.path if 'nvdbapi' in k]:
    print( "Adding NVDB api library to python search path")
    sys.path.append( '/mnt/c/data/leveranser/nvdbapi-V3' )
import nvdbapiv3 
import nvdbgeotricks

def lagStedfesting( row ): 
    """
    Avgjør om det er stedfesting på kj.felt 1 (med metrering) eller 2 (mot metrering) basert på bomstasjondata og vegnett fra NVDB

    Tar hensyn til om metreringen er snudd (kj.felt 1 == MOT, kj.felt 2 == MED) på stedet 

    ARGUMENTS
        row: Dictionary eller pandas Series 

    KEYWORDS: 
        N/A
    
    RETURNS 
        tallet 0 (ingen stedfesting på kj.felt), 1 (kj.felt 1, evt 1,3,5,..) eller 2 
    """
    def byttom( tall:int) -> int:  
        """
        Bytter om på stedfesting hvis metrering er snudd: 
            2 -> 1
            1 -> 2 
            0 -> 0 
        """
        if tall == 2: 
            return 1
        elif tall == 1: 
            return 2
        elif tall == 0: 
            return 0
        else: 
            raise ValueError( "Input skal være heltall 0, 1 eller 2")

    stedfest = 0 
    # datatype float betyr at vi har verdien np.isnan
    if row['Innkrevningsretning'] == 'Begge retninger' or isinstance( row['Innkrevningsretning'], float ): 
        stedfest = 0 
    elif row['Innkrevningsretning'] == 'Med metrering': 
        stedfest = 1
    elif row['Innkrevningsretning'] == 'Mot metrering': 
        stedfest = 2
    else: 
        raise ValueError( f"Ugyldig dataverdi for innkrevingsretning {row['Innkrevningsretning']}")

    if row['segmentretning'] == 'MOT': 
        stedfest = byttom( stedfest )

    return stedfest 

def hentFeltPunkt( stedfesting ): 
    """
    Henter kjørefelt for stedfesting som kommaseparert tekst 
    """
    pos, vid = stedfesting.split( '@' )
    pos = float( pos )
    
    r = requests.get( 'https://nvdbapiles-v3.atlas.vegvesen.no/vegnett/veglenkesekvenser/segmentert/' + \
                    vid + '.json' )
    feltoversikt = ''
    if r.ok: 
        mydf = pd.DataFrame( r.json())
        mydf2 = mydf[ (mydf['startposisjon'] <= pos) & (mydf['sluttposisjon'] > pos) ]
        assert len( mydf2 ) == 1, "Skal ha kun ett svar på dette filteret"
        if isinstance( mydf2.iloc[0]['feltoversikt'] , list ): 
            feltoversikt = ','.join( mydf2.iloc[0]['feltoversikt'] ) 
    
    return feltoversikt 

def tellAparFelt( row, apardata ) -> int: 
    """
    Teller hvor mange apar-oppføringer (dvs antall kjørefelt-oppføringer fra APAR) som matcher en NVDB bomstasjon

    ARGUMENTS
        row : pandas series eller dictionary med en NVDB bomstasjon 

        apardata: pandas dataframe med apardata 

    KEYWORDS: 
        N/A

    RETURNS 
        antallAparFelt: int 

    """

    treff = apardata[( apardata['operatorId'] == row['Operatør_Id']) & (apardata['tollStationCode'] == row['Bomstasjon_Id'])]
    return len( treff )


def vurderStedfest( row ) -> str: 
    """
    Integritetssjekk - sammenligner innkrevingsretning med stedfesting og retning på vegnett 
    """
    skalHaStedfest = lagStedfesting( row )
    harStedfest = 0
    felt = row['stedfesting_felt']
    # hvis felt er float, ikke string så er det fordi felt = np.nan 
    if isinstance( felt, float ) or row['stedfesting_felt'] == row['tilgjengeligeKjfelt'] or ( '1' in row['stedfesting_felt'] and '2' in row['stedfesting_felt'] ): 
        harStedfest = 0
    elif '1' in row['stedfesting_felt'] and '2' not in row['stedfesting_felt']: 
        harStedfest = 1
    elif '2' in row['stedfesting_felt'] and '1' not in row['stedfesting_felt']: 
        harStedfest = 2
    else: 
        raise ValueError( 'vurderstedfest: Ugyldig datakombinasjon')

    if skalHaStedfest == harStedfest: 
        return 'OK'
    elif skalHaStedfest > 0 and harStedfest == 0: 
        return f"Innkr sier felt {skalHaStedfest}"
    elif skalHaStedfest > 0 and harStedfest > 0: 
        return f"Snudd stedfesting {harStedfest} ift metrering {skalHaStedfest}"
    elif skalHaStedfest == 0 and harStedfest > 0: 
        return f"Skal IKKE ha kj.felt stedfesting"
    else: 
        raise ValueError (f"Rar datakombinasjon: Skal ha stedfest={skalHaStedfest} har stedfest={harStedfest}")


def finnTakst( row, takstType={ 'vehicle' : 'smallVehicle', 'priceType' : 'priceNoRebate' } ) -> float: 
    """
    Finner takst liten bil uten rabatt (hvis den finnes for dagens dato)
    """
    pris = np.nan 
    if takstType['vehicle'] in row and isinstance( row[ takstType['vehicle'] ], dict) and takstType['priceType'] in row[ takstType['vehicle'] ]:
        prisListe = row[ takstType['vehicle'] ][ takstType['priceType'] ] 
        if len( prisListe ) > 0: 
            prisListe = [ { 'price' :  x['price'], 'activeFrom' : datetime.fromisoformat( x['activeFrom'] ), 'activeTo' : datetime.fromisoformat( x['activeTo'] ) } for x in prisListe ]
            now = datetime.now()
            dennePrisen = [ x for x in prisListe if x['activeFrom'] <= now and x['activeTo'] > now]
            assert len( dennePrisen) <= 1, f"Fant {len(dennePrisen)} ulike priser av type  {takstType} for tidspunkt {now}"
            if len( dennePrisen ) == 1: 
                pris = dennePrisen[0]['price']

    return pris

def finnAparTakst2nvdbData( row, apardata, takstType={ 'vehicle' : 'smallVehicle', 'priceType' : 'priceNoRebate' } ) -> float: 
    """
    Bruker funksjonen finntakst for å finne APAR-pris som matcher NVDB bomstasjon 
    """

    pris = np.nan
    aparmatch = apardata[ (row['Operatør_Id'] == apardata['operatorId']) |  (row['Bomstasjon_Id'] == apardata['tollStationCode'])]
    priser = set()
    for junk, row in aparmatch.iterrows(): 
        priser.add( finnTakst( aparmatch.iloc[0], takstType=takstType ))

    if len( priser ) > 1: 
        print( f"{row['nvdbId']} {row['Navn bomstasjon']} operator={row['Operatør_Id']} key={row['Bomstasjon_Id']} har {len(priser)} prisoppføringer i APAR ")
    elif len( priser ) == 1: 
        pris = list( priser)[0]

    return pris

if __name__ == '__main__': 
    # with open( 'apardump.json') as f: 
    with open( 'takstendringMai2024/endret_bomstasjoner_sisteuker20240527.json') as f: 
        apardump = json.load( f )
    apardata = pd.DataFrame( apardump )

    # Fjerner duplikater
    apardata.drop_duplicates( subset='tollStationKey', inplace=True )

    apardata['lat'] = apardata['positionY'].apply( lambda x : float(x) if x and len(x.strip()) > 3 else np.nan )
    apardata['lon'] = apardata['positionX'].apply( lambda x : float(x) if x and len(x.strip()) > 3 else np.nan )
    # apardata['APAR takst liten bil'] = apardata.apply( finnTakst, axis=1 )

    # nvdbJson = nvdbapiv3.nvdbFagdata(45).to_records() 
    # with open( 'nvdbdump.json', 'w') as f: 
    #     json.dump( nvdbJson, f, indent=4, ensure_ascii=False )
    nvdbAlle = pd.DataFrame( nvdbapiv3.nvdbFagdata(45).to_records( relasjoner=False ) )
    nvdbAlle['stedfest'] = nvdbAlle['relativPosisjon'].astype(str) + '@' + nvdbAlle['veglenkesekvensid'].astype(str)
    nvdbAlle['tilgjengeligeKjfelt'] = nvdbAlle['stedfest'].apply( hentFeltPunkt )

    # nvdbAlle = pd.read_excel( 'nvdbBomst.xlsx' )

    # with open( 'nvdbdump.json') as f:
        # nvdbJson = json.load( f )
    # nvdbBomst = pd.DataFrame( nvdbJson )
    # nvdbAlle  = pd.DataFrame( nvdbJson )
    
    vegkartURL = 'https://vegkart.atlas.vegvesen.no/#valgt:'
    nvdbAlle['vegkart lenke'] = vegkartURL + nvdbAlle['nvdbId'].astype( 'str') + ':45' 

    nvdbBomst = nvdbAlle.copy()
    nvdbBomst['stedfesting QA'] = nvdbAlle.apply( vurderStedfest, axis=1 )
    nvdbBomst[ 'Antall APAR felt'] = nvdbAlle.apply( lambda row: tellAparFelt(row, apardata), axis=1)
    nvdbBomst['APAR takst liten bil'] = nvdbBomst.apply( lambda row: finnAparTakst2nvdbData( row, apardata ), axis=1 )
    nvdbBomst['APAR takst stor bensinbil'] = nvdbBomst.apply( lambda row: finnAparTakst2nvdbData( row, apardata, takstType={'vehicle' : 'largePetrol', 'priceType' : 'priceNoRebate' } ), axis=1 )

    # Hvilke NVDB-bomstasjoner mangler operatør ID og Bomstasjon ID? 
    nvdb_uten_autopasskobling = nvdbBomst[ (nvdbBomst['Operatør_Id'].isnull() ) | (nvdbBomst['Bomstasjon_Id'].isnull() )]
    print( f"{len( nvdb_uten_autopasskobling )} bomstasjoner uten Operatør ID eller Bomstasjon ID")
    if len( nvdb_uten_autopasskobling[ ~nvdb_uten_autopasskobling['Operatør_Id'].isnull() ] ) > 0: 
        print( f"ALARM - {len( nvdb_uten_autopasskobling[ ~nvdb_uten_autopasskobling['Operatør_Id'].isnull() ] )} bomstasjoner mangler Bomstasjon ID")
    if len( nvdb_uten_autopasskobling[ ~nvdb_uten_autopasskobling['Bomstasjon_Id'].isnull() ] ) > 0: 
        print( f"ALARM - {len( nvdb_uten_autopasskobling[ ~nvdb_uten_autopasskobling['Bomstasjon_Id'].isnull() ] )} bomstasjoner mangler Operatør ID")

    nvdbBomst = nvdbBomst[ ~nvdbBomst['nvdbId'].isin( nvdb_uten_autopasskobling['nvdbId'].to_list() )]
    nvdbBomst2 = nvdbBomst.copy()

    # Finner de bomstasjonene der vi har mer enn 1 forekomst i NVDB på operatør ID + bomstasjon ID 
    nvdb_duplikatId = nvdbBomst[  nvdbBomst.duplicated( subset=['Operatør_Id', 'Bomstasjon_Id'], keep=False ) ]
    print( f"Flertydig NVDB-representasjon: {len(nvdb_uten_autopasskobling)} bomstasjoner på {len(nvdb_duplikatId['Operatør_Id'].unique())} operatører")

    nvdbBomst = nvdbBomst[  ~nvdbBomst['nvdbId'].isin( nvdb_duplikatId['nvdbId'].to_list() ) ]

    apardata.drop( columns='nvdbId', inplace=True  )
    merged = pd.merge(  apardata, nvdbBomst, left_on=[ 'operatorId', 'tollStationCode' ], right_on=['Operatør_Id', 'Bomstasjon_Id'], how='inner'  )

    geometrikontroll = merged[ ( ~merged['lat'].isnull()) | ( ~merged['lon'].isnull() )].copy()
    geometrikontroll['nvdbgeom'] = geometrikontroll['geometri'].apply( wkt.loads )
    geometrikontroll = gpd.GeoDataFrame( geometrikontroll, geometry=gpd.points_from_xy( geometrikontroll['lon'], geometrikontroll['lat'] ), crs=4326 )
    geometrikontroll = geometrikontroll.to_crs( 5973 )

    geometrikontroll['geomavstand_nvdb_autopass'] = geometrikontroll.apply( lambda row: row['nvdbgeom'].distance(row['geometry']), axis=1)

    mycols = [ 'operatorId', 'operatorName', 'tollStationKey', 'tollStationCode',
       'tollStationName', 'projectNumber', 'projectName', 'link',
       'tollStationLane', 'tollStationDirection', 'smallVehicle',
       'smallDiesel', 'smallPetrol', 'smallChargableHybrid', 'smallElectric',
       'smallHydrogen', 'euro5', 'euro6', 'largeElectric', 'largeHydrogen',
       'largeHybrid', 'largePetrol', 'monthlyMaximumCharges',
       'priceDifferentiationTime', 'rushHour', 'timeRuleType',
       'timeRuleDuration', 'timeRuleGroup', 'freeHandicap', 'positionX',
       'positionY', 'positionSrid', 'lat', 'lon', 'objekttype', 'nvdbId',
       'versjon', 'startdato', 'Tidsdifferensiert takst', 'Timesregel',
       'Innkrevningsretning', 'Navn bompengeanlegg (fra CS)', 'Takst stor bil',
       'Link til bomstasjon', # 'APAR takst liten bil', 
       'Takst liten bil', 
       'Operatør_Id',
       'Bomstasjonstype', 'Navn bomstasjon', 'Bomstasjon_Id',
       'Gratis gjennomkjøring ved HC-brikke', #  'Bompengeanlegg_Id',
       'relasjoner', 'veglenkesekvensid', 'detaljnivå', 'typeVeg', 'kommune',
       'fylke', 'vref', 'veglenkeType', 'vegkategori', 'fase', 'vegnummer',
       'relativPosisjon', 'adskilte_lop', 'trafikantgruppe', 'geometri',
       'Rushtid morgen, til', 'Rushtidstakst liten bil',
       'Rushtidstakst stor bil', 'Timesregel, passeringsgruppe',
       'Timesregel, varighet', 'Etableringsår', 'Rushtid ettermiddag, fra',
       'Rushtid ettermiddag, til', 'Rushtid morgen, fra', 'Vedtatt til år',
       'Vedlikeholdsansvarlig', 'Eier', 'Prosjektreferanse',
       'Tilleggsinformasjon', 'segmentretning', 'ProsjektInternObjekt_ID',
       'nvdbgeom', 'geometry', 'geomavstand_nvdb_autopass' ]
    

    geomcols = [ 'operatorId', 'operatorName', 'tollStationKey', 'tollStationCode',
       'tollStationName', 'tollStationLane', 'tollStationDirection', 'nvdbId',
       'Innkrevningsretning', 'Navn bompengeanlegg (fra CS)',  'Navn bomstasjon',  'kommune',
       # 'APAR takst liten bil', 'Takst liten bil', 
        'vref', 'geomavstand_nvdb_autopass',  'Antall APAR felt', 'stedfesting QA', 'geometry']

    # geometrikontroll[geomcols].to_file( 'aparkontroll.gpkg', layer='geometrikontroll_enkel_juni', driver='GPKG')

    # Disse koblingene er vi skråsikre på
    mergedcols = [ 'operatorId', 'operatorName', 'tollStationKey', 'tollStationCode',
       'tollStationName', 'tollStationLane', 'tollStationDirection', 'APAR takst liten bil', 'Takst liten bil',  'nvdbId',
       'Innkrevningsretning',  'stedfesting_felt', 'tilgjengeligeKjfelt',  'stedfesting QA', 'Antall APAR felt', 
        'segmentretning',  'Navn bompengeanlegg (fra CS)',  'Navn bomstasjon',  'kommune',
        'vref', 'vegkart lenke' ]
    # nvdbgeotricks.skrivexcel( 'enkelNVDBkobling.xlsx',  merged[ mergedcols] )

    # Mer avansert flertydig kobling 
    flertydig = pd.merge(  apardata, nvdb_duplikatId, left_on=[ 'operatorId', 'tollStationCode' ], right_on=['Operatør_Id', 'Bomstasjon_Id'], how='inner'  )
    flertydig['geometry'] = flertydig['geometri'].apply( wkt.loads )
    flertydig = gpd.GeoDataFrame( flertydig, geometry='geometry', crs=5973 )
    # flertydig[ mergedcols + ['geometry'] ].to_file( 'aparkontroll.gpkg', layer='flertydigkobling', driver='GPKG')

    flere = flertydig.groupby( ['operatorId', 'tollStationCode'] ).agg( { 'tollStationName' : 'unique',  'nvdbId' : 'unique', 
                                                                'tollStationLane' : 'unique', 'tollStationKey' : 'unique', 
                                                                'tollStationDirection' : 'unique' } ).reset_index()
    

    flere['tollStationName']      = flere['tollStationName'].apply( lambda x : ','.join( [ str(y) for y in x ] )  )
    flere['nvdbId']               = flere['nvdbId'].apply( lambda x : ','.join( [ str(y) for y in x ] )  )
    flere['tollStationLane']      = flere['tollStationLane'].apply( lambda x : ','.join( [ str(y) for y in x ] )  )
    flere['tollStationKey']       = flere['tollStationKey'].apply( lambda x : ','.join( [ str(y) for y in x ] )  )
    flere['tollStationDirection'] = flere['tollStationDirection'].apply( lambda x : ','.join( [ str(y) for y in x ] )  )
    
    # nvdbgeotricks.skrivexcel( 'flertydigkobling.xlsx', flere )

    # Er det noen APAR-data som ikke er koblet mot NVDB? 
    apar_koblede = list( merged['tollStationKey'].unique() ) + list( flertydig['tollStationKey'].unique() )

    apar_uten_kobling = apardata[ ~apardata['tollStationKey'].isin( apar_koblede )]
    aparcols = ['operatorId', 'operatorName', 'tollStationKey', 'tollStationCode',
       'tollStationName', 'projectNumber', 'projectName', 'link',
       'tollStationLane', 'tollStationDirection', 'smallVehicle', # 'APAR takst liten bil',
        'lat', 'lon']
    

    # Av disse APAR-stasjonene som mangler NVDB-kobling, hvem mangler aktiv prisinformasjon? 
    apar_utenpris = apar_uten_kobling[ apar_uten_kobling['smallVehicle'].isnull() ]

    # Av disse APAR-stasjonene som mangler NVDB-kobling, hvem har aktiv prisinformasjon? 
    apar_utenkobling_medpris = apar_uten_kobling[ ~apar_uten_kobling['smallVehicle'].isnull() ]



    # Hvilke NVDB-bomstasjoner mangler kobling til APAR? 
    nvdb_koblede = list( merged['nvdbId'].unique() ) + list( flertydig['nvdbId'].unique() )    
    nvdb_utenkobling = nvdbAlle[ ~nvdbAlle['nvdbId'].isin( nvdb_koblede )]
    nvdbCol = [ 'nvdbId', 'vegkart lenke', 'Innkrevningsretning',
       'Navn bompengeanlegg (fra CS)', 'Link til bomstasjon',
        'Operatør_Id', 'Navn bomstasjon',
       'Bomstasjon_Id',
       # 'Bompengeanlegg_Id',
        'kommune','vref', 'Vedlikeholdsansvarlig',
       'Eier', 'Prosjektreferanse', 'Tilleggsinformasjon', 
       'ProsjektInternObjekt_ID']
    
    stedfestingQAcol =  [  'nvdbId', 'Navn bompengeanlegg (fra CS)',  'Navn bomstasjon', 'Operatør_Id', 'Bomstasjon_Id',
                         'APAR takst liten bil', 'Takst liten bil', 
                         'APAR takst stor bensinbil', 'Takst stor bil', 
                          'Innkrevningsretning',  'stedfesting_felt', 'tilgjengeligeKjfelt',  'stedfesting QA', 'Antall APAR felt', 
                        'segmentretning',   'kommune',
                        'vref', 'vegkart lenke' ]
    
    takstavvik = merged[ merged['Takst liten bil'] != merged['APAR takst liten bil'] ]

    takstavvik_geom = takstavvik.copy()
    takstavvik_geom['geometry'] = takstavvik_geom['geometri'].apply( wkt.loads )
    takstavvik_geom = gpd.GeoDataFrame( takstavvik_geom, geometry='geometry' )
    takstavvik_geom[ mergedcols + ['geometry'] ].to_file( 'takstavvik.gpkg')


    nvdbgeotricks.skrivexcel( 'koblingNvdbAutopass.xlsx', 
                          [ merged[mergedcols], flere, apar_utenkobling_medpris[aparcols], nvdb_utenkobling[nvdbCol],  apar_utenpris[aparcols], nvdbBomst[stedfestingQAcol], takstavvik[mergedcols] ], 
        sheet_nameListe = ['Enkel kobling', 'Flertydig kobling', 'APAR uten kobling', 'Nvdb uten kobling', 'inaktive Apar', 'NVDB stedfesting QA', 'Takst avvik'] )
    

    betalingskolonne = [ 'smallVehicle',
       'smallDiesel', 'smallPetrol', 'smallChargableHybrid', 'smallElectric',
       'smallHydrogen', 'euro5', 'euro6', 'largeElectric', 'largeHydrogen',
       'largeHybrid', 'largePetrol', 'monthlyMaximumCharges',
       'priceDifferentiationTime', 'rushHour', 'timeRuleType',
       'timeRuleDuration', 'timeRuleGroup', 'freeHandicap' ]
    
    # Ny versjon av geometrikontroll: 
    trans = Transformer.from_crs( "EPSG:4326", "EPSG:25833" )
    temp = apardata[ ~apardata['positionX'].isnull()].copy()
    temp = temp[  temp['positionX'] != '' ].copy()
    temp['_myKey'] = temp['operatorId'].astype(str) + '_' + temp['tollStationCode'].astype(str)
    myList = []
    for key in list( temp['_myKey'].unique()):
        temp2 = temp[ temp['_myKey'] == key]
        nvdb = nvdbAlle[ (nvdbAlle['Operatør_Id'] == temp2.iloc[0]['operatorId'] ) & (nvdbAlle['Bomstasjon_Id'] == temp2.iloc[0]['tollStationCode'] ) ]


        if len( nvdb ) == 0: 
            nvdbId = -999
            navn = f"Finner ikke NVDB bomstasjon med operatør={temp2.iloc[0]['operatorId']} og ID={temp2.iloc[0]['tollStationCode']}"
        elif len( nvdb ) == 1: 
            nvdbId = nvdb.iloc[0]['nvdbId']
            navn   = nvdb.iloc[0]['Navn bomstasjon']
        else: 
            nvdbId = -1 
            navn  = f"Flere NVDB bomstasjoner: {','.join( nvdb['Navn bomstasjon'].to_list() )}"

        print( f"Analyserer NVDB bomstasjoner: {navn}")

        count = 0
        for junk, row in temp2.iterrows():
            count += 1
            data = { 'NVDB navn'            : navn, 
                     'NVDB Id'              : nvdbId,
                      'tollStationLane'     : row['tollStationLane'], 
                     'tollStationDirection' : row['tollStationDirection'], 
                     'tollStationName'      : row['tollStationName'], 
                     'operatorId'           : row['operatorId'],
                     'tollStationKey'       : row['tollStationKey'], 
                     'projectNumber'        : row['projectNumber'], 
                     'projectName'          : row['projectName'], 
                     'tollStationCode'      : row['tollStationCode'], 
                    }
            # Henter geometri - enten fra APAR eller fra NVDB
            if row['positionX'] and len( row['positionX'].strip() ) > 3:
                X, Y  = trans.transform( float( row['positionY']), float( row['positionX'] ) )
                Y += count
                data['geometry'] = Point( X, Y)
            else:
                print(f"Mangler geometri for APAR-oppføring {data['tollStationName']} {data['tollStationKey']} ")
                data['geometry'] = wkt.loads( nvdb.iloc[0]['geometri'] ) 

            myList.append( data )


    aparRediger = pd.DataFrame( myList )
    aparRediger = gpd.GeoDataFrame( aparRediger, geometry='geometry', crs=25833 )

    nvdbBomst2['geometry'] = nvdbBomst2['geometri'].apply( lambda x : Point ( wkb.loads( wkb.dumps( wkt.loads( x ), output_dimension=2  ))))
    nvdbBomst2 = gpd.GeoDataFrame( nvdbBomst2, geometry='geometry', crs=25833 )

    nvdbCol2 = [ 'nvdbId', 'Navn bomstasjon',
                # 'Tidsdifferensiert takst', 'Timesregel',
                 'Innkrevningsretning',
                # 'Navn bompengeanlegg (fra CS)',  'Link til bomstasjon',
                'Takst liten bil', 'APAR takst liten bil', #  'Operatør_Id', 'Bomstasjonstype', 
                'Takst stor bil', 'APAR takst stor bensinbil',
                # 'Bomstasjon_Id', 'Gratis gjennomkjøring ved HC-brikke', 'relasjoner',
                # 'veglenkesekvensid', 'detaljnivå', 'typeVeg', 'kommune', 'fylke',
                'vref', #  'veglenkeType', 'vegkategori', 'fase', 'vegnummer',
                # 'relativPosisjon', 'adskilte_lop', 'trafikantgruppe', 
                # 'geometri', 'stedfesting_retning',
                'stedfesting_felt',  'sideposisjon', 'segmentretning',
                # 'Rushtid morgen, til', 'Rushtidstakst liten bil',
                # 'Rushtidstakst stor bil', 'Timesregel, passeringsgruppe',
                # 'Timesregel, varighet', 'Etableringsår', 'Rushtid ettermiddag, fra',
                # 'Rushtid ettermiddag, til', 'Rushtid morgen, fra', 'Vedtatt til år',
                # 'Vedlikeholdsansvarlig', 'Eier', 'Prosjektreferanse',
                # 'Tilleggsinformasjon',  'ProsjektInternObjekt_ID',
                'stedfest', 'tilgjengeligeKjfelt', 'stedfesting QA', 
                'Antall APAR felt', 
                'vegkart lenke', 
                'geometry']
    
    aparCol2 = ['NVDB navn', 'tollStationName', 'NVDB Id', 'tollStationLane', 'tollStationDirection',
                 'operatorId', 'tollStationKey', 'projectNumber',
                'projectName', 'tollStationCode', 'geometry']
    
    nvdbBomst2[ nvdbCol2 ].to_file( 'nyApardump.gpkg', layer='nvdb bomstasjon', driver='GPKG')
    aparRediger[ aparCol2].to_file( 'nyApardump.gpkg', layer='apar bomstasjon felt', driver='GPKG')