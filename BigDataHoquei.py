import re
import socket
from urllib.request import urlopen
import urllib.request, urllib.parse, urllib.error
import requests
import ssl
import sqlite3

# Connectem amb el fitxer que contindrà la Base de Dades, una per a cada categoria-temporada
#conn = sqlite3.connect('Fem11_2019_2020.sqlite') #73
#conn = sqlite3.connect('Fem13_2019_2020.sqlite') #72

#conn = sqlite3.connect('Benjami_2019_2020.sqlite')     #3
#conn = sqlite3.connect('Alevi_2019_2020.sqlite')       #4
conn = sqlite3.connect('Infantil_2019_2020.sqlite')    #5
#conn = sqlite3.connect('Juv_2019_2020.sqlite')          #6
cur = conn.cursor()

# Creem les Taules. Bàsicament 2: una pels partits i l'altre per la parella jugador al partit.
cur.executescript('''
DROP TABLE IF EXISTS Partits;       
CREATE TABLE Partits (
    id_partit           INTEGER NOT NULL PRIMARY KEY UNIQUE,
    competicio          INTEGER,
    territorial         VARCHAR(16),
    categoria           VARCHAR(32),
    grup_competicio     VARCHAR(64),
    jornada             INTEGER,
    data_jornada        DATE,
    hora_partit         TIME,
    dia_setmana_partit  VARCHAR(16),
    dia_mes_partit      DATE,
    codi_local          INTEGER,
    nom_local           VARCHAR(32),
    gols_local          INTEGER,
    gols_visitant       INTEGER,
    codi_visitant       INTEGER,
    nom_visitant        VARCHAR(32),
    codi_temporada      INTEGER,
    codi_categoria      INTEGER,
    arbitre             VARCHAR(64),
    adressa             VARCHAR(64),
    codipostal          VARCHAR(8),
    poblacio            VARCHAR(64),
    TotalFaltesLocal    VARCHAR(16),
    TotalFaltesVisitant VARCHAR(16)
)
''')
cur.executescript('''
DROP TABLE IF EXISTS JugadorPartits;
CREATE TABLE JugadorPartits(
    partit              INTEGER,
    nomjugador          VARCHAR(32),
    dorsaljugador       VARCHAR(8),
    categoriajugador    VARCHAR(8),
    golsjugador         INTEGER,
    equip               VARCHAR(32)
)
''')
def get_cookie():  
    session = requests.Session()  
    session.get('http://www.fecapa.com/eCompeticio/Welcome.do')  
    return session.cookies.get_dict()['JSESSIONID']  
def equips(jsessionid):  
    url = f"http://www.fecapa.com/eCompeticio/CompeticioAction.do;jsessionid={jsessionid}?action=report1"  
    cookies = {'JSESSIONID': jsessionid}  
    headers = {'cookie': f"JSESSIONID={jsessionid}"}  
    requests.get(url, headers=headers, cookies=cookies)  
def post_resultados(jsessionid,temporada,categoria,fecha_inicial,fecha_final):  #aquesta és la select 
    url = "http://www.fecapa.com/eCompeticio/PartitAction.do"  
  
    payload = f"action=doReport6&selectmode=0&listCodterri=0&listCodmoda=1&listFinalitzada=true&listCodtemp={temporada}&listNumfase=0&listCodcate={categoria}&listJornada=0&listData=&listDatajocdes={fecha_inicial}&listDatajocfins={fecha_final}&listCodcomp=0&listNom=&listCodenti=0"  
    # action=doReport6
    # selectmode=0
    # listCodterri=0.                                   Territorial: 1Federació 2Barcelona 3Girona 4Lleida 5Tarragona 6Altre	
    # listCodmoda=1                                     Modalitat: 1Hoquei Patins 2Hoquei Línia
    # listFinalitzada=false                             Booleana: si està finalitzada o no la temporada
    # listCodtemp=36                                    Temporada: 30  2017/2018      33  2018/2019        36 2019/2020
    # listNumfase=0                                     Codi de la Fase: per exemple: 733 és FEM Base o el 763 és el BASE PREFERENT_2ª Fase
    # listCodcate=0                                     Codi de la Categoría: 73Fem11 72Fem13 5 Infantil 4Aleví 6Juvenil 3Benjamí 
    # listJornada=0                                     Número de la jornada
    # listData=&listDatajocdes={last_last_monday}&listDatajocfins={last_last_sunday}
    # listCodcomp=0                                     Codi de la competició: p.ex: Infantil Gr P3 C. Catalunya és el 6680
    # listNom=&listCodenti=0"                           Codi del Club
    headers = {  
        'Content-Type': "application/x-www-form-urlencoded",  
        'cookie': f"JSESSIONID={jsessionid}"  
    }  
    response = requests.post(url, data=payload, headers=headers)  
    if "No s'han trobat dades" in response.text:  
        return False  
    else:  
        return True  
def get_csv_result(jsessionid):  
    ##############################################################################################################################################
    ######################                                   RECOLLIM DADES QUE NO SÓN A L'ACTA
    ##############################################################################################################################################
    url = "http://www.fecapa.com/eCompeticio/PartitAction.do?d-16544-p=&action=flushReport6&d-16544-o=&6578706f7274=1&d-16544-e=1&d-16544-s="  
    headers = {  
       'cookie': f"JSESSIONID={jsessionid}"  
	}  
    response = requests.get(url, headers=headers)  
    response_clean = response.text.split("\n", 1)[1]
    for line in response_clean.splitlines():  
        if line[0].isdigit():
            campos = line.split(',')  #camps del partido 
            nom_equip_local=campos[11]+' '+campos[12]
            nom_equip_visitant=campos[17]+' '+campos[18]
            if campos[17]=='"JESÚS':
                campos[22]=campos[23]
                campos[24]=campos[25]
                campos[25]=campos[26]
                print('Aquí el JMJ com a visitant')
            if campos[11]=='"JESÚS':
                campos[13]=campos[14]
                campos[15]=campos[16]
                campos[16]=campos[17]
                nom_equip_visitant=campos[18]+' '+campos[19]
                campos[22]=campos[23]
                campos[24]=campos[25]
                campos[25]=campos[26]
                print('Aquí el JMJ com a local')
    ##############################################################################################################################################
    ######################                                   RECOLLIM DADES QUE sÍ SÓN A L'ACTA
    ##############################################################################################################################################            
            if campos[25]!='0':                     # a la pantalla de la select si existeix el codi del partit vol dir que hi han dades i que hi ha acta
                url_acta="http://www.fecapa.com:9080/eCompeticio/PartitAction.do;jsessionid=jsessionid?action=callURL&codpartit="+campos[25]
                response_acta = requests.get(url_acta, headers=headers)  
                response_acta_clean=response_acta.text.split("\n",1)[1]             #carreguem a response_acta tooooot el html de l'acta
                arbitre='No definit'
                adressa='No definit'
                codipostal='No definit'
                poblacio='No definit'
                TotalFaltesLocal='No definit'
                TotalFaltesVisitant='No definit'
                NomRealsJugadorsLocal=dict()
                DorsalRealJugadorsLocal=dict()
                CatRealJugadorsLocal=dict()
                GolsRealJugadorsLocal=dict()
                NomRealsJugadorsVisit=dict()
                DorsalRealJugadorsVisit=dict()
                CatRealJugadorsVisit=dict()
                GolsRealJugadorsVisit=dict()
                for line_acta in response_acta_clean.splitlines():                  #per a cada línea del html fem recerques de camps
                    line_acta=line_acta.rstrip()
                    if re.search('capsaleraArbitre',line_acta):        #amb una regular expression cerquem la línia on és l'àrbitre
                        data=re.findall(r'value="(.*)',line_acta)      #busquem l'string "value="per saber on comença el nom de l'àrbitre
                        if data[0][0]!='"' and arbitre=='No definit':
                            arbitre=data[0]     
                            atpos=arbitre.find('"')                 #busquem el caràcter " per saber on acaba el nom de l'àrbitre
                            arbitre=arbitre[0:atpos]
                            #print(campos[25],arbitre)
                    if re.search('capsaleraAdressa',line_acta):        #amb una regular expression cerquem la línia on és l'adreça
                        data=re.findall(r'value="(.*)',line_acta)     
                        if data[0][0]!='"' and adressa=='No definit':
                            adressa=data[0]     
                            atpos=adressa.find('"')                 
                            adressa=adressa[0:atpos]
                            #print(campos[25],adressa)
                    if re.search('capsaleraCodiPostal',line_acta):        #amb una regular expression cerquem la línia on és l'àrbitre
                        data=re.findall(r'value="(.*)',line_acta)      
                        if data[0][0]!='"' and codipostal=='No definit':
                            codipostal=data[0]     
                            atpos=codipostal.find('"')                 #busquem el caràcter " per saber on acaba el nom de l'àrbitre
                            codipostal=codipostal[0:atpos]
                            #print(campos[25],codipostal)
                    if re.search('capsaleraPoblacio',line_acta):        #amb una regular expression cerquem la línia on és l'àrbitre
                        data=re.findall(r'value="(.*)',line_acta)      #busquem l'string "value="per saber on comença el nom de l'àrbitre
                        if data[0][0]!='"' and poblacio=='No definit':
                            poblacio=data[0]     
                            atpos=poblacio.find('"')                 #busquem el caràcter " per saber on acaba el nom de l'àrbitre
                            poblacio=poblacio[0:atpos]
                            #print(campos[25],poblacio)
                    if re.search('capsaleraTotalFaltesLocal',line_acta):        #amb una regular expression cerquem la línia on és l'àrbitre
                        data=re.findall(r'value="(.*)',line_acta)      #busquem l'string "value="per saber on comença el nom de l'àrbitre
                        if data[0][0]!='"' and TotalFaltesLocal=='No definit':
                            TotalFaltesLocal=data[0]     
                            atpos=TotalFaltesLocal.find('"')                 #busquem el caràcter " per saber on acaba el nom de l'àrbitre
                            TotalFaltesLocal=TotalFaltesLocal[0:atpos]
                            #print(campos[25],TotalFaltesLocal)
                    if re.search('capsaleraTotalFaltesVisit',line_acta):        #amb una regular expression cerquem la línia on és l'àrbitre
                        data=re.findall(r'value="(.*)',line_acta)      #busquem l'string "value="per saber on comença el nom de l'àrbitre
                        if data[0][0]!='"' and TotalFaltesVisitant=='No definit':
                            TotalFaltesVisitant=data[0]     
                            atpos=TotalFaltesVisitant.find('"')                 #busquem el caràcter " per saber on acaba el nom de l'àrbitre
                            TotalFaltesVisitant=TotalFaltesVisitant[0:atpos]
                            #print(campos[25],TotalFaltesVisitant)
                    #definim totes les variables i camps que hem de cercar a les actes en relació als jugadors: nom, dorsal, categoria, gols, blaves i vermelles
                    NomJugadorsLocal=['localNomPorter1','localNomPorter2','localNomJugador1','localNomJugador2','localNomJugador3','localNomJugador4','localNomJugador5','localNomJugador6','localNomJugador7','localNomJugador8','localNomJugador9']
                    DorsalJugadorsLocal=['localDorsalPorter1','localDorsalPorter2','localDorsalJugador1','localDorsalJugador2','localDorsalJugador3','localDorsalJugador4','localDorsalJugador5','localDorsalJugador6','localDorsalJugador7','localDorsalJugador8','localDorsalJugador9']
                    CatJugadorsLocal=['localCatPorter1','localCatPorter2','localCatJugador1','localCatJugador2','localCatJugador3','localCatJugador4','localCatJugador5','localCatJugador6','localCatJugador7','localCatJugador8','localCatJugador9']
                    GolsJugadorsLocal=['localGolPorter1','localGolPorter2','localGolJugador1','localGolJugador2','localGolJugador3','localGolJugador4','localGolJugador5','localGolJugador6','localGolJugador7','localGolJugador8','localGolJugador9']
                    NomJugadorsVisit=['visitNomPorter1','visitNomPorter2','visitNomJugador1','visitNomJugador2','visitNomJugador3','visitNomJugador4','visitNomJugador5','visitNomJugador6','visitNomJugador7','visitNomJugador8','visitNomJugador9']
                    DorsalJugadorsVisit =['visitDorsalPorter1','visitDorsalPorter2','visitDorsalJugador1','visitDorsalJugador2','visitDorsalJugador3','visitDorsalJugador4','visitDorsalJugador5','visitDorsalJugador6','visitDorsalJugador7','visitDorsalJugador8','visitDorsalJugador9']
                    CatJugadorsVisit=['visitCatPorter1','visitCatPorter2','visitCatJugador1','visitCatJugador2','visitCatJugador3','visitCatJugador4','visitCatJugador5','visitCatJugador6','visitCatJugador7','visitCatJugador8','visitCatJugador9']
                    GolsJugadorsVisit=['visitGolPorter1','visitGolPorter2','visitGolJugador1','visitGolJugador2','visitGolJugador3','visitGolJugador4','visitGolJugador5','visitGolJugador6','visitGolJugador7','visitGolJugador8','visitGolJugador9']
                    for jugador in NomJugadorsLocal:
                        if re.search(jugador,line_acta):        
                            data=re.findall(r'value="(.*)',line_acta)     
                            if data[0][0]!='"':
                                Camp=data[0]     
                                atpos=Camp.find('"')            
                                Camp=Camp[0:atpos]
                                #print(campos[25],jugador,Camp)
                                NomRealsJugadorsLocal[jugador]=Camp
                    for dorsal in DorsalJugadorsLocal:
                        if re.search(dorsal,line_acta):        
                            data=re.findall(r'value="(.*)',line_acta)     
                            if data[0][0]!='"':
                                Camp=data[0]     
                                atpos=Camp.find('"')            
                                Camp=Camp[0:atpos]
                                #print(campos[25],dorsal,Camp)
                                DorsalRealJugadorsLocal[dorsal]=Camp
                    for cat in CatJugadorsLocal:
                        if re.search(cat,line_acta):        
                            data=re.findall(r'value="(.*)',line_acta)     
                            if data[0][0]!='"':
                                Camp=data[0]     
                                atpos=Camp.find('"')            
                                Camp=Camp[0:atpos]
                                #print(campos[25],cat,Camp)
                                CatRealJugadorsLocal[cat]=Camp
                    for gols in GolsJugadorsLocal:
                        if re.search(gols,line_acta):        
                            data=re.findall(r'value="(.*)',line_acta)     
                            if data[0][0]!='"':
                                Camp=data[0]     
                                atpos=Camp.find('"')            
                                Camp=Camp[0:atpos]
                                #print(campos[25],gols,Camp)
                                GolsRealJugadorsLocal[gols]=Camp
                    for jugador in NomJugadorsVisit:
                        if re.search(jugador,line_acta):        
                            data=re.findall(r'value="(.*)',line_acta)      
                            if data[0][0]!='"':
                                Camp=data[0]     
                                atpos=Camp.find('"')            
                                Camp=Camp[0:atpos]
                                #print(campos[25],jugador,Camp)
                                NomRealsJugadorsVisit[jugador]=Camp
                    for dorsal in DorsalJugadorsVisit:          #visitDorsalJugador1DorsalJugadorsVisit
                        if re.search(dorsal,line_acta):        
                            data=re.findall(r'value="(.*)',line_acta)     
                            if data[0][0]!='"':
                                Camp=data[0]     
                                atpos=Camp.find('"')            
                                Camp=Camp[0:atpos]
                                #print(campos[25],dorsal,Camp)
                                DorsalRealJugadorsVisit[dorsal]=Camp
                    for cat in CatJugadorsVisit:
                        if re.search(cat,line_acta):        
                            data=re.findall(r'value="(.*)',line_acta)     
                            if data[0][0]!='"':
                                Camp=data[0]     
                                atpos=Camp.find('"')            
                                Camp=Camp[0:atpos]
                                #print(campos[25],cat,Camp)
                                CatRealJugadorsVisit[cat]=Camp
                    for gols in GolsJugadorsVisit:
                        if re.search(gols,line_acta):        
                            data=re.findall(r'value="(.*)',line_acta)     
                            if data[0][0]!='"':
                                Camp=data[0]     
                                atpos=Camp.find('"')            
                                Camp=Camp[0:atpos]
                                #print(campos[25],gols,Camp)
                                GolsRealJugadorsVisit[gols]=Camp
                                
##############################################################################################################################################
######################                    ESCRIBIM DADES  A LA TAULA JugadorsPartit
##############################################################################################################################################
                
                for jugador in NomRealsJugadorsLocal:
                    indiceDorsal='localDorsal'+jugador[8:]
                    indiceCat='localCat'+jugador[8:]
                    indiceGoles='localGol'+jugador[8:]
                    try:
                        Goles = GolsRealJugadorsLocal[indiceGoles]
                    except:
                        Goles = 0
                    cur.execute('''INSERT OR IGNORE INTO JugadorPartits(
                            partit,
                            nomjugador,
                            dorsaljugador,
                            categoriajugador,
                            golsjugador,
                            equip
                        )
                            VALUES(?,?,?,?,?,?)''',(campos[25],NomRealsJugadorsLocal[jugador],DorsalRealJugadorsLocal[indiceDorsal],CatRealJugadorsLocal[indiceCat],Goles,nom_equip_local))
                if campos[25]==294768:
                    print('acta que trenca')
                for jugador in NomRealsJugadorsVisit:
                    indiceDorsal='visitDorsal'+jugador[8:]
                    indiceCat='visitCat'+jugador[8:]
                    indiceGoles='visitGol'+jugador[8:]
                    try:
                        Goles = GolsRealJugadorsVisit[indiceGoles]
                    except:
                        Goles = 0
                    try:
                        cur.execute('''INSERT OR IGNORE INTO JugadorPartits(
                                partit,
                                nomjugador,
                                dorsaljugador,
                                categoriajugador,
                                golsjugador,
                                equip
                            )
                                VALUES(?,?,?,?,?,?)''',(campos[25],NomRealsJugadorsVisit[jugador],DorsalRealJugadorsVisit[indiceDorsal],CatRealJugadorsVisit[indiceCat],Goles,nom_equip_visitant))
                    except:
                        print('error al insertar')
                print('Finalitzada extracció del partit amb codi: ',campos[25])
##############################################################################################################################################
######################                    ESCRIBIM DADES QUE A LA TAULA Partits
##############################################################################################################################################
                cur.execute('''INSERT OR IGNORE INTO Partits (
                    id_partit,
                    competicio,
                    territorial,
                    categoria,
                    grup_competicio,
                    jornada,
                    data_jornada,
                    hora_partit,
                    dia_setmana_partit,
                    dia_mes_partit,
                    codi_local,
                    nom_local,
                    gols_local,
                    gols_visitant,
                    codi_visitant,
                    nom_visitant,
                    codi_temporada,
                    codi_categoria,
                    arbitre,
                    adressa,
                    codipostal,
                    poblacio,
                    TotalFaltesLocal,
                    TotalFaltesVisitant 
                    )
                    VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? )''', ( campos[25],campos[0],campos[1],campos[3],campos[4],campos[5],campos[6],campos[7],campos[8],campos[9],campos[10],nom_equip_local,campos[13],campos[15],campos[16],nom_equip_visitant,campos[22],campos[24],arbitre,adressa,codipostal,poblacio,TotalFaltesLocal,TotalFaltesVisitant))
def login(jsessionid):  
    url = "http://www.fecapa.com/eCompeticio/LoginAction.do"  
    payload = "user=fpuentes&password=41c4n4r3&action=find&nom=&cognoms=&email="  
    cookies = {'JSESSIONID': jsessionid}  
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",  
	    'cookie': f"JSESSIONID={jsessionid}",  
    }  
    requests.post(url, data=payload, headers=headers, cookies=cookies)  
    # print(response.text)  
id_session=get_cookie()         #aquí aconseguim la cockie que identifica la sessió que té el token que omple jsessionid
login(id_session)               #aquí iniciem la sessió amb el jsessionid que hem aconseguit
equips(id_session)              #això és la funció de validació d'entrada que vam veure que s'havia de fer amb el VuGen. Sense això, no es pot continuar
temporada=36                    #la 36 és la temporada 2019 a 2020
categoria=input ('Codi de la Categoria?')                     #la 5 és la categoria Infantil
#SETEMBRE I OCTUBRE
fecha_inicial="01-09-2019" 
fecha_final="31-10-2019"
post_resultados(id_session,temporada,categoria,fecha_inicial,fecha_final)     #aquí hi ha el posta que fa la select contra la base de dades
get_csv_result(id_session)      #aquí hi ha els gets que recullen la informació demanada pel post i el fiquen a la base de dades
#NOVEMBRE I DESEMBRE
fecha_inicial="01-11-2019" 
fecha_final="31-12-2019"
post_resultados(id_session,temporada,categoria,fecha_inicial,fecha_final)     #aquí hi ha el posta que fa la select contra la base de dades
get_csv_result(id_session)      #aquí hi ha els gets que recullen la informació demanada pel post i el fiquen a la base de dades
#GENER,FEBRER I MARÇ
fecha_inicial="01-01-2020" 
fecha_final="08-03-2020"
post_resultados(id_session,temporada,categoria,fecha_inicial,fecha_final)     #aquí hi ha el posta que fa la select contra la base de dades
get_csv_result(id_session)      #aquí hi ha els gets que recullen la informació demanada pel post i el fiquen a la base de dades
conn.commit()                   #fem el commit
cur.close()                     #tanquem la base de dades