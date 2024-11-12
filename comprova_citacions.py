# This Python file uses the following encoding: utf-8
import csv
import config
import psycopg2
import sys
import uuid
import unidecode
#import unicodedata
import re

header_names_citacions = ['Especie','CODI_ESP','GRUP','UTMX','UTMY','Localitat','Municipi','Comarca','Provincia','Data','Autors','Citacio','Font','Referencia','Observacions','Tipus cita','Habitat','Tipus mort','Abundancia','Codi ACA','Codi estacio','IND_Ha','Ind. Capt.']
header_names_1_1 = ['ESPECIE','CODI_ESP','GRUP','CODIUTM','Descripcio','Data','Autors','CITACIO','FONT','REF','Observacions','Tipus cita','Habitat','Revisio']
header_names_exotiques = ['Grup','IdGrup','Codi_Oracle','IdEsp','Especie','Referencia','Nom catala','Nom castella','Nom angles','Divisio','Classe','Ordre','Familia','Sinonims','Regio nativa_1','Regio nativa_2','Regio nativa_3','Via entrada','Habitat','Estatus historic','Estatus Catalunya','Estatus Espanya','Catalogo Nacional','Font_info','Observacions','Primera citacio','Font primera citacio','Fotos','Dades distribucio','Revisio Oracle','Taxonomia (ITIS)','BDBC','Revisio BIOCAT','DAISIE','InvasIber','GISD','CIESM','Algaebase','Fishbase','NOBANIS','Estatus CAC  (Llista patro, 2010)','Insectarium virtual','EPPO','Flora Iberica','Sanz Elorza et al. 2004','Bolos et al., 1990-2005','Flora catalana','Casasayas','Mapes Casasayas','Anthos','Mapa Anthos','Observacions nostres','Dades bibliografiques i herbari (no georeferenciades)']
conn_string = "host='" + config.params['db_host'] + "' dbname='" + config.params['db_name'] + "' user='" + config.params['db_user'] + "' password='" + config.params['db_password'] + "' port='" + config.params['db_port'] + "'"

GRUP = 0
ID_ESPECIE = 3
NOM_ESPECIE = 4
NOM_CA = 6
NOM_ES = 7
NOM_EN = 8
REGIO_NATIVA_1 = 18
REGIO_NATIVA_2 = 19
REGIO_NATIVA_3 = 20
VIA_ENTRADA = 21
HABITAT = 23
ESTATUS_HISTORIC = 26
ESTATUS_CATALUNYA = 25
PRESENT_CATALOGO = 27
REGLAMENT_UE = 28
OBSERVACIONS = 30
ID_GBIF = 44

sinonims_grups = {
    "Diatomees":"ALG",
    "Algues":"ALG",
    "Amfibis":"AMF",
    "Fongs":"FON",
    "INV_Aqua":"INV_Aqua",
    "INV_Ter":"INV_terre",
    "Mamífers":"MAM",
    "Ocells":"OCE",
    "Peixos":"PEI",
    "Peixos marins":"PEIM",
    "Plantes":"PLA",
    "Rèptils":"REP",
}


def remove_accents(str):
    return unidecode.unidecode(str)


def print_one_liner(message):
    sys.stdout.write('%s\r' % message)
    sys.stdout.flush()

def comprova_codi_ACA(row):
    return True

# Versio nova, comprova id_esp (no codi_oracle)
def comprova_codi_esp_nou(codi_esp):
    if codi_esp == '' or codi_esp is None:
        return False
    return True

def comprova_codi_esp(row):
    if row[2] == '':
        return False
    return True

def comprova_codi_quadricula(row):
    if row[3] == '':
        return False
    codi_q = row[3].strip()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM public.quadricula WHERE id=%s;""",
                   (codi_q, ))
    results = cursor.fetchall()
    if (len(results) == 0):
        return False
    return True

def fila_es_buida(row):
    for element in row:
        if element != '':
            return False
    return True

def check_especie_no_existeix(id_esp):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM public.especieinvasora WHERE id=%s;""",(id_esp,))
    results = cursor.fetchall()
    if (len(results) == 0):
        return True
    return False

def check_regionativa_no_existeix(idespecieinvasora,idzonageografica):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM public.regionativa WHERE idespecieinvasora=%s and idzonageografica=%s;""", (idespecieinvasora,idzonageografica,))
    results = cursor.fetchall()
    if (len(results) == 0):
        return True
    return False

def check_codi_especie(id):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT id FROM public.especieinvasora WHERE id=%s;""", (id,))
    results = cursor.fetchall()
    if (len(results) > 0):
        return True
    return False

def get_idspinvasora_deidtaxon(idtaxon):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT id FROM public.especieinvasora WHERE idtaxon=%s;""", (idtaxon,))
    results = cursor.fetchall()
    if (len(results) > 0):
        return results[0][0]
    else:
        return ''

def get_id_spinvasores():
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT id FROM public.especieinvasora;""")
    results = cursor.fetchall()
    id_list = []
    for result in results:
        id_list.append(result[0])
    return set(id_list)

def get_id_desempat(cursor_rows):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    number_hits = 0
    candidate_id = ''
    for row in cursor_rows:
        cursor.execute("""SELECT id FROM public.especieinvasora WHERE idtaxon=%s;""",(row[0],))
        results = cursor.fetchall()
        if(len(results) > 0):
            number_hits += 1
            candidate_id = results[0][0]
            candidate_id_taxon = row[0]
    if number_hits == 1:
        # return candidate_id
        return {'idinvasora': candidate_id, 'idtaxon': candidate_id_taxon}
    return {'idinvasora': '', 'idtaxon': ''}


def split_nom_especie(sp_name):
    result = {}
    elements_nom = sp_name.split(' ')

    genere = elements_nom[0].replace('\xc2\xa0', ' ').strip()
    especie = ''
    subespecie = ''
    try:
        especie = elements_nom[1].replace('\xc2\xa0', ' ').strip()
    except IndexError:
        pass

    try:
        subespecie = elements_nom[2].replace('\xc2\xa0', ' ').strip()
    except IndexError:
        pass

    if (len(elements_nom) == 3):
        subespecie = elements_nom[2]
    elif (len(elements_nom) > 3):
        subespecie = ' '.join(elements_nom[2:])
    else:
        pass

    result['genere'] = genere
    if especie != '':
        result['especie'] = especie
    if subespecie != '':
        result['subespecie'] = subespecie
    return result


def get_id_invasora_codi_oracle(codi_oracle):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT id FROM public.especieinvasora WHERE idtaxon=%s;""", (codi_oracle,))
    cursor_rows = cursor.fetchall()
    if len(cursor_rows) == 0:
        return ''
    else:
        return cursor_rows[0]


def get_id_invasora(sp_name):
    elements_nom = sp_name.split(' ')
    genere = ''
    especie = ''
    subespecie = ''
    if len(elements_nom) == 1:
        genere = elements_nom[0].replace('\xc2\xa0', ' ').strip()
    elif len(elements_nom) == 2:
        especie = elements_nom[1].replace('\xc2\xa0', ' ').strip()
    elif (len(elements_nom) == 3):
        subespecie = elements_nom[2]
    elif (len(elements_nom) > 3):
        subespecie = ' '.join(elements_nom[2:])
    else:
        pass

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    if subespecie != '':
        cursor.execute("""SELECT * FROM public.taxon WHERE trim(both from genere)=%s and trim(both from especie)=%s and trim(both from subespecie)=%s;""",(genere, especie, subespecie,))
    else:
        if especie != '':
            cursor.execute("""SELECT * FROM public.taxon WHERE trim(both from genere)=%s and trim(both from especie)=%s;""", (genere, especie,))
        else:
            cursor.execute("""SELECT * FROM public.taxon WHERE trim(both from genere)=%s;""",(genere,))
    cursor_rows = cursor.fetchall()
    if len(cursor_rows) == 0:
        #return ''
        return { 'idinvasora': '', 'idtaxon': '' }
    elif len(cursor_rows) == 1:
        # return get_idspinvasora_deidtaxon(cursor_rows[0][0])
        return {'idinvasora': get_idspinvasora_deidtaxon(cursor_rows[0][0]), 'idtaxon': cursor_rows[0][0]}
    else:
        print("Hi ha multiples especies per " + sp_name)
        #if subespecie != '':
            #print "Hi ha multiples especies amb genere " + genere + " especie " + especie + " i subespecie " + subespecie
        #else:
            #print "Hi ha multiples especies amb genere " + genere + " i especie " + especie

        print("Intentant desempatar multiples ids")
        res = get_id_desempat(cursor_rows)
        id_desempat = res['idinvasora']
        id_taxon_desempat = res['idtaxon']
        if id_desempat == '':
            rownum = 0
            for row in cursor_rows:
                print("opcio " + str(rownum) + ": " + ', '.join( item for item in row if item ))
                rownum += 1
            print("Impossible desempatar, cal entrada de l'usuari")
            opcio = input("Tria una opcio:")
            return {'idinvasora': get_idspinvasora_deidtaxon(cursor_rows[int(opcio)][0]), 'idtaxon': cursor_rows[int(opcio)][0] }
        else:
            print("Desempat amb exit, identificador assignat!")
            # return id_desempat
            return {'idinvasora': id_desempat, 'idtaxon': id_taxon_desempat}


def comprova_format_coordenades(row):
    utmx = row[3].strip()
    utmy = row[4].strip()
    if "," in utmx or "," in utmy:
        utmx = utmx.replace(',', '.')
        utmy = utmy.replace(',', '.')
    try:
        float(utmx)
        float(utmy)
        return True
    except ValueError:
        return False

def fila_presencia_es_a_la_base_dades(row):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM public.presencia_sp WHERE idquadricula=%s and idspinvasora=%s;""",
                   (row[3].strip(), row[1].strip(),))
    results = cursor.fetchall()
    return len(results) > 0

def fila_es_a_la_base_dades(row):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    query = "SELECT id FROM public.citacions WHERE especie='{0}' and utmx={1} and utmy={2};"
    f_query = query.format(row[0].strip(),float(row[3].strip()),float(row[4].strip()),)
    cursor.execute(f_query)
    results = cursor.fetchall()
    return len(results) > 0

def get_insert_taula_mtaxon(row):
    plantilla = "INSERT INTO public.taxon(ID,NOMSP,TESAUREBIOCAT,CODIBIOCAT,GENERE,ESPECIE,AUTORESPECIE) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}');"
    taxonomia = split_nom_especie(row[4])
    genere = taxonomia['genere']
    especie = ''
    subespecie = ''
    try:
        especie = taxonomia['especie']
        subespecie = taxonomia['subespecie']
    except KeyError:
        pass
    str_plantilla = plantilla.format(row[3].strip(),row[4].strip(),row[0].strip(),'0',genere,especie,row[5].strip(),)
    return str_plantilla

def translate_status(estatus):
    if estatus.strip() == '':
        return 'BLANC'
    estatus = estatus.strip()
    if estatus == 'Arqueòfit/Arqueozou?':
        return 'ARQUEF_ARQUEO?'
    if estatus == 'Possiblement translocada':
        return 'TRANS?'
    if estatus == 'Possiblement desapareguda':
        return 'POSSD'
    if estatus == 'Possible reintroduïda':
        return 'REINT?'
    if estatus == 'No establerta(exòtica?)' or estatus=='No establerta (exòtica?)':
        return 'NO_EST/EXO?'
    if estatus == 'No establerta' or estatus == 'No Establerta':
        return 'NO_EST'
    if estatus == 'Naturalitzada dubtosa':
        return 'Natur?'
    if estatus == 'Naturalitzada(citació puntual)' or estatus == 'Naturalitzada (citació puntual)':
        return 'Natur_cit punt'
    if estatus == 'Naturalitzada':
        return 'Natur'
    if estatus == 'Invasora localment':
        return 'INV_LOC'
    if estatus == 'Invasora':
        return 'INV'
    if estatus == 'Introduïda dubtosa':
        return 'INTROD?'
    if estatus == 'Introduïda(citació dubtosa)' or estatus=='Introduïda (citació dubtosa)':
        return 'INTROD_dubt'
    if estatus == 'Introduïda(potencialment invasora)':
        return 'INTROD_POTEN'
    if estatus == 'Introduïda(possiblement només plantada)' or estatus=='Introduïda (possiblement només plantada)':
        return 'INTROD_plant'
    if estatus == 'Introduïda(possiblement desapareguda)' or estatus=='Introduïda (possiblement desapareguda)':
        return 'INTROD_desap'
    if estatus == 'Introduïda(eliminada del medi)' or estatus=='Introduïda (eliminada del medi)':
        return 'INTROD_elim'
    if estatus == 'Introduïda(citacions antigues)':
        return 'INTROD_antic'
    if estatus == 'Introduïda (citació puntual)':
        return 'INTROD_cit punt'
    if estatus == 'Introduïda(citació dubtosa)':
        return 'INTROD_dubt'
    if estatus == 'Introduïda':
        return 'INTROD'
    if estatus == 'Exòtica?':
        return 'EXOT?'
    if estatus == 'Establerta(localment invasora)':
        return 'EST(INV)'
    if estatus == 'Establerta(exòtica?)':
        return 'EST/EXO?'
    if estatus == 'Establerta':
        return 'EST'
    if estatus == 'En vies de naturalització':
        return 'Natur_'
    if estatus == 'Citació puntual(potencialment invasora)':
        return 'CIT_PUNT_POTEN'
    if estatus == 'Citació puntual(possiblement desapareguda)':
        return 'DESAP'
    if estatus == 'Citació puntual':
        return 'CIT_PUNT'
    if estatus == 'Adventícia dubtosa':
        return 'Adven?'
    if estatus == 'Adventícia(possiblement desapareguda)' or estatus=='Adventícia (possiblement desapareguda)':
        return 'Adven_desap'
    if estatus == 'Adventícia(en regressió)' or estatus=='Adventícia (en regressió)':
        return 'Adven_regres'
    if estatus == 'Adventícia (citació puntual)':
        return 'Adven_cit punt'
    if estatus == 'Adventícia':
        return 'Adven'
    if estatus == 'Translocada(adventícia)' or estatus=='Translocada (adventícia)':
        return 'TRANS_adv'
    if estatus == 'Translocada':
        return 'TRANS'
    if estatus == 'Reintroduïda':
        return 'REINT'
    if estatus == 'Possible arqueozou':
        return 'POSS_ARQUE'
    if estatus == 'No avaluat':
        return 'NO_AVALUAT'
    if estatus == 'Arqueòfit/Arqueozou':
        return 'ARQUEF_ARQUE'
    if estatus == 'Autòctona':
        return 'AUTOC'
    if estatus == 'Criptogènic':
        return 'CRIPTOGENIC'
    if estatus=='Neòfit/Neozou':
        return 'NEOF_NEO'
    if estatus=='Neòfit' or estatus=='Neofit':
        return 'NEOF'
    if estatus=='Neozou':
        return 'NEO'
    if estatus=='Nativa':
        return 'NATIVA'
    if estatus=='Citació puntual (potencialment invasora)':
        return 'CIT_PUNT_POTEN'
    if estatus=='Citació puntual (possiblement desapareguda)':
        return 'DESAP'
    if estatus=='Introduïda(citacions antigues)' or estatus=='Introduïda (citacions antigues)':
        return 'INTROD_antic'
    if estatus=='Introduïda (potencialment invasora)':
        return 'INTROD_POTEN'
    if estatus == 'Establerta (localment invasora)':
        return 'EST(INV)'
    if estatus == 'Translocada?':
        return 'TRANS_?'
    if estatus == 'Translocada (possiblement desapareguda)':
        return 'TRANS_desap'
    if estatus == 'Establerta(exòtica?)' or estatus == 'Establerta (exòtica?)':
        return 'EST/EXO?'
    if estatus == 'Marí':
        return 'MARI'
    if estatus == 'MARI':
        return 'MARI'
    if estatus == 'Criptogènica':
        return 'CRIPTOGENIC'
    if estatus == 'Arqueozou':
        return 'ARQUE'
    if estatus == 'Assilvestrada':
        return 'ASILV'
    if estatus == 'Nativa / Translocada?':
        return 'NAT_TRANS?'
    if estatus == 'Arqueòfit':
        return 'ARQUEF'
    if estatus == 'Citació dubtosa':
        return 'CIT_DUB'
    if estatus == 'Citació Translocada (Arqueòfit)' or estatus == 'Translocada (Arqueòfit)':
        return 'TRANS_ARQUEF'
    if estatus == 'Possible arqueofit':
        return 'POSS_ARQUEF'
    if estatus == 'Possiblement translocada (Arqueòfit)':
        return 'TRANS?_ARQUEF'
    if estatus == 'Introduïda(sense més dades)' or 'Introduïda (sense més dades)':
        return 'INT_SMD'
    raise Exception('Estatus * ' + estatus + ' * desconegut')

def translate_catalogo_nacional(catalogo):
    if catalogo.strip().startswith('S'):
        return 'S'
    return 'N'

def check_status_is_present(idstatus):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM public.estatus WHERE id=%s;""",(idstatus,))
    results = cursor.fetchall()
    return len(results) > 0

def cleanup_id_gbif(url):
    # https://www.gbif.org/species/4448925
    if url.rfind('/') < 0:
        return None
    else:
        try:
            a = int(url[url.rfind('/') + 1:])
            return a
        except ValueError:
            print("error de format per url : " + url[url.rfind('/') + 1:])
            return None

def cleanup_observacions(observacions):
    observacions = observacions.replace("'", "''")
    observacions = observacions.replace("’", "''")
    observacions = observacions.replace("\n", " ")
    return observacions


def get_update_estatus_catalunya(row):
    idestatuscatalunya = translate_status(row[ESTATUS_CATALUNYA])
    if not check_status_is_present(idestatuscatalunya):
        raise Exception(row[ESTATUS_CATALUNYA] + " " + idestatuscatalunya + " no es a la base de dades, cal afegir el codi --> INSERT INTO public.estatus(id,nom,estatus_catalunya,estatus_historic) VALUES('" + idestatuscatalunya + "','" + row[ESTATUS_CATALUNYA] + "',TRUE,FALSE);")
    idestatusgeneral = idestatuscatalunya
    plantilla = "UPDATE public.especieinvasora set idestatuscatalunya='{0}' WHERE id='{1}';"
    str_plantilla = plantilla.format(idestatuscatalunya, row[ID_ESPECIE].strip())
    return str_plantilla

def get_update_taula_spinvasora(row):
    idestatushistoric = translate_status(row[ESTATUS_HISTORIC])
    if not check_status_is_present(idestatushistoric):
        raise Exception(row[ESTATUS_HISTORIC] + " " + idestatushistoric + " no es a la base de dades, cal afegir el codi --> INSERT INTO public.estatus(id,nom,estatus_catalunya,estatus_historic) VALUES('" + idestatushistoric + "','" + row[ESTATUS_HISTORIC] + "',FALSE,TRUE);")
    idestatuscatalunya = translate_status(row[ESTATUS_CATALUNYA])
    if not check_status_is_present(idestatuscatalunya):
        raise Exception(idestatuscatalunya + ' no es a la base de dades, cal afegir el codi')
    idestatusgeneral = idestatuscatalunya
    observacions = cleanup_observacions(row[OBSERVACIONS])
    present_catalogo = translate_catalogo_nacional(row[PRESENT_CATALOGO])
    nom_especie = cleanup_observacions(row[NOM_ESPECIE])
    id_gbif = cleanup_id_gbif(row[ID_GBIF])
    plantilla = "UPDATE public.especieinvasora set idestatushistoric='{0}',idestatuscatalunya='{1}',observacions='{2}',present_catalogo=" + ( "'{3}'" if present_catalogo == 'NULL' else "'{3}'") + ",idestatusgeneral='{4}', nom_especie='{5}', " + ( 'id_gbif={6}' if id_gbif is not None else 'id_gbif=NULL' ) + " WHERE id='{7}';"
    str_plantilla = plantilla.format(idestatushistoric, idestatuscatalunya, observacions, present_catalogo, idestatusgeneral, nom_especie, id_gbif, row[ID_ESPECIE].strip())
    return str_plantilla

def get_cleanup_taula_spinvasora(row):
    plantilla = "DELETE FROM public.especieinvasora where id='{0}';"
    str_plantilla = plantilla.format(row[ID_ESPECIE].strip())
    return str_plantilla

def get_insert_taula_spinvasora_nou(row):
    #Estatus_historic == Categoria general on excel
    idestatushistoric = translate_status(row[ESTATUS_HISTORIC])
    if not check_status_is_present(idestatushistoric):
        raise Exception(row[ESTATUS_HISTORIC] + ' ' + idestatushistoric + ' no es a la base de dades, cal afegir el codi')
    #was 24
    idestatuscatalunya = translate_status(row[ESTATUS_CATALUNYA])
    if not check_status_is_present(idestatuscatalunya):
        raise Exception(idestatuscatalunya + ' no es a la base de dades, cal afegir el codi')
    idestatusgeneral = idestatuscatalunya
    #was 29
    observacions = cleanup_observacions(row[OBSERVACIONS])
    #was 26
    present_catalogo = translate_catalogo_nacional(row[PRESENT_CATALOGO])
    #was 27
    reglament_ue = translate_catalogo_nacional(row[REGLAMENT_UE])
    nom_especie = cleanup_observacions(row[NOM_ESPECIE])
    #was 40
    id_gbif = cleanup_id_gbif(row[ID_GBIF])
    plantilla = "INSERT INTO public.especieinvasora(id,idestatushistoric,idestatuscatalunya,idimatgeprincipal,observacions,present_catalogo,idestatusgeneral,reglament_ue,nom_especie,id_gbif) VALUES ('{0}','{1}','{2}',{3},'{4}'," + ("'{5}'" if present_catalogo == 'NULL' else "'{5}'") + ",'{6}','{7}','{8}'," + ("NULL" if id_gbif is None else "{9}") + ");"
    str_plantilla = plantilla.format(row[ID_ESPECIE].strip(), idestatushistoric, idestatuscatalunya, 'NULL', observacions, present_catalogo, idestatusgeneral, reglament_ue, nom_especie, id_gbif)
    return str_plantilla


def get_insert_taula_spinvasora(row, idtaxon=None):
    idestatushistoric = translate_status(row[ESTATUS_HISTORIC])
    if not check_status_is_present(idestatushistoric):
        raise Exception(row[ESTATUS_HISTORIC] + ' ' + idestatushistoric + ' no es a la base de dades, cal afegir el codi')
    idestatuscatalunya = translate_status(row[ESTATUS_CATALUNYA])
    if not check_status_is_present(idestatuscatalunya):
        raise Exception(idestatuscatalunya + ' no es a la base de dades, cal afegir el codi')
    idestatusgeneral = idestatuscatalunya
    observacions = cleanup_observacions(row[OBSERVACIONS])
    present_catalogo = translate_catalogo_nacional(row[PRESENT_CATALOGO])
    reglament_ue = translate_catalogo_nacional(row[REGLAMENT_UE])
    plantilla = "INSERT INTO public.especieinvasora(id,idtaxon,idestatushistoric,idestatuscatalunya,idimatgeprincipal,observacions,present_catalogo,idestatusgeneral,reglament_ue) VALUES ('{0}','{1}','{2}','{3}',{4},'{5}'," + ("'{6}'" if present_catalogo == 'NULL' else "'{6}'") + ",'{7}','{8}');"
    if idtaxon is None:
        str_plantilla = plantilla.format(row[ID_ESPECIE].strip(), row[ID_ESPECIE].strip(), idestatushistoric, idestatuscatalunya, 'NULL', observacions, present_catalogo, idestatusgeneral, reglament_ue)
    else:
        str_plantilla = plantilla.format(row[ID_ESPECIE].strip(), idtaxon, idestatushistoric, idestatuscatalunya, 'NULL', observacions, present_catalogo, idestatusgeneral, reglament_ue)
    return str_plantilla


def get_id_grup_de_nom_grup(nomgrup):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM public.grup WHERE nom=%s;""", (nomgrup,))
    results = cursor.fetchall()
    if len(results) > 0:
        return results[0][0]
    return ''


def get_id_viaentrada_de_nom_viaentrada(nomviaentrada):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    nomv_strip = remove_accents(nomviaentrada)
    cursor.execute("""SELECT * FROM public.viaentrada WHERE viaentrada=%s;""", (nomv_strip,))
    results = cursor.fetchall()
    if len(results) > 0:
        return results[0][0]
    return ''

def get_id_zona_geografica_de_nom(nomzonageografica):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    nomz_strip = remove_accents(nomzonageografica)
    cursor.execute("""SELECT * FROM public.zonageografica WHERE nom=%s;""", (nomz_strip,))
    results = cursor.fetchall()
    if len(results) > 0:
        return results[0][0]
    return ''

def get_max_id_viaentradaespecie():
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""select max(to_number(id,'99999')) from public.viaentradaespecie;""")
    results = cursor.fetchall()
    if len(results) > 0:
        return results[0][0]
    return -1

def get_id_habitat_de_nom_habitat(nomhabitat):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM public.habitat WHERE habitat=%s;""", (nomhabitat,))
    results = cursor.fetchall()
    if len(results) > 0:
        return results[0][0]
    return ''


def get_idgrup_excepcio(grup_candidat):
    return{
        'Invertebrats aquàtics' : 'INV_Aqua',
        'Rèptils' : 'REP',
        'Macroalgues' : 'ALG',
        'Mamífers': 'MAM',
        'Peixos': 'PEI',
    }.get(grup_candidat,'')


def get_idzonageografica_excepcio(via_candidat):
    return {
        'Invertebrats aquàtics': 'INV_Aqua',
        'Rèptils': 'REP',
    }.get(via_candidat, '')


def genera_sentencia_cleanup_grup(fila):
    plantilla_sql = "DELETE FROM public.GRUPESPECIE where IDESPECIEINVASORA='{0}';"
    str_plantilla = plantilla_sql.format(fila[3].strip())
    return str_plantilla


def genera_sentencia_grup(fila):
    grup_candidat = fila[GRUP]
    id_grup = get_id_grup_de_nom_grup(grup_candidat)
    if id_grup == '':
        id_grup = get_idgrup_excepcio(grup_candidat)
    if id_grup == '':
        raise Exception("'" + grup_candidat + "' no és a la taula de grups, cal afegir-lo")
    plantilla_sql = "INSERT INTO public.GRUPESPECIE(ID,IDESPECIEINVASORA,IDGRUP) VALUES ('{0}','{1}','{2}');"
    str_plantilla = plantilla_sql.format(fila[ID_ESPECIE].strip(),fila[ID_ESPECIE].strip(),id_grup)
    return str_plantilla


def genera_sentencia_cleanup_viaentrada(fila):
    plantilla_sql = "DELETE FROM public.viaentradaespecie WHERE idespecieinvasora='{0}';"
    str_plantilla = plantilla_sql.format(fila[ID_ESPECIE].strip())
    return str_plantilla


def genera_sentencia_viaentrada(fila):
    viaentrada_candidat = fila[VIA_ENTRADA].strip().replace("'", "''")
    id_viaentrada = get_id_viaentrada_de_nom_viaentrada(viaentrada_candidat)
    idviaentradaespecie = uuid.uuid1()
    if id_viaentrada == '':
        id_viaentrada = fila[ID_ESPECIE].strip() + '_viaentrada'
        plantilla_sql = "INSERT INTO public.viaentrada(id,viaentrada) VALUES ('{0}','{1}');\nINSERT INTO public.viaentradaespecie(id,idespecieinvasora,idviaentrada) VALUES ('{2}','{3}','{4}');"
        str_plantilla = plantilla_sql.format(id_viaentrada, viaentrada_candidat, idviaentradaespecie, fila[ID_ESPECIE].strip(), id_viaentrada)
    else:
        plantilla_sql = "INSERT INTO public.viaentradaespecie(id,idespecieinvasora,idviaentrada) VALUES ('{0}','{1}','{2}');"
        str_plantilla = plantilla_sql.format(idviaentradaespecie, fila[ID_ESPECIE].strip(), id_viaentrada)
    return str_plantilla


def genera_sentencies_cleanup_noms(fila):
    plantilla_sql = "DELETE FROM public.nomvulgartaxon where idtaxon='{0}';"
    str_plantilla = plantilla_sql.format(fila[ID_ESPECIE].strip())
    return str_plantilla


def genera_sentencies_noms(fila,idtaxon=None):
    candidat_nom_ca = fila[NOM_CA].strip().replace("'", "''")
    candidat_nom_es = fila[NOM_ES].strip().replace("'", "''")
    candidat_nom_en = fila[NOM_EN].strip().replace("'", "''")
    resultats = []
    #idnomvulgartaxon = uuid.uuid1()
    str_plantilla_ca = ''
    str_plantilla_en = ''
    str_plantilla_es = ''
    str_plantilla = ''
    valor_nom_ca = 'NULL'
    valor_nom_en = 'NULL'
    valor_nom_es = 'NULL'

    plantilla_sql = "INSERT INTO public.nomvulgar(id,nomvulgar) VALUES ('{0}','{1}');"
    if candidat_nom_ca.strip() != '':
        str_plantilla_ca = plantilla_sql.format(fila[ID_ESPECIE].strip() + '_cat', candidat_nom_ca)
        valor_nom_ca = "'" + fila[ID_ESPECIE].strip() + "_cat'"
    if candidat_nom_en.strip() != '':
        str_plantilla_en = plantilla_sql.format(fila[ID_ESPECIE].strip() + '_eng', candidat_nom_en)
        valor_nom_en = "'" + fila[ID_ESPECIE].strip() + "_eng'"
    if candidat_nom_es.strip() != '':
        str_plantilla_es = plantilla_sql.format(fila[ID_ESPECIE].strip() + '_es', candidat_nom_es)
        valor_nom_es = "'" + fila[ID_ESPECIE].strip() + "_es'"


    #if valor_nom_ca != 'NULL' or valor_nom_en != 'NULL' or valor_nom_es != 'NULL':
        #plantilla_sql = "INSERT INTO public.nomvulgartaxon(id,idtaxon,idnomvulgar,idnomvulgar_eng,idnomvulgar_es) VALUES ('{0}','{1}',{2},{3},{4});"
        #str_plantilla = plantilla_sql.format(idnomvulgartaxon, fila[3].strip(), valor_nom_ca,valor_nom_en,valor_nom_es)

    if str_plantilla_ca != '':
        resultats.append(str_plantilla_ca)
    if str_plantilla_en != '':
        resultats.append(str_plantilla_en)
    if str_plantilla_es != '':
        resultats.append(str_plantilla_es)

    if valor_nom_ca != 'NULL':
        plantilla_sql = "INSERT INTO public.nomvulgartaxon(id,id_spinvasora,idnomvulgar) VALUES ('{0}','{1}',{2});"
        if idtaxon is None:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), fila[ID_ESPECIE].strip(), valor_nom_ca)
        else:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), idtaxon, valor_nom_ca)
        resultats.append(str_plantilla)

    if valor_nom_en != 'NULL':
        plantilla_sql = "INSERT INTO public.nomvulgartaxon(id,id_spinvasora,idnomvulgar) VALUES ('{0}','{1}',{2});"
        if idtaxon is None:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), fila[ID_ESPECIE].strip(), valor_nom_en)
        else:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), idtaxon, valor_nom_en)
        resultats.append(str_plantilla)

    if valor_nom_es != 'NULL':
        plantilla_sql = "INSERT INTO public.nomvulgartaxon(id,id_spinvasora,idnomvulgar) VALUES ('{0}','{1}',{2});"
        if idtaxon is None:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), fila[ID_ESPECIE].strip(), valor_nom_es)
        else:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), idtaxon, valor_nom_es)
        resultats.append(str_plantilla)

    return resultats


def genera_sentencia_cleanup_regionativa(fila):
    id_sp = fila[ID_ESPECIE].strip()
    plantilla_sql = "DELETE FROM public.regionativa WHERE idespecieinvasora='{0}';"
    str_plantilla = plantilla_sql.format(id_sp)
    return str_plantilla


def genera_sentencia_regionativa(fila,tesaure_zonageografica):
    regionativa_1_candidat = fila[REGIO_NATIVA_1]
    regionativa_2_candidat = fila[REGIO_NATIVA_2]
    regionativa_3_candidat = fila[REGIO_NATIVA_3]
    candidats = []
    resultats = []

    already_in = set()
    id_sp = fila[ID_ESPECIE].strip()
    if regionativa_1_candidat.split() != '':
        candidats.append(regionativa_1_candidat)
    if regionativa_2_candidat.split() != '':
        #if not regionativa_2_candidat.strip() in candidats:
        candidats.append(regionativa_2_candidat)
    if regionativa_3_candidat.split() != '':
        #if not regionativa_3_candidat.strip() in candidats:
        candidats.append(regionativa_3_candidat)
    for candidat in candidats:
        id_c_zonageografica = get_id_zona_geografica_de_nom(candidat)
        idregionativa = uuid.uuid1()
        if id_c_zonageografica == '':
            id_c_zonageografica = get_idzonageografica_excepcio(candidat)
        if id_c_zonageografica == '':
            id_c_zonageografica = id_sp + "_RNAT"
            try:
                tesaure_zonageografica[id_c_zonageografica]
                plantilla_sql = "INSERT INTO public.regionativa(id,idespecieinvasora,idzonageografica) VALUES ('{0}','{1}','{2}');"
                str_plantilla = plantilla_sql.format( idregionativa, id_sp, id_c_zonageografica)
            except KeyError:
                tesaure_zonageografica[id_c_zonageografica] = candidat
                plantilla_sql = "INSERT INTO public.ZONAGEOGRAFICA(ID,NOM) VALUES ('{0}','{1}');\nUPDATE public.ZONAGEOGRAFICA SET NOM='{2}' WHERE ID='{3}';\nINSERT INTO public.regionativa(id,idespecieinvasora,idzonageografica) VALUES ('{4}','{5}','{6}');"
                str_plantilla = plantilla_sql.format(id_c_zonageografica, candidat.replace("'", "''"), candidat.replace("'", "''"), id_c_zonageografica, idregionativa, id_sp, id_c_zonageografica)
        else:
            plantilla_sql = "INSERT INTO public.regionativa(id,idespecieinvasora,idzonageografica) VALUES ('{0}','{1}','{2}');"
            str_plantilla = plantilla_sql.format(idregionativa, id_sp, id_c_zonageografica)
        #if check_regionativa_no_existeix(id_sp,id_c_zonageografica):
        if not id_sp + id_c_zonageografica in already_in:
            resultats.append(str_plantilla)
            already_in.add(id_sp + id_c_zonageografica)
    return resultats


def genera_sentencia_cleanup_habitat(fila):
    plantilla_sql = "DELETE FROM public.HABITATESPECIE where idspinvasora='{0}';"
    str_plantilla = plantilla_sql.format(fila[ID_ESPECIE].strip())
    return str_plantilla


def genera_sentencia_habitat(fila, cached_habitat=None):
    habitat_candidat = fila[HABITAT]
    try:
        id_habitat = cached_habitat[habitat_candidat]
        #plantilla_sql = "INSERT INTO public.HABITAT(ID,HABITAT) VALUES ('{0}','{1}');\nINSERT INTO public.HABITATESPECIE(idspinvasora,idhabitat) VALUES ('{2}','{3}');"
        plantilla_sql = "INSERT INTO public.HABITATESPECIE(idspinvasora,idhabitat) VALUES ('{0}','{1}');"
        str_plantilla = plantilla_sql.format(fila[ID_ESPECIE].strip(), id_habitat)
        return str_plantilla
    except KeyError:
        if habitat_candidat.strip() == '':
            return "--HABITAT per {0} està en blanc".format(fila[3].strip())
        habitat_candidat = habitat_candidat.replace("'", "''")
        id_habitat = get_id_habitat_de_nom_habitat(habitat_candidat)
        if id_habitat == '':
            id_habitat = fila[ID_ESPECIE].strip() + '_HAB'
        cached_habitat[fila[HABITAT]] = id_habitat
        plantilla_sql = "INSERT INTO public.HABITAT(ID,HABITAT) VALUES ('{0}','{1}');\nINSERT INTO public.HABITATESPECIE(idspinvasora,idhabitat) VALUES ('{2}','{3}');"
        str_plantilla = plantilla_sql.format(id_habitat,habitat_candidat,fila[ID_ESPECIE].strip(),id_habitat)
        return str_plantilla


def genera_sentencies_actualitzacio_estatus_exotiques(file,dir_resultats,cached_taxon_resolution_results):
    with open(file, 'rt') as csvfile:
        file_array = []
        row_num = 0
        fails_codi_sp = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        print("Llegint fitxer de dades ...")
        for row in reader:
            file_array.append(row)
            if not check_codi_especie(row[ID_ESPECIE].strip()) and row_num != 0:
                print("Fila " + str(row_num) + " codi especie " + row[ID_ESPECIE] + " id_sp " + row[NOM_ESPECIE] + " no es a taula invasores ")
                fails_codi_sp.append(row_num)

        if len(fails_codi_sp) > 0:
            print("Afegeix les especies que falten i torna-ho a intentar, sortint...")
            return
        update_estatus_taxon = open(dir_resultats + "/update_status_taxon.sql", 'w')
        for row in file_array[1:]:
            row_num = row_num + 1
            try:
                #update = get_update_taula_spinvasora(row)
                update = get_update_estatus_catalunya(row)
            except Exception:
                print("Excepcio a fila - " + str(row_num))
                raise
            update_estatus_taxon.write(update)
            update_estatus_taxon.write("\n")


def genera_sentencies_llistat_exotiques_nou(file, dir_resultats):
    set_id_invasores = get_id_spinvasores()
    cached_habitat = {}
    tesaure_zonageografica = {}
    print("Llegint fitxer de dades...")
    with open(file, 'rt') as csvfile:
        file_array = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader)
        rows_insert = []
        for row in reader:
            file_array.append(row)
            if not row[ID_ESPECIE] in set_id_invasores:
                rows_insert.append(row)
                print("Not present " + row[ID_ESPECIE] + " " + row[NOM_ESPECIE])

        cleanup_grup = open(dir_resultats + "cleanup_grup_0.sql", 'w')
        cleanup_habitat = open(dir_resultats + "cleanup_habitat_1.sql", 'w')
        cleanup_regionativa = open(dir_resultats + "cleanup_regionativa_2.sql", 'w')
        cleanup_viaentrada = open(dir_resultats + "cleanup_viaentrada_3.sql", 'w')
        cleanup_noms = open(dir_resultats + "cleanup_noms_4.sql", 'w')
        cleanup_taxon = open(dir_resultats + "cleanup_taxon_5.sql", 'w')

        inserts_file_taxon = open(dir_resultats + "insert_taxon_6.sql", 'w')
        updates_file_taxon = open(dir_resultats + "update_taxon_6_1.sql", 'w')
        inserts_grup = open(dir_resultats + "insert_grup_7.sql", 'w')
        inserts_habitat = open(dir_resultats + "inserts_habitat_8.sql", 'w')
        inserts_regionativa = open(dir_resultats + "inserts_regionativa_9.sql", 'w')
        inserts_viaentrada = open(dir_resultats + "inserts_viaentrada_10.sql", 'w')
        inserts_noms = open(dir_resultats + "inserts_noms_11.sql", 'w')

        # inserts
        print("Escrivint inserts especies noves...")
        for fila in rows_insert:

            cleanup_taxon.write(get_cleanup_taula_spinvasora(fila))
            cleanup_taxon.write("\n")

            inserts_file_taxon.write(get_insert_taula_spinvasora_nou(fila))
            inserts_file_taxon.write("\n")

        print("Escrivint especies actualitzacio...")
        for fila in file_array:

            updates_file_taxon.write(get_update_taula_spinvasora(fila))
            updates_file_taxon.write("\n")

            cleanup_grup.write(genera_sentencia_cleanup_grup(fila))
            cleanup_grup.write("\n")

            inserts_grup.write(genera_sentencia_grup(fila))
            inserts_grup.write("\n")

            cleanup_habitat.write(genera_sentencia_cleanup_habitat(fila))
            cleanup_habitat.write("\n")

            str_sentencia_habitat = genera_sentencia_habitat(fila, cached_habitat)
            inserts_habitat.write(str_sentencia_habitat)
            inserts_habitat.write("\n")

            cleanup_regionativa.write(genera_sentencia_cleanup_regionativa(fila))
            cleanup_regionativa.write("\n")

            sentencies_regionativa = genera_sentencia_regionativa(fila, tesaure_zonageografica);
            for sentencia_regionativa in sentencies_regionativa:
                inserts_regionativa.write(sentencia_regionativa)
                inserts_regionativa.write("\n")

            cleanup_viaentrada.write(genera_sentencia_cleanup_viaentrada(fila))
            cleanup_viaentrada.write("\n")

            inserts_viaentrada.write(genera_sentencia_viaentrada(fila))
            inserts_viaentrada.write("\n")

            cleanup_noms.write(genera_sentencies_cleanup_noms(fila))
            cleanup_noms.write("\n")

            sentencies_noms = genera_sentencies_noms(fila)
            for sentencia_nom in sentencies_noms:
                inserts_noms.write(sentencia_nom)
                inserts_noms.write("\n")



def genera_sentencies_llistat_exotiques(file,dir_resultats,cached_taxon_resolution_results):
    with open(file, 'rb') as csvfile:
        file_array = []
        cached_habitat = {}
        row_num = 0
        fails_codi_sp = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        print("Llegint fitxer de dades ...")
        for row in reader:
            file_array.append(row)
            if row[ID_ESPECIE] == '':
                print("Fila " + str(row_num) + " no te codi oracle " + row[NOM_ESPECIE])
                fails_codi_sp.append(row_num)
            else:
                if not check_codi_especie(row[ID_ESPECIE].strip()) and row_num != 0:
                    print("Fila " + str(row_num) + " codi especie " + row[ID_ESPECIE] + " id_sp " + row[NOM_ESPECIE] + " no es a taula invasores ")
                    fails_codi_sp.append(row_num)
            row_num = row_num + 1

        inserts_file_taxon = open(dir_resultats + "insert_taxon_1.sql", 'w')
        inserts_file_spinvasora = open(dir_resultats + "insert_especieinvasora_2.sql", 'w')
        inserts_grup = open(dir_resultats + "insert_grup_3.sql", 'w')
        inserts_habitat = open(dir_resultats + "inserts_habitat_4.sql", 'w')
        inserts_regionativa = open(dir_resultats + "inserts_regionativa_5.sql", 'w')
        inserts_viaentrada = open(dir_resultats + "inserts_viaentrada_6.sql", 'w')
        inserts_noms = open(dir_resultats + "inserts_noms_7.sql", 'w')

        print("Recuperant invasores de taula de taxon")
        tesaure_zonageografica = {}


        for fila in file_array[1:]:
            sentencies_regionativa = genera_sentencia_regionativa(fila,tesaure_zonageografica);
            for sentencia_regionativa in sentencies_regionativa:
                inserts_regionativa.write(sentencia_regionativa)
                inserts_regionativa.write("\n")

        for num_fila in fails_codi_sp:
            fila = file_array[num_fila]
            print("Buscant idtaxon de " + fila[4] + "...")
            try:
                result = cached_taxon_resolution_results[fila[4]]
            except KeyError:
                result = get_id_invasora(fila[4])

            if type(result) is str:
                idtaxon = result
                #cached resolution successful
                print("Sentencia insert a public.especieinvasora ---> " + get_insert_taula_spinvasora(fila, idtaxon))
                inserts_file_spinvasora.write(get_insert_taula_spinvasora(fila, idtaxon))
                inserts_file_spinvasora.write("\n")
            elif type(result) is dict:
                idtaxon = result['idtaxon']
                idinvasora = result['idinvasora']
                if idtaxon == '' and idinvasora == '':
                    print( "Sentencia insert a sipan_mtaxon.taxon ---> " + get_insert_taula_mtaxon(fila) )
                    inserts_file_taxon.write(get_insert_taula_mtaxon(fila))
                    inserts_file_taxon.write("\n")
                    print( "Sentencia insert a public.especieinvasora ---> " + get_insert_taula_spinvasora(fila) )
                    inserts_file_spinvasora.write(get_insert_taula_spinvasora(fila))
                    inserts_file_spinvasora.write("\n")
                elif idtaxon != '' and idinvasora == '':
                    str_insert = get_insert_taula_spinvasora(fila, idtaxon)
                    print( "Sentencia insert a public.especieinvasora ---> " + str_insert )
                    inserts_file_spinvasora.write(str_insert)
                    inserts_file_spinvasora.write("\n")

            #idtaxon = get_id_invasora_codi_oracle(fila[2])
            # if (type(idtaxon) is str and idtaxon == '') or (type(idtaxon) is dict and idtaxon['idinvasora'] == ''):
            #     print fila[4] + " no te correspondencia a la taula de taxons "
            #     print "Sentencia insert a sipan_mtaxon.taxon ---> " + get_insert_taula_mtaxon(fila)
            #     inserts_file_taxon.write(get_insert_taula_mtaxon(fila))
            #     inserts_file_taxon.write("\n")
            #     print "Sentencia insert a public.especieinvasora ---> " + get_insert_taula_spinvasora(fila)
            #     inserts_file_spinvasora.write(get_insert_taula_spinvasora(fila))
            #     inserts_file_spinvasora.write("\n")
            # else:
            #     #print "Id invasora " + get_id_invasora(fila[4])
            #     if type(idtaxon) is dict:
            #         print "Id taxon ---> " + idtaxon['idinvasora']
            #         str_insert = get_insert_taula_spinvasora(fila, idtaxon['idinvasora'])
            #     else:
            #         print "Id taxon ---> " + idtaxon
            #         str_insert = get_insert_taula_spinvasora(fila, idtaxon)
            #     print "Sentencia insert a public.especieinvasora ---> " + str_insert
            #     inserts_file_spinvasora.write(str_insert)
            #     inserts_file_spinvasora.write("\n")
            inserts_grup.write(genera_sentencia_grup(fila))
            inserts_grup.write("\n")
            str_sentencia_habitat = genera_sentencia_habitat(fila, cached_habitat)
            inserts_habitat.write(str_sentencia_habitat)
            inserts_habitat.write("\n")
            #sentencies_regionativa = genera_sentencia_regionativa(fila,tesaure_zonageografica);
            #for sentencia_regionativa in sentencies_regionativa:
                #inserts_regionativa.write(sentencia_regionativa)
                #inserts_regionativa.write("\n")
            inserts_viaentrada.write(genera_sentencia_viaentrada(fila))
            inserts_viaentrada.write("\n")
            if idtaxon == '':
                sentencies_noms = genera_sentencies_noms(fila)
            else:
                sentencies_noms = genera_sentencies_noms(fila, idtaxon)
            for sentencia_nom in sentencies_noms:
                inserts_noms.write(sentencia_nom)
                inserts_noms.write("\n")


def genera_sentencies_biblio_nou(file, dir_resultats):
    index_camps = {
        'fitxa_catalogo_nacional': {'index_col': 37, 'label': 'Fitxa Catalogo Nacional'},
        'fitxa_descriptiva_atlas': {'index_col': 38, 'label': 'Fitxa descriptiva Atlas'},
        'gbif': {'index_col': 39, 'label': 'GBIF'},
        'daisie': {'index_col': 40, 'label': 'DAISIE'},
        'fitxa_daisie': {'index_col': 41, 'label': 'Fitxa DAISIE'},
        'invasiber': {'index_col': 42, 'label': 'Invasiber'},
        'gisd': {'index_col': 43, 'label': 'GISD'},
        'cabi': {'index_col': 44, 'label': 'CABI'},
        'nnss-uk': {'index_col': 45, 'label': 'NNSS-UK'},
        'ciesm': {'index_col': 46, 'label': 'CIESM'},
        'algaebase': {'index_col': 47, 'label': 'Algaebase'},
        'fishbase_sealifebase': {'index_col': 48, 'label': 'Fishbase / Sealifebase'},
        'nobanis': {'index_col': 49, 'label': 'NOBANIS'},
        'insectarium_virtual': {'index_col': 51, 'label': 'Insectarium virtual'},
        'eppo': {'index_col': 52, 'label': 'EPPO'},
        'flora_catalana': {'index_col': 56, 'label': 'Flora catalana'},
        'enc_virtual_vertebrados_espanoles': {'index_col': 63, 'label': 'Enciclopedia virtual Vertebrados Españoles'},
        'fitxa_enc_virtual_vertebrados_espanoles': {'index_col': 64,
                                                    'label': 'Fitxa Enciclopedia virtual Vertebrados Españoles'},
        'biblio_1': {'index_col': 65, 'label': 'Biblio 1'},
        'biblio_2': {'index_col': 66, 'label': 'Biblio 2'},
        'biblio_3': {'index_col': 67, 'label': 'Biblio 3'},
        'biblio_4': {'index_col': 68, 'label': 'Biblio 4'},
        'biblio_5': {'index_col': 69, 'label': 'Biblio 5'},
        'biblio_6': {'index_col': 70, 'label': 'Biblio 6'},
        'tesi_1': {'index_col': 71, 'label': 'Tesi 1'},
        'tesi_2': {'index_col': 72, 'label': 'Tesi 2'},
        'estrategia_control_1': {'index_col': 73, 'label': 'Estratègia control 1'},
        'estrategia_control_2': {'index_col': 74, 'label': 'Estratègia control 2'},
        'estrategia_control_3': {'index_col': 75, 'label': 'Estratègia control 3'},
    }

    with open(file, 'rt') as csvfile:
        file_array = []
        fails_especie_no_existeix = []
        fails_codi_sp = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        row_num = 0
        for row in reader:
            file_array.append(row)
            if row_num != 0:
                if not comprova_codi_esp(row):
                    fails_codi_sp.append(row_num)
                else:
                    success_codi_sp = True
            print("Processant fila " + str(row_num) + " ...")
            row_num += 1

        print("Trobats " + str(len(fails_codi_sp)) + " errors de codi especie")

        print("Intentant solventar problemes de codi especie")
        for rownum in fails_codi_sp:
            id_esp = file_array[rownum][3].replace('\xc2\xa0', ' ').strip()
            if check_especie_no_existeix(id_esp):
                fails_especie_no_existeix.append(id_esp)

        if len(fails_especie_no_existeix) > 0:
            for sp in fails_especie_no_existeix:
                print("La especie " + sp + " no es a la taula invasores")
            return

        inserts_docs = open(dir_resultats + "insert_docs_1" + ".sql", 'w')
        insert_docs_sp = open(dir_resultats + "insert_docs_sp_2" + ".sql", 'w')

        inserts_docs.write('delete from document where iddoc not in (select iddoc from actuacio);\n')
        inserts_docs.write('delete from documents where iddocument not in (select iddoc from actuacio);\n')

        first = True
        for fila in file_array:
            if first:
                first = False
            else:
                for key in index_camps.keys():
                    doc_candidate = fila[index_camps[key]['index_col']].replace("'", "''")
                    if doc_candidate.startswith('http'):
                        iddocument = key + '_' + fila[3]
                        titol = index_camps[key]['label']
                        str_insert_docs = "INSERT INTO documents(iddocument,titol,observacions,idextensio,nomoriginal) VALUES ('{0}','{1}','{2}','{3}','{4}');".format(
                            iddocument, titol, 'ext', 'html', doc_candidate)
                        inserts_docs.write(str_insert_docs)
                        inserts_docs.write("\n")
                        str_insert_docs_sp = "INSERT INTO document(id,idespecieinvasora,iddoc) VALUES ('{0}','{1}','{2}');".format(
                            iddocument, fila[3], iddocument)
                        insert_docs_sp.write(str_insert_docs_sp)
                        insert_docs_sp.write("\n")


def genera_sentencies_biblio(file,dir_resultats,cached_taxon_resolution_results):

    index_camps = {
        'fitxa_catalogo_nacional': {'index_col': 37, 'label': 'Fitxa Catalogo Nacional'},
        'fitxa_descriptiva_atlas': {'index_col': 38, 'label': 'Fitxa descriptiva Atlas'},
        'gbif': {'index_col': 39, 'label': 'GBIF'},
        'daisie': {'index_col': 40, 'label': 'DAISIE'},
        'fitxa_daisie': {'index_col': 41, 'label': 'Fitxa DAISIE'},
        'invasiber': {'index_col': 42, 'label': 'Invasiber'},
        'gisd': {'index_col': 43, 'label': 'GISD'},
        'cabi': {'index_col': 44, 'label': 'CABI'},
        'nnss-uk': {'index_col': 45, 'label': 'NNSS-UK'},
        'ciesm': {'index_col': 46, 'label': 'CIESM'},
        'algaebase': {'index_col': 47, 'label': 'Algaebase'},
        'fishbase_sealifebase': {'index_col': 48, 'label': 'Fishbase / Sealifebase'},
        'nobanis': {'index_col': 49, 'label': 'NOBANIS'},
        'insectarium_virtual': {'index_col': 51, 'label': 'Insectarium virtual'},
        'eppo': {'index_col': 52, 'label': 'EPPO'},
        'flora_catalana': {'index_col': 56, 'label': 'Flora catalana'},
        'enc_virtual_vertebrados_espanoles': {'index_col': 63, 'label': 'Enciclopedia virtual Vertebrados Españoles'},
        'fitxa_enc_virtual_vertebrados_espanoles': {'index_col': 64, 'label': 'Fitxa Enciclopedia virtual Vertebrados Españoles'},
        'biblio_1': {'index_col': 65, 'label': 'Biblio 1'},
        'biblio_2': {'index_col': 66, 'label': 'Biblio 2'},
        'biblio_3': {'index_col': 67, 'label': 'Biblio 3'},
        'biblio_4': {'index_col': 68, 'label': 'Biblio 4'},
        'biblio_5': {'index_col': 69, 'label': 'Biblio 5'},
        'biblio_6': {'index_col': 70, 'label': 'Biblio 6'},
        'tesi_1': {'index_col': 71, 'label': 'Tesi 1'},
        'tesi_2': {'index_col': 72, 'label': 'Tesi 2'},
        'estrategia_control_1': {'index_col': 73, 'label': 'Estratègia control 1'},
        'estrategia_control_2': {'index_col': 74, 'label': 'Estratègia control 2'},
        'estrategia_control_3': {'index_col': 75, 'label': 'Estratègia control 3'},
    }

    with open(file, 'rb') as csvfile:
        file_array = []
        fails_especie_no_existeix = []
        fails_codi_sp = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        row_num = 0
        for row in reader:
            file_array.append(row)
            if row_num != 0:
                if not comprova_codi_esp(row):
                    fails_codi_sp.append(row_num)
                else:
                    success_codi_sp = True
            print("Processant fila " + str(row_num) + " ...")
            row_num += 1

        print("Trobats " + str(len(fails_codi_sp)) + " errors de codi especie")

        print("Intentant solventar problemes de codi especie")
        for rownum in fails_codi_sp:
            sp_name = file_array[rownum][4].strip()
            try:
                id_spinvasora = cached_taxon_resolution_results[sp_name]
                if id_spinvasora == '':
                    if sp_name not in fails_especie_no_existeix:
                        fails_especie_no_existeix.append(sp_name)
            except KeyError:
                print("Recuperant codi especie invasora per " + sp_name)
                id_spinvasora = get_id_invasora(sp_name)
                if id_spinvasora == '':
                    if sp_name not in fails_especie_no_existeix:
                        fails_especie_no_existeix.append(sp_name)
                else:
                    cached_taxon_resolution_results[sp_name] = id_spinvasora

        if len(fails_especie_no_existeix) > 0:
            for sp in fails_especie_no_existeix:
                print("La especie " + sp + " no es a la taula invasores")
            return

        inserts_docs = open(dir_resultats + "insert_docs_1" + ".sql", 'w')
        insert_docs_sp = open(dir_resultats + "insert_docs_sp_2" + ".sql", 'w')

        inserts_docs.write('delete from document where iddoc not in (select iddoc from actuacio);\n')
        inserts_docs.write('delete from documents where iddocument not in (select iddoc from actuacio);\n')

        first = True
        for fila in file_array:
            if first:
                first = False
            else:
                for key in index_camps.keys():
                    doc_candidate =  fila[index_camps[key]['index_col']].replace("'", "''")
                    if doc_candidate.startswith('http'):
                        iddocument = key + '_' + fila[3]
                        titol = index_camps[key]['label']
                        str_insert_docs = "INSERT INTO documents(iddocument,titol,observacions,idextensio,nomoriginal) VALUES ('{0}','{1}','{2}','{3}','{4}');".format(iddocument, titol, 'ext', 'html', doc_candidate )
                        inserts_docs.write(str_insert_docs)
                        inserts_docs.write("\n")
                        str_insert_docs_sp = "INSERT INTO document(id,idespecieinvasora,iddoc) VALUES ('{0}','{1}','{2}');".format( iddocument, fila[3], iddocument )
                        insert_docs_sp.write(str_insert_docs_sp)
                        insert_docs_sp.write("\n")


def genera_sentencies_citacions_nou(file,dir_resultats):
    with open(file, 'rt') as csvfile:
        fails_codi_sp = []
        fails_sp_existeix = []
        fails_utm_format = []
        file_array = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        row_num = 0

        #read file, save errors
        print("Llegint fitxer de dades ...")
        for row in reader:
            file_array.append(row)
            if row_num != 0:
                if not comprova_codi_esp_nou(row[1].strip()):
                    fails_codi_sp.append(row)

                if check_especie_no_existeix(row[1].strip()):
                    fails_sp_existeix.append(row)

                if not comprova_format_coordenades(row):
                    fails_utm_format.append(row)

            print("Processant fila " + str(row_num) + " ...")
            row_num += 1

        print("Trobats " + str(len(fails_codi_sp)) + " errors de codi especie")
        print("Trobats " + str(len(fails_sp_existeix)) + " errors de especie no existent")
        print("Trobats " + str(len(fails_utm_format)) + " errors de format utm")


        if len(fails_codi_sp) > 0:
            for sp in fails_codi_sp:
                print("Fila no té codi especie -- " + str(sp))

        if len(fails_sp_existeix) > 0:

            for sp in fails_sp_existeix:
                print("Especie no existeix -- " + str(sp))

        print("Comprovant format d'UTMs")
        if len(fails_utm_format) == 0:
            print("UTMs Ok!")
        else:
            for row in fails_utm_format:
                print("Error utm a fila -- " + str(row))

        if len(fails_codi_sp) > 0 or len(fails_sp_existeix) > 0 or len(fails_utm_format) > 0:
            return

        inserts_file = open(dir_resultats + "insert_citacions.sql", 'w')
        deletes_file = open(dir_resultats + "delete_citacions.sql", 'w')
        plantilla_sql_insert = "INSERT INTO public.citacions(especie,idspinvasora,grup,utmx,utmy,localitat,municipi,comarca,provincia,data,autor_s,font,referencia,observacions,tipus_cita,habitat,tipus_mort,abundancia,codi_aca,codi_estacio,ind_ha,ind_capt) VALUES ('{0}','{1}','{2}',{3},{4},'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}',{20},{21});"
        plantilla_sql_delete = "DELETE FROM public.citacions WHERE especie='{0}' and idspinvasora='{1}' and grup='{2}' and utmx={3} and utmy={4} and localitat='{5}' and municipi='{6}' and comarca='{7}' and provincia='{8}' and data='{9}' and autor_s='{10}' and font='{11}' and referencia='{12}' and observacions='{13}' and tipus_cita='{14}' and habitat='{15}' and tipus_mort='{16}' and abundancia='{17}' and codi_aca='{18}' and codi_estacio='{19}' and ind_ha={20} and ind_capt={21};"
        iterlines = iter(file_array)
        next(iterlines)
        print("Escrivint fitxer...")
        line_num = 1
        inserts_file.write(" ALTER TABLE public.citacions ALTER COLUMN referencia TYPE character varying(4000); ")
        inserts_file.write("\n")
        inserts_file.write(" ALTER TABLE public.citacions ALTER COLUMN habitat TYPE character varying(500); ")
        inserts_file.write("\n")
        inserts_file.write(" DELETE FROM public.citacions where origen_dades is null; ")
        inserts_file.write("\n")
        for line in iterlines:
            clean_line = []
            item_num = 0
            for item in line:
                if (item_num == 20 or item_num == 21):
                    if item == '':
                        clean_line.append('NULL')
                    else:
                        clean_line.append(item)
                elif item_num == 3 or item_num == 4:
                    clean_line.append(item.replace(',','.'))
                else:
                    try:
                        clean_line.append(item.replace("'", "''"))
                    except AttributeError:
                        print ("Error replace - " + str(line_num) + " item " + str(item_num))
                item_num+=1
            try:
                inserts_file.write(plantilla_sql_insert.format(*clean_line))
                inserts_file.write("\n")
                deletes_file.write(plantilla_sql_delete.format(*clean_line))
                deletes_file.write("\n")
            except IndexError:
                print ("Error afegint linia - " + str(line_num) + " item " + str(item_num))
            line_num += 1
        inserts_file.write(" update citacions set geom=ST_PointFromText('POINT(' || utmx || ' ' || utmy || ')',23031); ")
        inserts_file.write(" update citacions set geom_4326=ST_Transform(geom,4326); ")
        inserts_file.write(" update citacions set geom_25831=st_transform(geom_4326,25831) where geom_4326 is not Null and geom_25831 is null; ")

def genera_sentencies_citacions(file, dir_resultats, cached_taxon_resolution_results):
    with open(file, 'rb') as csvfile:
        fails_codi_sp = []
        fails_utm_format = []
        fails_row_exists = []
        fails_especie_no_existeix = []
        file_array = []
        reader = csv.reader(csvfile, delimiter=';', quotechar='"')
        row_num = 0

        cached_taxon_resolution_results['Caulerpa cylindracea'] = 'Caul_race'

        #read file, save errors
        print("Llegint fitxer de dades ...")
        for row in reader:
            file_array.append(row)
            success_codi_sp = False
            success_format_coord = False
            if row_num != 0:
                if not comprova_codi_esp(row):
                    fails_codi_sp.append(row_num)
                else:
                    success_codi_sp = True

                if not comprova_format_coordenades(row):
                    fails_utm_format.append(row)
                else:
                    success_format_coord = True

                '''
                if success_codi_sp and success_format_coord:
                    if fila_es_a_la_base_dades(row):
                        fails_row_exists.append(row_num)
                '''

            '''
            if row_num != 0 and fila_es_a_la_base_dades(row):
                fails_row_exists.append(row_num)
            else:
                if not comprova_codi_esp(row):
                    if row_num != 0:
                        fails_codi_sp.append(row_num)

                if not comprova_format_coordenades(row):
                    if row_num != 0:
                        fails_utm_format.append(row_num)
            '''
            #print_one_liner("Processant fila " + str(row_num) + " ...")
            print("Processant fila " + str(row_num) + " ...")
            row_num += 1

        print("Trobats " + str(len(fails_codi_sp)) + " errors de codi especie")
        print("Trobats " + str(len(fails_utm_format)) + " errors de format utm")
        print("Trobades " + str(len(fails_row_exists)) + " linies ja presents a la base de dades")

        print("Intentant solventar problemes de codi especie")
        for rownum in fails_codi_sp:
            sp_name = file_array[rownum][0].strip()
            try:
                id_spinvasora = cached_taxon_resolution_results[sp_name]
                if id_spinvasora == '':
                    if sp_name not in fails_especie_no_existeix:
                        fails_especie_no_existeix.append(sp_name)
            except KeyError:
                print("Recuperant codi especie invasora per " + sp_name)
                id_spinvasora = get_id_invasora(sp_name)
                if id_spinvasora == '':
                    if sp_name not in fails_especie_no_existeix:
                        fails_especie_no_existeix.append(sp_name)
                else:
                    cached_taxon_resolution_results[sp_name] = id_spinvasora

        if len(fails_especie_no_existeix) > 0:
            for sp in fails_especie_no_existeix:
                print("La especie " + sp + " no es a la taula invasores")
            return

        print("Comprovant format d'UTMs")
        if len(fails_utm_format) == 0:
            print("UTMs Ok!")
        else:
            for rownum in fails_utm_format:
                print("Error utm a fila " + str(rownum))


        if len(fails_utm_format) > 0:
            print("Error critic, cal arreglar format de coordenades")
        else:
            #eliminem espais i merdes de noms especie
            iterlines = iter(file_array)
            next(iterlines)
            for line in iterlines:
                line[0] = line[0].strip()
            #arreglem problemes de ids
            for line_num in fails_codi_sp:
                try:
                    file_array[line_num][1] = cached_taxon_resolution_results[file_array[line_num][0].strip()]
                except IndexError:
                    print( str(line_num) )
            inserts_file = open(dir_resultats + "insert_citacions.sql", 'w')
            deletes_file = open(dir_resultats + "delete_citacions.sql", 'w')
            plantilla_sql_insert = "INSERT INTO public.citacions(especie,idspinvasora,grup,utmx,utmy,localitat,municipi,comarca,provincia,data,autor_s,citacio,font,referencia,observacions,tipus_cita,habitat,tipus_mort,abundancia,codi_aca,codi_estacio,ind_ha,ind_capt) VALUES ('{0}','{1}','{2}',{3},{4},'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}',{21},{22});"
            plantilla_sql_delete = "DELETE FROM public.citacions WHERE especie='{0}' and idspinvasora='{1}' and grup='{2}' and utmx={3} and utmy={4} and localitat='{5}' and municipi='{6}' and comarca='{7}' and provincia='{8}' and data='{9}' and autor_s='{10}' and citacio='{11}' and font='{12}' and referencia='{13}' and observacions='{14}' and tipus_cita='{15}' and habitat='{16}' and tipus_mort='{17}' and abundancia='{18}' and codi_aca='{19}' and codi_estacio='{20}' and ind_ha={21} and ind_capt={22};"
            iterlines = iter(file_array)
            next(iterlines)
            print("Escrivint fitxer...")
            line_num = 1
            for line in iterlines:
                if not line_num in fails_row_exists:
                    clean_line = []
                    item_num = 0
                    for item in line:
                        if(item_num == 21 or item_num == 22):
                            if item == '':
                                clean_line.append('NULL')
                            else:
                                clean_line.append(item)
                        else:
                            try:
                                clean_line.append(item.replace("'", "''"))
                            except AttributeError:
                                print ("Error replace - " + str(line_num) + " item " + str(item_num))
                        item_num+=1
                    try:
                        inserts_file.write(plantilla_sql_insert.format(*clean_line))
                        inserts_file.write("\n")
                        deletes_file.write(plantilla_sql_delete.format(*clean_line))
                        deletes_file.write("\n")
                    except IndexError:
                        print ("Error afegint linia - " + str(line_num) + " item " + str(item_num))
                line_num += 1
            inserts_file.write(" update citacions set geom=ST_PointFromText('POINT(' || utmx || ' ' || utmy || ')',23031); ")
            inserts_file.write(" update citacions set geom_4326=ST_Transform(geom,4326); ")


def genera_sentencies_presencia_nou(file, dir_resultats, mida_malla):
    mida_malla_str = str(mida_malla)
    with open(file, 'rt') as csvfile:
        file_array = []
        fails_codi_sp = []
        fails_especie_no_existeix = []
        fails_row_exists = []
        fails_quadricula = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        row_num = 0

        #read file, save errors
        print("Llegint fitxer de dades ...")
        for row in reader:
            # Forcem comprovacio de tots els codis
            file_array.append(row)
            if (row_num != 0 and fila_presencia_es_a_la_base_dades(row)) or fila_es_buida(row):
                fails_row_exists.append(row_num)
            else:
                if row_num != 0:
                    fails_codi_sp.append(row_num)
            if row_num != 0:
                if not comprova_codi_quadricula(row):
                    fails_quadricula.append(row)

            print("Processant fila " + str(row_num) + " ...")
            row_num += 1

        print("Trobades " + str(len(fails_row_exists)) + " linies ja presents a la base de dades")

        print("Intentant solventar problemes de codi especie")
        for rownum in fails_codi_sp:
            id_esp = file_array[rownum][1].replace('\xc2\xa0', ' ').strip()
            if check_especie_no_existeix(id_esp):
                fails_especie_no_existeix.append(id_esp)

        if len(fails_especie_no_existeix) > 0:
            print("Les seguents especies no son a la base de dades dinvasores, cal afegir-les:")
            for fail in fails_especie_no_existeix:
                print(fail)
            return 0

        if len(fails_quadricula) > 0:
            print("Els següents codis de quadricula no existeixen:")
            for fail in fails_quadricula:
                print(fail)
            return 0

        # eliminem espais i merdes de noms especie
        iterlines = iter(file_array)
        next(iterlines)
        for line in iterlines:
            line[0] = line[0].strip()

        inserts_file = open( dir_resultats + "insert_pres_" + mida_malla_str + "_" + mida_malla_str + ".sql", 'w')
        deletes_file = open( dir_resultats + "delete_pres_" + mida_malla_str + "_" + mida_malla_str + ".sql", 'w')
        plantilla_sql_insert = "INSERT INTO public.presencia_sp(idspinvasora,idquadricula) VALUES ('{0}','{1}');"
        plantilla_sql_delete = "DELETE FROM public.presencia_sp WHERE idspinvasora='{0}' and idquadricula='{1}';"

        iterlines = iter(file_array)
        next(iterlines)

        repeticions = set()
        print("Escrivint fitxer...")
        line_num = 1
        for line in iterlines:
            if not line_num in fails_row_exists:
                if not line_num in fails_row_exists:
                    clean_line = []
                    clean_line.append(line[1].strip())
                    clean_line.append(line[3].strip().upper())
                    clau = line[1].strip() + '_' + line[3].strip().upper()
                    if not clau in repeticions:
                        inserts_file.write(plantilla_sql_insert.format(*clean_line))
                        inserts_file.write("\n")
                        deletes_file.write(plantilla_sql_delete.format(*clean_line))
                        deletes_file.write("\n")
                        repeticions.add(clau)
            line_num += 1

        inserts_file_1 = open(dir_resultats + "insert_cintacions_" + mida_malla_str + "_" + mida_malla_str + ".sql", 'w')
        plantilla_sql_insert_1 = "INSERT INTO public.citacions_" + mida_malla_str + "(especie,idspinvasora,grup,utm_" + mida_malla_str + ",descripcio,data,anyo,autor_s,font,referencia) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}');"
        iterlines = iter(file_array)
        next(iterlines)
        print("Escrivint fitxer citacions...")
        inserts_file_1.write("DELETE FROM public.citacions_" + mida_malla_str + ";")
        inserts_file_1.write("\n")
        for line in iterlines:
            clean_line = []
            clean_line.append(line[0].strip().replace("'", "''"))
            clean_line.append(line[1].strip().replace("'", "''"))
            clean_line.append(sinonims_grups[line[2].strip()])
            clean_line.append(line[3].strip().replace("'", "''"))
            clean_line.append(line[4].strip().replace("'", "''"))
            clean_line.append(line[5].strip().replace("'", "''"))
            clean_line.append(line[6].strip().replace("'", "''"))
            clean_line.append(line[7].strip().replace("'", "''"))
            clean_line.append(line[8].strip().replace("'", "''"))
            clean_line.append(line[9].strip().replace("'", "''"))
            inserts_file_1.write(plantilla_sql_insert_1.format(*clean_line))
            inserts_file_1.write("\n")


def genera_sentencies_presencia(file,dir_resultats,cached_taxon_resolution_results,mida_malla):
    mida_malla_str = str(mida_malla)
    with open(file, 'rb') as csvfile:
        file_array = []
        fails_codi_sp = []
        fails_especie_no_existeix = []
        fails_row_exists = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        row_num = 0

        #read file, save errors
        print("Llegint fitxer de dades ...")
        for row in reader:
            # Forcem comprovacio de tots els codis
            file_array.append(row)
            if (row_num != 0 and fila_presencia_es_a_la_base_dades(row)) or fila_es_buida(row):
                fails_row_exists.append(row_num)
            else:
                if row_num != 0:
                    fails_codi_sp.append(row_num)

            #print_one_liner("Processant fila " + str(row_num) + " ...")
            print("Processant fila " + str(row_num) + " ...")
            row_num += 1

        print("Trobats " + str(len(fails_codi_sp)) + " errors de codi especie")
        print("Trobades " + str(len(fails_row_exists)) + " linies ja presents a la base de dades")

        print("Intentant solventar problemes de codi especie")
        for rownum in fails_codi_sp:
            sp_name = file_array[rownum][0].replace('\xc2\xa0', ' ').strip()
            try:
                id_spinvasora = cached_taxon_resolution_results[sp_name]
                if id_spinvasora == '':
                    if sp_name not in fails_especie_no_existeix:
                        fails_especie_no_existeix.append(sp_name)
            except KeyError:
                print("Recuperant codi especie invasora per " + sp_name)
                id_spinvasora = get_id_invasora(sp_name)
                if id_spinvasora == '':
                    if sp_name not in fails_especie_no_existeix:
                        fails_especie_no_existeix.append(sp_name)
                else:
                    cached_taxon_resolution_results[sp_name] = id_spinvasora
        print (cached_taxon_resolution_results)

        if len(fails_especie_no_existeix) > 0:
            print("Les seguents especies no son a la base de dades dinvasores, cal afegir-les:")
            for fail in fails_especie_no_existeix:
                print(fail)
            return 0

        # eliminem espais i merdes de noms especie
        iterlines = iter(file_array)
        next(iterlines)
        for line in iterlines:
            line[0] = line[0].strip()
        # arreglem problemes de ids
        for line_num in fails_codi_sp:
            try:
                file_array[line_num][1] = cached_taxon_resolution_results[file_array[line_num][0].replace('\xc2\xa0', ' ').strip()]
            except IndexError:
                print(str(line_num))

        inserts_file = open( dir_resultats + "insert_pres_" + mida_malla_str + "_" + mida_malla_str + ".sql", 'w')
        deletes_file = open( dir_resultats + "delete_pres_" + mida_malla_str + "_" + mida_malla_str + ".sql", 'w')
        plantilla_sql_insert = "INSERT INTO public.presencia_sp(idspinvasora,idquadricula) VALUES ('{0}','{1}');"
        plantilla_sql_delete = "DELETE FROM public.presencia_sp WHERE idspinvasora='{0}' and idquadricula='{1}';"

        iterlines = iter(file_array)
        next(iterlines)

        repeticions = set()
        print("Escrivint fitxer...")
        line_num = 1
        for line in iterlines:
            if not line_num in fails_row_exists:
                if not line_num in fails_row_exists:
                    clean_line = []
                    clean_line.append(line[1].strip())
                    clean_line.append(line[3].strip().upper())
                    clau = line[1].strip() + '_' + line[3].strip().upper()
                    if not clau in repeticions:
                        inserts_file.write(plantilla_sql_insert.format(*clean_line))
                        inserts_file.write("\n")
                        deletes_file.write(plantilla_sql_delete.format(*clean_line))
                        deletes_file.write("\n")
                        repeticions.add(clau)
            line_num += 1

def genera_sentencies_actualitzacio_sinonims_exotiques(file_llistat_exotiques,dir_resultats):
    with open(file_llistat_exotiques, 'rt') as csvfile:
        file_array = []
        row_num = 0
        fails_codi_sp = []
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        print("Llegint fitxer de dades ...")
        for row in reader:
            file_array.append(row)
            if not check_codi_especie(row[ID_ESPECIE].strip()) and row_num != 0:
                print("Fila " + str(row_num) + " codi especie " + row[ID_ESPECIE] + " id_sp " + row[NOM_ESPECIE] + " no es a taula invasores ")
                fails_codi_sp.append(row_num)

        if len(fails_codi_sp) > 0:
            print("Afegeix les especies que falten i torna-ho a intentar, sortint...")
            return
        plantilla_sql_update = "UPDATE public.especieinvasora set sinonims='{0}' where id='{1}';"
        update_sinonims_taxon = open(dir_resultats + "/update_sinonims_taxon.sql", 'w')
        for row in file_array[1:]:
            clean_line = []
            if row[14].strip().replace("'", "''") != '':
                clean_line.append(row[14].strip().replace("'", "''"))
                clean_line.append(row[ID_ESPECIE].strip().replace("'", "''"))
                update_sinonims_taxon.write(plantilla_sql_update.format(*clean_line))
                update_sinonims_taxon.write("\n")
            else:
                update_sinonims_taxon.write("UPDATE public.especieinvasora set sinonims=NULL where id='{0}';".format(row[ID_ESPECIE]))
                update_sinonims_taxon.write("\n")

def main():
    cached_taxon_resolution_results = {}
    cached_taxon_resolution_results['Echinochloa crus-galli'] = 'Echi_crus'
    cached_taxon_resolution_results['Prunus domestica'] = 'Prun_dome'
    cached_taxon_resolution_results['Prunus domestica subsp. domestica'] = 'Prun_domd'
    cached_taxon_resolution_results['Corbicula fluminalis'] = 'Corb_flui'
    cached_taxon_resolution_results['Ferrissia fragilis'] = 'Ferr_frag'
    cached_taxon_resolution_results['Corbicula fluminea'] = 'Corb_flum'
    cached_taxon_resolution_results['Aedes (Stegomyia) albopictus'] = 'Aede_albo'
    cached_taxon_resolution_results['Cartodere (Aridius) bifasciata'] = 'Cart_bifa'
    cached_taxon_resolution_results['Anas platyrhynchos var. domestica'] = 'Anas_plat'
    cached_taxon_resolution_results['Anser anser var.domestica'] = 'Anse_anse'
    cached_taxon_resolution_results['Anser anser var. domestica'] = 'Anse_anse'
    cached_taxon_resolution_results['Cairina moschata var.domestica'] = 'Cair_mosc'
    cached_taxon_resolution_results['Cairina moschata var. domestica'] = 'Cair_mosc'
    cached_taxon_resolution_results['Columba livia var. domestica'] = 'Colu_livi'
    cached_taxon_resolution_results['Melopsittacus undulatus var. domestica'] = 'Melo_undu'
    cached_taxon_resolution_results['Nymphicus hollandicus var. domestica'] = 'Nymp_holl'
    cached_taxon_resolution_results['Serinus canaria var. domestica'] = 'Seri_cana'
    cached_taxon_resolution_results['Streptopelia roseogrisea var. domestica'] = 'Stre_rose'
    cached_taxon_resolution_results['Amaranthus blitum subsp. emarginatus'] = 'Amar_blie'
    cached_taxon_resolution_results['Beta vulgaris subsp. vulgaris'] = 'Beta_vulg'
    cached_taxon_resolution_results['Crepis sancta subsp. sancta'] = 'Crep_sanc'
    cached_taxon_resolution_results['Cyperus alternifolius subsp. flabelliformis'] = 'Cype_alte'
    cached_taxon_resolution_results['Delphinium orientale subsp. orientale'] = 'Delp_orie'
    cached_taxon_resolution_results['Echinochloa crus-galli subsp. crus-galli'] = 'Echi_cruc'
    cached_taxon_resolution_results['Eleusine tristachya subsp. barcinonensis'] = 'Eleu_tris'
    cached_taxon_resolution_results['Elymus elongatus subsp. ponticus'] = 'Elym_elon'
    cached_taxon_resolution_results['Eragrostis mexicana subsp. virescens'] = 'Erag_mexi'
    cached_taxon_resolution_results['Hedera cf. algeriensis'] = 'Hede_alge'
    cached_taxon_resolution_results['Hesperis matronalis subsp. matronalis'] = 'Hesp_matr'
    cached_taxon_resolution_results['Lepidium virginicum subsp. virginicum'] = 'Lepi_virg'
    cached_taxon_resolution_results['Linum usitatissimum subsp. usitatissimum'] = 'Linu_usit'
    cached_taxon_resolution_results['Ludwigia peploides subsp. montevidensis'] = 'Ludw_pepl'
    cached_taxon_resolution_results['Lunaria annua subsp. annua'] = 'Luna_annu'
    cached_taxon_resolution_results['Medicago sativa subsp. sativa'] = 'Medi_sati'
    cached_taxon_resolution_results['Melissa officinalis subsp. officinalis'] = 'Meli_offi'
    cached_taxon_resolution_results['Oenothera biennis subsp. biennis'] = 'Oeno_bieb'
    cached_taxon_resolution_results['Oenothera x oehlkersii'] = 'Oeno_oehl'
    cached_taxon_resolution_results['Olea europaea var. europaea'] = 'Olea_euro'
    cached_taxon_resolution_results['Opuntia lindheimeri var. linguliformis'] = 'Opun_lind'
    cached_taxon_resolution_results['Opuntia microdasys var. microdasys'] = 'Opun_micr'
    cached_taxon_resolution_results['Oxalis debilis subsp. corymbosa'] = 'Oxal_debi'
    cached_taxon_resolution_results['Panicum philadelphicum subsp. gattingeri'] = 'Pani_phil'
    cached_taxon_resolution_results['Papaver somniferum subsp. somniferum'] = 'Papa_somn'
    cached_taxon_resolution_results['Phalaris canariensis subsp. canariensis'] = 'Phal_cana'
    cached_taxon_resolution_results['Platanus x hispanica'] = 'Plat_hisp'
    cached_taxon_resolution_results['Populus x canadensis'] = 'Popu_cana'
    cached_taxon_resolution_results['Solidago canadensis subsp. canadensis'] = 'Soli_canc'
    cached_taxon_resolution_results['Ursinia nana subsp. nana'] = 'Ursi_nana'
    cached_taxon_resolution_results['Veronica peregrina subsp. peregrina'] = 'Vero_pere'
    cached_taxon_resolution_results['Xanthium echinatum subsp. italicum'] = 'Xant_echi'
    cached_taxon_resolution_results['Xanthium echinatum subsp. Italicum'] = 'Xant_echi'
    cached_taxon_resolution_results['Kalanchoe x houghtonii'] = 'Kala_houg'
    cached_taxon_resolution_results['Berberis aquifolium'] = 'Maho_aqui'
    cached_taxon_resolution_results['Planorbella duryiduryi'] = 'Plan_dury'
    cached_taxon_resolution_results['Coronilla valentina subsp. glauca'] = 'Coro_vale'
    cached_taxon_resolution_results['Forsythia x intermedia'] = 'Fors_inte'
    cached_taxon_resolution_results['Helianthus x laetiflorus'] = 'Heli_laet'
    cached_taxon_resolution_results['Vitis x koberi'] = 'Viti_kobe'
    cached_taxon_resolution_results['Vitis x goliath'] = 'Viti_goli'
    cached_taxon_resolution_results['Melia azederach'] = 'Meli_azed'
    cached_taxon_resolution_results['Physalis viscosa'] = 'Phys_fusc'
    cached_taxon_resolution_results['Symphoricarpus albus'] = 'Symp_albu'
    cached_taxon_resolution_results['Xiphophorus maculatus'] = 'Xiph_sp'

    cached_taxon_resolution_results['Caulerpa cylindracea'] = 'Caul_race'
    cached_taxon_resolution_results['Helix (Helix) lucorum'] = 'Heli_luco'
    cached_taxon_resolution_results['Helix (Helix) melanostoma'] = 'Heli_mela'
    cached_taxon_resolution_results['Lyctus (Xylotrogus) brunneus'] = 'Lyct_brun'
    cached_taxon_resolution_results['Omosita (Saprobia) discoidea'] = 'Omos_disc'
    cached_taxon_resolution_results['Paromalus (Isolomalus) luderti'] = 'Paro_lude'
    cached_taxon_resolution_results['Pheidole indica'] = 'Phei_tene'
    cached_taxon_resolution_results['Saprinus (Saprinus) lugens'] = 'Sapr_luge'
    cached_taxon_resolution_results['Trachyopella (Trachyopella) straminea'] = 'Trac_stra'
    cached_taxon_resolution_results['Tribolium (Stene) confusum'] = 'Trib_conf'
    cached_taxon_resolution_results['Tribolium (Tribolium) castanaeum'] = 'Trib_cast'
    cached_taxon_resolution_results['Gallus gallus var. domestica'] = 'Gall_gall'
    cached_taxon_resolution_results['Varanus exanthematicus'] = 'Vara_exan'
    cached_taxon_resolution_results['Allium paniculatum subsp. fuscum'] = 'Alli_pani'
    cached_taxon_resolution_results['Althaea hirsuta subsp. Longiflora'] = 'Alth_hirs'
    cached_taxon_resolution_results['Ammannia baccifera subsp. aegyptiaca'] = 'Amma_bacc'
    cached_taxon_resolution_results['Brassica oleracea subsp. oleracea'] = 'Bras_oler'
    cached_taxon_resolution_results['Camelina sativa subsp. rumelica'] = 'Came_sati'
    cached_taxon_resolution_results['Cedrus libani subsp. atlantica'] = 'Cedr_liba'
    cached_taxon_resolution_results['Convolvulus sabatius subsp. mauritanicus'] = 'Conv_saba'
    cached_taxon_resolution_results['Convolvulus tricolor subsp. tricolor'] = 'Conv_tric'
    cached_taxon_resolution_results['Cymbalaria muralis subsp. muralis'] = 'Cymb_mura'
    cached_taxon_resolution_results['Echeveria cf. waltheri'] = 'Eche_walt'
    cached_taxon_resolution_results['Echinochloa crus-galli subsp. hispidula'] = 'Echi_cruh'
    cached_taxon_resolution_results['Echinochloa crus-galli subsp. oryzicola'] = 'Echi_cruo'
    cached_taxon_resolution_results['Echinochloa crus-galli subsp. oryzoides'] = 'Echi_crur'
    cached_taxon_resolution_results['Hypericum hircinum subsp. hircinum'] = 'Hype_hirc'
    cached_taxon_resolution_results['Lens culinaris subsp. culinaris'] = 'Lens_culi'
    cached_taxon_resolution_results['Leptochloa fusca subsp. uninervia'] = 'Lept_fusc'
    cached_taxon_resolution_results['Levisticum officinale subsp. officinale'] = 'Levi_offi'
    cached_taxon_resolution_results['Medicago arborea subsp. arborea'] = 'Medi_arbo'
    cached_taxon_resolution_results['Oenothera biennis subsp. suaveolens'] = 'Oeno_bies'
    cached_taxon_resolution_results['Phlomis purpurea subsp. purpurea'] = 'Phlo_purp'
    cached_taxon_resolution_results['Picea abies subsp. abies'] = 'Pice_abie'
    cached_taxon_resolution_results['Pyrus communis subsp. communis'] = 'Pyru_comm'
    cached_taxon_resolution_results['Pyrus malus subsp. mitis'] = 'Pyru_malu'
    cached_taxon_resolution_results['Raphanus raphanistrum subsp. sativus'] = 'Raph_raph'
    cached_taxon_resolution_results['Salix x rubens'] = 'Sali_rube'
    cached_taxon_resolution_results['Solidago canadensis subsp. altissima'] = 'Soli_cana'
    cached_taxon_resolution_results['Solidago gigantea subsp. serotina'] = 'Soli_giga'
    cached_taxon_resolution_results['Tritonia x crocosmiiflora'] = 'Trit_croc'
    cached_taxon_resolution_results['Vicia villosa subsp. varia'] = 'Vici_vill'
    cached_taxon_resolution_results['Hermetia illuscens'] = 'Herm_illu'
    cached_taxon_resolution_results['Succinea (Calcisuccinea) sp'] = 'Succ_sp'
    cached_taxon_resolution_results['Alnus alnobetula subsp. Alnobetula'] = 'Alnu_alno'
    cached_taxon_resolution_results['Diplachne fusca subsp. uninervia'] = 'Lept_fusc'
    cached_taxon_resolution_results['Silene coronaria'] = 'Lych_coro'
    cached_taxon_resolution_results['Fallopia japonica'] = 'Reyn_japo'
    cached_taxon_resolution_results['Solanum sisymbriifolium'] = 'Sola_sisy'
    cached_taxon_resolution_results['Delairea odorata'] = 'Sene_mika'
    cached_taxon_resolution_results['Austrocylindropuntia subulata'] = 'Opun_subu'
    cached_taxon_resolution_results['Pseudemys nelsoni'] = 'Pseu_nels'

    cached_taxon_resolution_results['Lucasianus levaillantii'] = 'Luca_leva'
    cached_taxon_resolution_results['Datura inoxia'] = 'Datu_inox'
    cached_taxon_resolution_results['Hyssopus officinalis subsp. officinalis'] = 'Hyss_offi'
    cached_taxon_resolution_results['Diplachne fusca subsp. Uninervia'] = 'Lept_fusc'
    cached_taxon_resolution_results['Onchorhynchus mykis'] = 'Onco_myki'
    cached_taxon_resolution_results['Parachondrostoma miegi'] = 'Para_mieg'
    cached_taxon_resolution_results['Poa subcarerulea'] = 'Poa_subc'
    cached_taxon_resolution_results['Festuca valesiaca'] = 'Fest_vale'
    cached_taxon_resolution_results['Agropyron cristatum var. pectiniforme'] = 'Agro_cris'
    cached_taxon_resolution_results['Lunaria annua subsp. Annua'] = 'Luna_annu'
    cached_taxon_resolution_results['Aloe x delaetii'] = 'Aloe_dela'
    cached_taxon_resolution_results['Begonia x semperflorens'] = 'Bego_semp'
    cached_taxon_resolution_results['Crassula tetragona subsp. robusta'] = 'Cras_tetr'
    cached_taxon_resolution_results['Fragaria x ananassa'] = 'Frag_anan'
    cached_taxon_resolution_results['Narcissus x cyclazetta'] = 'Narc_cycl'
    cached_taxon_resolution_results['Vitis x bacoi'] = 'Viti_baco'
    cached_taxon_resolution_results['Lamium galeobdolon subsp. argentatum'] = 'Lami_gale'
    cached_taxon_resolution_results['Aesculus hipposcastanum'] = 'Aesc_hipp'
    cached_taxon_resolution_results['Coreopsis cf. lanceolata'] = 'Core_lanc'
    cached_taxon_resolution_results['Mentha x piperita'] = 'Ment_pipe'
    cached_taxon_resolution_results['Senecio pteorophorus'] = 'Sene_pter'
    cached_taxon_resolution_results['Trifolium incarnatum subsp. incarnatum'] = 'Trif_inca'
    cached_taxon_resolution_results['Gypsophila elegans'] = 'Gyps_eleg'
    cached_taxon_resolution_results['Cereus jamacaru'] = 'Cere_peru'
    cached_taxon_resolution_results['Iris x sambucina'] = 'Iris_samb'
    cached_taxon_resolution_results['Aspagarus officinalis subsp. officinalis'] = 'Aspa_offi'
    cached_taxon_resolution_results['Aspagarus officinalis subsp. officinalis'] = 'Aspa_offi'
    cached_taxon_resolution_results['Pinctada radiata'] = '40908'
    cached_taxon_resolution_results['Berberis vulgaris subsp.vulgaris'] = '2794'


    file_llistat_exotiques = config.params['file_llistat_exotiques']
    file_citacions = config.params['file_citacions']
    file_presencia_1_1 = config.params['file_presencia_1_1']
    file_presencia_10_10 = config.params['file_presencia_10_10']
    dir_resultats = config.params['dir_resultats']
    # genera_sentencies_llistat_exotiques_nou(file_llistat_exotiques,dir_resultats)
    # in file citacions - column font is referencia, remove column Any, subst ',' with '.' on column IND/Ha and UTM
    # sed -i 's/Camp_intr/Camp_intro/g' exocat_citacions_2023.csv
    # sed -i 's/Camp_introo/Camp_intro/g' exocat_citacions_2023.csv
    # sed -i 's/Open_humi/Opun_humi/g' exocat_citacions_2023.csv
    # sed -i 's/Plat_x hi/Plat_hisp/g' exocat_citacions_2023.csv
    # sed -i 's/Spil_sene/Stre_sene/g' exocat_citacions_2023.csv
    # sed -i 's/Trac_scr/Trac_scsp/g' exocat_citacions_2023.csv
    # sed -i 's/Vane_arma/Vane_arm/g' exocat_citacions_2023.csv
    # sed -i 's/Aix _gale/Aix_gale/g' exocat_citacions_2023.csv
    # sed -i 's/Psit_mitr/Arat_mitr/g' exocat_citacions_2023.csv
    # sed -i 's/Trac_scspe/Trac_scre/g' exocat_citacions_2023.csv
    # sed -i 's/Trac_scspt/Trac_scrt/g' exocat_citacions_2023.csv
    # sed -i 's/Trac_scsps/Trac_scrs/g' exocat_citacions_2023.csv
    # sed -i 's/Anse_conf/Anse_anse/g' exocat_citacions_2023.csv
    # genera_sentencies_citacions_nou(file_citacions,dir_resultats)
    # sed -i 's/Stem_lute/Ster_lute/g' exocat_citacions_2023_utm_1_1.csv
    # sed -i 's/Omm_ophr/Omma_ophr/g' exocat_citacions_2023_utm_1_1.csv
    # sed -i 's/Gobi_loza/Gobi_luza/g' exocat_citacions_2023_utm_1_1.csv
    # sed -i 's/Frei_fili/Free_leic/g' exocat_citacions_2023_utm_1_1.csv
    # sed -i 's/Euph_post/Euph_pros/g' exocat_citacions_2023_utm_1_1.csv
    # sed -i 's/Elym_elong/Elym_elon/g' exocat_citacions_2023_utm_1_1.csv
    # sed -i 's/Brom_will/Brom_cath/g' exocat_citacions_2023_utm_1_1.csv
    # sed -i 's/Cf3781/CF3781/g' exocat_citacions_2024_utm_1_1.csv
    # sed -i 's/CF49687/CF4968/g' exocat_citacions_2024_utm_1_1.csv
    genera_sentencies_presencia_nou(file_presencia_1_1, dir_resultats,1)
    # sed -i 's/Gobi_loza/Gobi_luza/g' exocat_citacions_2023_utm_10_10.csv
    # sed -i 's/Misc_angu/Misg_angu/g' exocat_citacions_2023_utm_10_10.csv
    # genera_sentencies_presencia_nou(file_presencia_10_10, dir_resultats, 10)
    #genera_sentencies_actualitzacio_estatus_exotiques(file_llistat_exotiques,dir_resultats,cached_taxon_resolution_results)
    #genera_sentencies_actualitzacio_sinonims_exotiques(file_llistat_exotiques,dir_resultats)
    #genera_sentencies_biblio_nou(file_llistat_exotiques, dir_resultats)

    ##### COMPROVACIONS POST #####
    '''
    El camp geom de citacions ha de tenir valors
    Els id de citacions es corresponen amb id d'especie invasora:
    select distinct idspinvasora,especie from citacions where idspinvasora not in (select distinct id from especieinvasora);
    '''
    ##########

if __name__=='__main__':
    main()
