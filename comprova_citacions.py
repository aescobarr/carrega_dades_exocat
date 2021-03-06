# This Python file uses the following encoding: utf-8
import csv
import config
import psycopg2
import sys
import uuid
from sets import Set
import unicodedata
import re

header_names_citacions = ['Especie','CODI_ESP','GRUP','UTMX','UTMY','Localitat','Municipi','Comarca','Provincia','Data','Autors','Citacio','Font','Referencia','Observacions','Tipus cita','Habitat','Tipus mort','Abundancia','Codi ACA','Codi estacio','IND_Ha','Ind. Capt.']
header_names_1_1 = ['ESPECIE','CODI_ESP','GRUP','CODIUTM','Descripcio','Data','Autors','CITACIO','FONT','REF','Observacions','Tipus cita','Habitat','Revisio']
header_names_exotiques = ['Grup','IdGrup','Codi_Oracle','IdEsp','Especie','Referencia','Nom catala','Nom castella','Nom angles','Divisio','Classe','Ordre','Familia','Sinonims','Regio nativa_1','Regio nativa_2','Regio nativa_3','Via entrada','Habitat','Estatus historic','Estatus Catalunya','Estatus Espanya','Catalogo Nacional','Font_info','Observacions','Primera citacio','Font primera citacio','Fotos','Dades distribucio','Revisio Oracle','Taxonomia (ITIS)','BDBC','Revisio BIOCAT','DAISIE','InvasIber','GISD','CIESM','Algaebase','Fishbase','NOBANIS','Estatus CAC  (Llista patro, 2010)','Insectarium virtual','EPPO','Flora Iberica','Sanz Elorza et al. 2004','Bolos et al., 1990-2005','Flora catalana','Casasayas','Mapes Casasayas','Anthos','Mapa Anthos','Observacions nostres','Dades bibliografiques i herbari (no georeferenciades)']
conn_string = "host='" + config.params['db_host'] + "' dbname='" + config.params['db_name'] + "' user='" + config.params['db_user'] + "' password='" + config.params['db_password'] + "'"

def remove_accents(str):
    s = unicode(str,"utf-8")
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))


def print_one_liner(message):
    sys.stdout.write('%s\r' % message)
    sys.stdout.flush()

def comprova_codi_ACA(row):
    return True

def comprova_codi_esp(row):
    if row[1] == '':
        return False
    return True

def fila_es_buida(row):
    for element in row:
        if element != '':
            return False
    return True


def check_regionativa_no_existeix(idespecieinvasora,idzonageografica):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM sipan_mexocat.regionativa WHERE idespecieinvasora=%s and idzonageografica=%s;""", (idespecieinvasora,idzonageografica,))
    results = cursor.fetchall()
    if (len(results) == 0):
        return True
    return False

def check_codi_especie(id):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT id FROM sipan_mexocat.especieinvasora WHERE id=%s;""", (id,))
    results = cursor.fetchall()
    if (len(results) > 0):
        return True
    return False

def get_idspinvasora_deidtaxon(idtaxon):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT id FROM sipan_mexocat.especieinvasora WHERE idtaxon=%s;""", (idtaxon,))
    results = cursor.fetchall()
    if (len(results) > 0):
        return results[0][0]
    else:
        return ''

def get_id_desempat(cursor_rows):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    number_hits = 0
    candidate_id = ''
    for row in cursor_rows:
        cursor.execute("""SELECT id FROM sipan_mexocat.especieinvasora WHERE idtaxon=%s;""",(row[0],))
        results = cursor.fetchall()
        if(len(results) > 0):
            number_hits += 1
            candidate_id = results[0][0]
    if number_hits == 1:
        return candidate_id
    return ''


def split_nom_especie(sp_name):
    result = {}
    elements_nom = sp_name.split(' ')
    genere = elements_nom[0]
    especie = elements_nom[1]
    subespecie = ''
    if (len(elements_nom) == 3):
        subespecie = elements_nom[2]
    elif (len(elements_nom) > 3):
        subespecie = ' '.join(elements_nom[2:])
    else:
        pass
    result['genere']=genere
    result['especie'] = especie
    if subespecie != '':
        result['subespecie'] = subespecie
    return result


def get_id_invasora_codi_oracle(codi_oracle):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT id FROM sipan_mexocat.especieinvasora WHERE idtaxon=%s;""", (codi_oracle,))
    cursor_rows = cursor.fetchall()
    if len(cursor_rows) == 0:
        return ''
    else:
        return cursor_rows[0]


def get_id_invasora(sp_name):
    elements_nom = sp_name.split(' ')
    genere = elements_nom[0].replace('\xc2\xa0', ' ').strip()
    especie = elements_nom[1].replace('\xc2\xa0', ' ').strip()
    subespecie = ''
    if (len(elements_nom) == 3):
        subespecie = elements_nom[2]
    elif (len(elements_nom) > 3):
        subespecie = ' '.join(elements_nom[2:])
    else:
        pass

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    if subespecie != '':
        cursor.execute("""SELECT * FROM sipan_mtaxons.taxon WHERE trim(both from genere)=%s and trim(both from especie)=%s and trim(both from subespecie)=%s;""",(genere, especie, subespecie))
    else:
        cursor.execute("""SELECT * FROM sipan_mtaxons.taxon WHERE trim(both from genere)=%s and trim(both from especie)=%s;""", (genere, especie))
    cursor_rows = cursor.fetchall()
    if len(cursor_rows) == 0:
        return ''
    elif len(cursor_rows) == 1:
        #return cursor_rows[0][0]
        return get_idspinvasora_deidtaxon(cursor_rows[0][0])
    else:
        print "Hi ha multiples especies per " + sp_name
        #if subespecie != '':
            #print "Hi ha multiples especies amb genere " + genere + " especie " + especie + " i subespecie " + subespecie
        #else:
            #print "Hi ha multiples especies amb genere " + genere + " i especie " + especie

        print("Intentant desempatar multiples ids")
        id_desempat = get_id_desempat(cursor_rows)
        if id_desempat == '':
            rownum = 0
            for row in cursor_rows:
                print "opcio " + str(rownum) + ": " + ', '.join( item for item in row if item )
                rownum += 1
            print("Impossible desempatar, cal entrada de l'usuari")
            opcio = raw_input("Tria una opcio:")
            return cursor_rows[int(opcio)][0]
        else:
            print("Desempat amb exit, identificador assignat!")
            return id_desempat


def comprova_format_coordenades(row):
    utmx = row[3].strip()
    utmy = row[4].strip()
    if "," in utmx or "," in utmy:
        return False
    try:
        float(utmx)
        float(utmy)
        return True
    except ValueError:
        return False

def fila_presencia_es_a_la_base_dades(row):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM sipan_mexocat.presencia_sp WHERE idquadricula=%s and idspinvasora=%s;""",
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
    plantilla = "INSERT INTO SIPAN_MTAXONS.TAXON(ID,NOMSP,TESAUREBIOCAT,CODIBIOCAT,GENERE,ESPECIE,AUTORESPECIE) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}');"
    taxonomia = split_nom_especie(row[4])
    genere = taxonomia['genere']
    especie = taxonomia['especie']
    subespecie = ''
    try:
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
    cursor.execute("""SELECT * FROM sipan_mexocat.estatus WHERE id=%s;""",(idstatus,))
    results = cursor.fetchall()
    return len(results) > 0

def cleanup_observacions(observacions):
    observacions = observacions.replace("'", "''")
    observacions = observacions.replace("’", "''")
    return observacions

def get_update_taula_spinvasora(row):
    idestatushistoric = translate_status(row[19])
    if not check_status_is_present(idestatushistoric):
        raise Exception(row[19] + " " + idestatushistoric + " no es a la base de dades, cal afegir el codi --> INSERT INTO sipan_mexocat.estatus(id,nom) VALUES('" + idestatushistoric + "','" + row[19] + "');")
    idestatuscatalunya = translate_status(row[20])
    if not check_status_is_present(idestatuscatalunya):
        raise Exception(idestatuscatalunya + ' no es a la base de dades, cal afegir el codi')
    idestatusgeneral = idestatuscatalunya
    observacions = cleanup_observacions(row[25])
    present_catalogo = translate_catalogo_nacional(row[22])
    plantilla = "UPDATE sipan_mexocat.especieinvasora set idestatushistoric='{0}',idestatuscatalunya='{1}',observacions='{2}',present_catalogo=" + ( "'{3}'" if present_catalogo == 'NULL' else "'{3}'") + ",idestatusgeneral='{4}' WHERE id='{5}';"
    str_plantilla = plantilla.format(idestatushistoric, idestatuscatalunya, observacions, present_catalogo, idestatusgeneral, row[3].strip())
    return str_plantilla

def get_insert_taula_spinvasora(row, idtaxon=None):
    idestatushistoric = translate_status(row[19])
    if not check_status_is_present(idestatushistoric):
        raise Exception(row[19] + ' ' + idestatushistoric + ' no es a la base de dades, cal afegir el codi')
    idestatuscatalunya = translate_status(row[20])
    if not check_status_is_present(idestatuscatalunya):
        raise Exception(idestatuscatalunya + ' no es a la base de dades, cal afegir el codi')
    idestatusgeneral = idestatuscatalunya
    observacions = cleanup_observacions(row[25])
    present_catalogo = translate_catalogo_nacional(row[22])
    plantilla = "INSERT INTO sipan_mexocat.especieinvasora(id,idtaxon,idestatushistoric,idestatuscatalunya,idimatgeprincipal,observacions,present_catalogo,idestatusgeneral) VALUES ('{0}','{1}','{2}','{3}',{4},'{5}'," + ("'{6}'" if present_catalogo == 'NULL' else "'{6}'") + ",'{7}');"
    if idtaxon is None:
        str_plantilla = plantilla.format(row[3].strip(), row[3].strip(), idestatushistoric, idestatuscatalunya, 'NULL', observacions, present_catalogo, idestatusgeneral)
    else:
        str_plantilla = plantilla.format(row[3].strip(), idtaxon, idestatushistoric, idestatuscatalunya, 'NULL', observacions, present_catalogo, idestatusgeneral)
    return str_plantilla

def get_id_grup_de_nom_grup(nomgrup):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM sipan_mexocat.grup WHERE nom=%s;""", (nomgrup,))
    results = cursor.fetchall()
    if len(results) > 0:
        return results[0][0]
    return ''


def get_id_viaentrada_de_nom_viaentrada(nomviaentrada):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    nomv_strip = remove_accents(nomviaentrada)
    cursor.execute("""SELECT * FROM sipan_mexocat.viaentrada WHERE viaentrada=%s;""", (nomv_strip,))
    results = cursor.fetchall()
    if len(results) > 0:
        return results[0][0]
    return ''

def get_id_zona_geografica_de_nom(nomzonageografica):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    nomz_strip = remove_accents(nomzonageografica)
    cursor.execute("""SELECT * FROM sipan_mexocat.zonageografica WHERE nom=%s;""", (nomz_strip,))
    results = cursor.fetchall()
    if len(results) > 0:
        return results[0][0]
    return ''

def get_max_id_viaentradaespecie():
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""select max(to_number(id,'99999')) from sipan_mexocat.viaentradaespecie;""")
    results = cursor.fetchall()
    if len(results) > 0:
        return results[0][0]
    return -1

def get_id_habitat_de_nom_habitat(nomhabitat):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM sipan_mexocat.habitat WHERE habitat=%s;""", (nomhabitat,))
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

def genera_sentencia_grup(fila):
    grup_candidat = fila[0]
    id_grup = get_id_grup_de_nom_grup(grup_candidat)
    if id_grup == '':
        id_grup = get_idgrup_excepcio(grup_candidat)
    if id_grup == '':
        raise Exception("'" + grup_candidat + "' no és a la taula de grups, cal afegir-lo")
    plantilla_sql = "INSERT INTO SIPAN_MEXOCAT.GRUPESPECIE(ID,IDESPECIEINVASORA,IDGRUP) VALUES ('{0}','{1}','{2}');"
    str_plantilla = plantilla_sql.format(fila[3].strip(),fila[3].strip(),id_grup)
    return str_plantilla

def genera_sentencia_viaentrada(fila):
    viaentrada_candidat = fila[17].strip().replace("'", "''")
    id_viaentrada = get_id_viaentrada_de_nom_viaentrada(viaentrada_candidat)
    idviaentradaespecie = uuid.uuid1()
    if id_viaentrada == '':
        id_viaentrada = fila[3].strip() + '_viaentrada'
        plantilla_sql = "INSERT INTO sipan_mexocat.viaentrada(id,viaentrada) VALUES ('{0}','{1}');\nINSERT INTO SIPAN_MEXOCAT.viaentradaespecie(id,idespecieinvasora,idviaentrada) VALUES ('{2}','{3}','{4}');"
        str_plantilla = plantilla_sql.format(id_viaentrada, viaentrada_candidat, idviaentradaespecie, fila[3].strip(), id_viaentrada)
    else:
        plantilla_sql = "INSERT INTO SIPAN_MEXOCAT.viaentradaespecie(id,idespecieinvasora,idviaentrada) VALUES ('{0}','{1}','{2}');"
        str_plantilla = plantilla_sql.format(idviaentradaespecie, fila[3].strip(), id_viaentrada)
    return str_plantilla

def genera_sentencies_noms(fila,idtaxon=None):
    candidat_nom_ca = fila[6].strip().replace("'", "''")
    candidat_nom_es = fila[7].strip().replace("'", "''")
    candidat_nom_en = fila[8].strip().replace("'", "''")
    resultats = []
    #idnomvulgartaxon = uuid.uuid1()
    str_plantilla_ca = ''
    str_plantilla_en = ''
    str_plantilla_es = ''
    str_plantilla = ''
    valor_nom_ca = 'NULL'
    valor_nom_en = 'NULL'
    valor_nom_es = 'NULL'

    plantilla_sql = "INSERT INTO sipan_mtaxons.nomvulgar(id,nomvulgar) VALUES ('{0}','{1}');"
    if candidat_nom_ca.strip() != '':
        str_plantilla_ca = plantilla_sql.format(fila[3].strip() + '_cat', candidat_nom_ca)
        valor_nom_ca = "'" + fila[3].strip() + "_cat'"
    if candidat_nom_en.strip() != '':
        str_plantilla_en = plantilla_sql.format(fila[3].strip() + '_eng', candidat_nom_en)
        valor_nom_en = "'" + fila[3].strip() + "_eng'"
    if candidat_nom_es.strip() != '':
        str_plantilla_es = plantilla_sql.format(fila[3].strip() + '_es', candidat_nom_es)
        valor_nom_es = "'" + fila[3].strip() + "_es'"


    #if valor_nom_ca != 'NULL' or valor_nom_en != 'NULL' or valor_nom_es != 'NULL':
        #plantilla_sql = "INSERT INTO sipan_mtaxons.nomvulgartaxon(id,idtaxon,idnomvulgar,idnomvulgar_eng,idnomvulgar_es) VALUES ('{0}','{1}',{2},{3},{4});"
        #str_plantilla = plantilla_sql.format(idnomvulgartaxon, fila[3].strip(), valor_nom_ca,valor_nom_en,valor_nom_es)

    if str_plantilla_ca != '':
        resultats.append(str_plantilla_ca)
    if str_plantilla_en != '':
        resultats.append(str_plantilla_en)
    if str_plantilla_es != '':
        resultats.append(str_plantilla_es)

    if valor_nom_ca != 'NULL':
        plantilla_sql = "INSERT INTO sipan_mtaxons.nomvulgartaxon(id,idtaxon,idnomvulgar) VALUES ('{0}','{1}',{2});"
        if idtaxon is None:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), fila[3].strip(), valor_nom_ca)
        else:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), idtaxon, valor_nom_ca)
        resultats.append(str_plantilla)

    if valor_nom_en != 'NULL':
        plantilla_sql = "INSERT INTO sipan_mtaxons.nomvulgartaxon(id,idtaxon,idnomvulgar) VALUES ('{0}','{1}',{2});"
        if idtaxon is None:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), fila[3].strip(), valor_nom_en)
        else:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), idtaxon, valor_nom_en)
        resultats.append(str_plantilla)

    if valor_nom_es != 'NULL':
        plantilla_sql = "INSERT INTO sipan_mtaxons.nomvulgartaxon(id,idtaxon,idnomvulgar) VALUES ('{0}','{1}',{2});"
        if idtaxon is None:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), fila[3].strip(), valor_nom_es)
        else:
            str_plantilla = plantilla_sql.format(uuid.uuid1(), idtaxon, valor_nom_es)
        resultats.append(str_plantilla)

    return resultats

def genera_sentencia_regionativa(fila,tesaure_zonageografica):
    regionativa_1_candidat = fila[14]
    regionativa_2_candidat = fila[15]
    regionativa_3_candidat = fila[16]
    candidats = []
    resultats = []

    already_in = Set()
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
            id_c_zonageografica = fila[3].strip() + "_RNAT"
            try:
                tesaure_zonageografica[id_c_zonageografica]
                plantilla_sql = "INSERT INTO sipan_mexocat.regionativa(id,idespecieinvasora,idzonageografica) VALUES ('{0}','{1}','{2}');"
                str_plantilla = plantilla_sql.format(idregionativa, fila[3].strip(), id_c_zonageografica)
            except KeyError:
                tesaure_zonageografica[id_c_zonageografica] = candidat
                plantilla_sql = "INSERT INTO SIPAN_MEXOCAT.ZONAGEOGRAFICA(ID,NOM) VALUES ('{0}','{1}');\nUPDATE SIPAN_MEXOCAT.ZONAGEOGRAFICA SET NOM='{2}' WHERE ID='{3}';\nINSERT INTO sipan_mexocat.regionativa(id,idespecieinvasora,idzonageografica) VALUES ('{4}','{5}','{6}');"
                str_plantilla = plantilla_sql.format(id_c_zonageografica, candidat.replace("'", "''"), candidat.replace("'", "''"), id_c_zonageografica, idregionativa, fila[3].strip(), id_c_zonageografica)
        else:
            plantilla_sql = "INSERT INTO sipan_mexocat.regionativa(id,idespecieinvasora,idzonageografica) VALUES ('{0}','{1}','{2}');"
            str_plantilla = plantilla_sql.format(idregionativa,fila[3].strip(),id_c_zonageografica)
        if check_regionativa_no_existeix(fila[3].strip(),id_c_zonageografica):
            if not fila[3].strip() + id_c_zonageografica in already_in:
                resultats.append(str_plantilla)
                already_in.add(fila[3].strip() + id_c_zonageografica)
    return resultats


def genera_sentencia_habitat(fila):
    habitat_candidat = fila[18]
    if habitat_candidat.strip() == '':
        return "--HABITAT per {0} està en blanc".format(fila[3].strip())
    habitat_candidat = habitat_candidat.replace("'", "''")
    id_habitat = get_id_habitat_de_nom_habitat(habitat_candidat)
    if id_habitat == '':
        id_habitat = fila[3].strip() + '_HAB'
    plantilla_sql = "INSERT INTO SIPAN_MEXOCAT.HABITAT(ID,HABITAT) VALUES ('{0}','{1}');\nINSERT INTO SIPAN_MEXOCAT.HABITATESPECIE(idspinvasora,idhabitat) VALUES ('{2}','{3}');"
    str_plantilla = plantilla_sql.format(id_habitat,habitat_candidat,fila[3].strip(),id_habitat)
    return str_plantilla


def genera_sentencies_actualitzacio_estatus_exotiques(file,dir_resultats,cached_taxon_resolution_results):
    with open(file, 'rb') as csvfile:
        file_array = []
        row_num = 0
        fails_codi_sp = []
        reader = csv.reader(csvfile, delimiter=';', quotechar='"')
        print("Llegint fitxer de dades ...")
        for row in reader:
            file_array.append(row)
            if not check_codi_especie(row[3].strip()) and row_num != 0:
                print "Fila " + str(row_num) + " codi especie " + row[3] + " id_sp " + row[4] + " no es a taula invasores "
                fails_codi_sp.append(row_num)

        if len(fails_codi_sp) > 0:
            print "Afegeix les especies que falten i torna-ho a intentar, sortint..."
            return
        update_estatus_taxon = open(dir_resultats + "/update_status_taxon.sql", 'w')
        for row in file_array[1:]:
            row_num = row_num + 1
            try:
                update = get_update_taula_spinvasora(row)
            except Exception:
                print "Excepcio a fila - " + str(row_num)
                raise
            update_estatus_taxon.write(update)
            update_estatus_taxon.write("\n")



def genera_sentencies_llistat_exotiques(file,dir_resultats,cached_taxon_resolution_results):
    with open(file, 'rb') as csvfile:
        file_array = []
        row_num = 0
        fails_codi_sp = []
        reader = csv.reader(csvfile, delimiter=';', quotechar='"')
        print("Llegint fitxer de dades ...")
        for row in reader:
            file_array.append(row)
            if row[3] == '':
                print "Fila " + str(row_num) + " no te codi oracle " + row[4]
            else:
                if not check_codi_especie(row[3].strip()) and row_num != 0:
                    print "Fila " + str(row_num) + " codi especie " + row[3] + " id_sp " + row[4] + " no es a taula invasores "
                    fails_codi_sp.append(row_num)
            #print "Fila " + str(row_num) + " codi especie " + row[3] + " id_sp " + row[4]
            row_num = row_num + 1

        inserts_file_taxon = open(dir_resultats + "/insert_taxon_1.sql", 'w')
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
            print "Buscant idtaxon de " + fila[4] + "..."
            #idtaxon = get_id_invasora(fila[4])
            idtaxon = get_id_invasora_codi_oracle(fila[2])
            if idtaxon == '':
                print fila[4] + " no te correspondencia a la taula de taxons "
                print "Sentencia insert a sipan_mtaxon.taxon ---> " + get_insert_taula_mtaxon(fila)
                inserts_file_taxon.write(get_insert_taula_mtaxon(fila))
                inserts_file_taxon.write("\n")
                print "Sentencia insert a sipan_mexocat.especieinvasora ---> " + get_insert_taula_spinvasora(fila)
                inserts_file_spinvasora.write(get_insert_taula_spinvasora(fila))
                inserts_file_spinvasora.write("\n")
            else:
                #print "Id invasora " + get_id_invasora(fila[4])
                print "Id taxon ---> " + idtaxon
                str_insert = get_insert_taula_spinvasora(fila, idtaxon)
                print "Sentencia insert a sipan_mexocat.especieinvasora ---> " + str_insert
                inserts_file_spinvasora.write(str_insert)
                inserts_file_spinvasora.write("\n")
            inserts_grup.write(genera_sentencia_grup(fila))
            inserts_grup.write("\n")
            inserts_habitat.write(genera_sentencia_habitat(fila))
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


def genera_sentencies_citacions(file,dir_resultats,cached_taxon_resolution_results):
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
                    fails_utm_format.append(row_num)
                else:
                    success_format_coord = True

                if success_codi_sp and success_format_coord:
                    if fila_es_a_la_base_dades(row):
                        fails_row_exists.append(row_num)

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
                print("Error utm a fila " + str(rownum+1) + ": " + file_array[rownum-1][3] + ", " + file_array[rownum-1][4])


        if len(fails_utm_format) > 0:
            print("Error critic, cal arreglar format de coordenades a files " + ' ,'.join(map(str,fails_utm_format)))
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
                    print str(line_num)
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

def genera_sentencies_presencia(file,dir_resultats,cached_taxon_resolution_results,mida_malla):
    mida_malla_str = str(mida_malla)
    with open(file, 'rb') as csvfile:
        file_array = []
        fails_codi_sp = []
        fails_especie_no_existeix = []
        fails_row_exists = []
        reader = csv.reader(csvfile, delimiter=';', quotechar='"')
        row_num = 0

        cached_taxon_resolution_results['Echinochloa crus-galli'] = 'Echi_crus'
        cached_taxon_resolution_results['Prunus domestica'] = 'Prun_dome'
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
        print cached_taxon_resolution_results

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
                print str(line_num)

        inserts_file = open( dir_resultats + "insert_pres_" + mida_malla_str + "_" + mida_malla_str + ".sql", 'w')
        deletes_file = open( dir_resultats + "delete_pres_" + mida_malla_str + "_" + mida_malla_str + ".sql", 'w')
        plantilla_sql_insert = "INSERT INTO sipan_mexocat.presencia_sp(idspinvasora,idquadricula) VALUES ('{0}','{1}');"
        plantilla_sql_delete = "DELETE FROM sipan_mexocat.presencia_sp WHERE idspinvasora='{0}' and idquadricula='{1}';"

        iterlines = iter(file_array)
        next(iterlines)

        repeticions = Set()
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

def main():
    cached_taxon_resolution_results = {}
    #cached_taxon_resolution_results['Caulerpa cylindracea'] = 'Caul_race'
    file_llistat_exotiques = '/home/webuser/dev/python/carrega_dades_exocat/actualitzacio_dades_6/llistat_exotiques_exocat_dec_2018.csv'
    file_citacions = '/home/webuser/dev/python/carrega_dades_exocat/actualitzacio_dades_6/exocat_citacions_2018.csv'
    file_presencia_1_1 = '/home/webuser/dev/python/carrega_dades_exocat/actualitzacio_dades_6/exocat_citacions_2018_utm_1_1.csv'
    file_presencia_10_10 = '/home/webuser/dev/python/carrega_dades_exocat/actualitzacio_dades_6/exocat_citacions_2018_utm_10_10.csv'
    dir_resultats = '/home/webuser/dev/python/carrega_dades_exocat/actualitzacio_dades_6/'
    #genera_sentencies_llistat_exotiques(file_llistat_exotiques,dir_resultats,cached_taxon_resolution_results)
    #genera_sentencies_citacions(file_citacions,dir_resultats,cached_taxon_resolution_results)
    #genera_sentencies_presencia(file_presencia_1_1, dir_resultats, cached_taxon_resolution_results,1)
    genera_sentencies_presencia(file_presencia_10_10, dir_resultats, cached_taxon_resolution_results, 10)
    #genera_sentencies_actualitzacio_estatus_exotiques(file_llistat_exotiques,dir_resultats,cached_taxon_resolution_results)


if __name__=='__main__':
    main()