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

def get_id_invasora(sp_name):
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

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    if subespecie != '':
        cursor.execute("""SELECT * FROM sipan_mtaxons.taxon WHERE genere=%s and especie=%s and subespecie=%s;""",(genere, especie, subespecie))
    else:
        cursor.execute("""SELECT * FROM sipan_mtaxons.taxon WHERE genere=%s and especie=%s;""", (genere, especie))
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
    cursor.execute("""SELECT id FROM public.citacions WHERE especie=%s and utmx=%s and utmy=%s;""",(row[0].strip(),row[3].strip(),row[4].strip(),))
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
        return ''
    estatus = estatus.strip()
    if estatus == 'Arqueòfit/Arqueozou?':
        return 'ARQUEF_ARQUEO?'
    if estatus == 'Possiblement translocada':
        return 'TRANS?'
    if estatus == 'Possiblement desapareguda':
        return 'POSSD'
    if estatus == 'Possible reintroduïda':
        return 'REINT?'
    if estatus == 'No establerta(exòtica?)':
        return 'NO_EST/EXO?'
    if estatus == 'No establerta':
        return 'NO_EST'
    if estatus == 'Naturalitzada dubtosa':
        return 'Natur?'
    if estatus == 'Naturalitzada(citació puntual)':
        return 'Natur_cit punt'
    if estatus == 'Naturalitzada':
        return 'Natur'
    if estatus == 'Invasora localment':
        return 'INV_LOC'
    if estatus == 'Invasora':
        return 'INV'
    if estatus == 'Introduïda dubtosa':
        return 'INTROD?'
    if estatus == 'Introduïda(potencialment invasora)':
        return 'INTROD_POTEN'
    if estatus == 'Introduïda(possiblement només plantada)':
        return 'INTROD_plant'
    if estatus == 'Introduïda(possiblement desapareguda)':
        return 'INTROD_desap'
    if estatus == 'Introduïda(eliminada del medi)':
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
    if estatus == 'Adventícia(possiblement desapareguda)':
        return 'Adven_desap'
    if estatus == 'Adventícia(en regressió)':
        return 'Adven_regre'
    if estatus == 'Adventícia (citació puntual)':
        return 'Adven_cit punt'
    if estatus == 'Adventícia':
        return 'Adven'
    if estatus == 'Translocada(adventícia)':
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
    if estatus=='Nativa':
        return 'NATIVA'
    raise Exception('Estatus ' + estatus + ' desconegut')

def translate_catalogo_nacional(catalogo):
    if catalogo.strip() == '':
        return 'NULL'
    if catalogo.strip().startswith('S'):
        return 'S'
    if catalogo.strip().startswith('N'):
        return 'N'
    return 'NULL'

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

def get_insert_taula_spinvasora(row):
    idestatushistoric = translate_status(row[19])
    if not check_status_is_present(idestatushistoric):
        raise Exception(row[19] + ' ' + idestatushistoric + ' no es a la base de dades, cal afegir el codi')
    idestatuscatalunya = translate_status(row[20])
    if not check_status_is_present(idestatuscatalunya):
        raise Exception(idestatuscatalunya + ' no es a la base de dades, cal afegir el codi')
    idestatusgeneral = idestatuscatalunya
    observacions = cleanup_observacions(row[24])
    present_catalogo = translate_catalogo_nacional(row[22])
    plantilla = "INSERT INTO sipan_mexocat.especieinvasora(id,idtaxon,idestatushistoric,idestatuscatalunya,idimatgeprincipal,observacions,present_catalogo,idestatusgeneral) VALUES ('{0}','{1}','{2}','{3}',{4},'{5}'," + ("'{6}'" if present_catalogo == 'NULL' else "'{6}'") + ",'{7}');"
    str_plantilla = plantilla.format(row[3].strip(), row[3].strip(), idestatushistoric, idestatuscatalunya, 'NULL', observacions, present_catalogo, idestatusgeneral)
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

def genera_sentencies_noms(fila):
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
        str_plantilla = plantilla_sql.format(uuid.uuid1(), fila[3].strip(), valor_nom_ca)
        resultats.append(str_plantilla)

    if valor_nom_en != 'NULL':
        plantilla_sql = "INSERT INTO sipan_mtaxons.nomvulgartaxon(id,idtaxon,idnomvulgar) VALUES ('{0}','{1}',{2});"
        str_plantilla = plantilla_sql.format(uuid.uuid1(), fila[3].strip(), valor_nom_en)
        resultats.append(str_plantilla)

    if valor_nom_es != 'NULL':
        plantilla_sql = "INSERT INTO sipan_mtaxons.nomvulgartaxon(id,idtaxon,idnomvulgar) VALUES ('{0}','{1}',{2});"
        str_plantilla = plantilla_sql.format(uuid.uuid1(), fila[3].strip(), valor_nom_es)
        resultats.append(str_plantilla)

    return resultats

def genera_sentencia_regionativa(fila,tesaure_zonageografica):
    regionativa_1_candidat = fila[14]
    regionativa_2_candidat = fila[15]
    regionativa_3_candidat = fila[16]
    candidats = []
    resultats = []
    if regionativa_1_candidat.split() != '':
        candidats.append(regionativa_1_candidat)
    if regionativa_2_candidat.split() != '':
        candidats.append(regionativa_2_candidat)
    if regionativa_3_candidat.split() != '':
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
                plantilla_sql = "INSERT INTO SIPAN_MEXOCAT.ZONAGEOGRAFICA(ID,NOM) VALUES ('{0}','{1}');\nINSERT INTO sipan_mexocat.regionativa(id,idespecieinvasora,idzonageografica) VALUES ('{2}','{3}','{4}');"
                str_plantilla = plantilla_sql.format(id_c_zonageografica, candidat.replace("'", "''"), idregionativa,fila[3].strip(), id_c_zonageografica)
        else:
            plantilla_sql = "INSERT INTO sipan_mexocat.regionativa(id,idespecieinvasora,idzonageografica) VALUES ('{0}','{1}','{2}');"
            str_plantilla = plantilla_sql.format(idregionativa,fila[3].strip(),id_c_zonageografica)
        resultats.append(str_plantilla)
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

def genera_sentencies_llistat_exotiques(file,dir_resultats,cached_taxon_resolution_results):
    with open(file, 'rb') as csvfile:
    #with open('llistat_exotiques_EXOCAT_Dec_2016.csv', 'rb') as csvfile:
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
        for num_fila in fails_codi_sp:
            fila = file_array[num_fila]
            print "Buscant idtaxon de " + fila[4] + "..."
            idtaxon = get_id_invasora(fila[4])
            if idtaxon == '':
                print fila[4] + " no te correspondencia a la taula de taxons "
                print "Sentencia insert a sipan_mtaxon.taxon ---> " + get_insert_taula_mtaxon(fila)
                inserts_file_taxon.write(get_insert_taula_mtaxon(fila))
                inserts_file_taxon.write("\n")
                print "Sentencia insert a sipan_mexocat.especieinvasora ---> " + get_insert_taula_spinvasora(fila)
                inserts_file_spinvasora.write(get_insert_taula_spinvasora(fila))
                inserts_file_spinvasora.write("\n")
            else:
                print "Id invasora " + get_id_invasora(fila[4])
                print "Sentencia insert a sipan_mexocat.especieinvasora ---> " + get_insert_taula_spinvasora(fila)
                inserts_file_spinvasora.write(get_insert_taula_spinvasora(fila))
                inserts_file_spinvasora.write("\n")
            inserts_grup.write(genera_sentencia_grup(fila))
            inserts_grup.write("\n")
            inserts_habitat.write(genera_sentencia_habitat(fila))
            inserts_habitat.write("\n")
            sentencies_regionativa = genera_sentencia_regionativa(fila,tesaure_zonageografica);
            for sentencia_regionativa in sentencies_regionativa:
                inserts_regionativa.write(sentencia_regionativa)
                inserts_regionativa.write("\n")
            inserts_viaentrada.write(genera_sentencia_viaentrada(fila))
            inserts_viaentrada.write("\n")
            sentencies_noms = genera_sentencies_noms(fila)
            for sentencia_nom in sentencies_noms:
                inserts_noms.write(sentencia_nom)
                inserts_noms.write("\n")


def genera_sentencies_citacions(file,dir_resultats,cached_taxon_resolution_results):
    #with open('EXOCAT_citacions-QUIQUE 2016.csv','rb') as csvfile:
    with open(file, 'rb') as csvfile:
        fails_codi_sp = []
        fails_utm_format = []
        fails_row_exists = []
        fails_especie_no_existeix = []
        file_array = []
        cached_taxon_resolution_results = {}
        reader = csv.reader(csvfile, delimiter=';', quotechar='"')
        row_num = 0

        #read file, save errors
        print("Llegint fitxer de dades ...")
        for row in reader:
            file_array.append(row)

            if row_num != 0 and fila_es_a_la_base_dades(row):
                fails_row_exists.append(row_num)
            else:
                if not comprova_codi_esp(row):
                    if row_num != 0:
                        fails_codi_sp.append(row_num)

                if not comprova_format_coordenades(row):
                    if row_num != 0:
                        fails_utm_format.append(row_num)
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
                    fails_especie_no_existeix.append(sp_name)
            except KeyError:
                print("Recuperant codi especie invasora per " + sp_name)
                id_spinvasora = get_id_invasora(sp_name)
                if id_spinvasora == '':
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
        print cached_taxon_resolution_results

def genera_sentencies_presencia(file,dir_resultats,cached_taxon_resolution_results,mida_malla):
    mida_malla_str = str(mida_malla)
    #with open('EXOCAT_citacions_2016_utm_' + mida_malla_str + '_' + mida_malla_str + '.csv', 'rb') as csvfile:
    with open(file, 'rb') as csvfile:
        file_array = []
        fails_codi_sp = []
        fails_especie_no_existeix = []
        fails_row_exists = []
        reader = csv.reader(csvfile, delimiter=';', quotechar='"')
        row_num = 0

        cached_taxon_resolution_results['Cyperus alternifolius subsp. flabelliformis']='Cype_alte'
        cached_taxon_resolution_results['Elymus elongatus subsp. ponticus'] = 'Elym_elon'
        cached_taxon_resolution_results['Oenothera biennis subsp. biennis'] = 'Oeno_bieb'
        #cached_taxon_resolution_results['Senecio pseudolongifolius'] = ''
        #cached_taxon_resolution_results['Iris orientalis'] = ''
        cached_taxon_resolution_results['Echinochloa crus-galli']='FLORA003471'
        cached_taxon_resolution_results['Echinochloa crus-galli subsp. crus-galli'] = 'Echi_cruc'
        cached_taxon_resolution_results['Echinochloa crus-galli subsp. oryzicola'] = 'Echi_cruo'
        cached_taxon_resolution_results['Echinochloa crus-galli subsp. oryzoides'] = 'Echi_crur'
        cached_taxon_resolution_results['Medicago sativa subsp. sativa'] = 'Medi_sati'
        cached_taxon_resolution_results['Populus x canadensis'] = 'Popu_cana'
        cached_taxon_resolution_results['Pyrus communis subsp. communis'] = 'Pyru_comm'
        cached_taxon_resolution_results['Salix x rubens'] = 'Sali_rube'
        cached_taxon_resolution_results['Eleusine tristachya subsp. barcinonensis'] = 'Eleu_tris'
        cached_taxon_resolution_results['Eragrostis mexicana subsp. virescens'] = 'Erag_mexi'
        cached_taxon_resolution_results['Pisum sativum'] = 'FLORA000498'
        cached_taxon_resolution_results['Prunus domestica'] = '89'
        cached_taxon_resolution_results['Ulex europaeus'] = '2639'
        cached_taxon_resolution_results['Althaea hirsuta subsp. Longiflora'] = 'Alth_hirs'
        cached_taxon_resolution_results['Hesperis matronalis subsp. matronalis'] = 'Hesp_matr'
        cached_taxon_resolution_results['Kalanchoe x houghtonii'] = 'Kala_houg'
        cached_taxon_resolution_results['Asparagus asparagoides'] = 'Aspa_aspa'
        cached_taxon_resolution_results['Opuntia lindheimeri var. linguliformis'] = 'Opun_lind'
        cached_taxon_resolution_results['Symphocarpus albus'] = 'Symp_albu'


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
            sp_name = file_array[rownum][0].strip()
            try:
                id_spinvasora = cached_taxon_resolution_results[sp_name]
                if id_spinvasora == '':
                    fails_especie_no_existeix.append(sp_name)
            except KeyError:
                print("Recuperant codi especie invasora per " + sp_name)
                id_spinvasora = get_id_invasora(sp_name)
                if id_spinvasora == '':
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
                file_array[line_num][1] = cached_taxon_resolution_results[file_array[line_num][0].strip()]
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
                    clean_line.append(line[1])
                    clean_line.append(line[3])
                    clau = line[1] + '_' + line[3]
                    if not clau in repeticions:
                        inserts_file.write(plantilla_sql_insert.format(*clean_line))
                        inserts_file.write("\n")
                        deletes_file.write(plantilla_sql_delete.format(*clean_line))
                        deletes_file.write("\n")
                        repeticions.add(clau)
            line_num += 1

def main():
    cached_taxon_resolution_results = {}
    #genera_sentencies_presencia(cached_taxon_resolution_results,10)
    file_llistat_exotiques = '/home/webuser/dev/python/carrega_dades_exocat/actualitzacio_dades/llistat_exotiques_exocat_dec_2017.csv'
    #file_citacions = '/home/webuser/python/carrega_dades_exocat/dades_2017/EXOCAT_2017/EXOCAT_citacions_2017_definitiu_fusionat.csv'
    #file_presencia_1_1 = '/home/webuser/python/carrega_dades_exocat/dades_2017/EXOCAT_2017/EXOCAT_citacions_2017_utm_1_1.csv'
    #file_presencia_10_10 = '/home/webuser/python/carrega_dades_exocat/dades_2017/EXOCAT_2017/EXOCAT_citacions_2017_utm_10_10.csv'
    dir_resultats = '/home/webuser/dev/python/carrega_dades_exocat/actualitzacio_dades/'
    genera_sentencies_llistat_exotiques(file_llistat_exotiques,dir_resultats,cached_taxon_resolution_results)
    #genera_sentencies_citacions(file_citacions,dir_resultats,cached_taxon_resolution_results)
    #genera_sentencies_presencia(file_presencia_1_1, dir_resultats, cached_taxon_resolution_results,1)
    #genera_sentencies_presencia(file_presencia_10_10, dir_resultats, cached_taxon_resolution_results, 10)

if __name__=='__main__':
    main()