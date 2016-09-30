import csv
import config
import psycopg2
import sys

header_names = ['Especie','CODI_ESP','GRUP','UTMX','UTMY','Localitat','Municipi','Comarca','Provincia','Data','Autors','Citacio','Font','Referencia','Observacions','Tipus cita','Habitat','Tipus mort','Abundancia','Codi ACA','Codi estacio','IND_Ha','Ind. Capt.']
conn_string = "host='" + config.params['db_host'] + "' dbname='" + config.params['db_name'] + "' user='" + config.params['db_user'] + "' password='" + config.params['db_password'] + "'"

def print_one_liner(message):
    sys.stdout.write('%s\r' % message)
    sys.stdout.flush()

def comprova_codi_ACA(row):
    return True

def comprova_codi_esp(row):
    if row[1] == '':
        return False
    return True


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
        return cursor_rows[0][0]
    else:
        if subespecie != '':
            print "Hi ha multiples especies amb genere " + genere + " especie " + especie + " i subespecie " + subespecie
        else:
            print "Hi ha multiples especies amb genere " + genere + " i especie " + especie

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

def fila_es_a_la_base_dades(row):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("""SELECT id FROM public.citacions WHERE especie=%s and utmx=%s and utmy=%s;""",(row[0].strip(),row[3].strip(),row[4].strip(),))
    results = cursor.fetchall()
    return len(results) > 0

with open('EXOCAT_citacions-QUIQUE 2016.csv','rb') as csvfile:
#with open('Lepomis_gibbosus.csv', 'rb') as csvfile:

    fails_codi_sp = []
    fails_utm_format = []
    fails_row_exists = []
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
        except KeyError:
            print("Recuperant codi especie invasora per " + sp_name)
            id_spinvasora = get_id_invasora(sp_name)
            cached_taxon_resolution_results[sp_name] = id_spinvasora

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
        inserts_file = open("insert_citacions.sql", 'w')
        deletes_file = open("delete_citacions.sql", 'w')
        plantilla_sql_insert = "INSERT INTO public.citacions(especie,idspinvasora,grup,utmx,utmy,localitat,municipi,comarca,provincia,data,autor_s,citacio,font,referencia,observacions,tipus_cita,habitat,tipus_mort,abundancia,codi_aca,codi_estacio,ind_ha,ind_capt) VALUES ('{0}','{1}','{2}',{3},{4},'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}',{21},{22});"
        plantilla_sql_delete = "DELETE FROM public.citacions WHERE especie='{0}' and idspinvasora='{1}' and grup='{2}' and utmx={3} utmy={4} and localitat='{5}' and municipi='{6}' and comarca='{7}' and provincia='{8}' and data='{9}' and autor_s='{10}' and citacio='{11}' and font='{12}' and referencia='{13}' and observacions='{14}' and tipus_cita='{15}' and habitat='{16}' and tipus_mort='{17}' and abundancia='{18}' and codi_aca='{19}' and codi_estacio='{20}' and ind_ha={21} and ind_capt={22};"
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
