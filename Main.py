from pymongo import MongoClient
import psycopg2



connection = psycopg2.connect(" dbname = 'data' user = 'postgres' host = '192.168.47.160' password = '12345678' ")

cur = connection.cursor()



client = MongoClient('AlphaBI',3333)
db = client.default_alpha

###################------------------------------------------

cur.execute(''' SELECT max(id) FROM "dim_terr" ''')
DBresponse = cur.fetchall()

if DBresponse[0][0] != None:
    IDterr = DBresponse[0][0] + 1
else:
    IDterr = 1

if  DBresponse[0][0] == None:

    cursor = db.dictionary.department.find({},{'Name':1, '_id':0})

    for table_values in cursor:

        cur.execute(''' INSERT INTO public."dim_terr"("territory_name", "id") VALUES ('{}','{}');'''.format(table_values['Name'], IDterr))
        connection.commit()
        IDterr += 1

    IDterr = 1

    cursor = db.dictionary.department.find({},{'Name':1,'ParentId':1})
    for table_values in cursor:

        table_value_parent_id = table_values['ParentId']

        if table_value_parent_id != 'null':
            cursor_parent_id = db.dictionary.department.find({'_id':table_value_parent_id},{'Name':1, '_id':0 })
            cursor_parent_id = list(cursor_parent_id)

            if len(cursor_parent_id) != 0:
                parent_name = cursor_parent_id[0]['Name']
                cur.execute('''SELECT "dim_terr".id FROM "dim_terr" WHERE "territory_name" = '{}' '''.format(parent_name))
                DBresponse = cur.fetchall()
                parent_id = DBresponse[0][0]
            else: parent_id = 'null'
        else: parent_id = 'null'

        cur.execute(''' UPDATE "dim_terr" SET "parent_id" = {} WHERE id = {} '''.format(parent_id, IDterr))
        connection.commit()
        IDterr += 1



#########################---------------------------

cursorTerr = db.consolidatedreporting.storedformdata.find({'StoredConstantsValues':{'$exists':'true'}, '_id':1795}, {'StoredConstantsValues':1,'_id':0})
cursorTerr = list(cursorTerr)
Terrname = cursorTerr[0]['StoredConstantsValues']['EntityTreeNodeCode']
Terrname = Terrname.split(' ')
Terrname = Terrname[0]
cursor = db.consolidatedreporting.storedformdata.find({'rt_pokazateli':{'$exists':'true'}, '_id':1795}, {'rt_pokazateli':1,'_id':0})
cursor = list(cursor)
array = cursor[0]['rt_pokazateli']['minimumMoney']['minimumMoney']

for rowindoc in array:
    value = rowindoc['value']
    pokazatel = rowindoc['pokazatel']
    year = rowindoc['year']
    year = str(int(year)) +  '-Y'
    unit = rowindoc['unit']

# get id unit

    cur.execute(''' SELECT "dim_unit".id FROM "dim_unit" WHERE name = '{}' '''.format(unit))
    DBresponse = cur.fetchall()
    if len(DBresponse) == 0:
        cur.execute(''' SELECT max(id) FROM "dim_unit" ''')
        DBresponse = cur.fetchall()
        if DBresponse[0][0] == None:
            IDunit = 1
        else:
            IDunit = DBresponse[0][0] + 1
        cur.execute(''' INSERT INTO "dim_unit"(id, name, name_full) VALUES ({},'{}','{}');'''.format(IDunit,unit,unit))
        connection.commit()
    else:
        IDunit = DBresponse[0][0]

# get id pokazatel

    cur.execute(''' SELECT "dim_pokazatel".id FROM "dim_pokazatel" WHERE name = '{}' '''.format(pokazatel))
    DBresponse = cur.fetchall()
    if len(DBresponse) == 0:
        cur.execute(''' SELECT max(id) FROM "dim_pokazatel" ''')
        DBresponse = cur.fetchall()
        if DBresponse[0][0] == None:
            IDpokazatel = 1
        else:
            IDpokazatel = DBresponse[0][0] + 1
        cur.execute(''' INSERT INTO "public".dim_pokazatel(id, name, name_full, category, subcategory) VALUES ({},'{}','{}','{}','{}');'''.format(IDpokazatel,unit,unit,unit,unit))
        connection.commit()
    else:
        IDpokazatel = DBresponse[0][0]

# get id fact

    cur.execute(''' SELECT max(id) FROM "fact";''')
    DBresponse = cur.fetchall()

    if DBresponse[0][0] != None:
        IDfact = DBresponse[0][0] + 1
    else:
        IDfact = 1

# get id terr
    cur.execute('''SELECT "dim_terr".id FROM "dim_terr" WHERE territory_name = '{}' '''.format(Terrname))
    DBresponse = cur.fetchall()
    IDterr = DBresponse[0][0]

# insert val in fact
    cur.execute(''' INSERT INTO "public".fact(id, id_date, id_terr, id_pokazatel, id_unit, value_additive)
      VALUES ({},'{}',{},{},{},{})'''.format(IDfact,year,IDterr,IDpokazatel,IDunit,value))
    connection.commit()






#cursor = db.consolidatedreporting.storedformdata.find({'Test_Vvod':{'$exists':'true'}}, {'Test_Vvod':1,'_id':0})

#for test_vvod in cursor:
#    val1 =  test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель1']
#    val2 =  test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель2']
#    val3 =  test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель3']
#    val4 =  test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель4']
#    val5 =  test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель5']
#    val6 =  test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель6']
#    val7 =  test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель7']
#    val8 =  test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель8']
#    val9 =  test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель9']
#    val10 = test_vvod['Test_Vvod']['Лист1']['Лист1']['Значение']['Показатель10']


