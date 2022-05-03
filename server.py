import datetime
from socket import *
import xml.etree.ElementTree as ET
import pymongo
import re

global serverPort
serverPort = 50030
def doTask(msg: str):
    msg = msg.split(sep = '\n')
    command = msg[0]
    retval = None
    if command == 'Create Database':
        databaseName = msg[1]
        retval = createDatabase(databaseName)
    elif command == 'Create Table':
        databaseName = msg[1]
        tableName = msg[2]
        rows = msg[3:]
        retval = createTable(databaseName,tableName, rows)
    elif command == 'Delete Database':
        databaseName = msg[1]
        retval = deleteDatabase(databaseName)
    elif command == 'Delete Table':
        databaseName = msg[1]
        tableName = msg[2]
        retval = deleteTable(databaseName,tableName)
    elif command == 'Insert':
        databaseName = msg[1]
        tableName = msg[2]
        data = msg[3]
        retval = insertData(databaseName, tableName, data)
    elif command == 'Delete':
        databaseName = msg[1]
        tableName = msg[2]
        conditions = msg[3]
        retval = deleteData(databaseName, tableName, conditions)
    elif command == 'Select':
        databaseName = msg[1]
        dataName = msg[2].split(sep = ' ')[1:]
        tableName = msg[3].split(sep = ' ' )[1]
        
        conditions = msg[4].split(sep = ' ' )[1:]
        if len(conditions) == 1 and conditions[0] == '':
            conditions = None
        retval = selectData(databaseName, dataName, tableName, conditions)
    
    root = ET.parse('Catalog.xml').getroot()
    return retval

def selectData(databaseName, dataName, tableName, conditions):

    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
        
    if database == None:
        return -2 # Trying to delete from non-existing database

    table = None
    for tb in database:
        if tb.attrib['tableName'] == tableName:
            table = tb

    if table == None:
        return -3 # Trying to delete from non-existing table

    pk = table.findall('.//primaryKey//pkAttribute')[0].text # Save the primary key

    structure = ''                                           # Save the structure of the table for later usage
    types = {}
    for column in table.findall('.//Structure//Attribute'):
        if column.attrib['attributeName'] != pk:
            structure += (column.attrib['attributeName'] + '#')
            types[column.attrib['attributeName']] = column.attrib['type']

    structure = structure[:-1]
    structure = structure.split(sep = '#')

    indexes = table.findall('IndexFiles//IndexFile//IndexAttributes//IAttribute') # Save the attributes we have indexes for and the files for them
    indexfiles = table.findall('IndexFiles//IndexFile')

    collection = mongoclient.get_database(databaseName).get_collection(tableName)

    attribs = [] # Save the attributes of the table

    for i in table.findall('.//Structure//Attribute'):
        attribs.append(i.attrib['attributeName'])

    separators = []
    columns = []
    operators = []
    toCompare = []
    comparators = []
    ops = []

    if(conditions != None):             # Working around the conditions to be able to use them, also validating them
        separators = [elem for i, elem in enumerate(conditions)
                        if i % 2 == 1]

        conditions = [elem for i, elem in enumerate(conditions)
                    if i % 2 == 0]

        for i in separators:
            if i != 'and':
                return -4 # Wrong input, bad separators

        for i in conditions:
            if re.search('^[a-zA-Z0-9]+-?[0-9]*-?[0-9]*-?[0-9]*:?[0-9]*(!=|=|[<>]=?)[a-zA-Z0-9]+-?[0-9]*-?[0-9]*-?[0-9]*:?[0-9]*',i) == None:
                return -5 # Wrong input, bad conditions

        for condition in conditions:
            operators.append(re.search(pattern = '(<=)|(>=)|<|>|=',string = condition).group(0))
            columns.append(condition.split(sep = operators[-1]))
    
        for i,column in enumerate(columns):     # Splitting conditions into columns, operators and what we have to compare them to
            newAttrib = None
            for attribName in attribs:
                if attribName == column[0]:
                    toCompare.append(column[0])
                    comparators.append(column[1])
                    newAttrib = 1
                elif attribName == column[1]:
                    comparators.append(column[0])
                    toCompare.append(column[1])
                    newAttrib = 1
            if newAttrib == None:
                return -6
        
        # Converting operators so they can be used easily in mongodb.find
        
        for op in operators:
            if op == '<':
                op = '$lt'
            elif op == '<=':
                op = '$lte'
            elif op == '=':
                op = '$eq'
            elif op == '>':
                op = '$gt'
            elif op == '>=':
                op = '$gte'
            ops.append(op)

    if len(operators) == 0:
        data = collection.find();
    else:
        pass
        #Filter data
    
    if len(dataName) == 1 and dataName[0] == '*': # Select * case
        msg = ''
        msg += "%35s"%(pk)
        for struct in structure:
            msg += "%35s"%(struct)
        print(msg)
        print('\n')
        for dat in data:
            msg = ''
            msg += "%35s"%(dat['_id'])
            for i,struct in enumerate(structure):
                msg += "%35s"%(dat['Value'].split(sep='#')[i])
            print(msg)
    else:                                         # Selecting given columns
        msg = ''
        if pk in dataName:
            msg += "%35s"%(pk)
        for struct in structure:
            if struct in dataName:
                msg += "%35s"%(struct)
        print(msg)
        print('\n')
        for dat in data:
            msg = ''
            if pk in dataName:
                msg += "%35s"%(dat['_id'])
            for i,struct in enumerate(structure):
              if struct in dataName:
                msg += "%35s"%(dat['Value'].split(sep='#')[i])
            print(msg)
    
def createIndex(database, table, indexName, index, key, isUnique):
    collection = mongoclient.get_database(database).get_collection(table + '.' +  indexName +'.ind')
    if isUnique == 1:
        data = {
            "_id":index,
            "Value":key
            }
        collection.insert_one(data)
        return 0
    else:
        
        try:
            val = collection.find({"_id" : index})[0]['Value']
        except:
            data = {
                "_id":index,
                "Value":key
                }
            collection.insert_one(data)
            return 0
        
        collection.update_one({"_id" : index}, { "$set" : {"Value" : val + '#' + key } })
        return 0

def deleteIndexData(databaseName, dict, indexes, indexFiles):
    database = mongoclient.get_database(databaseName)
    print(dict)
    for j in range(len(indexes)):
        print(indexFiles[j].attrib['indexName'])
        isUnique = indexFiles[j].attrib['isUnique']
        collection = database.get_collection(indexFiles[j].attrib['indexName'])
        data = collection.find({"_id" : dict[indexes[j].text]})[0]['Value']
        collection.delete_one({"_id" : dict[indexes[j].text]})
        if isUnique == '0':
            data = data.split(sep = '#')
            newData = ''
            for d in data:
                if d != dict["key"]:
                    if newData == '':
                        newData = d
                    else:
                        newData += '#' + d
            if newData != '':
                collection.insert_one({"_id" : dict[indexes[j].text], "Value" : newData})

def deleteData(databaseName, tableName, conditions):

    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
        
    if database == None:
        return -2 # Trying to delete from non-existing database

    table = None
    for tb in database:
        if tb.attrib['tableName'] == tableName:
            table = tb

    if table == None:
        return -3 # Trying to delete from non-existing table


    collection = mongoclient.get_database(databaseName).get_collection(tableName)

    separators = [elem for i, elem in enumerate(conditions.split(sep = " "))
                  if i % 2 == 1]

    conditions = [elem for i, elem in enumerate(conditions.split(sep = " "))
                  if i % 2 == 0]

    for i in separators:
        if i != 'and' and i != 'or':
            return -4 # Wrong input, bad separators

    for i in conditions:
        if re.search('^[a-zA-Z0-9]+-?[0-9]*-?[0-9]*-?[0-9]*:?[0-9]*(!=|=|[<>]=?)[a-zA-Z0-9]+-?[0-9]*-?[0-9]*-?[0-9]*:?[0-9]*',i) == None:
            return -5 # Wrong input, bad conditions

    attribs = []

    for i in table.findall('.//Structure//Attribute'):
        attribs.append(i.attrib['attributeName'])

    columns = []
    operators = []
    for condition in conditions:
        columns.append(re.split(pattern = '(<=)|(>=)|<|>|=',string = condition))
        operators.append(re.search(pattern = '(<=)|(>=)|<|>|=',string = condition).group(0))

    toCompare = []
    comparators = []
    
    for i,column in enumerate(columns):
        newAttrib = None
        for attribName in attribs:
            if attribName == column[0]:
                toCompare.append(column[0])
                comparators.append(column[3])
                newAttrib = 1
            elif attribName == column[3]:
                comparators.append(column[0])
                toCompare.append(column[3])
                newAttrib = 1
        if newAttrib == None:
            return -6
    
    ops = []
    for op in operators:
        if op == '<':
            op = '$lt'
        elif op == '<=':
            op = '$lte'
        elif op == '=':
            op = '$eq'
        elif op == '>':
            op = '$gt'
        elif op == '>=':
            op = '$gte'
        ops.append(op)

    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break

    table = None
    for tb in database:
        if tb.attrib['tableName'] == tableName:
                table = tb


    pk = table.findall('.//primaryKey//pkAttribute')[0].text
    structure = ''
    types = {}
    for column in table.findall('.//Structure//Attribute'):
        if column.attrib['attributeName'] != pk:
            structure += (column.attrib['attributeName'] + '#')
            types[column.attrib['attributeName']] = column.attrib['type']

    structure = structure[:-1]
    structure = structure.split(sep = '#')

    indexes = table.findall('IndexFiles//IndexFile//IndexAttributes//IAttribute')
    indexfiles = table.findall('IndexFiles//IndexFile')

    json = {}
    bigdict = []
    json2 = {}
    data = collection.find()


    for dat in data:
        helper = {}
        for i,struct in enumerate(structure):
            helper[struct] = dat['Value'].split(sep='#')[i]
            helper['key'] = dat['_id']
        bigdict.append(helper)
        
    separators.append('or')

    referencedByTables = table.find('.//ReferencedBy//Structure//Table')
    referencedByAttrs = table.find('.//ReferencedBy//Structure//RefAttribute')

    if type(referencedByAttrs) == list:
        for i in range(len(referencedByAttrs)):
            referencedByAttrs[i] = referencedByAttrs[i].text
            referencedByTables[i] = referencedByTables[i].text
    elif referencedByAttrs != None:
        referencedByAttrs = referencedByAttrs.text
        referencedByTables = referencedByTables.text

    for i,compare in enumerate(toCompare):

        if compare == pk:
            json2[ops[i]] = comparators[i]
            json['_id'] = json2
            if type(referencedByTables) == list:
                for k in range(len(referencedByTables)):
                    indColl = mongoclient.get_database(databaseName).get_collection(referencedByTables[k] + '.' + referencedByAttrs[k] + '.ind')
                    try:
                        val = indColl.find(json)[0]['Value']
                    except:
                        continue
                    return -10
                collection.delete_many(json)
            elif referencedByAttrs != None:
                indColl = mongoclient.get_database(databaseName).get_collection(referencedByTables + '.' + referencedByAttrs + '.ind')
                try:
                    val = indColl.find(json)[0]['Value']
                except:
                    collection.delete_many(json)
                    continue
                return -10
            else:
                deleteThis = collection.find(json)
                bigDelete = []
                for delete in deleteThis:
                    helper = {}
                    for i,struct in enumerate(structure):
                        helper[struct] = delete['Value'].split(sep='#')[i]
                        helper['key'] = delete['_id']
                    bigDelete.append(helper)
                for dicti in bigDelete:                 
                    deleteIndexData(databaseName,dicti,indexes,indexfiles)
                collection.delete_one(json)

            
        else:
            deleteThis = []
            if types[compare] == 'string':
                for dicti in bigdict:
                    if dicti[compare] == comparators[i] and operators[i] == '=':
                        deleteThis.append(dicti)
                    elif dicti[compare] != comparators[i] and operators[i] == '!=':
                        deleteThis.append(dicti)
                    elif dicti[compare] <= comparators[i] and operators[i] == '<=':
                        deleteThis.append(dicti)
                    elif dicti[compare] < comparators[i] and operators[i] == '<':
                        deleteThis.append(dicti)
                    elif dicti[compare] >= comparators[i] and operators[i] == '>=':
                        deleteThis.append(dicti)
                    elif dicti[compare] > comparators[i] and operators[i] == '>':
                        deleteThis.append(dicti)
                    elif separators[i-1] == 'and'  and i != 0:
                        deleteThis.remove(dicti)

            elif types[compare] != 'date' and types[compare] != 'datetime':
                for dicti in bigdict:
                    if operators[i] == '=':
                        evalthis = dicti[compare] + operators[i] + operators[i] + comparators[i]
                    else:
                        evalthis = dicti[compare] + operators[i] + comparators[i]
                    if eval(evalthis) == True:
                        deleteThis.append(dicti)
                    elif separators[i-1] == 'and'  and i != 0:
                        deleteThis.remove(dicti)
            else:
                for dicti in bigdict:
                    if types[compare] == 'date':
                        date1 = datetime.datetime.strptime(dicti[compare],'%Y-%m-%d')
                        date2 = datetime.datetime.strptime(comparators[i],'%Y-%m-%d')
                        diff = (date1 - date2).days
                    else:
                        date1 = datetime.datetime.strptime(dicti[compare],'%Y-%m-%d-%H:%M')
                        date2 = datetime.datetime.strptime(comparators[i],'%Y-%m-%d-%H:%M')
                        diff = (date1 - date2).days
                    if diff == 0 and operators[i] == '=':
                        deleteThis.append(dicti)
                    elif diff != 0 and operators[i] == '!=':
                        deleteThis.append(dicti)
                    elif diff <= 0 and operators[i] == '<=':
                        deleteThis.append(dicti)
                    elif diff < 0 and operators[i] == '<':
                        deleteThis.append(dicti)
                    elif diff >= 0 and operators[i] == '>=':
                        deleteThis.append(dicti)
                    elif diff > 0 and operators[i] == '>':
                        deleteThis.append(dicti)
                    elif separators[i-1] == 'and'  and i != 0:
                        deleteThis.remove(dicti)

            if separators[i] == 'or' or i == len(toCompare):  
                for dicti in deleteThis:
                    json['_id'] = dicti['key']                            
                    deleteIndexData(databaseName,dicti,indexes,indexfiles)
                    collection.delete_one(json)
                deleteThis = []
            elif separators[i] == 'and':
                pass

def insertData(databaseName, tableName, data):
    data = data[1:]
    data = data[:-1]
    data = data.split(sep = ',')

    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
        
    if database == None:
        return -2

    table = None
    for tb in database:
        if tb.attrib['tableName'] == tableName:
            table = tb

    if table == None:
        return -3 

    print(database.attrib['dataBaseName'],table.attrib['tableName'])

    try:
        pk = table.findall('.//primaryKey//pkAttribute')[0].text
    except:
        pk = None

    try:
        uniqueKeys = table.findall('.//uniqueKeys//UniqueAttribute')
        for i in range(len(uniqueKeys)):
            uniqueKeys[i] = uniqueKeys[i].text
    except:
        uniqueKeys = None
    
    try:
        refTables = table.findall('.//foreignKeys//foreignKey//references//refTable')
        refAttrs = table.findall('.//foreignKeys//foreignKey//references//refAttr')
        foreignKeys = table.findall('.//foreignKeys//foreignKey//foreignAttribute')
        for i in range(len(refTables)):
            refTables[i] = refTables[i].text
            refAttrs[i] = refAttrs[i].text
            foreignKeys[i] = foreignKeys[i].text
    except:
        refTables = None
        refAttrs = None
        foreignKeys = None


    msg = ''
    indexes = table.findall('IndexFiles//IndexFile//IndexAttributes//IAttribute')
    indexfiles = table.findall('IndexFiles//IndexFile')
    i = 0
    for column in table.findall('.//Structure//Attribute'):
        # Typeerror

        if column.attrib['type'] == 'bit':
            if re.search('^0|1$',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'int':
            if re.search('^[0-9]+$',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'float':
            if re.search('^[+-]?[0-9]+.[0-9]+$',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'string':
            if re.search('.*',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'date':
            if re.search('^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'datetime':
            if re.search('^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])-(2[0-3]|[01][0-9]):[0-5][0-9]$',data[i]) == None:
                return -1


        if column.attrib['attributeName'] != pk:
            if msg == '':
                msg += data[i]
            else:
                msg += '#' + data[i]

            for uk in uniqueKeys:
                if column.attrib['attributeName'] == uk:
                    ukindex = mongoclient.get_database(databaseName).get_collection(tableName + '.' + uk + '.ind')
                    try:
                        val = ukindex.find({"_id" : data[i]})[0]['Value']
                    except:
                        break
                    print("UK exists");
                    return -10 # Unique key already exists

            for j in range(len(foreignKeys)):
                if column.attrib['attributeName'] == foreignKeys[j]:
                    refTable = mongoclient.get_database(databaseName).get_collection(refTables[j])
                    try:
                        val = refTable.find({"_id" : data[i]})[0]['Value']
                    except:
                        print("No referenced row");
                        return -11 # Referenced row doesn't exist


        else:
            pk = data[i]
  
        i += 1


    i = 0 
    for column in table.findall('.//Structure//Attribute'):
        for j in range(len(indexes)):
            if column.attrib['attributeName'] == indexes[j].text:
                print(indexes[j].text,indexfiles[j].attrib['isUnique'])
                createIndex(databaseName,tableName,indexes[j].text,data[i],pk,indexfiles[j].attrib['isUnique'])
        i += 1

    if pk != '':
        data = {
            "_id":pk,
            "Value":msg
            }
    else:
        data = {
            "Value":msg
        }

    database = mongoclient.get_database(databaseName)
    collection = database.get_collection(tableName)
    try:
        collection.insert_one(document=data)
    except:
        print("Failed to insert")
        return -4 # Failed to insert because PK exists


    print("Successful insert");
    return 0 # Successful insert

def deleteDatabase(databaseName: str):
    for db in root:
        if db.attrib['dataBaseName'] == databaseName: 
            root.remove(db)
            ET.indent(tree, space = '\t', level = 0)
            tree.write('Catalog.xml', encoding = 'utf-8')
            mongoclient.drop_database(databaseName)
            return 0 # Successful delete

    return -1 # Trying to delete non-existing database

def deleteTable(databaseName: str, tableName: str):
    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
        
    if db == None:
        return -1 # Trying to delete from non-existing database
    
    for tb in database:
        if tb.attrib['tableName'] != tableName:
            for refT in tb.findall('.//foreignKeys//foreignKey//references//refTable'):
                if refT.text == tableName:
                    return -3 # Trying to delete table that is referenced by another table

    for tb in database:
        if tb.attrib['tableName'] == tableName: 
            database.remove(tb)
            ET.indent(tree, space = '\t', level = 0)
            tree.write('Catalog.xml', encoding = 'utf-8')
            mongoclient.get_database(databaseName).drop_collection(tableName)
            return 0 # Successful delete

    return -2 # Trying to delete non-existing table 
    
def createDatabase(databaseName: str):
    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName: 
            return -1 # Trying to create existing database
    
    database  = ET.SubElement(root,'DataBase')
    database.set('dataBaseName', databaseName)
    ET.indent(tree, space = '\t', level = 0)
    tree.write('Catalog.xml', encoding = 'utf-8')
    return 0

def createTable(databaseName : str, tableName : str, rows):
    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
    
    if database == None: 
        return -1 # Trying to create table in non-existing database

    for child in database:
        if child.attrib['tableName'] == tableName: 
            return -2 # Trying to create existing table

    for i in rows:
        i = i.split(sep = ' ')
        rowIsForeign = int(i[5])
        if rowIsForeign:
            rowReference = i[6]
            rowReference = rowReference.split(sep = '(')
            rowReference[1] = rowReference[1][:-1]
            refT = None
            for table in database:
                if table.attrib['tableName'] == rowReference[0]:
                    refT = table
            if refT == None: 
                return -3 # Reference on non-existing table

            refCol = None
            for attribute in refT.findall(".//Structure//Attribute"):
                if attribute.attrib['attributeName'] == rowReference[1]:
                    refCol = attribute
            if refCol == None: 
                return -4 # Reference on non-existing column in table

    table = ET.SubElement(database,'Table')
    table.set('tableName',tableName)
    table.set('rowLength',str(len(rows)))

    structure = ET.SubElement(table,'Structure')
    pkeys = ET.SubElement(table,'primaryKey')
    ukeys = ET.SubElement(table,'uniqueKeys')
    fkeys = ET.SubElement(table,'foreignKeys')
    indexfiles = ET.SubElement(table,'IndexFiles')

    for i in rows:
        i = i.split(sep = ' ')
        rowName = i[0]
        rowType = i[1]
        rowIsUnique = int(i[2])
        rowIsIndex = int(i[3])
        rowIsPrimary = int(i[4])
        rowIsForeign = int(i[5])
        if rowIsForeign == 1:
            rowReference = i[6]
            rowReference = rowReference.split('(')
            rowReference[1] = rowReference[1][:-1]
    
        attr = ET.SubElement(structure, 'Attribute')
        attr.set('attributeName',rowName)
        attr.set('type',rowType)
        if rowIsUnique:
            ukey = ET.SubElement(ukeys,'UniqueAttribute')
            ukey.text = rowName
            index = ET.SubElement(indexfiles, 'IndexFile')
            index.set('indexName',tableName + '.' + rowName + '.ind')
            index.set('isUnique',str(rowIsUnique))
            indexAttrs = ET.SubElement(index,'IndexAttributes')
            indexAttr=  ET.SubElement(indexAttrs,'IAttribute')
            indexAttr.text = rowName

        if rowIsPrimary:
            pkey = ET.SubElement(pkeys,'pkAttribute')
            pkey.text = rowName

        if rowIsForeign:
            fkey = ET.SubElement(fkeys,'foreignKey')
            fkAttr = ET.SubElement(fkey,'foreignAttribute')
            fkAttr.text = rowName
            reference = ET.SubElement(fkey,'references')
            refTable = ET.SubElement(reference,'refTable')
            refTable.text = rowReference[0]
            refAttr = ET.SubElement(reference,'refAttr')
            refAttr.text = rowReference[1]
            index = ET.SubElement(indexfiles, 'IndexFile')
            index.set('indexName',tableName + '.' + rowName + '.ind')
            index.set('isUnique',str(rowIsUnique))
            indexAttrs = ET.SubElement(index,'IndexAttributes')
            indexAttr=  ET.SubElement(indexAttrs,'IAttribute')
            indexAttr.text = rowName

            for tb in database:
                if tb.attrib['tableName'] == rowReference[0]:
                    referencedBy = ET.SubElement(tb,"ReferencedBy")
                    referencedStructure = ET.SubElement(referencedBy, "RefStructure")
                    referencedByTable = ET.SubElement(referencedStructure, "Table")
                    referencedAttr = ET.SubElement(referencedStructure, "RefAttribute")
                    referencedByTable.text = tableName
                    referencedAttr.text = rowName


        if rowIsIndex:
            index = ET.SubElement(indexfiles, 'IndexFile')
            index.set('indexName',tableName + '.' + rowName + '.ind')
            index.set('isUnique',str(rowIsUnique))
            indexAttrs = ET.SubElement(index,'IndexAttributes')
            indexAttr=  ET.SubElement(indexAttrs,'IAttribute')
            indexAttr.text = rowName

    ET.indent(tree, space = '\t', level = 0)
    tree.write('Catalog.xml', encoding = 'utf-8')
    return 0

if __name__ == '__main__':
    serverSocket = socket(AF_INET,SOCK_STREAM)
    serverSocket.bind(('',serverPort))
    serverSocket.listen(10)

    print("Server is starting")
    global tree, root, mongoclient
    mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    tree = ET.parse('Catalog.xml')
    root = tree.getroot()
    #print(root)
 
    while True:
        (clientSocket, addr) = serverSocket.accept()
        msg = clientSocket.recv(256).decode()
        if msg == 'EXIT':
            print("Server is shutting down")
            clientSocket.close()
            break 

        retval = doTask(msg)
        clientSocket.send(str(retval).encode())
        clientSocket.close()

    serverSocket.close()

