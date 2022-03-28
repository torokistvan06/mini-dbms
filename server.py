from socket import *
from xml.dom.minidom import Element
import xml.etree.ElementTree as ET
from xml.dom import minidom
import pymongo
import re

global serverPort
serverPort = 2546

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
    
    root = ET.parse('Catalog.xml').getroot()
    return retval

def insertData(databaseName, tableName, data):
    data = data[1:]
    data = data[:-1]
    data = data.split(sep = ',')



    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
        
    if db == None:
        return -1 # Trying to delete from non-existing database


    for tb in database:
        if tb.attrib['tableName'] == tableName:
            table = tb

    print(database.attrib['dataBaseName'],table.attrib['tableName'])

    i = 0

    pk = table.findall('.//primaryKey//pkAttribute')[0].text

    msg = ''

    for column in table.findall('.//Structure//Attribute'):
        print(column.attrib['type'])
        print(data[i])
        
        if column.attrib['type'] == 'bit':
            if re.search('^[01]$',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'int':
            if re.search('^\d+$',data[i]) == None:
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
            if re.search('^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]$',data[i]) == None:
                return -1

        if column.attrib['attributeName'] != pk:
            if msg == '':
                msg += data[i]
            else:
                msg += '#' + data[i]
        else:
            pk = data[i]
  
        i += 1

    
    print(pk)
    print(msg)

    data = {
        "_id":pk,
        "Value":msg
        }

    database = mongoclient.get_database(databaseName)
    collection = database.get_collection(tableName)
    try:
        collection.insert_one(data)
    except:
        return -2
    
    return 0

def deleteDatabase(databaseName: str):
    for db in root:
        if db.attrib['dataBaseName'] == databaseName: 
            root.remove(db)
            ET.indent(tree, space = '\t', level = 0)
            tree.write('Catalog.xml', encoding = 'utf-8')
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

        if rowIsPrimary:
            pkey = ET.SubElement(pkeys,'pkAttribute')
            pkey.text = rowName
            index = ET.SubElement(indexfiles, 'IndexFile')
            index.set('indexName',tableName + '.' + rowName + '.ind')
            #index.set('keyLength') - keylength
            index.set('isUnique',str(rowIsUnique))
            index.set('indexType', 'BTree')
            indexAttrs = ET.SubElement(index,'IndexAttributes')
            indexAttr=  ET.SubElement(indexAttrs,'IAttribute')
            indexAttr.text = rowName

        if rowIsForeign:
            fkey = ET.SubElement(fkeys,'foreignKey')
            fkAttr = ET.SubElement(fkey,'foreignAttribute')
            fkAttr.text = rowName
            reference = ET.SubElement(fkey,'references')
            refTable = ET.SubElement(reference,'refTable')
            refTable.text = rowReference[0]
            refAttr = ET.SubElement(reference,'refAttr')
            refAttr.text = rowReference[1]

        if rowIsIndex:
            index = ET.SubElement(indexfiles, 'IndexFile')
            index.set('indexName',rowName + '.ind')
            #index.set('keyLength') - keylength
            index.set('isUnique',str(rowIsUnique))
            index.set('indexType', 'BTree')
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
    tree = ET.parse('Catalog.xml')
    mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
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

