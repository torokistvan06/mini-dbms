from socket import *
from xml.dom.minidom import Element
import xml.etree.ElementTree as ET
from xml.dom import minidom

global serverPort
serverPort = 2524


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
    
    root = ET.parse('Catalog.xml').getroot()
    return retval

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
    global tree, root
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

