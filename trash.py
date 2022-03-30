from socket import *
from xml.dom.minidom import Element
import xml.etree.ElementTree as ET
from xml.dom import minidom
import pymongo
import re


mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

databaseName = "Gyumolcsok"
tableName = "Szilva"



collection = mongoclient.get_database(databaseName).get_collection(tableName)
collection.insert_one()
tree = ET.parse('Catalog.xml')
root = tree.getroot()

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
for column in table.findall('.//Structure//Attribute'):
    if column.attrib['attributeName'] != pk:
        structure += (column.attrib['attributeName'] + '#')

structure = structure[:-1]
print(pk)
alma=f"{pk}anyad"
print(structure)

anyad = {}

data = collection.find()
bigdict = []

for dat in data:
    anyad = {}
    for i,struct in enumerate(structure.split(sep = "#")):
        anyad[struct] = dat['Value'].split(sep='#')[i]
    bigdict.append(anyad)
ata = collection.find()
print(bigdict)


