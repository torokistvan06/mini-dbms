import datetime
from socket import *
import xml.etree.ElementTree as ET
import pymongo
import re
import random
import names


usedRandom = []

groups = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
dates = ['2012-12-27','2012-01-25','2012-06-24','2012-09-31','2012-04-20']

data = []
data2 = []

customerIDIndex = []

for i in range(500,600,1):
  
  rand = random.randint(2000, 2500)
  while rand in usedRandom:
    rand = random.randint(2000, 2500)
  usedRandom.append(rand)
  rand2 = random.randint(0,4)
  name = names.get_first_name();
  date = dates[random.randint(0,4)]
  tokom =  ("%d#%s#%s"%(rand,name,date))
  

  
  json = {}
  json["_id"] = i
  json["Value"] = tokom

  json2 = {}
  json2["_id"] = rand
  json2["Value"] = str(i)

  print(i)
  data.append(json);
  data2.append(json2)

for d in data:
  print(d)

for d in data2:
  print(d)

print("Generating done")

mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
database = mongoclient.get_database("Test")

customers = database.get_collection("employees")
customers.insert_many(data)
groupIndex = database.get_collection("employees.CustomerID.ind")
groupIndex.insert_many(data2)
