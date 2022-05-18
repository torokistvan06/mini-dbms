import datetime
from socket import *
import xml.etree.ElementTree as ET
import pymongo
import re
import random
import names


salaries = [5000, 5250, 5500, 5740, 6000, 6500, 7000, 2500, 10000]
nations = ['Romania', 'Hungary', 'Ukraine', 'USA', 'Russia', 'France', 'UK', 'Germany']
groups = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
dates = ['2012-01-02','2012-01-12','2012-02-24','2012-03-05','2012-04-20']
emails = ['@gmail.com', '@yahoo.com', '@hotmail.com', '@freemail.com']

data = []
data2 = []
data3 = []
groupIndexes = ['' for _ in range(10)] 
nationIndexes = ['' for _ in range(8)] 
for i in range(0,100000,1):
  
  rand = random.randint(0,8)
  rand2 = random.randint(0,2);
  rand3 = random.randint(0,7);
  rand4 = random.randint(0,9);
  name = names.get_full_name();
  email = name.split(" ")[0].lower() + name.split(" ")[1].lower() + emails[rand2];
  date = dates[random.randint(0,4)]
  tokom =  ("%s#%d#%s#%d#%s#%s"%(name,salaries[rand],nations[rand3],groups[rand4],email,date))
  
  groupIndexes[groups[rand4]] += "%d#"%(i)
  nationIndexes[rand3] += "%d#"%(i)
  
  json = {}
  json["_id"] = i
  json["Value"] = tokom

  print(i)
  data.append(json);


print("Generating done")

for i in range(len(nationIndexes)):
  json = {}
  json["_id"] = nations[i]
  json["Value"] = nationIndexes[i][:-1]
  data3.append(json)

for i in range(len(groupIndexes)):
  json = {}
  json["_id"] = groups[i]
  json["Value"] = groupIndexes[i][:-1]
  data2.append(json)

print("Indexes done")

mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
database = mongoclient.get_database("Shop")

customers = database.get_collection("customers")
customers.insert_many(data)
groupIndex = database.get_collection("customers.Group.ind")
groupIndex.insert_many(data2)
nationIndex = database.get_collection("customers.Nation.ind")
nationIndex.insert_many(data3)