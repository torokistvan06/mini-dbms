from random import randint
import pymongo

f = open('statesInput.txt')

line = f.readline()

id = 0
list = []

while line != '':
  line = line.replace('\n','')
  line = line.split(' ')

  json = {}
  json['_id'] = id
  json['Value'] = ('%s#%s#%s'%(line[0],line[1],str(randint(100000,1000000))))
  list.append(json)
  id += 1
  line = f.readline()

print(list)
  
mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
database = mongoclient.get_database("shop")

customers = database.get_collection("states")
customers.insert_many(list)


customers = database.get_collection("states")
customers.insert_many(list)