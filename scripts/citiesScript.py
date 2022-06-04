from random import randint
import pymongo

f = open('citiesInput.txt')

line = f.readline()

list = []
usedrand = []
indexes = [{ '_id': i, 'Value': ''} for i in range(0,50)]
ids = []

while line != '':
  line = line.replace('\n','')
  line = line.split(' ')

  json = {}
  json['_id'] = randint(0, 1000)
  while json['_id'] in usedrand:
    json['_id'] = randint(0, 1000)
  usedrand.append(json['_id'])
  try:
    print(line[0], line[1])
  except:
    break
  json['Value'] = ('%s#%s#%s'%(line[0],line[1],str(randint(10000,250000))))
  if indexes[int(line[0])]['Value'] == '':
    indexes[int(line[0])]['Value'] += str(json['_id'])
  else:
    indexes[int(line[0])]['Value'] += '#' + str(json['_id'])

  ids.append(json['_id'])
  list.append(json)
  line = f.readline()

print(list)

outFile = open("cityIDs.txt",'w')
for id in ids:
  outFile.write(('%d\n'%(id)))

mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
database = mongoclient.get_database("shop")

customers = database.get_collection("cities")
customers.insert_many(list)

customers = database.get_collection("cities.stateID.ind")
customers.insert_many(indexes)