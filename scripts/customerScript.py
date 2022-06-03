import pymongo

f = open('customerInput.txt','r')

list = []
line = f.readline()
while line != '':
  line = line.replace('(','')
  line = line.replace('),','')
  line = line.replace(');','')
  line = line.replace('\n','')
  line = line.replace('\'','')
  line = line.split(',')
  if line != ['']:
  
    line[0] = int(line[0])

    json = {}
    json['_id'] = line[0]
    json['Value'] = ('%s#%s#%s#%s#%s#%s'%(line[1],line[2],line[3],line[4],line[9],line[12]))
    print(json)
    list.append(json)
  line = f.readline()

mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
database = mongoclient.get_database("shop")

customers = database.get_collection("customers")
customers.insert_many(list)