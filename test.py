import pymongo

mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

collection = mongoclient.get_database('University').get_collection('disciplines.CreditNr.ind')

try:
    val = collection.find({"_id" : 2})[0]['Value']
except:
    print('No such id')
addon = "FI"


collection.update_one({"_id" : 2}, { "$set" : {"Value" : val + '#' + addon } })
