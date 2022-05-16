import datetime
from socket import *
import xml.etree.ElementTree as ET
import pymongo
import re
import random
import names


salaries = [5000, 5250, 5500, 5740, 6000, 6500, 7000, 2500, 10000]
nations = ['Romania', 'Hungary', 'Ukraine', 'USA', 'Russia', 'France', 'UK', 'Germany']
groups = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10]
dates = ['2012-01-02','2012-01-12','2012-02-24','2012-03-05','2012-04-20']
emails = ['@gmail.com', '@yahoo.com', '@hotmail.com', '@freemail.com']

for i in range(0,200,1):
  dataBaseName = "Shop"
  tableName = "employees"

  date = dates[random.randint(0,4)]
  name = names.get_first_name()
  data = "(%d,%d,%s,%s)"%(i,i,name,date)
  print(data)

  message = "Insert\n" + dataBaseName + '\n' + tableName + '\n' + data

  serverName = 'localhost'
  clientSocket = socket(AF_INET,SOCK_STREAM)
  clientSocket.connect((serverName,50004))
  clientSocket.send(message.encode())

  msg = clientSocket.recv(256).decode()
      
  clientSocket.close()