import datetime
from socket import *
import xml.etree.ElementTree as ET
import pymongo
import re
import random
import names


groups = [531,532,533,631,632,633];
emails = ["@gmail.com", "@yahoo.com", "@hotmail.com"]

for i in range(5,100000,1):
  dataBaseName = "University"
  tableName = "students"

  rand = random.randint(0,5)
  rand2 = random.randint(0,2);
  name = names.get_full_name();
  email = name.split(" ")[0].lower() + name.split(" ")[1].lower() + emails[rand2];
  data = "(%d,%d,%s,%s)"%(i,groups[rand],name,email);
  print(data)

  message = "Insert\n" + dataBaseName + '\n' + tableName + '\n' + data

  serverName = 'localhost'
  clientSocket = socket(AF_INET,SOCK_STREAM)
  clientSocket.connect((serverName,50004))
  clientSocket.send(message.encode())

  msg = clientSocket.recv(256).decode()
      
  clientSocket.close()