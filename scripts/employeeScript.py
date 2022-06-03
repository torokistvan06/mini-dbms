import pymongo

f = open('customerInput.txt','r')

customerIDs = []
line = f.readline()
while line != '':
  line = line.replace('(','')
  line = line.replace('),','')
  line = line.replace(');','')
  line = line.replace('\n','')
  line = line.replace('\'','')
  line = line.split(',')
  if line != ['']:
    customerIDs.append(line[0])
  line = f.readline()

f.close()

f = open('employeeInput.txt', 'r')

line = f.readline()
while line != '':
  line = line.replace('(','')
  line = line.replace('),','')
  line = line.replace(');','')
  line = line.replace('\n','')
  line = line.replace('\'','')
  line = line.split(',')
  if line != ['']:
    print(line)
  line = f.readline()


print(customerIDs)