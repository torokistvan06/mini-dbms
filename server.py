from copy import deepcopy
import datetime
from ntpath import join
from socket import *
import xml.etree.ElementTree as ET
import pymongo
import re

global serverPort
serverPort = 50034

def merge(arr, l, m, r, key):
    n1 = m - l + 1
    n2 = r - m

    L = [0] * (n1)
    R = [0] * (n2)

    for i in range(0, n1):
        L[i] = arr[l + i]
 
    for j in range(0, n2):
        R[j] = arr[m + 1 + j]

    i = 0     # Initial index of first subarray
    j = 0     # Initial index of second subarray
    k = l     # Initial index of merged subarray
 
    while i < n1 and j < n2:
        if (L[i][key] != 'NULL' and R[j][key] == 'NULL') or L[i][key] <= R[j][key]:
            arr[k] = L[i]
            i += 1
        else:
            arr[k] = R[j]
            j += 1
        k += 1
 
    # Copy the remaining elements of L[], if there
    while i < n1:
        arr[k] = L[i]
        i += 1
        k += 1
 
    # Copy the remaining elements of R[], if there
    while j < n2:
        arr[k] = R[j]
        j += 1
        k += 1

def mergeSort(arr, l, r, key):
    if l < r:
        m = l+(r-l)//2
 
        # Sort first and second halves
        mergeSort(arr, l, m, key)
        mergeSort(arr, m+1, r, key)
        merge(arr, l, m, r, key)

def doTask(msg: str):
    msg = msg.split(sep = '\n')
    command = msg[0]
    retval = None
    if command == 'Create Database':
        databaseName = msg[1]
        retval = createDatabase(databaseName)
    elif command == 'Create Table':
        databaseName = msg[1]
        tableName = msg[2]
        rows = msg[3:]
        retval = createTable(databaseName,tableName, rows)
    elif command == 'Delete Database':
        databaseName = msg[1]
        retval = deleteDatabase(databaseName)
    elif command == 'Delete Table':
        databaseName = msg[1]
        tableName = msg[2]
        retval = deleteTable(databaseName,tableName)
    elif command == 'Insert':
        databaseName = msg[1]
        tableName = msg[2]
        data = msg[3]
        retval = insertData(databaseName, tableName, data)
    elif command == 'Delete':
        databaseName = msg[1]
        tableName = msg[2]
        conditions = msg[3]
        retval = deleteData(databaseName, tableName, conditions)
    elif command == 'Select':
        databaseName = msg[1]
        dataName = msg[2].split(sep = ' ')[1:]
        tableName = msg[3].split(sep = ' ' )[1:]
        
        conditions = msg[4].replace("WHERE ","");
        if conditions == '':
            conditions = None

        joinTables = msg[5].replace("JOIN ","")
        groups = msg[6].replace("GROUP BY ","")
        if groups == '':
            groups = None
        retval = selectData(databaseName, dataName, tableName, conditions, joinTables, groups)
    
    root = ET.parse('Catalog.xml').getroot()
    return retval

def descartes(database, groups, data, dictList, dicti, allTableNicks, allIndexes, allIndexFiles):
    usedgroups = []
    for g in groups:
        nick = g.split('.')[0]
        tableIndex = None
        for i, c in enumerate(allTableNicks):
            if c == nick:
                tableIndex = i
                break

        indexIndex = None
        for i, index in enumerate(allIndexes[tableIndex]):
            if index == g.split('.')[1]:
                indexIndex = i
                break

        if indexIndex == None:
            for dat in data:
                if not dat[g] in usedgroups:
                    usedgroups.append(dat[g])
                    dicti[g] = dat[g]
                    descartes(database, groups[1:],data, dictList, dicti, allTableNicks, allIndexes, allIndexFiles)
                    dictList.append(deepcopy(dicti))
        else:
            localCollection = mongoclient.get_database(database).get_collection(allIndexFiles[tableIndex][indexIndex])
            localData = localCollection.find()
            for dat in localData:
                dicti[g] = str(dat['_id'])
                descartes(database, groups[1:],data, dictList, dicti, allTableNicks, allIndexes, allIndexFiles)
                dictList.append(deepcopy(dicti))

def functions(database, dataName, data, allTypes, allTableNicks, groups, outFile, allIndexes, allIndexFiles):

    functionsUsed = False
    for name in dataName:
        try:
            column = name.split('(')[1]
            functionsUsed = True
            break
        except:
            pass

    if not functionsUsed:
        msg = ''
        lines = ''
        for key in data[0].keys():
            if key in dataName:
                msg += "%35s"%(key)
        print(msg)
        print('\n')
        lines += (msg + '\n\n')
        for dat in data:
            msg = ''
            for key in dat.keys():
                if key in dataName:
                    msg += "%35s"%(dat[key])
            print(msg)
            lines += (msg + '\n')
        print('\n')
        lines += '\n'
        outFile.writelines(lines)
        return 0

    elif groups == None:

        dicti = {}
        for name in dataName:
            list = []
            func = name.split('(')[0]
            column = name.split('(')[1][:-1]
            if column != '*':
                tableIndex = None
                for i, c in enumerate(allTableNicks):
                    if c == column.split('.')[0]:
                        tableIndex = i
                        break
                columnType = allTypes[tableIndex][column]
                for dat in data:
                    if columnType == 'int':
                        list.append(int(dat[column]))
                    if columnType == 'float':
                        list.append(float(dat[column]))
            else:
                list = data

            if func == 'AVG':
                if len(list) != 0:
                    dicti[('AVG(%s)'%(column))] = sum(list) / len(list)
                else:
                    dicti[('AVG(%s)'%(column))] = 0
            elif func == 'SUM':
                dicti[('SUM(%s)'%(column))] = sum(list)
            elif func == 'MAX':
                dicti[('MAX(%s)'%(column))] = max(list)
            elif func == 'MIN':
                dicti[('MIN(%s)'%(column))] = min(list)
            elif func == 'COUNT':
                dicti[('COUNT(%s)'%(column))] = len(list)
            
        
        msg = ''
        lines = ''
        for key in dicti.keys():
            msg += "%35s"%(key)
        print(msg)
        print()
        lines += (msg + '\n\n')
        msg = ''
        for key in dicti.keys():
            msg += "%35s"%(dicti[key])
        print(msg)
        lines += msg
        outFile.writelines(lines)
        return 0

    else:
        dictList = []
        dicti = {}
        descartes(database, groups, data, dictList, dicti, allTableNicks, allIndexes, allIndexFiles)

        newDictList = []
        for d in dictList:
            if not d in newDictList:
                newDictList.append(d)
        
        dictList = newDictList
        datas = [[] for _ in range(len(dictList))]

        for i, dict in enumerate(dictList):
            print(dict)
            for dat in data:
                matching = True
                for key in dict.keys():
                    if dict[key] != dat[key]:
                        matching = False
                        break
                if matching:
                    datas[i].append(dat)
            
            for dat in datas[i]:
                data.remove(dat)

            for name in dataName:
                if name not in groups:
                    list = []
                    func = name.split('(')[0]
                    column = name.split('(')[1][:-1]
                    if column != '*':
                        tableIndex = None
                        for j, c in enumerate(allTableNicks):
                            if c == column.split('.')[0]:
                                tableIndex = j
                                break
                        columnType = allTypes[tableIndex][column]
                        for dat in datas[i]:
                            if columnType == 'int':
                                list.append(int(dat[column]))
                            if columnType == 'float':
                                list.append(float(dat[column]))
                    else:
                        list = datas[i]
                
                    if func == 'AVG':
                        if len(list) != 0:
                            dict[('AVG(%s)'%(column))] = sum(list) / len(list)
                        else:
                            dict[('AVG(%s)'%(column))] = 0
                    elif func == 'SUM':
                        dict[('SUM(%s)'%(column))] = sum(list)
                    elif func == 'MAX':
                        dict[('MAX(%s)'%(column))] = max(list)
                    elif func == 'MIN':
                        dict[('MIN(%s)'%(column))] = min(list)
                    elif func == 'COUNT':
                        dict[('COUNT(%s)'%(column))] = len(list)
        
        msg = ''
        lines = ''
        for key in dataName:
            msg += "%35s"%(key)
        print(msg)
        print('\n')
        lines += (msg + '\n\n')
        for dat in dictList:
            msg = ''
            for key in dat.keys():
                if key in dataName:
                    msg += "%35s"%(dat[key])
            print(msg)
            lines += (msg + '\n')
        print('\n')
        lines += '\n'
        outFile.writelines(lines)
        return 0


def selectData(databaseName, dataName, tableName, conditions, joinTables, groups):

    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
        
    if database == None:
        return -2 # Trying to delete from non-existing database

    tableNick = tableName[1]
    tableName = tableName[0]

    table = None
    for tb in database:
        if tb.attrib['tableName'] == tableName:
            table = tb
            break

    if table == None:
        return -3 # Trying to delete from non-existing table

    tableColumns = table.findall('.//Structure//Attribute')
    for dat in dataName:
        if dat != '*':
            if 'AVG' in dat or 'COUNT' in dat or 'MAX' in dat or 'MIN' in dat or 'SUM' in dat:
                temp = dat.split('(')[1]
                columnN = temp.split(')')[0]
                if columnN == '*':
                    continue
                columnNick = columnN.split('.')[0]
                columnName = columnN.split('.')[1]
            else:
                columnNick = dat.split('.')[0]
                columnName = dat.split('.')[1]

            tbColumn = None
            if columnNick == tableNick:
                for column in tableColumns:
                    if column.attrib['attributeName'] == columnName:
                        tbColumn = column
                        break
                if tbColumn == None:
                    return -7 # trying to select non existing column

        elif dat == '*' and groups != None:
            return -9
    allTableNames = []
    allTableNames.append(tableName)
    allTableNicks = []
    allTableNicks.append(tableNick)
    joinTableNames = []
    joinTableNicks = []
    joinTableConditions = []
    joinTypes = []
    if joinTables != '':
        joinTables = joinTables.split(sep = " and ")
        for jt in joinTables:
            joinT = jt.split(sep = ' ')
            joinTypes.append(joinT[0])
            allTableNames.append(joinT[2])
            joinTableNames.append(joinT[2])
            allTableNicks.append(joinT[3])
            joinTableNicks.append(joinT[3])
            joinTableConditions.append(joinT[5])

    print(joinTypes)

    allPks = []
    allCollections = []
    allTables = []
    allStructures = []
    allTypes = []
    allIndexes = []
    allIndexFiles = []
    allIndexFileIsUnique = []
    allAttribs = []

    collection = mongoclient.get_database(databaseName).get_collection(tableName)
    allCollections.append(collection)
    pk = tableNick + '.' + table.findall('.//primaryKey//pkAttribute')[0].text # Save the primary key
    allPks.append(pk)
    allTables.append(table)
    structure = ''                                           # Save the structure of the table for later usage
    types = {}
    for column in table.findall('.//Structure//Attribute'):
        types[tableNick + '.' + column.attrib['attributeName']] = column.attrib['type']
        if (tableNick + '.' + column.attrib["attributeName"]) != pk:
          structure += (column.attrib['attributeName'] + '#')

    structure = structure[:-1]
    structure = structure.split(sep = '#')

    allStructures.append(structure)
    allTypes.append(types)

    indexestmp = table.findall('IndexFiles//IndexFile//IndexAttributes//IAttribute') # Save the attributes we have indexes for and the files for them
    indexfilestmp = table.findall('IndexFiles//IndexFile')

    indexes = []
    indexfiles = []
    indexfilesisunique = []

    for i in range(len(indexestmp)):
        indexes.append(indexestmp[i].text)
        indexfiles.append(indexfilestmp[i].attrib["indexName"])
        indexfilesisunique.append(indexfilestmp[i].attrib["isUnique"])
    
    allIndexes.append(indexes)
    allIndexFiles.append(indexfiles)
    allIndexFileIsUnique.append(indexfilesisunique)

    attribs = [] # Save the attributes of the table

    for i in table.findall('.//Structure//Attribute'):
        attribs.append(i.attrib['attributeName'])

    allAttribs.append(attribs)

    if conditions == None and len(dataName) == 1 and dataName[0] != '*' and dataName[0] != 'COUNT(*)' and dataName[0].split('.')[1] in indexes:
        outFile = open('clientOutput.txt', 'w')
        lines = ''
        indexfile = None
        for i, ind in enumerate(indexes):
            if ind == dataName[0].split('.')[1]:
                indexfile = indexfiles[i]
        localCollection = mongoclient.get_database(databaseName).get_collection(indexfile)
        data = localCollection.find()
        print("%35s\n"%(dataName[0]))
        lines += ("%35s\n\n"%(dataName[0]))
        for dat in data:
            print("%35s"%(dat["_id"]))
            lines += ("%35s\n"%(dat["_id"]))
        print()
        lines += '\n'
        outFile.writelines(lines)
        outFile.close()
        return 0

    for i, joinTableName in enumerate(joinTableNames):

        # Validate table names and save the reference to their xml
        joinTable = None
        for tb in database:
            if tb.attrib['tableName'] == joinTableName:
                joinTable = tb
        
        if joinTable == None:
            return -3
        else:
            allTables.append(joinTable)

        # Save the primary keys of tables

        allPks.append(joinTableNicks[i] + '.' + joinTable.findall('.//primaryKey//pkAttribute')[0].text)

        # Get table collections
        allCollections.append(mongoclient.get_database(databaseName).get_collection(joinTableName))

        # Save the structure of joined tables for later usage
        joinStructure = ''                                         
        joinType = {}
        for column in joinTable.findall('.//Structure//Attribute'):
            joinType[joinTableNicks[i] + '.' +  column.attrib['attributeName']] = column.attrib['type']
            if (joinTableNicks[i] + '.' + column.attrib["attributeName"]) != allPks[i + 1]:
                joinStructure += (column.attrib['attributeName'] + '#')

        joinStructure = joinStructure[:-1]
        joinStructure = joinStructure.split(sep = '#')

        allStructures.append(joinStructure)
        allTypes.append(joinType)

        # Save the indexes of joined tables

        indexestmp = joinTable.findall('IndexFiles//IndexFile//IndexAttributes//IAttribute') # Save the attributes we have indexes for and the files for them
        indexfilestmp = joinTable.findall('IndexFiles//IndexFile')
        

        joinIndex = []
        joinIndexFile = []
        joinIndexFileIsUnique = []

        for i in range(len(indexestmp)):
            joinIndex.append(indexestmp[i].text)
            joinIndexFile.append(indexfilestmp[i].attrib["indexName"])
            joinIndexFileIsUnique.append(indexfilestmp[i].attrib["isUnique"])

        allIndexes.append(joinIndex)
        allIndexFiles.append(joinIndexFile)
        allIndexFileIsUnique.append(joinIndexFileIsUnique)
        # Save the attributes of joined tables for later usage

        joinAttrib = [] 

        for i in joinTable.findall('.//Structure//Attribute'):
            joinAttrib.append(i.attrib['attributeName'])
        
        allAttribs.append(joinAttrib)
    
    datas = []
    for i, table in enumerate(allTables):
        data = allCollections[i].find()
        newData = []
        for dat in data:
            dicti = {}
            dicti[allPks[i]] = dat["_id"]
            val = dat['Value'].split(sep = "#")
            for j, struct in enumerate(allStructures[i]):
                dicti[(allTableNicks[i] + '.' + struct)] = val[j]
            newData.append(dicti)

        datas.append(newData)

    columns = []
    operators = []
    toCompare = []
    comparators = []
    ops = []
    antiops = []
    attribs = []

    for i, attr in enumerate(allAttribs):
        for a in attr:
            attribs.append(allTableNicks[i] + '.' + a)

    if(conditions != None):             # Working around the conditions to be able to use them, also validating them

        conditions = conditions.split(sep = " and ")

        for i in conditions:
            if re.search('^[a-z].[a-zA-Z0-9]+-?[0-9]*-?[0-9]*-?[0-9]*:?[0-9]*(!=|=|[<>]=?)[a-zA-Z0-9]+-?[0-9]*-?[0-9]*-?[0-9]*:?[0-9]*',i) == None:
                return -5 # Wrong input, bad conditions

        for condition in conditions:
            operators.append(re.search(pattern = '(<=)|(>=)|<|>|=',string = condition).group(0))
            columns.append(condition.split(sep = operators[-1]))
    
        for i,column in enumerate(columns):     # Splitting conditions into columns, operators and what we have to compare them to
            newAttrib = None
            for attribName in attribs:
                if attribName == column[0]:
                    toCompare.append(column[0])
                    comparators.append(column[1])
                    newAttrib = 1
                    break
                elif attribName == column[1]:
                    comparators.append(column[0])
                    toCompare.append(column[1])
                    newAttrib = 1
                    break
            if newAttrib == None:
                return -6
        
        # Converting operators so they can be used easily in mongodb.find
        
        antiop = None
        for op in operators:
            if op == '<':
                op = '$lt'
                antiop = '$gte'
            elif op == '<=':
                op = '$lte'
                antiop = '$gt'
            elif op == '=':
                op = '$eq'
                antiop = '$ne'
            elif op == '>':
                op = '$gt'
                antiop = '$lte'
            elif op == '>=':
                op = '$gte'
                antiop = '$lt'
            ops.append(op)
            antiops.append(antiop)

    filteredData = [[] for _ in range(len(allTables))]
    for i in range(len(filteredData)):
        filteredData[i] = datas[i]

    for i, tc in enumerate(toCompare):
        tableIndex = None
        for j, tNick in enumerate(allTableNicks):
            if tc.split('.')[0] == tNick:
                tableIndex = j
                break
        data = select(databaseName, filteredData[tableIndex], tc, allPks[tableIndex], allTypes[tableIndex], comparators[i], ops[i] , antiops[i], operators[i], allCollections[tableIndex], allIndexes[tableIndex], allIndexFiles[tableIndex])
        filteredData[tableIndex] = data

    data = filteredData[0]
    joined = []
    joined.append(0)
    for index, cond in enumerate(joinTableConditions):
        tableOneIndex = None
        tableTwoIndex = None
        tableOneComparator = cond.split(sep = '=')[0]
        tableTwoComparator = cond.split(sep = '=')[1]
        for i, nick in enumerate(allTableNicks):
            if nick == tableOneComparator.split(sep='.')[0]:
                tableOneIndex = i
            if nick == tableTwoComparator.split(sep='.')[0]:
                tableTwoIndex = i
        
        if tableOneIndex == None or tableTwoIndex == None:
            return -4

        joinIndex = None
        dataIndex = None
        joinComparator = None
        dataComparator = None
        if tableOneIndex in joined:
            joinIndex = tableTwoIndex
            dataIndex = tableOneIndex
            joinComparator = tableTwoComparator
            dataComparator = tableOneComparator
        else:
            joinIndex = tableOneIndex
            dataIndex = tableTwoIndex
            joinComparator = tableOneComparator
            dataComparator = tableTwoComparator


        newData = []
        joinComparatorAdd = joinComparator.split('.')[1]
    
        # if not joinComparatorAdd in allIndexes[joinIndex]:

        if joinTypes[index] == 'inner':
            for dat in data:
                dicti = {}
                for dataTwo in filteredData[joinIndex]: 
                    if str(dat[dataComparator]) == str(dataTwo[joinComparator]):
                        for key in dat.keys():
                            dicti[key] = dat[key]
                        for key in dataTwo.keys():
                            dicti[key] = dataTwo[key]
                        newData.append(dicti)

        elif joinTypes[index] == 'left':
            for dat in data:
                leftJoined = False
                dicti = {}
                for dataTwo in filteredData[joinIndex]: 
                    if str(dat[dataComparator]) == str(dataTwo[joinComparator]):
                        leftJoined = True
                        for key in dat.keys():
                            dicti[key] = dat[key]
                        for key in dataTwo.keys():
                            dicti[key] = dataTwo[key]
                        newData.append(dicti)
                if not leftJoined:
                    for key in dat.keys():
                        dicti[key] = dat[key]
                    dicti[allPks[index + 1]] = 'NULL'
                    for key in allStructures[joinIndex]:
                        dicti[allTableNicks[joinIndex] + '.' + key] = 'NULL'
                    newData.append(dicti)

        elif joinTypes[index] == 'right':
            for dataTwo in filteredData[joinIndex]:
                rightJoined = False
                dicti = {}
                for dat in data: 
                    if str(dat[dataComparator]) == str(dataTwo[joinComparator]):
                        rightJoined = True
                        for key in dat.keys():
                            dicti[key] = dat[key]
                        for key in dataTwo.keys():
                            dicti[key] = dataTwo[key]
                        newData.append(dicti)
                if not rightJoined:
                    for key in data[0].keys():
                        dicti[key] = 'NULL'
                    for key in dataTwo.keys():
                        dicti[key] = dataTwo[key]
                    newData.append(dicti)
        
        elif joinTypes[index] == 'full':
            for dat in data:
                leftJoined = False
                dicti = {}
                for dataTwo in filteredData[joinIndex]: 
                    if str(dat[dataComparator]) == str(dataTwo[joinComparator]):
                        leftJoined = True
                        for key in dat.keys():
                            dicti[key] = dat[key]
                        for key in dataTwo.keys():
                            dicti[key] = dataTwo[key]
                        newData.append(dicti)
                if not leftJoined:
                    for key in dat.keys():
                        dicti[key] = dat[key]
                    dicti[allPks[index + 1]] = 'NULL'
                    for key in allStructures[joinIndex]:
                        dicti[allTableNicks[joinIndex] + '.' + key] = 'NULL'
                    newData.append(dicti)
            
            for dataTwo in filteredData[joinIndex]:
                rightJoined = False
                dicti = {}
                for dat in data: 
                    if str(dat[dataComparator]) == str(dataTwo[joinComparator]):
                        rightJoined = True
                if not rightJoined:
                    for key in data[0].keys():
                        dicti[key] = 'NULL'
                    for key in dataTwo.keys():
                        dicti[key] = dataTwo[key]
                    newData.append(dicti)
        
        else:
            return -11
            
                
        # else:
        #     localCollection = None
            
        #     if joinComparator != allPks[joinIndex]:
        #         for i, index in enumerate(allIndexes[joinIndex]):
        #             if index == joinComparator.split('.')[1]:
        #                 localCollection = mongoclient.get_database(databaseName).get_collection(allIndexFiles[joinIndex][i])
        #     else:
        #         localCollection = allCollections[joinIndex]

            
        #     ids = []
        #     pkType = None
        #     for dat in data:
        #         localComparator = dat[dataComparator]
        #         localCompType = allTypes[dataIndex][dataComparator]
        #         if localCompType == 'int':
        #             localComparator = int(localComparator)
        #         if localCompType == 'float':
        #             localComparator = float(localComparator)
        #         if localCompType == 'bit':
        #             localComparator = bool(localComparator)
        #         if joinComparator == allPks[joinIndex]:
        #             dataTwos = localCollection.find( {'_id': localComparator} )
        #         else:
        #             dataTwos = localCollection.find( {'_id': localComparator} )

        #         for dataTwo in dataTwos:
        #             oldIds = dataTwo['Value'].split('#')
        #             pkType = allTypes[joinIndex][joinComparator]
        #             for id in oldIds:
        #                 if pkType == 'int':
        #                     ids.append(int(id))
        #                 if pkType == 'float':  
        #                     ids.append(float(id))
        #                 if pkType == 'bit':
        #                     ids.append(bool(id))
                
        #     tempData = []
        #     for dat in filteredData[joinIndex]:
        #         compThis = dat[allPks[joinIndex]]
        #         if pkType == 'int':
        #             compThis = int(compThis)
        #         if pkType == 'float':  
        #             compThis = float(compThis)
        #         if pkType == 'bit':
        #             compThis = bool(compThis)
        #         if compThis in ids:
        #             tempData.append(dat)
        #             if len(tempData) == len(ids):
        #                 break
            
        #     filteredData[joinIndex] = tempData

        #     for dat in data:
        #         for dataTwo in filteredData[joinIndex]: 
        #             if str(dat[dataComparator]) == str(dataTwo[joinComparator]):
        #                 dicti = {}
        #                 for key in dat.keys():
        #                     dicti[key] = dat[key]
        #                 for key in dataTwo.keys():
        #                     dicti[key] = dataTwo[key]
        #                 newData.append(dicti)

        data = newData
        joined.append(joinIndex)

    length = len(data) - 1
    if groups != None:
        groups = groups.split(' ')
        for name in dataName:
            try:
                tmp = name.split('(')[1]
            except:
                if not name in groups:
                    return -8

    if groups != None:
        for group in groups:           
            mergeSort(data, 0, length, group)


    outFile = open('clientOutput.txt', 'w')
    
    if len(data) != 0:
        if len(dataName) == 1 and dataName[0] == '*': # Select * case

            if groups != None:
                return -9
            msg = ''
            lines = ''
            for key in data[0].keys():
                msg += "%35s"%(key)
            print(msg)
            print('\n')
            lines += (msg + '\n\n')
            
            for dat in data:
                msg = ''
                for key in dat.keys():
                    msg += "%35s"%(dat[key])
                print(msg)
                lines += (msg + '\n')
            print('\n')
            lines += '\n'
            outFile.writelines(lines)
        else: # Selecting given columns
            functions(databaseName, dataName, data, allTypes, allTableNicks, groups, outFile, allIndexes, allIndexFiles)

    outFile.close()

def select(databaseName, data, tc, pk, types, comparator, op, antiop, operator, collection, indexes, indexFiles):
# Filter by primary key
    if tc == pk:
        json = {}
        json2 = {}
        localComparator = comparator
        localToCompareType = types[tc]
        if localToCompareType == 'int':
            localComparator = int(localComparator)
        if localToCompareType == 'float':  
            localComparator = float(localComparator)
        if localToCompareType == 'bit':
            localComparator = bool(localComparator)
        json2[op] = localComparator
        json["_id"] = json2
        ids = []
        newData = collection.find(json)
        for dat in newData:
            localID = dat["_id"]
            if localToCompareType == 'int':
                localID = int(localID)
            if localToCompareType == 'float':  
                localID = float(localID)
            if localToCompareType == 'bit':
                localID = bool(localID) 
            ids.append(localID)
        
        newData = []
        for dat in data:
            if dat[tc] in ids:
                newData.append(dat)
                if len(newData) == len(ids):
                    break
        
        data = newData

    
    for j, index in enumerate(indexes):      # Filter by indexes
        if tc.split('.')[1] == index:
            json = {}
            json2 = {}
            localComparator = comparator
            localToCompareType = types[tc]
            if localToCompareType == 'int':
                localComparator = int(localComparator)
            if localToCompareType == 'float':  
                localComparator = float(localComparator)
            if localToCompareType == 'bit':
                localComparator = bool(localComparator)

            localCollection = mongoclient.get_database(databaseName).get_collection(indexFiles[j])
            json2[op] = localComparator
            json["_id"] = json2
            localData = localCollection.find(json);
            ids = []
            for dat in localData:
                localIds = dat["Value"].split(sep = "#")
                pkType = types[pk]
                for id in localIds:
                    if pkType == 'int':
                        ids.append(int(id))
                    if pkType == 'float':  
                        ids.append(float(id))
                    if pkType == 'bit':
                        ids.append(bool(id))

            newData = []
            for dat in data:
                if dat[pk] in ids:
                    newData.append(dat)
                    if len(newData) == len(ids):
                        break
                
            data = newData

    if tc != pk and not tc.split('.')[1] in indexes:
        removeThis = []

        if types[tc] == 'string':
            for dat in data:
                if dat[tc] != comparator and operator == '=':
                    removeThis.append(dat)
        elif types[tc] != 'date' and types[tc] != 'datetime':
            for dat in data:
                if operator == '=':
                    evalthis = dat[tc] + operator + operator + comparator
                else:
                    evalthis = dat[tc] + operator + comparator
                if eval(evalthis) == False:
                    removeThis.append(dat)
        else:
            for dat in data:
                date1 = None
                date2 = None
                diff = None
                if types[tc] == 'date':
                        date1 = datetime.datetime.strptime(dat[tc],'%Y-%m-%d')
                        date2 = datetime.datetime.strptime(comparator,'%Y-%m-%d')
                        diff = (date1 - date2).days
                else:
                    date1 = datetime.datetime.strptime(dat[tc],'%Y-%m-%d-%H:%M')
                    date2 = datetime.datetime.strptime(comparator,'%Y-%m-%d-%H:%M')
                    diff = (date1 - date2).days

                if diff != 0 and operator == '=':
                    removeThis.append(dat)
                elif diff == 0 and operator == '!=':
                    removeThis.append(dat)
                elif diff > 0 and operator == '<=':
                    removeThis.append(dat)
                elif diff >= 0 and operator == '<':
                    removeThis.append(dat)
                elif diff < 0 and operator == '>=':
                    removeThis.append(dat)
                elif diff <= 0 and operator == '>':
                    removeThis.append(dat)
        
        for rm in removeThis:
            data.remove(rm)
    
    return data

def createIndex(database, table, indexName, index, key, isUnique):
    collection = mongoclient.get_database(database).get_collection(table + '.' +  indexName +'.ind')
    if isUnique == 1:
        data = {
            "_id":index,
            "Value":key
            }
        collection.insert_one(data)
        return 0
    else:
        
        try:
            val = collection.find({"_id" : index})[0]['Value']
        except:
            data = {
                "_id":index,
                "Value":key
                }
            collection.insert_one(data)
            return 0
        
        collection.update_one({"_id" : index}, { "$set" : {"Value" : val + '#' + key } })
        return 0

def deleteIndexData(databaseName, dict, indexes, indexFiles):
    database = mongoclient.get_database(databaseName)
    print(dict)
    for j in range(len(indexes)):
        print(indexFiles[j].attrib['indexName'])
        isUnique = indexFiles[j].attrib['isUnique']
        collection = database.get_collection(indexFiles[j].attrib['indexName'])
        data = collection.find({"_id" : dict[indexes[j].text]})[0]['Value']
        collection.delete_one({"_id" : dict[indexes[j].text]})
        if isUnique == '0':
            data = data.split(sep = '#')
            newData = ''
            for d in data:
                if d != dict["key"]:
                    if newData == '':
                        newData = d
                    else:
                        newData += '#' + d
            if newData != '':
                collection.insert_one({"_id" : dict[indexes[j].text], "Value" : newData})

def deleteData(databaseName, tableName, conditions):

    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
        
    if database == None:
        return -2 # Trying to delete from non-existing database

    table = None
    for tb in database:
        if tb.attrib['tableName'] == tableName:
            table = tb

    if table == None:
        return -3 # Trying to delete from non-existing table


    collection = mongoclient.get_database(databaseName).get_collection(tableName)

    separators = [elem for i, elem in enumerate(conditions.split(sep = " "))
                  if i % 2 == 1]

    conditions = [elem for i, elem in enumerate(conditions.split(sep = " "))
                  if i % 2 == 0]

    for i in separators:
        if i != 'and' and i != 'or':
            return -4 # Wrong input, bad separators

    for i in conditions:
        if re.search('^[a-zA-Z0-9]+-?[0-9]*-?[0-9]*-?[0-9]*:?[0-9]*(!=|=|[<>]=?)[a-zA-Z0-9]+-?[0-9]*-?[0-9]*-?[0-9]*:?[0-9]*',i) == None:
            return -5 # Wrong input, bad conditions

    attribs = []

    for i in table.findall('.//Structure//Attribute'):
        attribs.append(i.attrib['attributeName'])

    columns = []
    operators = []
    for condition in conditions:
        columns.append(re.split(pattern = '(<=)|(>=)|<|>|=',string = condition))
        operators.append(re.search(pattern = '(<=)|(>=)|<|>|=',string = condition).group(0))

    toCompare = []
    comparators = []
    
    for i,column in enumerate(columns):
        newAttrib = None
        for attribName in attribs:
            if attribName == column[0]:
                toCompare.append(column[0])
                comparators.append(column[3])
                newAttrib = 1
            elif attribName == column[3]:
                comparators.append(column[0])
                toCompare.append(column[3])
                newAttrib = 1
        if newAttrib == None:
            return -6
    
    ops = []
    for op in operators:
        if op == '<':
            op = '$lt'
        elif op == '<=':
            op = '$lte'
        elif op == '=':
            op = '$eq'
        elif op == '>':
            op = '$gt'
        elif op == '>=':
            op = '$gte'
        ops.append(op)

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
    types = {}
    for column in table.findall('.//Structure//Attribute'):
        if column.attrib['attributeName'] != pk:
            structure += (column.attrib['attributeName'] + '#')
            types[column.attrib['attributeName']] = column.attrib['type']

    structure = structure[:-1]
    structure = structure.split(sep = '#')

    indexes = table.findall('IndexFiles//IndexFile//IndexAttributes//IAttribute')
    indexfiles = table.findall('IndexFiles//IndexFile')

    json = {}
    bigdict = []
    json2 = {}
    data = collection.find()


    for dat in data:
        helper = {}
        for i,struct in enumerate(structure):
            helper[struct] = dat['Value'].split(sep='#')[i]
            helper['key'] = dat['_id']
        bigdict.append(helper)
        
    separators.append('or')

    referencedByTables = table.find('.//ReferencedBy//Structure//Table')
    referencedByAttrs = table.find('.//ReferencedBy//Structure//RefAttribute')

    if type(referencedByAttrs) == list:
        for i in range(len(referencedByAttrs)):
            referencedByAttrs[i] = referencedByAttrs[i].text
            referencedByTables[i] = referencedByTables[i].text
    elif referencedByAttrs != None:
        referencedByAttrs = referencedByAttrs.text
        referencedByTables = referencedByTables.text

    for i,compare in enumerate(toCompare):

        if compare == pk:
            json2[ops[i]] = comparators[i]
            json['_id'] = json2
            if type(referencedByTables) == list:
                for k in range(len(referencedByTables)):
                    indColl = mongoclient.get_database(databaseName).get_collection(referencedByTables[k] + '.' + referencedByAttrs[k] + '.ind')
                    try:
                        val = indColl.find(json)[0]['Value']
                    except:
                        continue
                    return -10
                collection.delete_many(json)
            elif referencedByAttrs != None:
                indColl = mongoclient.get_database(databaseName).get_collection(referencedByTables + '.' + referencedByAttrs + '.ind')
                try:
                    val = indColl.find(json)[0]['Value']
                except:
                    collection.delete_many(json)
                    continue
                return -10
            else:
                deleteThis = collection.find(json)
                bigDelete = []
                for delete in deleteThis:
                    helper = {}
                    for i,struct in enumerate(structure):
                        helper[struct] = delete['Value'].split(sep='#')[i]
                        helper['key'] = delete['_id']
                    bigDelete.append(helper)
                for dicti in bigDelete:                 
                    deleteIndexData(databaseName,dicti,indexes,indexfiles)
                collection.delete_one(json)

            
        else:
            deleteThis = []
            if types[compare] == 'string':
                for dicti in bigdict:
                    if dicti[compare] == comparators[i] and operators[i] == '=':
                        deleteThis.append(dicti)
                    elif dicti[compare] != comparators[i] and operators[i] == '!=':
                        deleteThis.append(dicti)
                    elif dicti[compare] <= comparators[i] and operators[i] == '<=':
                        deleteThis.append(dicti)
                    elif dicti[compare] < comparators[i] and operators[i] == '<':
                        deleteThis.append(dicti)
                    elif dicti[compare] >= comparators[i] and operators[i] == '>=':
                        deleteThis.append(dicti)
                    elif dicti[compare] > comparators[i] and operators[i] == '>':
                        deleteThis.append(dicti)
                    elif separators[i-1] == 'and'  and i != 0:
                        deleteThis.remove(dicti)

            elif types[compare] != 'date' and types[compare] != 'datetime':
                for dicti in bigdict:
                    if operators[i] == '=':
                        evalthis = dicti[compare] + operators[i] + operators[i] + comparators[i]
                    else:
                        evalthis = dicti[compare] + operators[i] + comparators[i]
                    if eval(evalthis) == True:
                        deleteThis.append(dicti)
                    elif separators[i-1] == 'and'  and i != 0:
                        deleteThis.remove(dicti)
            else:
                for dicti in bigdict:
                    if types[compare] == 'date':
                        date1 = datetime.datetime.strptime(dicti[compare],'%Y-%m-%d')
                        date2 = datetime.datetime.strptime(comparators[i],'%Y-%m-%d')
                        diff = (date1 - date2).days
                    else:
                        date1 = datetime.datetime.strptime(dicti[compare],'%Y-%m-%d-%H:%M')
                        date2 = datetime.datetime.strptime(comparators[i],'%Y-%m-%d-%H:%M')
                        diff = (date1 - date2).days
                    if diff == 0 and operators[i] == '=':
                        deleteThis.append(dicti)
                    elif diff != 0 and operators[i] == '!=':
                        deleteThis.append(dicti)
                    elif diff <= 0 and operators[i] == '<=':
                        deleteThis.append(dicti)
                    elif diff < 0 and operators[i] == '<':
                        deleteThis.append(dicti)
                    elif diff >= 0 and operators[i] == '>=':
                        deleteThis.append(dicti)
                    elif diff > 0 and operators[i] == '>':
                        deleteThis.append(dicti)
                    elif separators[i-1] == 'and'  and i != 0:
                        deleteThis.remove(dicti)

            if separators[i] == 'or' or i == len(toCompare):  
                for dicti in deleteThis:
                    json['_id'] = dicti['key']                            
                    deleteIndexData(databaseName,dicti,indexes,indexfiles)
                    collection.delete_one(json)
                deleteThis = []
            elif separators[i] == 'and':
                pass

def insertData(databaseName, tableName, data):
    data = data[1:]
    data = data[:-1]
    data = data.split(sep = ',')

    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
        
    if database == None:
        return -2

    table = None
    for tb in database:
        if tb.attrib['tableName'] == tableName:
            table = tb

    if table == None:
        return -3 

    structure = ''                                           # Save the structure of the table for later usage
    types = {}
    for column in table.findall('.//Structure//Attribute'):
        structure += (column.attrib['attributeName'] + '#')
        types[column.attrib['attributeName']] = column.attrib['type']

    structure = structure[:-1]
    try:
        pk = table.findall('.//primaryKey//pkAttribute')[0].text
    except:
        pk = None

    pkType = types[pk]

    try:
        uniqueKeys = table.findall('.//uniqueKeys//UniqueAttribute')
        for i in range(len(uniqueKeys)):
            uniqueKeys[i] = uniqueKeys[i].text
    except:
        uniqueKeys = None
    
    try:
        refTables = table.findall('.//foreignKeys//foreignKey//references//refTable')
        refAttrs = table.findall('.//foreignKeys//foreignKey//references//refAttr')
        foreignKeys = table.findall('.//foreignKeys//foreignKey//foreignAttribute')
        for i in range(len(refTables)):
            refTables[i] = refTables[i].text
            refAttrs[i] = refAttrs[i].text
            foreignKeys[i] = foreignKeys[i].text
    except:
        refTables = None
        refAttrs = None
        foreignKeys = None


    msg = ''
    indexes = table.findall('IndexFiles//IndexFile//IndexAttributes//IAttribute')
    indexfiles = table.findall('IndexFiles//IndexFile')
    i = 0
    stringPK = None
    for column in table.findall('.//Structure//Attribute'):
        # Typeerror

        if column.attrib['type'] == 'bit':
            if re.search('^0|1$',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'int':
            if re.search('^[0-9]+$',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'float':
            if re.search('^[+-]?[0-9]+.[0-9]+$',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'string':
            if re.search('.*',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'date':
            if re.search('^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$',data[i]) == None:
                return -1
        elif column.attrib['type'] == 'datetime':
            if re.search('^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])-(2[0-3]|[01][0-9]):[0-5][0-9]$',data[i]) == None:
                return -1


        if column.attrib['attributeName'] != pk:
            if msg == '':
                msg += data[i]
            else:
                msg += '#' + data[i]

            for uk in uniqueKeys:
                if column.attrib['attributeName'] == uk:
                    ukindex = mongoclient.get_database(databaseName).get_collection(tableName + '.' + uk + '.ind')
                    try:
                        val = ukindex.find({"_id" : data[i]})[0]['Value']
                    except:
                        break
                    print("UK exists");
                    return -10 # Unique key already exists

            for j in range(len(foreignKeys)):
                if column.attrib['attributeName'] == foreignKeys[j]:
                    refTable = mongoclient.get_database(databaseName).get_collection(refTables[j])
                    localType = types[foreignKeys[j]]
                    refVal = data[i]
                    if localType == 'int':
                        refVal = int(refVal)
                    if localType == 'float':
                        refVal = float(refVal)
                    if localType == 'bit':
                        refVal = bool(refVal)
                    try:
                        val = refTable.find({"_id" : refVal})[0]['Value']
                    except:
                        print("No referenced row");
                        return -11 # Referenced row doesn't exist


        else:
            stringPK = data[i]
            if pkType == 'int':
                pk = int(data[i])
            if pkType == 'float':
                pk = float(data[i])
            if pkType == 'bit':
                pk = bool(data[i])
  
        i += 1


    i = 0 
    for column in table.findall('.//Structure//Attribute'):
        for j in range(len(indexes)):
            if column.attrib['attributeName'] == indexes[j].text:
                indexType = types[indexes[j].text]
                indexVal = None
                if indexType == 'int':
                    indexVal = int(data[i])
                elif indexType == 'float':
                    indexVal = float(data[i])
                elif indexType == 'bit':
                    indexVal = bool(data[i])
                else:
                    indexVal = data[i]
                print(indexes[j].text,indexfiles[j].attrib['isUnique'])
                createIndex(databaseName,tableName,indexes[j].text,indexVal,stringPK,indexfiles[j].attrib['isUnique'])
        i += 1

    if pk != '':
        data = {
            "_id":pk,
            "Value":msg
            }
    else:
        data = {
            "Value":msg
        }

    database = mongoclient.get_database(databaseName)
    collection = database.get_collection(tableName)
    try:
        collection.insert_one(document=data)
    except:
        print("Failed to insert")
        return -4 # Failed to insert because PK exists


    print("Successful insert");
    return 0 # Successful insert

def deleteDatabase(databaseName: str):
    for db in root:
        if db.attrib['dataBaseName'] == databaseName: 
            root.remove(db)
            ET.indent(tree, space = '\t', level = 0)
            tree.write('Catalog.xml', encoding = 'utf-8')
            mongoclient.drop_database(databaseName)
            return 0 # Successful delete

    return -1 # Trying to delete non-existing database

def deleteTable(databaseName: str, tableName: str):
    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
        
    if db == None:
        return -1 # Trying to delete from non-existing database
    
    for tb in database:
        if tb.attrib['tableName'] != tableName:
            for refT in tb.findall('.//foreignKeys//foreignKey//references//refTable'):
                if refT.text == tableName:
                    return -3 # Trying to delete table that is referenced by another table

    for tb in database:
        if tb.attrib['tableName'] == tableName: 
            database.remove(tb)
            ET.indent(tree, space = '\t', level = 0)
            tree.write('Catalog.xml', encoding = 'utf-8')
            mongoclient.get_database(databaseName).drop_collection(tableName)
            return 0 # Successful delete

    return -2 # Trying to delete non-existing table 
    
def createDatabase(databaseName: str):
    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName: 
            return -1 # Trying to create existing database
    
    database  = ET.SubElement(root,'DataBase')
    database.set('dataBaseName', databaseName)
    ET.indent(tree, space = '\t', level = 0)
    tree.write('Catalog.xml', encoding = 'utf-8')
    return 0

def createTable(databaseName : str, tableName : str, rows):
    database = None
    for db in root:
        if db.attrib['dataBaseName'] == databaseName:
            database = db
            break
    
    if database == None: 
        return -1 # Trying to create table in non-existing database

    for child in database:
        if child.attrib['tableName'] == tableName: 
            return -2 # Trying to create existing table

    for i in rows:
        i = i.split(sep = ' ')
        rowIsForeign = int(i[5])
        if rowIsForeign:
            rowReference = i[6]
            rowReference = rowReference.split(sep = '(')
            rowReference[1] = rowReference[1][:-1]
            refT = None
            for table in database:
                if table.attrib['tableName'] == rowReference[0]:
                    refT = table
            if refT == None: 
                return -3 # Reference on non-existing table

            refCol = None
            for attribute in refT.findall(".//Structure//Attribute"):
                if attribute.attrib['attributeName'] == rowReference[1]:
                    refCol = attribute
            if refCol == None: 
                return -4 # Reference on non-existing column in table

    table = ET.SubElement(database,'Table')
    table.set('tableName',tableName)
    table.set('rowLength',str(len(rows)))

    structure = ET.SubElement(table,'Structure')
    pkeys = ET.SubElement(table,'primaryKey')
    ukeys = ET.SubElement(table,'uniqueKeys')
    fkeys = ET.SubElement(table,'foreignKeys')
    indexfiles = ET.SubElement(table,'IndexFiles')

    for i in rows:
        i = i.split(sep = ' ')
        rowName = i[0]
        rowType = i[1]
        rowIsUnique = int(i[2])
        rowIsIndex = int(i[3])
        rowIsPrimary = int(i[4])
        rowIsForeign = int(i[5])
        if rowIsForeign == 1:
            rowReference = i[6]
            rowReference = rowReference.split('(')
            rowReference[1] = rowReference[1][:-1]
    
        attr = ET.SubElement(structure, 'Attribute')
        attr.set('attributeName',rowName)
        attr.set('type',rowType)
        if rowIsUnique:
            ukey = ET.SubElement(ukeys,'UniqueAttribute')
            ukey.text = rowName
            index = ET.SubElement(indexfiles, 'IndexFile')
            index.set('indexName',tableName + '.' + rowName + '.ind')
            index.set('isUnique',str(rowIsUnique))
            indexAttrs = ET.SubElement(index,'IndexAttributes')
            indexAttr=  ET.SubElement(indexAttrs,'IAttribute')
            indexAttr.text = rowName

        if rowIsPrimary:
            pkey = ET.SubElement(pkeys,'pkAttribute')
            pkey.text = rowName

        if rowIsForeign:
            fkey = ET.SubElement(fkeys,'foreignKey')
            fkAttr = ET.SubElement(fkey,'foreignAttribute')
            fkAttr.text = rowName
            reference = ET.SubElement(fkey,'references')
            refTable = ET.SubElement(reference,'refTable')
            refTable.text = rowReference[0]
            refAttr = ET.SubElement(reference,'refAttr')
            refAttr.text = rowReference[1]
            index = ET.SubElement(indexfiles, 'IndexFile')
            index.set('indexName',tableName + '.' + rowName + '.ind')
            index.set('isUnique',str(rowIsUnique))
            indexAttrs = ET.SubElement(index,'IndexAttributes')
            indexAttr=  ET.SubElement(indexAttrs,'IAttribute')
            indexAttr.text = rowName

            for tb in database:
                if tb.attrib['tableName'] == rowReference[0]:
                    referencedBy = ET.SubElement(tb,"ReferencedBy")
                    referencedStructure = ET.SubElement(referencedBy, "RefStructure")
                    referencedByTable = ET.SubElement(referencedStructure, "Table")
                    referencedAttr = ET.SubElement(referencedStructure, "RefAttribute")
                    referencedByTable.text = tableName
                    referencedAttr.text = rowName


        if rowIsIndex:
            index = ET.SubElement(indexfiles, 'IndexFile')
            index.set('indexName',tableName + '.' + rowName + '.ind')
            index.set('isUnique',str(rowIsUnique))
            indexAttrs = ET.SubElement(index,'IndexAttributes')
            indexAttr=  ET.SubElement(indexAttrs,'IAttribute')
            indexAttr.text = rowName

    ET.indent(tree, space = '\t', level = 0)
    tree.write('Catalog.xml', encoding = 'utf-8')
    return 0

if __name__ == '__main__':
    serverSocket = socket(AF_INET,SOCK_STREAM)
    serverSocket.bind(('',serverPort))
    serverSocket.listen(10)

    print("Server is starting")
    global tree, root, mongoclient
    tree = ET.parse('Catalog.xml')
    mongoclient = pymongo.MongoClient("mongodb+srv://istu:Ceruza12.@cluster0.n8gxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")    
    root = tree.getroot()
    #print(root)
 
    while True:
        (clientSocket, addr) = serverSocket.accept()
        msg = clientSocket.recv(256).decode()
        if msg == 'EXIT':
            print("Server is shutting down")
            clientSocket.close()
            break 

        retval = doTask(msg)
        clientSocket.send(str(retval).encode())
        clientSocket.close()

    serverSocket.close()

