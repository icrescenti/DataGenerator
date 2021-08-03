import sys
import json
import random

f = None

if len(sys.argv) > 1:
    f = open(sys.argv[1],)
else:
    f = open('structure.json',)

data = json.load(f)

def convertToFormat(headers, values):
    if(data['format'] == "sql"):
        print("INSERT INTO " + data['tablename'] + 
        " (" +
            (",".join(headers)) + 
        ") VALUES (" + 
            (",".join(values))
        + ");")
    elif(data['format'] == "csv"):
        print("(TO-DO)")

for _ in range(data['quanitity']):
    headers = []
    values = []

    for item in data['fields']:
        headers.append("\'" + item['label'] + "\'")

        valuesFStream = None
        xtype = 0
        value = 0
        
        if (item['value'] == "integer"):
            xtype = 0
        elif (item['value'] == "first_name"):
            valuesFStream = open('data/first_names.json',)
            xtype = 1
        elif (item['value'] == "middle_name"):
            valuesFStream = open('data/middle-names.json',)
            xtype = 1
        
        if (xtype == 0):
            try:
                value = item['autoIncrementFrom'] + _
            except KeyError:
                try:
                    xrange = item['range']
                    value = random.randrange(xrange[0],xrange[1])
                except KeyError:
                    value = random.randrange(0,100)

        elif (xtype == 1):
            valuesArray = json.load(valuesFStream)
            value = "\'" + valuesArray[random.randrange(0,len(valuesArray))] + "\'"
            valuesFStream.close()

        values.append(str(value))
    convertToFormat(headers, values)
  
f.close()