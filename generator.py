import sys
import json
import random

if len(sys.argv) <= 1:
    sys.exit()

def convertToFormat(label, xtype, value):
    if(sys.argv[1] == "sql"):
        print("(Sample) INSERT INTO " + label + " VALUES (" + value + ");")

f = open('structure.json',)

data = json.load(f)
  
for item in data:
    valuesFStream = ""
    xtype = 0
    
    if (item['value'] == "first_name"):
        valuesFStream = open('data/first_names.json',)
        xtype = 1
    elif (item['value'] == "middle_name"):
        valuesFStream = open('data/middle-names.json',)
        xtype = 1
    
    valuesArray = json.load(valuesFStream)
    convertToFormat(item['label'], xtype, valuesArray[random.randrange(1,len(valuesArray))])
    valuesFStream.close()
  
f.close()