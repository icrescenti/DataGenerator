import sys
import json
import random
import time
import os.path
from os import path
import progressbar
from time import sleep

f = None

if len(sys.argv) > 1:
    f = open(sys.argv[1],)
else:
    f = open('structure.json',)

data = json.load(f)

errors = 0
quote = "\'"

try:
    quote = data['quote']
except KeyError:
    quote = "\'"

def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))
 
def random_date(start, end, xformat, prop):
    return str_time_prop(start, end, xformat, prop)

def convertToFormat(pos, headers, values):
    if(data['format'] == "sql"):
        writeToFile("INSERT INTO " + data['tablename'] + 
        " (" +
            (",".join(headers)) + 
        ") VALUES (" + 
            (",".join(values))
        + ");\n")
    elif(data['format'] == "csv"):
        value = None

        if (pos == 0):
            value = headers
        else:
            value = values

        writeToFile(
        (data['delimiter'].join(value)) + 
        data['csvnewline']
        )

def writeToFile(value):
    gen = open('generated.' + data['format'],'a')
    gen.write(value)
    gen.close()

if (path.exists('generated.' + data['format'])):
    print("Cleaning generated data file.........", end = '')
    gen = open('generated.' + data['format'],'w')
    gen.write('')
    gen.close()
    print('OK')

print("Running.........")

bar = progressbar.ProgressBar(maxval=data['quanitity'], \
    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
bar.start()

for _ in range(data['quanitity']):
    headers = []
    values = []

    for item in data['fields']:
        headers.append(quote + item['label'] + quote)

        valuesFStream = None
        xtype = 0
        value = 0
        
        if (item['value'] == "integer"):
            try:
                value = item['autoIncrementFrom'] + _
            except KeyError:
                try:
                    xrange = item['range']
                    value = random.randrange(xrange[0],xrange[1])
                except KeyError:
                    value = random.randrange(0,100)

        elif (item['value'] == "first_name"):
            valuesFStream = open('data/first_names.json',)
            xtype = 1

        elif (item['value'] == "middle_name"):
            valuesFStream = open('data/middle-names.json',)
            xtype = 1

        elif (item['value'] == "last_name"):
            valuesFStream = open('data/last-names.json',)
            xtype = 1

        elif (item['value'] == "date"):
            try:
                xrange = item['range']
                value = quote + random_date(xrange[0], xrange[1], item['format'], random.random()) + quote
            except KeyError:
                print("ERROR: " + item['label'] + " doesen't have range or format attribute")
                errors += 1
                value = quote + random_date("1/1/1970 00:00:00", "31/12/2099 00:00:00", '%d/%m/%Y %H:%M:%S', random.random()) + quote
            
        if (xtype == 1):
            valuesArray = json.load(valuesFStream)
            value = quote + valuesArray[random.randrange(0,len(valuesArray))] + quote
            valuesFStream.close()

        values.append(str(value))
    convertToFormat(_, headers, values)
    bar.update(_)

f.close()
bar.finish()
print("Script ended, information generated (" + str(data['quanitity']) + " rows) with " + str(errors) + " errors")