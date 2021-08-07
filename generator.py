import sys
import json
import random
import time
import os.path
from os import path
import progressbar
from time import sleep
import mysql.connector

#region global variables

f = None
mydb = None
errors = 0
quote = "\'"

sql_settings = None
csv_settings = None

#endregion

#region read strucutre file

if len(sys.argv) > 1:
    f = open(sys.argv[1],)
else:
    f = open('structure.json',)

data = json.load(f)

#endregion

#region read preferences

try:
    quote = data['quote']
except KeyError:
    None

try:
    sql_settings = data['sql_settings']
    execute = sql_settings['execute']
    if (execute):
        mydb = mysql.connector.connect(
            host=sql_settings['host'],
            user=sql_settings['user'],
            password=sql_settings['password'],
            database=sql_settings['database']
        )
except KeyError:
    None

try:
    csv_settings = data['csv_settings']
except KeyError:
    None

#endregion

#region random date

def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))
 
def random_date(start, end, xformat, prop):
    return str_time_prop(start, end, xformat, prop)

#endregion

#region generation functions

def convertToFormat(pos, headers, values):
    if(data['show']):
        print("(BATCH #" + str(pos) + ") ", end = '')
        for _ in range(len(headers)):
            print(values[_], end = '')

        print('')

    if(data['format'] == "sql"):
        sql = ("INSERT INTO " + sql_settings['tablename'] + 
        " (" +
            (",".join(headers)) + 
        ") VALUES (" + 
            (",".join(values))
        + ");\n")

        if(sql_settings['execute']):
            mycursor = mydb.cursor()
            mycursor.execute(sql)
            mydb.commit()
        else:
            writeToFile(sql)

    elif(data['format'] == "csv"):
        value = None

        if (pos == 0):
            value = headers
        else:
            value = values

        writeToFile(
        csv_settings['delimiter'].join(value) + 
        csv_settings['newline']
        )

def writeToFile(value):
    gen = open('generated.' + data['format'], 'a', encoding="utf8")
    gen.write(value)
    gen.close()

#endregion

#region clear generation file

if (path.exists('generated.' + data['format'])):
    print("Cleaning generated data file.........", end = '')
    gen = open('generated.' + data['format'],'w')
    gen.write('')
    gen.close()
    print('OK')

#endregion

print("Running.........")

#region progress bar

bar = progressbar.ProgressBar(maxval=data['quanitity'], \
    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

if(data['show'] == False):
    bar.start()

#endregion

for _ in range(data['quanitity']):
    headers = []
    values = []

    for item in data['fields']:
        headers.append(item['label'])

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
                value = quote + random_date("1/1/1990 00:00:00", "31/12/2080 00:00:00", '%d/%m/%Y %H:%M:%S', random.random()) + quote
            
        elif (item['value'] == "emoji"):
            valuesFStream = open('data/emojis.json', encoding="utf8")
            xtype = 1

        if (xtype == 1):
            valuesArray = json.load(valuesFStream)
            value = quote
            leng = 1

            try:
                leng = item['length']
            except KeyError:
                None
            
            for __ in range(leng):
                value += valuesArray[random.randrange(0,len(valuesArray))]
            
            value += quote
            valuesFStream.close()

        values.append(str(value))
    convertToFormat(_, headers, values)
    if(data['show'] == False):
        bar.update(_)

f.close()
if(data['show'] == False):
    bar.finish()
print("Script ended, information generated (" + str(data['quanitity']) + " rows) with " + str(errors) + " errors")