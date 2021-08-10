import sys
import json
import random
import time
import os.path
from os import path
import progressbar
from time import sleep
import mysql.connector
import http.server
import socketserver

#region global variables

data = None
mydb = None
bar = None
quote = "\'"

sql_settings = None
csv_settings = None
http_settings = None
xml_settings = None

#endregion

#region load variables function

def loadStructFile():
    global data
    global sql_settings
    global csv_settings
    global http_settings
    global xml_settings
    global bar
    global mydb

    #region read strucutre file
    f = None

    if len(sys.argv) > 1:
        f = open(sys.argv[1],)
    else:
        f = open('structure.json',)

    data = json.load(f)
    f.close()

    #endregion

    #region read preferences

    quote = "\'"
    try:
        quote = data['quote']
    except KeyError:
        None

    try:
        sql_settings = data['sql_settings']
        execute = sql_settings['execute']
        if (execute):
            mydb = mysql.connector.connect(
                charset='utf8',
                host=sql_settings['host'],
                user=sql_settings['user'],
                password=sql_settings['password'],
                database=sql_settings['database']
            )

            if(sql_settings['execute']):
                mycursor = mydb.cursor()
                mycursor.execute("SET NAMES utf8mb4;") #or utf8 or any other charset you want to handle
                mycursor.execute("SET CHARACTER SET utf8mb4;") #same as above
                mycursor.execute("SET character_set_connection=utf8mb4;") #same as abov
                mydb.commit()

    except KeyError:
        None

    try:
        csv_settings = data['csv_settings']
    except KeyError:
        None

    try:
        http_settings = data['http_settings']
    except KeyError:
        None
        
    try:
        xml_settings = data['xml_settings']
    except KeyError:
        None

    #endregion

    #region progress bar

    bar = progressbar.ProgressBar(maxval=data['quanitity'], \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

    if(data['show'] == False):
        bar.start()

    #endregion

#endregion

#load variables before continue
loadStructFile()

#region random date

def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))
 
def random_date(start, end, xformat, prop):
    return str_time_prop(start, end, xformat, prop)

#endregion

#region HTTP Server

class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        loadStructFile()
        generate()
        if self.path == '/':
            self.path = 'generated.' + data['format']
        return http.server.SimpleHTTPRequestHandler.do_GET(self)

handler_object = MyHttpRequestHandler

PORT = 8080
try:
    PORT = http_settings['port']
except:
    None

my_server = socketserver.TCPServer(("", PORT), handler_object)

#endregion

#region generation functions

def convertToFormat(pos, headers, values):
    global mydb
    
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
    
    elif(data['format'] == "xml"):
        writeToFile("   <item>\n")
        for headerIndex in range(len(headers)):
            writeToFile(
            "       <" + headers[headerIndex] + ">" + values[headerIndex] + "</" + headers[headerIndex] + ">" + 
            xml_settings['newline']
            )
        writeToFile("   </item>\n")
        
    elif(data['format'] == "json"):
        jsonString = "\n  {\n"
        for headerIndex in range(len(headers)):
            jsonString += "    \"" + headers[headerIndex] + "\": " + values[headerIndex]
            if (headerIndex < (len(headers)-1)):
                jsonString += ","
            jsonString += "\n"
        
        jsonString += "  }"
        if (pos < (data['quanitity']-1)):
            jsonString += ","

        writeToFile(jsonString)

def writeToFile(value):
    gen = open('generated.' + data['format'], 'a', encoding="utf8")
    gen.write(value)
    gen.close()

#endregion

#region generation

def generate():
    global quote
    errors = 0

    print("Running.........")

    #region clear generation file

    if (path.exists('generated.' + data['format'])):
        print("Cleaning generated data file.........", end = '')
        gen = open('generated.' + data['format'],'w')
        gen.write('')
        gen.close()
        print('OK')

    #endregion

    if(data['format'] == "json"):
        quote = "\""
        writeToFile("[")
    elif(data['format'] == "xml"):
        writeToFile("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        writeToFile("<items>\n")

    for _ in range(data['quanitity']):
        headers = []
        values = []

        for item in data['fields']:
            headers.append(item['label'])

            valuesFStream = None
            readFromFile = True
            value = 0
            
            if (item['value'] == "integer"):
                readFromFile = False
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

            elif (item['value'] == "middle_name"):
                valuesFStream = open('data/middle-names.json',)

            elif (item['value'] == "last_name"):
                valuesFStream = open('data/last-names.json',)
            
            elif (item['value'] == "places"):
                valuesFStream = open('data/places.json',)

            elif (item['value'] == "date"):
                readFromFile = False
                try:
                    xrange = item['range']
                    value = quote + random_date(xrange[0], xrange[1], item['format'], random.random()) + quote
                except KeyError:
                    print("ERROR: " + item['label'] + " doesen't have range or format attribute")
                    errors += 1
                    value = quote + random_date("1/1/1990 00:00:00", "31/12/2080 00:00:00", '%d/%m/%Y %H:%M:%S', random.random()) + quote
                
            elif (item['value'] == "emoji"):
                valuesFStream = open('data/emojis.json', encoding="utf8")

            if (readFromFile):
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

    if(data['format'] == "json"):
        writeToFile("\n]")
    elif(data['format'] == "xml"):
        writeToFile("</items>\n")

    if(data['show'] == False):
        bar.finish()
    
    print("Script ended, information generated (" + str(data['quanitity']) + " rows) with " + str(errors) + " errors.\n")
    errors = 0

#endregion

generate()

# Start the server
try:
    if (http_settings['execute']):
        my_server.serve_forever()
except:
    None