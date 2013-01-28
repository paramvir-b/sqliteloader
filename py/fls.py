#!/usr/bin/env python3

################################################################################
# The code is available under LGPL3 license. License can be found at:
# http://www.gnu.org/licenses/lgpl-3.0.txt 
# The code will soon be available on github at
# https://github.com/paramvir-b
################################################################################


import argparse
import json
import logging
import sqlite3
import os


parser = argparse.ArgumentParser(
        description = 'Converts fixed length files to sqlite database')

parser.add_argument('-i', required = True,
        metavar = '<input_file>',
        help = 'Input fixed length text file containing records to be ported to sqlite data base.')

parser.add_argument('-l', required = True,
        metavar = '<layout_file>',
        help = 'Layout file containing field definitions. These \
                field definitions will be used for parsing fixed length record')

parser.add_argument('-c',
        metavar = '<N>',
        type = int,
        default = 10,
        help = 'Number of records after which commit operation is called on the data base')

parser.add_argument('-d',
        action='store_true',
        help = 'Number of records after which commit operation is called on the data base')

args = parser.parse_args()

if args.d:
    logging.basicConfig(level = logging.DEBUG)

logging.debug(args)

layoutFileName = args.l
inputFileName = args.i
commitAfter = args.c


logging.debug(layoutFileName)

layoutFile = open(layoutFileName)
layout = json.load(layoutFile)
layoutFile.close()

logging.debug(layout)

layoutFieldList = layout['fieldList']
logging.debug(layoutFieldList)

## TODO NEED TO EXTRACT TABLE NAME SOMEHOW
tableName = 't'

layoutFieldListLen = len(layoutFieldList)
logging.debug('layoutFieldListLen={}'.format(layoutFieldListLen))
logging.debug('Looping over layout field list')
recordLen = 0
createTableQry = 'CREATE TABLE ' + tableName + ' ('
fieldCounter = 0
for field in layoutFieldList:
    logging.debug('field={}'.format(field))
    logging.debug('name={} length={}'.format(field['name'], field['length']))
    try:
        type = field['type']
    except:
        field['type'] = 'text'
        type = 'text'

    createTableQry += field['name'] + ' '
    if type == 'text':
        createTableQry += ' TEXT'
    if type == 'integer':
        createTableQry += ' INTEGER'
    if type == 'real':
        createTableQry += ' REAL'

    if fieldCounter < layoutFieldListLen - 1:
        createTableQry += ', '

    recordLen += field['length']
    fieldCounter += 1

createTableQry += ');'

logging.debug('createTableQry={}'.format(createTableQry))

inFile = open(inputFileName)
dbFileName = inputFileName + '.db'
try:
    os.remove(dbFileName)
except:
    pass

sqlconn = sqlite3.connect(dbFileName)
cursor = sqlconn.cursor()

cursor.execute(createTableQry)
cursor.execute('"PRAGMA synchronous=OFF;')

recordCounter = 0
for line in inFile:
    logging.debug('line={} len={}'.format(line,len(line)))
    insertQry = 'INSERT INTO ' + tableName + ' VALUES ('
    index = 0
    fieldCounter = 0
    for field in layoutFieldList:
        fieldLength = field['length']
        value = line[index:index + fieldLength]
        logging.debug('fieldLength={} index={}'.format(fieldLength, index))
        logging.debug('valueRaw={}'.format(value))

        type = field['type']
        if type == 'text':
            value = value.strip(' ')
            value = value.replace('\'', '\'\'')
            if len(value) > 0:
                insertQry += '\'' + value + '\''
            else:
                insertQry += 'NULL'

        if type == 'integer':
            value = value.strip('0')
            value = value.strip(' ')
            logging.debug('value={}'.format(value))
            if len(value) > 0:
                try:
                    insertQry += str(int(value))
                except Exception as exp:
                    insertQry += 'NULL'
                    logging.error('FAILED exp = {} '.format(repr(exp)));
                    logging.error('FAILED field = {} '.format(field));
                    logging.error('FAILED line = {} '.format(line));
            else:
                insertQry += 'NULL'

        if type == 'real':
            value = value.strip('0')
            value = value.strip(' ')
            logging.debug('value={}'.format(value))
            if len(value) > 0:
                try:
                    insertQry += str(float(value))
                except Exception as exp:
                    insertQry += 'NULL'
                    logging.error('FAILED exp = {} '.format(repr(exp)));
                    logging.error('FAILED field = {} '.format(field));
                    logging.error('FAILED line = {} '.format(line));
            else:
                insertQry += 'NULL'

        if fieldCounter < layoutFieldListLen - 1:
            insertQry += ', '

        index += fieldLength
        fieldCounter += 1

    insertQry += ');'
    logging.debug('insertQry={}'.format(insertQry));
    try:
        cursor.execute(insertQry)
    except Exception as exp:
        logging.error('FAILED exp = {} '.format(repr(exp)));
        logging.error('FAILED field = {} '.format(field));
        logging.error('FAILED line = {} '.format(line));
        logging.error('FAILED insertQry={}'.format(insertQry));

    recordCounter += 1
    if recordCounter % commitAfter == 0:
        logging.debug('Committing at recordCounter={}'.format(recordCounter));
        sqlconn.commit()


inFile.close()
sqlconn.commit()
sqlconn.close()
