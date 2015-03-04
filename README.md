Sqlite Loader
=============

Imports csv files to sqlite database file

Motivation
----------

During one of my personal projects, I had to use '.import' command of the sqlite3 shell. The .import
command is very limited in functionality. It also always binds column data as text. This has
performance hit as well as disk usage hit. I also need some very basic conversion support like store
numbers as numbers in db files as well as some way to parse date related data.

All these needs leads for the development of this project. Apart from the above mentioned features,
one of the key requirement for this project is performance.

Features
--------

 * Highly performant by writing it in C/C++. It also used sqlite features to optimize for speed.
 * Support triming text
 * Support date/time input formats
 * Support pivotYear for date/time
 * Support missing value declaration for columns
 * Separate types for int, real and text as opposed to always text with '.import' option of sqlite3
   shell
 * Support skipping of input fields in the csv file
 * Support storing dates as text or epoch

Usage for project
-----------------

This project is available as a command line utility which can be used to create sqltie db files from
csv files.

*TBD - Fixed length file support might come in future. Right now CSV looks like a better choice*

Options
-------

```text
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -t <table_name>       Table to be created
  -i <input_file>       Input fixed length text file containing records to be
                        ported to sqlite data base.
  -o <output_file>      Output file name for the sqlite data base.
  -l <layout_file>      Layout file containing field definitions. These
                                   field definitions will be used for parsing
                        fixed length record
  -c <N>                Number of records after which commit operation is called
                        on the data base
  -d                    Delete if table already exists
  -a                    Append to existing table if it does exist
  -v                    Debug mode
  -s                    Show stats
  -p <comma-separated-pragma-list>
                        Comma separated pragma which ran before creation of DB.
  -b <N>                Read buffer size in bytes. Default: 1048576
  -f <N>                Field buffer size in bytes. Default: 1048576
```

Layout Definition
-----------------

General layout structure is define in json format:

```text
<LayoutDefinition>
  <IndexList>
    <Index>
      <IndexColumnList>
        <IndexColumn>
  <FieldDefinitionList>
    <FieldDefinition>
```

*Layout Definition Parameters*

| Parameter Name  | Description |
|-----------------|-------------|
| name            | Layout name that will be used as table name. Can also be passed as an argument to utility.|
| type            | csv. 'csv' for delimited file.|
| separator       | Separator used for file. Only valid for csv files.|
| storeDateAsEpoch| Store date as Epoch seconds. This will help reduce file size.|

*Layout Field Definition Parameters*

| Parameter Name | Description |
|----------------|-------------|
| name           | Field name. It will become the column name in db |
| type           | text/integer/real/date/time. 'text' text field like "hello". 'integer' for integers like 10. 'real' for decimals like 5.6 |
| format         | If type is date/time then we need to provide this. Use date format from http://pubs.opengroup.org/onlinepubs/009695399/functions/strftime.html. Example: %m%d%Y for 11022014 which is 2014-02-11. %d/%m/%Y-%H-%M-%S for 02/11/2014-21-56-53 which is 2014-02-11T21:56:53 |
| pivotYear      | If type is date/time then we can provide pivot year. Refer to http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormatter.html#withPivotYear(int). Example: 2000|
| missingValue   | If this is the value then it will be replaced with null in db. |
| isSkip         | If true, this field is skipped while parsing the input file.  Default is false |
| isTrim         | If type is text, then if false, this field is NOT trimmed. Default is true |

*Index Definition Parameters*

| Parameter Name | Description |
|----------------|-------------|
| name           | Name for the index. If left blank it will be auto generated.  Example: idx_col_name_1 |

*Index Column Definition Parameters*

| Parameter Name | Description |
|----------------|-------------|
| name           | Column name on which index is based on.|


Example of csv layout:


```json
{
    "name" : "in",
    "type" : "csv",
    "separator" : "   ",
    "indexList": [
       {
            "columnList": [
                {
                    "name": "rid1"
                },
                {
                    "name": "rid2"
                }
            ]
       }
    ],
    "fieldList": [
        {
            "name": "record_id",
            "type": "integer"
        },
        {
            "name": "name"
        },
        {
            "name": "balance",
            "type": "real"
        },
        {
            "name": "date1",
            "type": "date",
            "format": "%m%d%Y"
        },
        {
            "name": "date2",
            "type": "date",
            "format": "%Y-%m-%d"
        },
        {
            "name": "time1",
            "type": "time",
            "format": "%Y-%m-%dT%H:%M:%S"
        },
        {
            "name": "time2",
            "type": "time",
            "format": "%d/%m/%Y-%H-%M-%S"
        }
    ]
}
```
