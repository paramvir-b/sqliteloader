Usage: sqliteloader [options]

Converts fixed length files to sqlite database

Options:
  -h, --help            show this help message and exit
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
  -p <comma-separated-pragma-list>
                        Comma separated pragma which ran before creation of DB.


Using Sqlite Version: 3.8.5

Layout definition:
 General layout structure is define in json format:
   <LayoutDefinition>
     <FieldDefinitionList>
       <FieldDefinition>
 Layout Definition Parameters:
   name      : Layout name that will be used as table name. Can also be passed
as an argument to utility.
               Example: book_info
   type      : csv. 'csv' for delimited file
               Example: csv
   separator : Separator used for file. Only valid for csv files.
 Layout Field Definition Parameters:
   name     : Field name. It will become the column name in db
              Example: balance
   type     : text/integer/real/date/time. 'text' text field like "hello".
'integer' for integers like 10. 'real' for decimals like 5.6
              Example: real
   format   : If type is date/time then we need to provide this. Use date format
from 
   http://pubs.opengroup.org/onlinepubs/009695399/functions/strftime.html
              Example: %m%d%Y for 11022014 which is 2014-02-11
              Example: %d/%m/%Y-%H-%M-%S for 02/11/2014-21-56-53 which is 2014-02-11T21:56:53
   pivotYear: If type is date/time then we can provide pivot year. Refer to 
   http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormatter.html#withPivotYear(int)
              Example: 2000
   missingValue: If this is the value then it will be replaced with null in db
              Example: 2000

Example of csv layout:
{
    "name" : "in",
    "type" : "csv",
    "separator" : "   ",
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
