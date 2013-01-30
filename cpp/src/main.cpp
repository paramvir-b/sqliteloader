/*
 * The code is available under LGPL3 license. License can be found at:
 * http://www.gnu.org/licenses/lgpl-3.0.txt 
 * The code will soon be available on github at
 * https://github.com/paramvir-b
 */

// #define DISABLE_SQL_CODE

#include <iostream>
#include <fstream>
#include <string.h>
#include <stdlib.h>

#ifndef DISABLE_SQL_CODE
#include "sqlite3.h"
#endif

#include "cJSON.h"
#include "OptionParser.h"

using namespace std;
using optparse::OptionParser;

#define ENABLE_SQL_CHECKS

string get_file_contents(string filename)
{
    ifstream in;
    in.open(filename.c_str(), ios::in | ios::binary);
    if (in.is_open())
    {
        string contents;
        in.seekg(0, ios::end);
        contents.resize(in.tellg());
        in.seekg(0, ios::beg);
        in.read(&contents[0], contents.size());
        in.close();
        return(contents);
    }

    return string("");
}

/*
   string trim(const string& str, const string& whitespace = " \t")
   {
   const int strBegin = str.find_first_not_of(whitespace);
   if (strBegin == string::npos)
   return ""; // no content

   const int strEnd = str.find_last_not_of(whitespace);
   const int strRange = strEnd - strBegin + 1;

   return str.substr(strBegin, strRange);
   }

   string & replaceAll(std::string& str, const std::string& from, const std::string& to) {
   if(from.empty())
   return str;
   size_t start_pos = 0;
   while((start_pos = str.find(from, start_pos)) != std::string::npos) {
   str.replace(start_pos, from.length(), to);
   start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
   }

   return str;
   }
   */
struct Layout;

struct Field {
    Layout *layoutPtr;
    string name;
    char type;
    int length;
    int endOffSet;
    Field() : type('T'), length(0), endOffSet(0) {
    }

    friend ostream&  operator<<(ostream &outStream, Field &field) {
        cout<<"[ name="<<field.name<<", type="<<field.type<<", length="<<field.length<<", endOffSet="<<field.endOffSet<<" ]";
        return outStream;
    }
};

struct Layout {
    Field *fieldList;
    int fieldListLen;
    char type;
    Layout(int size) : type('P') {
        fieldList = new Field[size];
        fieldListLen = size;
    }

    ~Layout() {
        delete[] fieldList;
    }

    friend ostream&  operator<<(ostream &outStream, Layout &layout) {
        outStream<<"[ type="<<layout.type<<", fieldList=";
        for (int i = 0; i<layout.fieldListLen; i++) {
            outStream<<layout.fieldList[i]<<", ";
        }
        outStream<<" ]";
        return outStream;
    }
};

void printStr(char *str, int start, int len) {
    cout<<"'";
    for(int i=start; i<start + len; i++) 
        cout<<*(str+i);
    cout<<"'"<<endl;
}

int main(int argc, char **argv) {
    time_t  cStartTime;
    time_t  cInsertStartTime;

    OptionParser parser = OptionParser() .description("Converts fixed length files to sqlite database");

    parser.add_option("-i").dest("i")
        .metavar("<input_file>")
        .help("Input fixed length text file containing records to be ported to sqlite data base.");
    parser.add_option("-l").dest("l")
        .metavar("<layout_file>")
        .help("Layout file containing field definitions. These \
                field definitions will be used for parsing fixed length record");
    parser.add_option("-c").dest("c")
        .metavar("<N>").set_default("10")
        .help("Number of records after which commit operation is called on the data base");
    parser.add_option("-d").dest("d")
        .set_default("0").action("store_true")
        .help("Number of records after which commit operation is called on the data base");
    optparse::Values options = parser.parse_args(argc, argv);

    string layoutFileName = options["l"];
    string inputFileName = options["i"];
    long commitAfter = atol(options["c"].c_str());
    bool isDebug = atoi(options["d"].c_str()) == 1? true : false;

    cStartTime = time(NULL);
    if(isDebug) {
        cout<<"layoutFileName="<<layoutFileName<<endl;
        cout<<"inputFileName="<<inputFileName<<endl;
        cout<<"commitAfter="<<commitAfter<<endl;
        cout<<"isDebug="<<isDebug<<endl;
    }

    // parsing json
    string json_str = get_file_contents(layoutFileName);
    cJSON *root = cJSON_Parse(json_str.c_str());
    if(isDebug) cout<<cJSON_Print(root)<<endl;

    cJSON *fieldList = cJSON_GetObjectItem(root,"fieldList");
    if(isDebug) cout<<cJSON_Print(fieldList)<<endl;

    // building create table query
    // TODO  extract table name from file name or something
    string tableName = "t";
    int recordLen = 0;
    int fieldCounter = 0;
    int fieldListCount = cJSON_GetArraySize(fieldList);
    Layout layout(fieldListCount);
    string createTableQry;
    createTableQry = "CREATE TABLE " + tableName + " ( id INTEGER PRIMARY KEY, ";
    string insertBindQry;
    insertBindQry = "INSERT INTO " + tableName + " VALUES ( NULL, ";
    for (int i=0; i<fieldListCount; i++) {
        cJSON *jsonField=cJSON_GetArrayItem(fieldList,i);
        if(isDebug) cout<<cJSON_Print(jsonField)<<endl;

        if(cJSON_GetObjectItem(jsonField, "name") == NULL) {
            if(isDebug) cout<<"Name cannot be empty"<<endl;
            return 1;
        }

        if(cJSON_GetObjectItem(jsonField, "length") == NULL) {
            if(isDebug) cout<<"Length cannot be empty"<<endl;
            return 1;
        }

        layout.fieldList[i].name = cJSON_GetObjectItem(jsonField, "name")->valuestring;
        createTableQry += layout.fieldList[i].name;
        char * type = (char *)"text";
        if(cJSON_GetObjectItem(jsonField, "type") != NULL)
            type = cJSON_GetObjectItem(jsonField, "type")->valuestring;

        if(strcmp(type, "text") == 0) {
            createTableQry += " TEXT";
            layout.fieldList[i].type = 'T';
        }

        if(strcmp(type, "integer") == 0) {
            createTableQry += " INTEGER";
            layout.fieldList[i].type = 'I';
        }

        if(strcmp(type, "real") == 0) {
            createTableQry += " REAL";
            layout.fieldList[i].type = 'R';
        }

        insertBindQry += "?";
        if(fieldCounter < fieldListCount - 1) {
            insertBindQry += ", ";
            createTableQry += ", ";
        }

        layout.fieldList[i].length = cJSON_GetObjectItem(jsonField, "length")->valueint;
        if(i > 0)
            layout.fieldList[i].endOffSet = layout.fieldList[i-1].endOffSet + layout.fieldList[i].length;
        else
            layout.fieldList[i].endOffSet = layout.fieldList[i].length;
        recordLen += layout.fieldList[i].length;
        fieldCounter++;
    }
    insertBindQry += ");";
    createTableQry += ");";
    if(isDebug) cout<<"createTableQry="<<createTableQry<<endl;
    if(isDebug) cout<<"insertBindQry="<<insertBindQry<<endl;

    if(isDebug) cout<<layout<<endl;

    ifstream inFile;
    inFile.open(inputFileName.c_str());

    string dbFileName = inputFileName + ".db";
    remove(dbFileName.c_str());

#ifndef DISABLE_SQL_CODE
    int rc;
    sqlite3 *db;
    char *zErrMsg = 0;

    rc = sqlite3_open(dbFileName.c_str(), &db);
    if(rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return(1);
    }

    rc = sqlite3_exec(db, createTableQry.c_str(), NULL, 0, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    rc = sqlite3_exec(db, "PRAGMA synchronous=OFF;", NULL, 0, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    rc = sqlite3_exec(db, "PRAGMA journal_mode = MEMORY;", NULL, 0, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    cInsertStartTime = time(NULL);

    rc = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, 0, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    sqlite3_stmt *sqlStmt;
    rc = sqlite3_prepare_v2(db, insertBindQry.c_str(), insertBindQry.length(), &sqlStmt, NULL);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: preparing failed\n", rc);
        return 1;
    }
#endif

    string line;

    long recordCounter = 0;
    int index = 0;
    fieldCounter = 0;
    char *lineStr = NULL;
    while(getline(inFile, line)) {

        /*
        // if(isDebug) cout<<"line="<<line<<endl;
        // string insertQry;
        // insertQry = "INSERT INTO " + tableName + " VALUES (";
        */
        index = 0;
        fieldCounter = 0;
        lineStr = (char *)line.c_str();
        // cout<<lineStr<<endl;
        for (int i=0; i<fieldListCount; i++) {
            Field &field = layout.fieldList[i];
            int fieldLength = field.length;
            int start = -1;
            int end = -1;
            int len = -1;
            int ch = 0;

            if(field.type == 'T') {
                // cout<<"index="<<index<<" endOffset="<<field.endOffSet<<endl;
                for(int t=index; t<field.endOffSet; t++) {
                    ch = *(lineStr + t);
                    // cout<<"char="<<*(lineStr + t)<<endl;
                    if(start == -1 && ch != ' ' && ch != '\n' && ch != '\r') {
                        start = t;
                        // cout<<"setting start="<<start<<endl;
                    } else if(start > -1 && end == -1 && (ch == ' ' || ch == '\n' || ch == '\r')) {
                        end = t;
                        // cout<<"setting end="<<end<<endl;
                    }
                }

                // cout<<"out start="<<start<<" end="<<end<<endl;
                if(start == -1 && end == -1) {
                    // empty put NULL
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                    sqlite3_bind_null(sqlStmt, fieldCounter + 1);
#    else
                    rc = sqlite3_bind_null(sqlStmt, fieldCounter + 1);
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: text null binding failed\n", rc);
                        return 1;
                    }
#    endif
#endif
                } else if(start > -1) {
                    if(end == -1)
                        end = field.endOffSet;
                    len = end - start;
                    // Found something to bind
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                    sqlite3_bind_text(sqlStmt, fieldCounter + 1, lineStr + start, len, SQLITE_TRANSIENT);
#    else
                    rc = sqlite3_bind_text(sqlStmt, fieldCounter + 1, lineStr + start, len, SQLITE_TRANSIENT);
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                        return 1;
                    }
#    endif
#endif
                }
                // cout<<"start="<<start<<" end="<<end<<" len="<<len<<endl;
                // printStr(lineStr, start, len);
            } else if(field.type == 'I' || field.type == 'R') {
                for(int t=index; t<field.endOffSet; t++) {
                    ch = *(lineStr + t);
                    // cout<<"char="<<*(lineStr + t)<<endl;
                    if(start == -1 && ch != ' ' && ch != '\n' && ch != '\r' && ch > '0' && ch <= '9') {
                        start = t;
                        // cout<<"setting start="<<start<<endl;
                    } else if(start > -1 && end == -1 && (ch == ' ' || ch == '\n' || ch == '\r')) {
                        end = t;
                        // cout<<"setting end="<<end<<endl;
                    }
                }
                // cout<<"out start="<<start<<" end="<<end<<endl;
                if(start == -1 && end == -1) {
                    // empty put NULL
                    // cout<<"empty num"<<endl;
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                    sqlite3_bind_null(sqlStmt, fieldCounter + 1);
#    else
                    rc = sqlite3_bind_null(sqlStmt, fieldCounter + 1);
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: text null binding failed\n", rc);
                        return 1;
                    }
#    endif
#endif
                } else if(start > -1) {
                    if(end == -1)
                        end = field.endOffSet;
                    // Found something to bind
                    len = end - start;
#ifndef DISABLE_SQL_CODE
                    // We are doing text binding as Sqlite can do the conversion based on the
                    // infinity we specified in the schema. Also it does that without any loss to
                    // real numbers
#    ifndef ENABLE_SQL_CHECKS
                    sqlite3_bind_text(sqlStmt, fieldCounter + 1, lineStr + start, len, SQLITE_TRANSIENT);
#    else
                    rc = sqlite3_bind_text(sqlStmt, fieldCounter + 1, lineStr + start, len, SQLITE_TRANSIENT);
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                        return 1;
                    }
#    endif
#endif
                }
            }

            index += fieldLength;
            fieldCounter++;
        }

#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
        sqlite3_step(sqlStmt);
        sqlite3_reset(sqlStmt);
#    else
        rc = sqlite3_step(sqlStmt);
        if(rc == SQLITE_DONE) {
            rc = sqlite3_reset(sqlStmt);
            if(rc != SQLITE_OK) {
                fprintf(stderr, "SQL error %d: reset failed\n", rc);
                return 1;
            }
        } else {
            fprintf(stderr, "SQL error %d: step failed\n", rc);
            return 1;
        }
#    endif
#endif
        recordCounter++;
    }

#ifndef DISABLE_SQL_CODE
    rc = sqlite3_exec(db, "END TRANSACTION;", NULL, 0, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    long insertTimeInSecs = time(NULL) - cInsertStartTime;
    printf("Inserted %ld records in %ld seconds with %.2f opts/sec \n", recordCounter, insertTimeInSecs, ((double)recordCounter/insertTimeInSecs));

    time_t  cIndexStartTime;
    string indexQry = "CREATE INDEX '" + tableName + "_" + layout.fieldList[0].name +"_index' ON '"
        + tableName + "' ('" + layout.fieldList[0].name + "');";
    if(isDebug) cout<<"indexQry="<<indexQry<<endl;
    cIndexStartTime = time(NULL);
    rc = sqlite3_exec(db, indexQry.c_str(), NULL, NULL, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    long indexTimeInSecs = time(NULL) - cIndexStartTime;
    printf("Indexed %ld records in %ld seconds with %.2f opts/sec \n", recordCounter, indexTimeInSecs, ((double)recordCounter/indexTimeInSecs));

    rc = sqlite3_finalize(sqlStmt);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: reset failed\n", rc);
    }

    sqlite3_close(db);
#endif
    long timeInSecs = time(NULL) - cStartTime;
    // printf("Imported %ld records in %4.2f seconds with %.2f opts/sec \n", recordCounter, timeInSecs, recordCounter/timeInSecs);
    printf("Imported %ld records in %ld seconds with %.2f opts/sec \n", recordCounter, timeInSecs, ((double)recordCounter/timeInSecs));
    inFile.close();
    cJSON_Delete(root);
}

