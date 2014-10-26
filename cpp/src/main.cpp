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

#include "sqlite3.h"
#ifndef DISABLE_SQL_CODE
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
    string name;
    Field *fieldList;
    int fieldListLen;
    char type;

    Layout(const int size) : type('P') {
        fieldList = new Field[size];
        fieldListLen = size;
    }

    ~Layout() {
        delete[] fieldList;
    }

    friend ostream&  operator<<(ostream &outStream, Layout &layout) {
        outStream<<"[ "<<"name="<<layout.name<<", type="<<layout.type<<", fieldList=";
        for (int i = 0; i<layout.fieldListLen; i++) {
            outStream<<layout.fieldList[i]<<", ";
        }
        outStream<<" ]";
        return outStream;
    }
};

struct DelimFileData {
    char separator;
};

struct Data {
    char *str;
    int len;
    int actualLen;
    int startIdx;
    int endIdx;
};

void printStr(char *str, int start, int len) {
    cout<<"'";
    for(int i=start; i<start + len; i++) 
        cout<<*(str+i);
    cout<<"'"<<endl;
}

int parseDelimRecord(Layout &layout, void *pInfo, char *lineStr, sqlite3_stmt *sqlStmt) {
    DelimFileData *pDelimInfo = (DelimFileData *)pInfo;
    //char separator = pDelimInfo->separator;
    char separator = '\t';
#ifndef DISABLE_SQL_CODE
    int rc;
#endif
    int index = 0;
    int fieldListCount = layout.fieldListLen;
        for (int fieldCounter=0; fieldCounter<fieldListCount; fieldCounter++) {
            Field &field = layout.fieldList[fieldCounter];
            int fieldLength = 0;
            int ti = index;
            int ch = 0;
            ch = *(lineStr + index);
            while(!(ch == separator || ch == 0 || ch == '\n' || ch == '\r')) {
                fieldLength++;
                ti++;
                ch = *(lineStr + ti);
            }
            for(;ti>=index && (ch = *(lineStr + ti)) == ' ';ti--) {
            }
            
            field.endOffSet = index + fieldLength;
            int start = -1;
            int end = -1;
            int len = -1;

            ch = 0;
            if(field.type == 'T') {
                // cout<<"index="<<index<<" endOffset="<<field.endOffSet<<endl;
                for(int t=index; t<field.endOffSet; t++) {
                    ch = *(lineStr + t);
                    // cout<<"char="<<*(lineStr + t)<<endl;
                    if(start == -1 && ch != ' ' && ch != '\n' && ch != '\r') {
                        start = t;
                        // cout<<"setting start="<<start<<endl;
                    } else if(start > -1 && end == -1 && (ch == '\n' || ch == '\r')) {
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
                    if(start == -1 && ch != ' ' && ch != '\n' && ch != '\r' && ( ch == '-' || (ch > '0' && ch <= '9'))) {
                        start = t;
                        // cout<<"setting start="<<start<<endl;
                    } else if(start > -1 && end == -1 && (ch == '\n' || ch == '\r')) {
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
            index += fieldLength + 1;
        }
     return 0;
//   return pData;
}
int parseFixedLenRecord(Layout &layout, char *lineStr, sqlite3_stmt *sqlStmt) {
/*
    int fieldListCount = pLay->fieldListLen;
    Data *pData = new Data[fieldListCount];
    int index = 0;
    char *strIdx = str;
    for (int i=0; i<fieldListCount; i++) {

        Field &field = pLay->fieldList[i];
        int fieldLength = field.length;
        char * localStrStartIdx = strIdx;
        char * localStrEndIdx = strIdx + fieldLength - 1;
        int fieldConsumeLen = 0;

        if(field.trim == 'L' || field.trim == 'B' ) {
            while(*localStrStartIdx == ' ' && localStrStartIdx <= localStrEndIdx) {
                localStrStartIdx++;
            }
        }
        if(localStrStartIdx < localStrEndIdx && (field.trim == 'R' || field.trim == 'B' )) {
            while(*localStrEndIdx == ' ' && localStrEndIdx >= localStrStartIdx) {
                localStrEndIdx--;
            }
        }
        pData[i].str = localStrStartIdx;
        pData[i].len = field.length;
        pData[i].actualLen = localStrEndIdx - localStrStartIdx + 1;
        strIdx += field.length;
        index += field.length;
    }
*/

#ifndef DISABLE_SQL_CODE
    int rc;
#endif
    int index = 0;
    int fieldListCount = layout.fieldListLen;
        for (int fieldCounter=0; fieldCounter<fieldListCount; fieldCounter++) {
            Field &field = layout.fieldList[fieldCounter];
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
        }
     return 0;
//   return pData;
}

int checkIfTableExist(sqlite3 *db, string tableNameToCheck) {
    int doesTableExist = 0;

#ifndef DISABLE_SQL_CODE
    int rc;
    sqlite3_stmt *sqlStmt;
    string queryStr = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + tableNameToCheck + "'";
    rc = sqlite3_prepare_v2(db, queryStr.c_str(), queryStr.length(), &sqlStmt, NULL);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: preparing failed\n", rc);
        return 2;
    }

    rc = sqlite3_step(sqlStmt);
    if(rc != SQLITE_DONE && rc != SQLITE_ROW) {
        fprintf(stderr, "SQL error %d: reset failed\n", rc);
        return 2;
    }

    int count = sqlite3_column_int(sqlStmt, 0);
    if(count > 0) {
        doesTableExist = 1;
    }

    rc = sqlite3_finalize(sqlStmt);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: finalize failed\n", rc);
        return 2;
    }
#endif
    return doesTableExist;
}

int createTable(Layout &layout, sqlite3 *db, string tableNameToCreate) {
    return 0;
}

Layout * parsetLayout(string layoutFileName, string argTableName) {
    // parsing json
    string json_str = get_file_contents(layoutFileName);
    cJSON *root = cJSON_Parse(json_str.c_str());
    /* TODO WHEN WE RE-ENABLE FLAT FILE PARSING
    cJSON *jFileType = cJSON_GetObjectItem(root,"fileType");
    char fileType = 'D';

    if( jFileType != NULL ) {
        if(strcmp("csv", jFileType->valuestring)) {
            fileType = 'D';
        } else if(strcmp("flat", jFileType->valuestring)) {
            fileType = 'F';
        } else {
            cout<<"Invalid file type specified:"<<jFileType->valuestring<<endl;
            return 1;
        }
    }
    if(isDebug) cout<<"fileType="<<fileType<<endl;
    */
    cJSON *fieldList = cJSON_GetObjectItem(root,"fieldList");

    // building create table query
    // TODO  extract table name from file name or something
    int recordLen = 0;
    int fieldCounter = 0;
    int fieldListCount = cJSON_GetArraySize(fieldList);

    // TODO If we need to give default name. In future may be
    // string tableName = "t";
    string tableName;

    cJSON *jLayoutName = cJSON_GetObjectItem(root, "name");
    string layoutTableName;

    if(jLayoutName != NULL ) {
        layoutTableName = jLayoutName->valuestring;
    }

    if(argTableName.length() > 0 ) {
        tableName = argTableName;
    } else if(layoutTableName.length() > 0) {
        tableName = layoutTableName;
    }

    if(tableName.length() <=0) {
        throw "Table name not specified either in layout or as argument";
    }


    Layout *pLayout = new Layout(fieldListCount);
    pLayout->name = tableName;
    for (int i = 0; i < fieldListCount; i++) {
        cJSON *jsonField = cJSON_GetArrayItem(fieldList, i);

        cJSON *jsonFieldName = cJSON_GetObjectItem(jsonField, "name");
        if (jsonFieldName == NULL) {
            throw "Name cannot be empty";
        }

        if (cJSON_GetObjectItem(jsonField, "length") == NULL) {
            string e = string("Name: ");
            e += jsonFieldName->valuestring;
            e += "\nLength cannot be empty";
            throw e;
        }

        pLayout->fieldList[i].name =
                cJSON_GetObjectItem(jsonField, "name")->valuestring;
        char * type = (char *) "text";
        if (cJSON_GetObjectItem(jsonField, "type") != NULL)
            type = cJSON_GetObjectItem(jsonField, "type")->valuestring;

        if (strcmp(type, "text") == 0) {
            pLayout->fieldList[i].type = 'T';
        }

        if (strcmp(type, "integer") == 0) {
            pLayout->fieldList[i].type = 'I';
        }

        if (strcmp(type, "real") == 0) {
            pLayout->fieldList[i].type = 'R';
        }

        pLayout->fieldList[i].length =
                cJSON_GetObjectItem(jsonField, "length")->valueint;
        if (i > 0)
            pLayout->fieldList[i].endOffSet =
                    pLayout->fieldList[i - 1].endOffSet
                            + pLayout->fieldList[i].length;
        else
            pLayout->fieldList[i].endOffSet = pLayout->fieldList[i].length;
        recordLen += pLayout->fieldList[i].length;
        fieldCounter++;
    }

    cJSON_Delete(root);

    return pLayout;
}

string getCreateTableQuery(Layout& layout) {
    // building create table query
    int recordLen = 0;
    int fieldCounter = 0;

    string createTableQry;
    createTableQry = "CREATE TABLE " + layout.name + " ( id INTEGER PRIMARY KEY, ";
    for (int i=0; i<layout.fieldListLen; i++) {
        createTableQry += layout.fieldList[i].name;

        if(layout.fieldList[i].type == 'T') {
            createTableQry += " TEXT";
        }

        if(layout.fieldList[i].type == 'I') {
            createTableQry += " INTEGER";
        }

        if(layout.fieldList[i].type == 'R') {
            createTableQry += " REAL";
        }

        if(fieldCounter < layout.fieldListLen - 1) {
            createTableQry += ", ";
        }

        if(i > 0)
            layout.fieldList[i].endOffSet = layout.fieldList[i-1].endOffSet + layout.fieldList[i].length;
        else
            layout.fieldList[i].endOffSet = layout.fieldList[i].length;
        recordLen += layout.fieldList[i].length;
        fieldCounter++;
    }
    createTableQry += ");";
    return createTableQry;
}

string getInsertQuery(Layout& layout) {
    // building insert table query
    int recordLen = 0;
    int fieldCounter = 0;

    string insertBindQry;
    insertBindQry = "INSERT INTO " + layout.name + " VALUES ( NULL, ";
    for (int i=0; i<layout.fieldListLen; i++) {

        insertBindQry += "?";
        if(fieldCounter < layout.fieldListLen - 1) {
            insertBindQry += ", ";
        }

        if(i > 0)
            layout.fieldList[i].endOffSet = layout.fieldList[i-1].endOffSet + layout.fieldList[i].length;
        else
            layout.fieldList[i].endOffSet = layout.fieldList[i].length;
        recordLen += layout.fieldList[i].length;
        fieldCounter++;
    }
    insertBindQry += ");";
    return insertBindQry;
}

int createIndex(sqlite3 *db, const Layout& layout, string indexOnFieldName) {
#ifndef DISABLE_SQL_CODE
    int rc;
    char *zErrMsg = 0;

    string indexQry = "CREATE INDEX '" + layout.name + "_" + indexOnFieldName +"_index' ON '"
        + layout.name + "' ('" + layout.fieldList[0].name + "');";
    cout<<"indexQry="<<indexQry<<endl;
    rc = sqlite3_exec(db, indexQry.c_str(), NULL, NULL, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }

    return 0;
#endif
}

int main(int argc, char **argv) {
    time_t  cStartTime;
    time_t  cInsertStartTime;

    OptionParser parser = OptionParser() .description("Converts fixed length files to sqlite database");

    parser.add_option("-t").dest("t")
        .metavar("<table_name>")
        .help("Table to be created");
    parser.add_option("-i").dest("i")
        .metavar("<input_file>")
        .help("Input fixed length text file containing records to be ported to sqlite data base.");
    parser.add_option("-o").dest("o")
        .metavar("<output_file>")
        .help("Output file name for the sqlite data base.");
    parser.add_option("-l").dest("l")
        .metavar("<layout_file>")
        .help("Layout file containing field definitions. These \
                field definitions will be used for parsing fixed length record");
    parser.add_option("-c").dest("c")
        .metavar("<N>").set_default("10")
        .help("Number of records after which commit operation is called on the data base");
    parser.add_option("-d").dest("d")
        .set_default("0").action("store_true")
        .help("Delete if table already exists");
    parser.add_option("-a").dest("a")
        .set_default("0").action("store_true")
        .help("Append to existing table if it does exist");
    parser.add_option("-x").dest("x")
        .set_default("0").action("store_true")
        .help("Debug mode");
    optparse::Values options = parser.parse_args(argc, argv);

    string layoutFileName = options["l"];
    string inputFileName = options["i"];
    string outputFileName = options["o"];
    string argTableName = options["t"];
    long commitAfter = atol(options["c"].c_str());
    bool isDebug = atoi(options["x"].c_str()) == 1? true : false;
    bool isAppendMode = atoi(options["a"].c_str()) == 1? true : false;
    bool isDeleteMode = atoi(options["d"].c_str()) == 1? true : false;

    if (layoutFileName.length() == 0) {
        cout << "Layout file name cannot be empty" << endl;
        parser.print_help();
        return 1;
    }

    if (isAppendMode && isDeleteMode) {
        cout << "Append mode and delete mode cannot co-exist" << endl;
        parser.print_help();
        return 1;
    }

    cStartTime = time(NULL);
    if (isDebug) {
        cout << "layoutFileName=" << layoutFileName << endl;
        cout << "inputFileName=" << inputFileName << endl;
        cout << "outputFileName=" << outputFileName << endl;
        cout << "commitAfter=" << commitAfter << endl;
        cout << "isDebug=" << isDebug << endl;
    }

    Layout *pLayout;
    try {
        pLayout = parsetLayout(layoutFileName, argTableName);
    } catch (string& e) {
        cout << e << endl;
        parser.print_help();
        return 1;
    }

    if(isDebug) cout<<*pLayout<<endl;

    istream *inStream;
    ifstream inFile;

    if(inputFileName.empty() || inputFileName.compare("-") == 0) {
        // Disable the sync to improve performance.
        // NOTE: Make sure we don't use C like I/O going forward.
        cin.sync_with_stdio(false);
        inStream = &cin;
        if(isDebug) cout<<"Reading from stdin"<<endl;
    } else {
        if(isDebug) cout<<"Reading from "<<inputFileName<<endl;
        inFile.open(inputFileName.c_str());
        inStream = &inFile;
    }

    string dbFileName;
    if(!outputFileName.empty()) dbFileName = outputFileName;
    else if(!inputFileName.empty() && inputFileName != "-") dbFileName = inputFileName + ".db";
    else {
        cerr<<"No output file name can be determined"<<endl;
        return -1;
    }

    //remove(dbFileName.c_str());

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

    int doesTableExist = checkIfTableExist(db, pLayout->name);
    if(isDebug) cout<<"doesTableExist="<<doesTableExist<<endl;
    if(doesTableExist == 2) {
        return 1;
    } 
    
    if(doesTableExist == 0) {
        
    }

    rc = sqlite3_exec(db, getCreateTableQuery(*pLayout).c_str(), NULL, 0,
            &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Create query=%s\n",
                getCreateTableQuery(*pLayout).c_str());
        fprintf(stderr, "SQL error for create table: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    rc = sqlite3_exec(db, "PRAGMA synchronous=OFF;", NULL, 0, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    rc = sqlite3_exec(db, "PRAGMA journal_mode = WAL;", NULL, 0, &zErrMsg);
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

#endif
    sqlite3_stmt *sqlStmt;
#ifndef DISABLE_SQL_CODE
    string insertBindQry = getInsertQuery(*pLayout);
    rc = sqlite3_prepare_v2(db, insertBindQry.c_str(), insertBindQry.length(),
            &sqlStmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Prepare query=%s\n", insertBindQry.c_str());
        fprintf(stderr, "SQL error %d: preparing failed\n", rc);
        return 1;
    }
#endif

    string line;

    long recordCounter = 0;
    int index = 0;
    int fieldCounter = 0;
    char *lineStr = NULL;
//    Data *pData = new Data[fieldListCount];
    while(getline(*inStream, line)) {

        /*
        // if(isDebug) cout<<"line="<<line<<endl;
        // string insertQry;
        // insertQry = "INSERT INTO " + tableName + " VALUES (";
        */
        index = 0;
        lineStr = (char *)line.c_str();
        // cout<<lineStr<<endl;
        //parseFixedLenRecord(layout, lineStr, sqlStmt);
        parseDelimRecord(*pLayout, NULL, lineStr, sqlStmt);

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

    rc = sqlite3_finalize(sqlStmt);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: reset failed\n", rc);
    }

    time_t  cIndexStartTime;
    cIndexStartTime = time(NULL);
    createIndex(db, *pLayout, pLayout->fieldList[0].name);
    long indexTimeInSecs = time(NULL) - cIndexStartTime;
    printf("Indexed %ld records in %ld seconds with %.2f opts/sec \n", recordCounter, indexTimeInSecs, ((double)recordCounter/indexTimeInSecs));


    rc = sqlite3_exec(db, "PRAGMA journal_mode = DELETE;", NULL, 0, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    sqlite3_close(db);
#endif
    long timeInSecs = time(NULL) - cStartTime;
    // printf("Imported %ld records in %4.2f seconds with %.2f opts/sec \n", recordCounter, timeInSecs, recordCounter/timeInSecs);
    printf("Imported %ld records in %ld seconds with %.2f opts/sec \n", recordCounter, timeInSecs, ((double)recordCounter/timeInSecs));
    inFile.close();
}

