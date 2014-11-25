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
#include <time.h>

#include "sqlite3.h"
#ifndef DISABLE_SQL_CODE
#endif

#include "cJSON.h"
#include "OptionParser.h"

using namespace std;
using optparse::OptionParser;

#define ENABLE_SQL_CHECKS

string get_file_contents(string filename) {
    ifstream in;
    in.open(filename.c_str(), ios::in | ios::binary);
    if (in.is_open()) {
        string contents;
        in.seekg(0, ios::end);
        contents.resize(in.tellg());
        in.seekg(0, ios::beg);
        in.read(&contents[0], contents.size());
        in.close();
        return (contents);
    } else {
        throw string("Unable to open file: " + filename);
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
    string format;
    int pivotYear;
    string *missingValue;
    int endOffSet;
    Field() :
            type('S'), length(0), format(""), pivotYear(-1), missingValue(NULL), endOffSet(
                    0) {
    }

    friend ostream& operator<<(ostream &outStream, Field &field) {
        cout << "[ name=" << field.name << ", type=" << field.type
                << ", length=" << field.length << ", format=" << field.format
                << ", pivotYear=" << field.pivotYear << ", missingValue="
                << (field.missingValue == NULL ?
                        "NULL" : field.missingValue->c_str()) << ", endOffSet="
                << field.endOffSet << " ]";
        return outStream;
    }
};

struct Layout {
    string name;
    Field *fieldList;
    int fieldListLen;
    char type;
    char separator;

    Layout(const int size) :
            type('D'), separator('\t') {
        fieldList = new Field[size];
        fieldListLen = size;
    }

    ~Layout() {
        delete[] fieldList;
    }

    friend ostream& operator<<(ostream &outStream, Layout &layout) {
        outStream << "[ " << "name=" << layout.name << ", type=" << layout.type
                << ", fieldList=";
        for (int i = 0; i < layout.fieldListLen; i++) {
            outStream << layout.fieldList[i] << ", ";
        }
        outStream << " ]";
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
    cout << "'";
    for (int i = start; i < start + len; i++)
        cout << *(str + i);
    cout << "'" << endl;
}

int getLastTwoDigits(int num) {
    int d1 = num % 10;
    int d2 = (num / 10) % 10;
    return d2 * 10 + d1;
}

void fixYear(int pivotYear, struct tm *ptm) {
    int twoDigitYear = getLastTwoDigits(ptm->tm_year + 1900);
    int startPY = pivotYear - 50;

    if (twoDigitYear < 50) {
        ptm->tm_year = pivotYear + twoDigitYear - 1900;
    } else if (twoDigitYear >= 50) {
        ptm->tm_year = startPY + (twoDigitYear - 50) - 1900;
    }
}

int parseDelimRecord(Layout &layout, void *pInfo, char *lineStr, int lineStrLen,
        sqlite3_stmt *sqlStmt) {
    static char datestring[19];
    DelimFileData *pDelimInfo = (DelimFileData *) pInfo;
//char separator = pDelimInfo->separator;
    char separator = layout.separator;
#ifndef DISABLE_SQL_CODE
    int rc;
#endif
    struct tm tm;

    int indexInLine = 0;
    int fieldListCount = layout.fieldListLen;
    for (int fieldCounter = 0; fieldCounter < fieldListCount; fieldCounter++) {
        Field &field = layout.fieldList[fieldCounter];
        char *fieldStart = lineStr + indexInLine;
        int fieldLength = 0;
        int ti = 0;
        int nonSpaceStartIndex = -1;
        int nonSpaceEndIndex = -1;
        int ch = 0;
        ch = *fieldStart;
        while (!(ch == separator || ch == 0)) {
            if (ch != ' ' && nonSpaceStartIndex == -1) {
                nonSpaceStartIndex = ti;
            }
            fieldLength++;
            ti++;
            ch = *(fieldStart + ti);
            if (ch == separator) {
                *(fieldStart + ti) = 0;
            }
        }

        for (; ti >= 0 && (ch = *(fieldStart + ti - 1)) == ' '; ti--) {
        }

        nonSpaceEndIndex = ti;

//        cout << field << endl;
//        cout << "value='" << fieldStart << "'" << endl;
//        cout << "nonSpaceStartIndex=" << nonSpaceStartIndex
//                << " nonSpaceEndIndex=" << nonSpaceEndIndex << endl;
//        cout << "fieldLength=" << fieldLength << endl;
//
////        *(fieldStart + nonSpaceEndIndex) = 0;
//        cout << "value='" << fieldStart + nonSpaceStartIndex << "' after"
//                << endl;

        if (field.missingValue != NULL
                && strcmp(fieldStart, field.missingValue->c_str()) == 0) {
            nonSpaceStartIndex = -1;
        }
        if (nonSpaceStartIndex == -1) {
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
            sqlite3_bind_null(sqlStmt, fieldCounter + 1);
#    else
            rc = sqlite3_bind_null(sqlStmt, fieldCounter + 1);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "SQL error %d: text null binding failed\n", rc);
                return 1;
            }
#    endif
#endif
            indexInLine += fieldLength + 1;
            continue;
        }

        if (field.type == 'S' || field.type == 'I' || field.type == 'R') {
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
            sqlite3_bind_text(sqlStmt, fieldCounter + 1,
                    fieldStart + nonSpaceStartIndex,
                    nonSpaceEndIndex - nonSpaceStartIndex,
                    SQLITE_TRANSIENT);
#    else
            rc = sqlite3_bind_text(sqlStmt, fieldCounter + 1,
                    fieldStart + nonSpaceStartIndex,
                    nonSpaceEndIndex - nonSpaceStartIndex,
                    SQLITE_TRANSIENT);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                return 1;
            }
#    endif
#endif
            indexInLine += fieldLength + 1;
            continue;
        }

        if (field.type == 'D') {
            if (strptime(fieldStart, field.format.c_str(), &tm) != NULL) {
                if (field.pivotYear != -1) {
                    fixYear(field.pivotYear, &tm);
                }
                strftime(datestring, 19, "%Y-%m-%d", &tm);
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                sqlite3_bind_text(sqlStmt, fieldCounter + 1,
                        datestring,
                        10,
                        SQLITE_TRANSIENT);
#    else
                rc = sqlite3_bind_text(sqlStmt, fieldCounter + 1, datestring,
                        10,
                        SQLITE_TRANSIENT);

                if (rc != SQLITE_OK) {
                    fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                    return 1;
                }
#    endif
#endif
            }
            indexInLine += fieldLength + 1;
            continue;
        }

        if (field.type == 'T') {
            if (strptime(fieldStart, field.format.c_str(), &tm) != NULL) {
                if (field.pivotYear != -1) {
                    fixYear(field.pivotYear, &tm);
                }
                strftime(datestring, 19, "%Y-%m-%dT%H:%M:%S", &tm);
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                sqlite3_bind_text(sqlStmt, fieldCounter + 1,
                        datestring,
                        19,
                        SQLITE_TRANSIENT);
#    else
                rc = sqlite3_bind_text(sqlStmt, fieldCounter + 1, datestring,
                        19,
                        SQLITE_TRANSIENT);

                if (rc != SQLITE_OK) {
                    fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                    return 1;
                }
#    endif
#endif
            }
        }

        indexInLine += fieldLength + 1;
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
    for (int fieldCounter = 0; fieldCounter < fieldListCount; fieldCounter++) {
        Field &field = layout.fieldList[fieldCounter];
        int fieldLength = field.length;
        int start = -1;
        int end = -1;
        int len = -1;
        int ch = 0;

        if (field.type == 'S') {
            // cout<<"index="<<index<<" endOffset="<<field.endOffSet<<endl;
            for (int t = index; t < field.endOffSet; t++) {
                ch = *(lineStr + t);
                // cout<<"char="<<*(lineStr + t)<<endl;
                if (start == -1 && ch != ' ' && ch != '\n' && ch != '\r') {
                    start = t;
                    // cout<<"setting start="<<start<<endl;
                } else if (start > -1 && end == -1
                        && (ch == ' ' || ch == '\n' || ch == '\r')) {
                    end = t;
                    // cout<<"setting end="<<end<<endl;
                }
            }

            // cout<<"out start="<<start<<" end="<<end<<endl;
            if (start == -1 && end == -1) {
                // empty put NULL
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                sqlite3_bind_null(sqlStmt, fieldCounter + 1);
#    else
                rc = sqlite3_bind_null(sqlStmt, fieldCounter + 1);
                if (rc != SQLITE_OK) {
                    fprintf(stderr, "SQL error %d: text null binding failed\n",
                            rc);
                    return 1;
                }
#    endif
#endif
            } else if (start > -1) {
                if (end == -1)
                    end = field.endOffSet;
                len = end - start;
                // Found something to bind
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                sqlite3_bind_text(sqlStmt, fieldCounter + 1, lineStr + start, len, SQLITE_TRANSIENT);
#    else
                rc = sqlite3_bind_text(sqlStmt, fieldCounter + 1,
                        lineStr + start, len, SQLITE_TRANSIENT);
                if (rc != SQLITE_OK) {
                    fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                    return 1;
                }
#    endif
#endif
            }
            // cout<<"start="<<start<<" end="<<end<<" len="<<len<<endl;
            // printStr(lineStr, start, len);
        } else if (field.type == 'I' || field.type == 'R') {
            for (int t = index; t < field.endOffSet; t++) {
                ch = *(lineStr + t);
                // cout<<"char="<<*(lineStr + t)<<endl;
                if (start == -1 && ch != ' ' && ch != '\n' && ch != '\r'
                        && ch > '0' && ch <= '9') {
                    start = t;
                    // cout<<"setting start="<<start<<endl;
                } else if (start > -1 && end == -1
                        && (ch == ' ' || ch == '\n' || ch == '\r')) {
                    end = t;
                    // cout<<"setting end="<<end<<endl;
                }
            }
            // cout<<"out start="<<start<<" end="<<end<<endl;
            if (start == -1 && end == -1) {
                // empty put NULL
                // cout<<"empty num"<<endl;
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                sqlite3_bind_null(sqlStmt, fieldCounter + 1);
#    else
                rc = sqlite3_bind_null(sqlStmt, fieldCounter + 1);
                if (rc != SQLITE_OK) {
                    fprintf(stderr, "SQL error %d: text null binding failed\n",
                            rc);
                    return 1;
                }
#    endif
#endif
            } else if (start > -1) {
                if (end == -1)
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
                rc = sqlite3_bind_text(sqlStmt, fieldCounter + 1,
                        lineStr + start, len, SQLITE_TRANSIENT);
                if (rc != SQLITE_OK) {
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

Layout * parseLayout(string layoutFileName, string argTableName) {
// parsing json
    string json_str = get_file_contents(layoutFileName);

    if (layoutFileName.length() <= 0) {
        throw string("Empty layout file layoutFileName=" + layoutFileName);
    }

    cJSON *root = cJSON_Parse(json_str.c_str());
    /* TODO WHEN WE RE-ENABLE FLAT FILE PARSING
     cJSON *jFileType = cJSON_GetObjectItem(root,"type");
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
    cJSON *fieldList = cJSON_GetObjectItem(root, "fieldList");

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

    if (jLayoutName != NULL) {
        layoutTableName = jLayoutName->valuestring;
    }

    if (argTableName.length() > 0) {
        tableName = argTableName;
    } else if (layoutTableName.length() > 0) {
        tableName = layoutTableName;
    }

    if (tableName.length() <= 0) {
        throw "Table name not specified either in layout or as argument";
    }

    Layout *pLayout = new Layout(fieldListCount);
    pLayout->name = tableName;

    if (pLayout->type == 'D') {
        cJSON *jSeparator = cJSON_GetObjectItem(root, "separator");
        if (jSeparator != NULL && strlen(jSeparator->valuestring) > 0) {
            pLayout->separator = jSeparator->valuestring[0];
        }
    }

    for (int i = 0; i < fieldListCount; i++) {
        cJSON *jsonField = cJSON_GetArrayItem(fieldList, i);

        cJSON *jsonFieldName = cJSON_GetObjectItem(jsonField, "name");
        if (jsonFieldName == NULL) {
            throw "Name cannot be empty";
        }

        cJSON *jsonMissingValue = cJSON_GetObjectItem(jsonField,
                "missingValue");
        if (jsonMissingValue != NULL) {
            pLayout->fieldList[i].missingValue = new string(
                    jsonMissingValue->valuestring);
        }

        if (pLayout->type == 'F') {
            // Check length if it is flat file
            if (cJSON_GetObjectItem(jsonField, "length") == NULL) {
                string e = string("Name: ");
                e += jsonFieldName->valuestring;
                e += "\nLength cannot be empty";
                throw e;
            }

            pLayout->fieldList[i].length = cJSON_GetObjectItem(jsonField,
                    "length")->valueint;
            if (i > 0)
                pLayout->fieldList[i].endOffSet =
                        pLayout->fieldList[i - 1].endOffSet
                                + pLayout->fieldList[i].length;
            else
                pLayout->fieldList[i].endOffSet = pLayout->fieldList[i].length;
            recordLen += pLayout->fieldList[i].length;
        }

        pLayout->fieldList[i].name = jsonFieldName->valuestring;
        char * type = (char *) "text";
        if (cJSON_GetObjectItem(jsonField, "type") != NULL)
            type = cJSON_GetObjectItem(jsonField, "type")->valuestring;

        if (strcmp(type, "text") == 0) {
            pLayout->fieldList[i].type = 'S';
        }

        if (strcmp(type, "integer") == 0) {
            pLayout->fieldList[i].type = 'I';
        }

        if (strcmp(type, "real") == 0) {
            pLayout->fieldList[i].type = 'R';
        }

        if (strcmp(type, "date") == 0) {
            pLayout->fieldList[i].type = 'D';
        }

        if (strcmp(type, "time") == 0) {
            pLayout->fieldList[i].type = 'T';
        }

        if (pLayout->fieldList[i].type == 'D'
                || pLayout->fieldList[i].type == 'T') {
            cJSON *jsonDateFormat = cJSON_GetObjectItem(jsonField, "format");
            if (jsonDateFormat == NULL) {
                string e = string("Name: ");
                e += jsonFieldName->valuestring;
                e += "\nFormat cannot be empty";
                throw e;
            }
            pLayout->fieldList[i].format = jsonDateFormat->valuestring;

            cJSON *jsonPivotYear = cJSON_GetObjectItem(jsonField, "pivotYear");
            if (jsonPivotYear != NULL) {
                pLayout->fieldList[i].pivotYear = jsonPivotYear->valueint;
            }

        }

        fieldCounter++;
    }

    cJSON_Delete(root);

    return pLayout;
}

string getCreateTableQuery(Layout & layout) {
// building create table query
    int recordLen = 0;
    int fieldCounter = 0;

    string createTableQry;
    createTableQry = "CREATE TABLE '" + layout.name + "' ( ";
    for (int i = 0; i < layout.fieldListLen; i++) {
        createTableQry += layout.fieldList[i].name;

        if (layout.fieldList[i].type == 'S' || layout.fieldList[i].type == 'D'
                || layout.fieldList[i].type == 'T') {
            createTableQry += " TEXT";
        }

        if (layout.fieldList[i].type == 'I') {
            createTableQry += " INTEGER";
        }

        if (layout.fieldList[i].type == 'R') {
            createTableQry += " REAL";
        }

        if (fieldCounter < layout.fieldListLen - 1) {
            createTableQry += ", ";
        }

        if (i > 0)
            layout.fieldList[i].endOffSet = layout.fieldList[i - 1].endOffSet
                    + layout.fieldList[i].length;
        else
            layout.fieldList[i].endOffSet = layout.fieldList[i].length;
        recordLen += layout.fieldList[i].length;
        fieldCounter++;
    }
    createTableQry += ");";
    return createTableQry;
}

string getInsertQuery(Layout & layout) {
// building insert table query
    int recordLen = 0;
    int fieldCounter = 0;

    string insertBindQry;
    insertBindQry = "INSERT INTO '" + layout.name + "' VALUES ( ";
    for (int i = 0; i < layout.fieldListLen; i++) {

        insertBindQry += "?";
        if (fieldCounter < layout.fieldListLen - 1) {
            insertBindQry += ", ";
        }

        if (i > 0)
            layout.fieldList[i].endOffSet = layout.fieldList[i - 1].endOffSet
                    + layout.fieldList[i].length;
        else
            layout.fieldList[i].endOffSet = layout.fieldList[i].length;
        recordLen += layout.fieldList[i].length;
        fieldCounter++;
    }
    insertBindQry += ");";
    return insertBindQry;
}

int checkIfTableExist(sqlite3 *db, string tableNameToCheck) {
    int doesTableExist = 0;

#ifndef DISABLE_SQL_CODE
    int rc;
    sqlite3_stmt *sqlStmt;
    string queryStr =
            "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='"
                    + tableNameToCheck + "'";
    rc = sqlite3_prepare_v2(db, queryStr.c_str(), queryStr.length(), &sqlStmt,
    NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: preparing failed\n", rc);
        return 2;
    }

    rc = sqlite3_step(sqlStmt);
    if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
        fprintf(stderr, "SQL error %d: reset failed\n", rc);
        return 2;
    }

    int count = sqlite3_column_int(sqlStmt, 0);
    if (count > 0) {
        doesTableExist = 1;
    }

    rc = sqlite3_finalize(sqlStmt);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: finalize failed\n", rc);
        return 2;
    }
#endif
    return doesTableExist;
}

void createTable(sqlite3 *db, Layout &layout) {

#ifndef DISABLE_SQL_CODE
    int rc;
    char *zErrMsg = 0;

    rc = sqlite3_exec(db, getCreateTableQuery(layout).c_str(), NULL, 0,
            &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Create query=%s\n",
                getCreateTableQuery(layout).c_str());
        fprintf(stderr, "SQL error for create table: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        throw string("Error occurred creating table");
    }
#endif
}

void deleteTable(sqlite3 *db, Layout &layout) {

#ifndef DISABLE_SQL_CODE
    int rc;
    char *zErrMsg = 0;

    string query = "DROP table '" + layout.name + "'";
    rc = sqlite3_exec(db, query.c_str(), NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Delete query=%s\n", query.c_str());
        fprintf(stderr, "SQL error for delete table: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        throw string("Error occurred deleting table");
    }
#endif
}

int createIndex(sqlite3 *db, const Layout& layout, string indexOnFieldName) {
#ifndef DISABLE_SQL_CODE
    int rc;
    char *zErrMsg = 0;

    string indexQry = "CREATE INDEX '" + layout.name + "_" + indexOnFieldName
            + "_index' ON '" + layout.name + "' ('" + layout.fieldList[0].name
            + "');";

    rc = sqlite3_exec(db, indexQry.c_str(), NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "indexQry: %s\n", indexQry.c_str());
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
#endif
    return 0;
}

string getCSVLayoutHelpExample() {
    return "\nExample of csv layout:"
            "\n{"
            "\n    \"name\" : \"in\","
            "\n    \"type\" : \"csv\","
            "\n    \"separator\" : \"   \","
            "\n    \"fieldList\": ["
            "\n        {"
            "\n            \"name\": \"record_id\","
            "\n            \"type\": \"integer\""
            "\n        },"
            "\n        {"
            "\n            \"name\": \"name\""
            "\n        },"
            "\n        {"
            "\n            \"name\": \"balance\","
            "\n            \"type\": \"real\""
            "\n        },"
            "\n        {"
            "\n            \"name\": \"date1\","
            "\n            \"type\": \"date\","
            "\n            \"format\": \"%m%d%Y\""
            "\n        },"
            "\n        {"
            "\n            \"name\": \"date2\","
            "\n            \"type\": \"date\","
            "\n            \"format\": \"%Y-%m-%d\""
            "\n        },"
            "\n        {"
            "\n            \"name\": \"time1\","
            "\n            \"type\": \"time\","
            "\n            \"format\": \"%Y-%m-%dT%H:%M:%S\""
            "\n        },"
            "\n        {"
            "\n            \"name\": \"time2\","
            "\n            \"type\": \"time\","
            "\n            \"format\": \"%d/%m/%Y-%H-%M-%S\""
            "\n        }"
            "\n    ]"
            "\n}";
}

string getLayoutHelp() {
    return "\nLayout definition:"
            "\n General layout structure is define in json format:"
            "\n   <LayoutDefinition>"
            "\n     <FieldDefinitionList>"
            "\n       <FieldDefinition>"
            "\n Layout Definition Parameters:"
            "\n   name      : Layout name that will be used as table name. Can also be passed as an argument to utility."
            "\n               Example: book_info"
//            "\n   type      : csv/flat. 'csv' for delimited file and 'flat' for flat files"
            "\n   type      : csv. 'csv' for delimited file"
            "\n               Example: csv"
            "\n   separator : Separator used for file. Only valid for csv files."
            "\n Layout Field Definition Parameters:"
            "\n   name     : Field name. It will become the column name in db"
            "\n              Example: balance"
            "\n   type     : text/integer/real/date/time. 'text' text field like \"hello\". 'integer' for integers like 10. 'real' for decimals like 5.6"
            "\n              Example: real"
            "\n   format   : If type is date/time then we need to provide this. Use date format from \n   http://pubs.opengroup.org/onlinepubs/009695399/functions/strftime.html"
            "\n              Example: %m%d%Y for 11022014 which is 2014-02-11"
            "\n              Example: %d/%m/%Y-%H-%M-%S for 02/11/2014-21-56-53 which is 2014-02-11T21:56:53"
            "\n   pivotYear: If type is date/time then we can provide pivot year. Refer to \n   http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormatter.html#withPivotYear(int)"
            "\n              Example: 2000"
//            "\n   length : Any integer number. Length for a given field. Only valid for flat files"
//            "\n            Example: 7"
            "\n";
}

OptionParser createParser() {
    OptionParser parser = OptionParser().description(
            "Converts fixed length files to sqlite database");

    parser.add_option("-t").dest("t").metavar("<table_name>").help(
            "Table to be created");
    parser.add_option("-i").dest("i").metavar("<input_file>").help(
            "Input fixed length text file containing records to be ported to sqlite data base.");
    parser.add_option("-o").dest("o").metavar("<output_file>").help(
            "Output file name for the sqlite data base.");
    parser.add_option("-l").dest("l").metavar("<layout_file>").help(
            "Layout file containing field definitions. These \
                    field definitions will be used for parsing fixed length record");
    parser.add_option("-c").dest("c").metavar("<N>").set_default("10").help(
            "Number of records after which commit operation is called on the data base");
    parser.add_option("-d").dest("d").set_default("0").action("store_true").help(
            "Delete if table already exists");
    parser.add_option("-a").dest("a").set_default("0").action("store_true").help(
            "Append to existing table if it does exist");
    parser.add_option("-v").dest("v").set_default("0").action("store_true").help(
            "Debug mode");
    parser.epilog(getLayoutHelp() + getCSVLayoutHelpExample());
    return parser;
}

// TODO NEED TO ADD META TABLE TO ADD CONTENTS LIKE TABLE UPDATED, CREATED ETC
int main(int argc, char **argv) {
    time_t cStartTime;
    time_t cInsertStartTime;

    OptionParser parser = createParser();
    optparse::Values options = parser.parse_args(argc, argv);

    string layoutFileName = options["l"];
    string inputFileName = options["i"];
    string outputFileName = options["o"];
    string argTableName = options["t"];
    long commitAfter = atol(options["c"].c_str());
    bool isDebug = atoi(options["v"].c_str()) == 1 ? true : false;
    bool isAppendMode = atoi(options["a"].c_str()) == 1 ? true : false;
    bool isDeleteMode = atoi(options["d"].c_str()) == 1 ? true : false;

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
        pLayout = parseLayout(layoutFileName, argTableName);
    } catch (string& e) {
        cout << e << endl;
        parser.print_help();
        return 1;
    }

    if (isDebug)
        cout << *pLayout << endl;

    istream *inStream;
    ifstream inFile;

    if (inputFileName.empty() || inputFileName.compare("-") == 0) {
        // Disable the sync to improve performance.
        // NOTE: Make sure we don't use C like I/O going forward.
        cin.sync_with_stdio(false);
        inStream = &cin;
        if (isDebug)
            cout << "Reading from stdin" << endl;
    } else {
        if (isDebug)
            cout << "Reading from " << inputFileName << endl;
        inFile.open(inputFileName.c_str());
        inStream = &inFile;
    }

    string dbFileName;
    if (!outputFileName.empty())
        dbFileName = outputFileName;
    else if (!inputFileName.empty() && inputFileName != "-")
        dbFileName = inputFileName + ".db";
    else {
        cerr << "No output file name can be determined" << endl;
        return -1;
    }

//remove(dbFileName.c_str());

#ifndef DISABLE_SQL_CODE
    int rc;
    sqlite3 *db;
    char *zErrMsg = 0;

    rc = sqlite3_open(dbFileName.c_str(), &db);
    if (rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return (1);
    }

    int doesTableExist = checkIfTableExist(db, pLayout->name);
    if (isDebug)
        cout << "doesTableExist=" << doesTableExist << endl;
    if (doesTableExist == 2) {
        return 1;
    }

    try {
        if (doesTableExist == 0 && isAppendMode) {
            createTable(db, *pLayout);
        } else

        if (doesTableExist == 0 && isDeleteMode) {
            createTable(db, *pLayout);
        } else

        if (doesTableExist == 1 && isDeleteMode) {
            deleteTable(db, *pLayout);
            createTable(db, *pLayout);
        } else if (doesTableExist == 1 && isAppendMode) {
        } else {
            createTable(db, *pLayout);
        }
    } catch (string& e) {
        cout << e << endl;
        return 1;
    }

    rc = sqlite3_exec(db, "PRAGMA synchronous=OFF;", NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

// TODO COMMENTING OUT WAL MODE
//    rc = sqlite3_exec(db, "PRAGMA journal_mode = WAL;", NULL, 0, &zErrMsg);
//    if (rc != SQLITE_OK) {
//        fprintf(stderr, "SQL error: %s\n", zErrMsg);
//        sqlite3_free(zErrMsg);
//        return 1;
//    }

    // Switching off journal
    rc = sqlite3_exec(db, "PRAGMA journal_mode = OFF;", NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    cInsertStartTime = time(NULL);

    rc = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
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
    while (getline(*inStream, line)) {

        /*
         // if(isDebug) cout<<"line="<<line<<endl;
         // string insertQry;
         // insertQry = "INSERT INTO " + tableName + " VALUES (";
         */
        index = 0;
        lineStr = (char *) line.c_str();
        // cout<<lineStr<<endl;
        //parseFixedLenRecord(layout, lineStr, sqlStmt);
        parseDelimRecord(*pLayout, NULL, lineStr, line.length(), sqlStmt);

#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
        sqlite3_step(sqlStmt);
        sqlite3_reset(sqlStmt);
#    else
        rc = sqlite3_step(sqlStmt);
        if (rc == SQLITE_DONE) {
            rc = sqlite3_reset(sqlStmt);
            if (rc != SQLITE_OK) {
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
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    long insertTimeInSecs = time(NULL) - cInsertStartTime;
    printf("Inserted %ld records in %ld seconds with %.2f opts/sec \n",
            recordCounter, insertTimeInSecs,
            ((double) recordCounter / insertTimeInSecs));

    rc = sqlite3_finalize(sqlStmt);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: reset failed\n", rc);
    }

//    time_t cIndexStartTime;
//    cIndexStartTime = time(NULL);
//    createIndex(db, *pLayout, pLayout->fieldList[0].name);
//    long indexTimeInSecs = time(NULL) - cIndexStartTime;
//    printf("Indexed %ld records in %ld seconds with %.2f opts/sec \n",
//            recordCounter, indexTimeInSecs,
//            ((double) recordCounter / indexTimeInSecs));

//    rc = sqlite3_exec(db, "PRAGMA journal_mode = DELETE;", NULL, 0, &zErrMsg);
//    if (rc != SQLITE_OK) {
//        fprintf(stderr, "SQL error: %s\n", zErrMsg);
//        sqlite3_free(zErrMsg);
//        return 1;
//    }

    sqlite3_close(db);
#endif
    long timeInSecs = time(NULL) - cStartTime;
// printf("Imported %ld records in %4.2f seconds with %.2f opts/sec \n", recordCounter, timeInSecs, recordCounter/timeInSecs);
    printf("Imported %ld records in %ld seconds with %.2f opts/sec \n",
            recordCounter, timeInSecs, ((double) recordCounter / timeInSecs));
    printf("DB file is at: %s\n", dbFileName.c_str());
    delete pLayout;
    inFile.close();
}

