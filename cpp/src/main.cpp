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
#include <algorithm>

#include "sqlite3.h"
#ifndef DISABLE_SQL_CODE
#endif

#include "cJSON.h"
#include "OptionParser.h"

using namespace std;
using optparse::OptionParser;

#define ENABLE_SQL_CHECKS

string to_lower_copy(string str) {
    string newStr = str;
    transform(newStr.begin(), newStr.end(), newStr.begin(), ::tolower);
    return newStr;
}

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

struct Layout;

struct Field {
    Layout *layoutPtr;
    int isSkip;
    string name;
    char type;
    int length;
    string format;
    int pivotYear;
    int pivotYearLow;
    int pivotYearT;
    string *missingValue;
    int missingValueLen;
    int isTrim;
    int endOffSet;
    Field() :
            layoutPtr(NULL), isSkip(0), type('S'), length(0), format(""), pivotYear(-1), pivotYearLow(
                    -1), pivotYearT(-1), missingValue(
            NULL), missingValueLen(0), isTrim(1), endOffSet(0) {
    }

    friend ostream& operator<<(ostream &outStream, Field &field) {
        cout << "[ isSkip=" << field.isSkip << " name=" << field.name << ", type=" << field.type
                << ", length=" << field.length << ", format=" << field.format << ", pivotYear="
                << field.pivotYear << ", missingValue="
                << (field.missingValue == NULL ? "NULL" : field.missingValue->c_str())
                << ", isTrim=" << field.isTrim << ", endOffSet = " << field.endOffSet << " ]";
        return outStream;
    }

    void setPivotYear(int pivotYear) {
        this->pivotYear = pivotYear;
        this->pivotYearLow = this->pivotYear - 50;

        if (this->pivotYearLow >= 0) {
            this->pivotYearT = this->pivotYearLow % 100;
        } else {
            this->pivotYearT = 99 + ((this->pivotYearLow + 1) % 100);
        }

    }
};

struct IndexColumn {
    string name;
    string query;
    IndexColumn(const string name) :
            query("") {
        if (name.length() < 1) {
            throw string("Index column name cannot be empty.");
        }
        this->name = name;
    }

    IndexColumn(const string name, const string query) {
        if (name.length() < 1) {
            throw string("Index column name cannot be empty.");
        }
        this->name = name;
        this->query = query;
    }

    friend ostream& operator<<(ostream &outStream, IndexColumn &indexColumn) {
        outStream << "[ " << "name=" << indexColumn.name << ", query=" << indexColumn.query << " ]";
        return outStream;
    }
};

struct Index {
    string name;
    bool isUnique;
    vector<IndexColumn> indexColumnList;
    string whereClause;
    Index() :
            name(""), isUnique(true), whereClause("") {
    }

    Index(bool isUnique, string whereClause) {
        this->isUnique = isUnique;
        this->whereClause = whereClause;
    }

    void setIndexName(const string name) {
        this->name = name;
    }

    void addIndexColumn(IndexColumn ic) {
        indexColumnList.push_back(ic);
    }

    friend ostream& operator<<(ostream &outStream, Index &index) {
        outStream << "[ " << "name=" << index.name << ", whereClause=" << index.whereClause;
        outStream << ", indexColumnList(" << index.indexColumnList.size() << ")=[ ";
        for (int i = 0; i < index.indexColumnList.size(); i++) {
            outStream << index.indexColumnList[i] << ", ";
        }
        outStream << " ]";
        outStream << " ]";
        return outStream;
    }

    ~Index() {
    }
};

struct Layout {
    string name;
    vector<Index> indexList;
    Field *fieldList;
    int fieldListLen;
    char type;
    char separator;
    bool storeDateAsEpoch;

    Layout(const int fieldListSize) :
            type('D'), separator('\t'), storeDateAsEpoch(false) {
        fieldList = new Field[fieldListSize];
        fieldListLen = fieldListSize;
    }

    ~Layout() {
        delete[] fieldList;
    }

    void addIndex(Index index) {
        indexList.push_back(index);
    }

    friend ostream& operator<<(ostream &outStream, Layout &layout) {
        outStream << "[ " << "name=" << layout.name << ", type=" << layout.type;
        outStream << ", storeDateAsEpoch=" << layout.storeDateAsEpoch;
        outStream << ", indexList(" << layout.indexList.size() << ")=[ ";
        for (int i = 0; i < layout.indexList.size(); i++) {
            outStream << layout.indexList[i] << ", ";
        }
        outStream << " ]";
        outStream << ", fieldList=[ ";
        for (int i = 0; i < layout.fieldListLen; i++) {
            outStream << layout.fieldList[i] << ", ";
        }
        outStream << " ]";
        outStream << " ]";
        return outStream;
    }
};

struct DelimFileData {
    char separator;
};

inline void fixYear(Field *pField, struct tm *ptm) {
    int year = ptm->tm_year + 1900;
    int twoDigitYear = ((year / 10) % 10) * 10 + year % 10;

    twoDigitYear += pField->pivotYearLow + ((twoDigitYear < pField->pivotYearT) ? 100 : 0)
            - pField->pivotYearT;
    ptm->tm_year = twoDigitYear - 1900;
}

// For init refer to http://www.cplusplus.com/reference/ctime/tm/ or
// http://pubs.opengroup.org/onlinepubs/7908799/xsh/time.h.html
// Setting it to mid-night of Jan 1, 1900

#define INIT_TM(x) \
        x.tm_sec = 0; \
        x.tm_min = 0; \
        x.tm_hour = 0;\
        x.tm_mday = 1;\
        x.tm_mon = 0;\
        x.tm_year = 0;\
        x.tm_wday = 1; /* January 1, 1900 is a Monday refer http://www.timeanddate.com/date/weekday.html */ \
        x.tm_yday = 0;\
        x.tm_isdst = 0;\

struct Buffer {
    FILE *fp;
    int eofFlag;
    long bufferSize;
    const long maxBufferSize;
    char *buffer;
    int bufferIndex;
    const long maxFieldBufferSize;
    char *fieldStr;
    Buffer(FILE *inFp, long readBufferSize, long fieldBufferSize) :
            fp(inFp), eofFlag(0), maxBufferSize(readBufferSize), maxFieldBufferSize(fieldBufferSize) {
        buffer = new char[maxBufferSize];
        bufferSize = maxBufferSize;
        bufferIndex = bufferSize;

        fieldStr = new char[maxFieldBufferSize];
    }

    ~Buffer() {
        delete buffer;
        delete fieldStr;
    }
};

inline int parseDelimRecord(Layout &layout, Buffer *buffer, sqlite3_stmt *sqlStmt) {
#define MAX_DATE_STRING_LEN 20
    static char datestring[MAX_DATE_STRING_LEN];
    char separator = layout.separator;
#ifndef DISABLE_SQL_CODE
    int rc;
#endif
    struct tm tm;
    char *fieldStart = buffer->fieldStr;
    register int fieldLength;
    int nonSpaceStartIndex;
    register int nonSpaceEndIndex;
    register char ch = 0;

    int fieldListCount = layout.fieldListLen;
    for (int fieldCounter = 0, bindFieldCounter = 0; fieldCounter < fieldListCount;
            fieldCounter++) {
        Field &field = layout.fieldList[fieldCounter];
        fieldLength = 0;
        nonSpaceStartIndex = -1;
        nonSpaceEndIndex = -1;
        if (ch == '\n') {
            break;
        }
        do {
            if (buffer->bufferIndex >= buffer->bufferSize) {
                buffer->bufferIndex = 0;
                if (feof(buffer->fp)) {
                    buffer->bufferSize = 0;
                    buffer->eofFlag = 1;
                } else {
                    buffer->bufferSize = fread(buffer->buffer, 1, buffer->bufferSize, buffer->fp);
                }
            }
            if (fieldLength == buffer->maxFieldBufferSize) {
                fprintf(stderr, "Field length exceeds max field buffer size=%ld\n",
                        buffer->maxFieldBufferSize);
                return -3;
            }
            buffer->fieldStr[fieldLength] = ch = buffer->buffer[buffer->bufferIndex];
            buffer->bufferIndex++;
            if (ch == separator || ch == '\n' || buffer->eofFlag == 1) {
                buffer->fieldStr[fieldLength] = 0;
                break;
            } else if (ch != ' ') {
                if (nonSpaceStartIndex == -1) {
                    nonSpaceStartIndex = fieldLength;
                }
                nonSpaceEndIndex = fieldLength + 1;
            }

            fieldLength++;

        } while (true);

        if (buffer->eofFlag == 1 && fieldLength == 0) {
            return -1;
        }
        if (field.isSkip == 1) {
            continue;
        }
        bindFieldCounter++;

        // First check if missing then bind it to null
        if (field.missingValue != NULL && fieldLength == field.missingValueLen
                && strncmp(fieldStart, field.missingValue->c_str(), field.missingValueLen) == 0) {
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
            sqlite3_bind_null(sqlStmt, bindFieldCounter);
#    else
            rc = sqlite3_bind_null(sqlStmt, bindFieldCounter);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "SQL error %d: text null binding failed\n", rc);
                return 1;
            }
#    endif
#endif
            continue;
        }

        // If it is as string and no trimming required
        if (field.type == 'S' && field.isTrim == 0) {
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
            sqlite3_bind_text(sqlStmt, bindFieldCounter, fieldStart,
                    fieldLength,
                    SQLITE_TRANSIENT);
#    else
            rc = sqlite3_bind_text(sqlStmt, bindFieldCounter, fieldStart, fieldLength,
            SQLITE_TRANSIENT);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                return 1;
            }
#    endif
#endif
            continue;
        }

        // Looks like the whole string is filled with spaces so bind with null
        if (nonSpaceStartIndex == -1) {
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
            sqlite3_bind_null(sqlStmt, bindFieldCounter);
#    else
            rc = sqlite3_bind_null(sqlStmt, bindFieldCounter);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "SQL error %d: text null binding failed\n", rc);
                return 1;
            }
#    endif
#endif
            continue;
        }

        if (field.type == 'S' || field.type == 'I' || field.type == 'R') {
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
            sqlite3_bind_text(sqlStmt, bindFieldCounter,
                    fieldStart + nonSpaceStartIndex,
                    nonSpaceEndIndex - nonSpaceStartIndex,
                    SQLITE_TRANSIENT);
#    else
            rc = sqlite3_bind_text(sqlStmt, bindFieldCounter, fieldStart + nonSpaceStartIndex,
                    nonSpaceEndIndex - nonSpaceStartIndex,
                    SQLITE_TRANSIENT);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                return 1;
            }
#    endif
#endif
            continue;
        }

        if (field.type == 'D') {
            INIT_TM(tm)
            if (strptime(fieldStart, field.format.c_str(), &tm) != NULL) {
                if (field.pivotYear != -1) {
                    fixYear(&field, &tm);
                }
                if (layout.storeDateAsEpoch) {
                    time_t epoch = mktime(&tm);
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                    sqlite3_bind_int64(sqlStmt, bindFieldCounter, epoch);
#    else
                    rc = sqlite3_bind_int64(sqlStmt, bindFieldCounter, epoch);

                    if (rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: date epoch binding failed\n", rc);
                        return 1;
                    }
#    endif
#endif
                } else {
                    sprintf(datestring, "%04d-%02d-%02d", (tm.tm_year + 1900), (tm.tm_mon + 1),
                            tm.tm_mday);
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                    sqlite3_bind_text(sqlStmt, bindFieldCounter, datestring, 10,
                            SQLITE_TRANSIENT);
#    else
                    rc = sqlite3_bind_text(sqlStmt, bindFieldCounter, datestring, 10,
                    SQLITE_TRANSIENT);

                    if (rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: date text binding failed\n", rc);
                        return 1;
                    }
#    endif
#endif
                }
            }
            continue;
        }

        if (field.type == 'T') {
            INIT_TM(tm)
            if (strptime(fieldStart, field.format.c_str(), &tm) != NULL) {
                if (field.pivotYear != -1) {
                    fixYear(&field, &tm);
                }
                sprintf(datestring, "%04d-%02d-%02dT%02d:%02d:%02d", (tm.tm_year + 1900),
                        (tm.tm_mon + 1), tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                sqlite3_bind_text(sqlStmt, bindFieldCounter, datestring,
                        MAX_DATE_STRING_LEN,
                        SQLITE_TRANSIENT);
#    else
                rc = sqlite3_bind_text(sqlStmt, bindFieldCounter, datestring,
                MAX_DATE_STRING_LEN,
                SQLITE_TRANSIENT);

                if (rc != SQLITE_OK) {
                    fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                    return 1;
                }
#    endif
#endif
            }
        }

    }
    return 0;
}

Layout * parseLayout(string layoutFileName, string argTableName, bool isRetainCase) {
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
    cJSON *jIndexList = cJSON_GetObjectItem(root, "indexList");

    // building create table query
    // TODO  extract table name from file name or something
    int recordLen = 0;
    int fieldCounter = 0;
    int fieldListCount = cJSON_GetArraySize(fieldList);
    int indexListCount = jIndexList != NULL ? cJSON_GetArraySize(jIndexList) : 0;

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
        throw string("Table name not specified either in layout or as argument");
    }

    Layout *pLayout = new Layout(fieldListCount);
    if (isRetainCase) {
        pLayout->name = tableName;
    } else {
        pLayout->name = to_lower_copy(tableName);
    }

    if (pLayout->type == 'D') {
        cJSON *jSeparator = cJSON_GetObjectItem(root, "separator");
        if (jSeparator != NULL && strlen(jSeparator->valuestring) > 0) {
            pLayout->separator = jSeparator->valuestring[0];
        }
    }

    cJSON *jStoreDateAsEpoch = cJSON_GetObjectItem(root, "storeDateAsEpoch");
    if (jStoreDateAsEpoch != NULL && strlen(jStoreDateAsEpoch->valuestring) > 0) {
        pLayout->storeDateAsEpoch = strcmp(jStoreDateAsEpoch->valuestring, "true") == 0 ? 1 : 0;
    }

    // Parse index part of layout
    char str[20];
    for (int i = 0; i < indexListCount; i++) {
        cJSON *jIndex = cJSON_GetArrayItem(jIndexList, i);
        string preferredIndexName =
                cJSON_GetObjectItem(jIndex, "name") != NULL ?
                        cJSON_GetObjectItem(jIndex, "name")->valuestring : "";
        string isUniqueStr =
                cJSON_GetObjectItem(jIndex, "isUnique") != NULL ?
                        cJSON_GetObjectItem(jIndex, "isUnique")->valuestring : "";
        bool isUnique = false;
        isUnique = strcmp(isUniqueStr.c_str(), "true") == 0;
        string whereClause =
                cJSON_GetObjectItem(jIndex, "where") != NULL ?
                        cJSON_GetObjectItem(jIndex, "where")->valuestring : "";
        cJSON *jColumnList = cJSON_GetObjectItem(jIndex, "columnList");
        int columnListLen = cJSON_GetArraySize(jColumnList);

        Index *idx = new Index(isUnique, whereClause);

        string genIndexName = "IDX_" + pLayout->name;
        for (int colI = 0; colI < columnListLen; colI++) {
            cJSON *jIndexColumn = cJSON_GetArrayItem(jColumnList, colI);
            string columnName =
                    cJSON_GetObjectItem(jIndexColumn, "name") != NULL ?
                            cJSON_GetObjectItem(jIndexColumn, "name")->valuestring : "";
            string query =
                    cJSON_GetObjectItem(jIndexColumn, "query") != NULL ?
                            cJSON_GetObjectItem(jIndexColumn, "query")->valuestring : "";
            if (columnName.length() < 1) {
                char str[20];
                sprintf(str, "%d", (colI + 1));
                string errStr = "Index column name cannot be empty at index ";
                errStr += str;
                sprintf(str, "%d", (i + 1));
                errStr += " of index definition index at ";
                errStr += str;
                errStr += " indexName=";
                errStr += idx->name;
                throw errStr;
            }
            genIndexName += "_" + columnName;
            if (!isRetainCase) {
                columnName = to_lower_copy(columnName);
                query = to_lower_copy(query);
            }
            IndexColumn *ic = new IndexColumn(columnName, query);
            idx->addIndexColumn(*ic);
        }
        sprintf(str, "%d", (i + 1));
        genIndexName += "_";
        genIndexName += str;

        if (preferredIndexName.length() > 0) {
            idx->setIndexName(preferredIndexName);
        } else {
            idx->setIndexName(genIndexName);
        }

        if (!isRetainCase) {
            idx->setIndexName(to_lower_copy(idx->name));
        }
        pLayout->addIndex(*idx);

    }

    for (int i = 0; i < fieldListCount; i++) {
        cJSON *jsonField = cJSON_GetArrayItem(fieldList, i);

        cJSON *jsonFieldName = cJSON_GetObjectItem(jsonField, "name");
        if (jsonFieldName == NULL) {
            throw string("Name cannot be empty");
        }

        if (isRetainCase) {
            pLayout->fieldList[i].name = jsonFieldName->valuestring;
        } else {
            pLayout->fieldList[i].name = to_lower_copy(jsonFieldName->valuestring);
        }

        cJSON *jsonIsSkip = cJSON_GetObjectItem(jsonField, "isSkip");
        if (jsonIsSkip != NULL) {
            if (strcmp(jsonIsSkip->valuestring, "true") == 0) {
                pLayout->fieldList[i].isSkip = 1;
                continue;
            } else if (strcmp(jsonIsSkip->valuestring, "false") == 0) {
                pLayout->fieldList[i].isSkip = 0;
                continue;
            } else {
                char str[20];
                sprintf(str, "%d", (i + 1));
                string errStr = "Invalid value='";
                errStr += jsonIsSkip->valuestring;
                errStr += "' for isSkip at field number=";
                errStr += str;
                throw errStr;
            }
        }

        cJSON *jsonIsTrim = cJSON_GetObjectItem(jsonField, "isTrim");
        if (jsonIsTrim != NULL) {
            if (strcmp(jsonIsTrim->valuestring, "true") == 0) {
                pLayout->fieldList[i].isTrim = 1;
                continue;
            } else if (strcmp(jsonIsTrim->valuestring, "false") == 0) {
                pLayout->fieldList[i].isTrim = 0;
                continue;
            } else {
                char str[20];
                sprintf(str, "%d", (i + 1));
                string errStr = "Invalid value='";
                errStr += jsonIsTrim->valuestring;
                errStr += "' for isTrim at field number=";
                errStr += str;
                throw errStr;
            }
        }

        cJSON *jsonMissingValue = cJSON_GetObjectItem(jsonField, "missingValue");
        if (jsonMissingValue != NULL) {
            pLayout->fieldList[i].missingValue = new string(jsonMissingValue->valuestring);
            pLayout->fieldList[i].missingValueLen = strlen(
                    pLayout->fieldList[i].missingValue->c_str());
        }

        if (pLayout->type == 'F') {
            // Check length if it is flat file
            if (cJSON_GetObjectItem(jsonField, "length") == NULL) {
                string e = string("Name: ");
                e += pLayout->fieldList[i].name;
                e += "\nLength cannot be empty";
                throw e;
            }

            pLayout->fieldList[i].length = cJSON_GetObjectItem(jsonField, "length")->valueint;
            if (i > 0)
                pLayout->fieldList[i].endOffSet = pLayout->fieldList[i - 1].endOffSet
                        + pLayout->fieldList[i].length;
            else
                pLayout->fieldList[i].endOffSet = pLayout->fieldList[i].length;
            recordLen += pLayout->fieldList[i].length;
        }

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

        if (pLayout->fieldList[i].type == 'D' || pLayout->fieldList[i].type == 'T') {
            cJSON *jsonDateFormat = cJSON_GetObjectItem(jsonField, "format");
            if (jsonDateFormat == NULL) {
                string e = string("Name: ");
                e += pLayout->fieldList[i].name;
                e += "\nFormat cannot be empty";
                throw e;
            }
            pLayout->fieldList[i].format = jsonDateFormat->valuestring;

            cJSON *jsonPivotYear = cJSON_GetObjectItem(jsonField, "pivotYear");
            if (jsonPivotYear != NULL) {
                pLayout->fieldList[i].setPivotYear(jsonPivotYear->valueint);
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
    createTableQry = "CREATE TABLE \"" + layout.name + "\" ( ";
    for (int i = 0; i < layout.fieldListLen; i++) {
        Field &field = layout.fieldList[i];
        if (field.isSkip != 1) {

            if (fieldCounter != 0) {
                createTableQry += ", ";
            }
            createTableQry += "\"" + field.name + "\"";

            if (field.type == 'S' || field.type == 'T') {
                createTableQry += " TEXT";
            }

            if (field.type == 'D') {
                if (layout.storeDateAsEpoch) {
                    createTableQry += " INTEGER";
                } else {
                    createTableQry += " TEXT";
                }
            }

            if (field.type == 'I') {
                createTableQry += " INTEGER";
            }

            if (field.type == 'R') {
                createTableQry += " REAL";
            }

            fieldCounter++;
        }
        if (i > 0)
            field.endOffSet = layout.fieldList[i - 1].endOffSet + layout.fieldList[i].length;
        else
            field.endOffSet = field.length;
        recordLen += field.length;
    }
    createTableQry += ");";
    return createTableQry;
}

string getInsertQuery(Layout & layout) {
    // building insert table query
    int fieldCounter = 0;

    string columnQry = " (";
    string valueQry = " VALUES ( ";
    for (int i = 0; i < layout.fieldListLen; i++) {
        Field &field = layout.fieldList[i];
        if (field.isSkip != 1) {
            if (fieldCounter != 0) {
                columnQry += ", ";
                valueQry += ", ";
            }
            columnQry += "\"" + field.name + "\"";
            valueQry += "?";
            fieldCounter++;
        }
    }
    columnQry += ")";
    valueQry += ")";
    string insertBindQry;
    insertBindQry = "INSERT INTO \"" + layout.name + "\"" + columnQry + valueQry + ";";
    return insertBindQry;
}

int checkIfTableExist(sqlite3 *db, string tableNameToCheck) {
    int doesTableExist = 0;

#ifndef DISABLE_SQL_CODE
    int rc;
    sqlite3_stmt *sqlStmt;
    tableNameToCheck = to_lower_copy(tableNameToCheck);
    string queryStr = "SELECT count(name) FROM sqlite_master WHERE type='table' AND lower(name)='"
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

void createTable(sqlite3 *db, Layout &layout, bool isDebug) {

#ifndef DISABLE_SQL_CODE
    int rc;
    char *zErrMsg = 0;

    string createTableQry = getCreateTableQuery(layout);

    if (isDebug) {
        cout << "createTableQry=" << createTableQry << endl;
    }

    rc = sqlite3_exec(db, createTableQry.c_str(), NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Create query=%s\n", createTableQry.c_str());
        fprintf(stderr, "SQL error for create table: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        throw string("Error occurred creating table");
    }
#endif
}

void deleteTable(sqlite3 *db, Layout &layout, bool isDebug) {

#ifndef DISABLE_SQL_CODE
    int rc;
    char *zErrMsg = 0;

    string delQuery = "DROP table \"" + layout.name + "\"";
    if (isDebug) {
        cout << "delQuery=" << delQuery << endl;
    }

    rc = sqlite3_exec(db, delQuery.c_str(), NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Delete query=%s\n", delQuery.c_str());
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

    string indexQry = "CREATE INDEX \"" + layout.name + "_" + indexOnFieldName + "_index\" ON \""
            + layout.name + "\" (\"" + layout.fieldList[0].name + "\");";

    rc = sqlite3_exec(db, indexQry.c_str(), NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "indexQry: %s\n", indexQry.c_str());
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
#endif
    return 0;
}

int createIndex(sqlite3 *db, const string tableName, const Index& index, const bool isDebug) {
#ifndef DISABLE_SQL_CODE
    int rc;
    char *zErrMsg = 0;

    string indexQry = "CREATE";

    if (index.isUnique) {
        indexQry += " UNIQUE";
    }
    indexQry += " INDEX \"" + index.name + "\" ON \"" + tableName + "\" (";

    int indexColListLen = index.indexColumnList.size();
    for (int i = 0; i < indexColListLen; i++) {
        IndexColumn ic = index.indexColumnList[i];
        indexQry += "\"";
        indexQry += ic.name;
        indexQry += "\"";
        if (ic.query.length() > 0) {
            indexQry += " ";
            indexQry += ic.query;
        }
        if (i + 1 < indexColListLen) {
            indexQry += ", ";
        }
    }
    indexQry += ")";

    if (index.whereClause.length() > 0) {
        indexQry += " ";
        indexQry += index.whereClause;
    }
    indexQry += ";";

    if (isDebug) {
        cout << "IndexQry=" << indexQry << endl;
    }

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
            "\n    \"indexList\": ["
            "\n       {"
            "\n            \"columnList\": ["
            "\n                {"
            "\n                    \"name\": \"rid1\""
            "\n                },"
            "\n                {"
            "\n                    \"name\": \"rid2\""
            "\n                }"
            "\n            ]"
            "\n       }"
            "\n    ],"
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
    return "\n\nLayout definition:"
            "\n General layout structure is define in json format:"
            "\n   <LayoutDefinition>"
            "\n     <IndexList>"
            "\n       <Index>"
            "\n         <IndexColumnList>"
            "\n           <IndexColumn>"
            "\n     <FieldDefinitionList>"
            "\n       <FieldDefinition>"
            "\n Layout Definition Parameters:"
            "\n   name      : Layout name that will be used as table name. Can also be passed as an argument to utility."
            "\n               Example: book_info"
            //"\n   type      : csv/flat. 'csv' for delimited file and 'flat' for flat files"
            "\n   type      : csv. 'csv' for delimited file"
            "\n               Example: csv"
            "\n   separator : Separator used for file. Only valid for csv files."
            "\n   storeDateAsEpoch: Store date as Epoch seconds. This will help reduce file size."
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
            "\n   missingValue: If this is the value then it will be replaced with null in db"
            "\n              Example: 2000"
            "\n   isSkip   : If true, this field is skipped while parsing the input file. Default is false"
            "\n   isTrim   : If type is text, then if false, this field is NOT trimmed. Default is true"
            //"\n   length : Any integer number. Length for a given field. Only valid for flat files"
            //"\n            Example: 7"
            "\n Index Definition Parameters:"
            "\n   name      : Name for the index. If left blank it will be auto generated."
            "\n               Example: idx_col_name_1"
            "\n Index Column Definition Parameters:"
            "\n   name     : Column name on which index is based on."
            "\n              Example: col_name"
            "\n";
}

OptionParser createParser() {
    OptionParser parser = OptionParser().description("Converts csv files to sqlite database");

    parser.add_option("-t").dest("t").metavar("<table_name>").help("Table to be created");
    parser.add_option("-i").dest("i").metavar("<input_file>").help(
            "Input fixed length text file containing records to be ported to sqlite data base.");
    parser.add_option("-o").dest("o").metavar("<output_file>").help(
            "Output file name for the sqlite data base.");
    parser.add_option("-l").dest("l").metavar("<layout_file>").help(
            "Layout file containing field definitions. These \
                    field definitions will be used for parsing fixed length record");
    parser.add_option("-c").dest("c").metavar("<N>").set_default("10000").help(
            "Number of records after which commit operation is called on the data base");
    parser.add_option("-d").dest("d").set_default("0").action("store_true").help(
            "Delete if table already exists");
    parser.add_option("-a").dest("a").set_default("0").action("store_true").help(
            "Append to existing table if it does exist");
    parser.add_option("-v").dest("v").set_default("0").action("store_true").help("Debug mode");
    parser.add_option("-s").dest("s").set_default("0").action("store_true").help("Show stats");
    parser.add_option("-p").dest("p").metavar("<comma-separated-pragma-list>").help(
            "Comma separated pragma which ran before creation of DB.");
    parser.add_option("-b").dest("b").metavar("<N>").set_default("1048576").help(
            "Read buffer size in bytes. Default: 1048576");
    parser.add_option("-f").dest("f").metavar("<N>").set_default("1048576").help(
            "Field buffer size in bytes. Default: 1048576");
    string versionStr = "sqliteloader 1.1.0-SNAPSHOT\n";
    string sqliteVersion = string(sqlite3_libversion());
    versionStr += "Using Sqlite Version: " + sqliteVersion;
    parser.version(versionStr);

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
    bool isShowStats = atoi(options["s"].c_str()) == 1 ? true : false;
    bool isAppendMode = atoi(options["a"].c_str()) == 1 ? true : false;
    bool isDeleteMode = atoi(options["d"].c_str()) == 1 ? true : false;
    string pragmaList = options["p"];
    long readBufferSize = atol(options["b"].c_str());
    long fieldBufferSize = atol(options["f"].c_str());

    if (layoutFileName.length() == 0) {
        cout << "Layout file name cannot be empty" << endl;
        parser.print_short_help();
        return 1;
    }

    if (isAppendMode && isDeleteMode) {
        cout << "Append mode and delete mode cannot co-exist" << endl;
        parser.print_short_help();
        return 1;
    }

    cStartTime = time(NULL);
    if (isDebug) {
        cout << "layoutFileName=" << layoutFileName << endl;
        cout << "inputFileName=" << inputFileName << endl;
        cout << "outputFileName=" << outputFileName << endl;
        cout << "argTableName=" << argTableName << endl;
        cout << "commitAfter=" << commitAfter << endl;
        cout << "isDebug=" << isDebug << endl;
        cout << "isShowStats=" << isShowStats << endl;
        cout << "isAppendMode=" << isAppendMode << endl;
        cout << "isDeleteMode=" << isDeleteMode << endl;
        cout << "readBufferSize=" << readBufferSize << endl;
        cout << "fieldBufferSize=" << fieldBufferSize << endl;
    }

    Layout *pLayout;
    try {
        pLayout = parseLayout(layoutFileName, argTableName, false);
    } catch (string& e) {
        cout << "\nError Occurred: " << e << endl;
        return 1;
    }

    if (isDebug)
        cout << *pLayout << endl;

    FILE *inStream;
    FILE *fp = NULL;

    if (inputFileName.empty() || inputFileName.compare("-") == 0) {
        inStream = stdin;
        if (isDebug)
            cout << "Reading from stdin" << endl;
    } else {
        if (isDebug)
            cout << "Reading from " << inputFileName << endl;
        fp = fopen(inputFileName.c_str(), "r");
        if (fp == NULL)
            return 1;
        inStream = fp;
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

#ifndef DISABLE_SQL_CODE
    int rc;
    sqlite3 *db;
    char *zErrMsg = 0;

    rc = sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
    if (rc) {
        fprintf(stderr, "Can't set thread mode to SQLITE_CONFIG_SINGLETHREAD\n");
    }

    rc = sqlite3_open(dbFileName.c_str(), &db);
    if (rc) {
        fprintf(stderr, "Can't open database file: %s\n", dbFileName.c_str());
        fprintf(stderr, "Can't open database sqlite error(%d): %s\n", rc, sqlite3_errmsg(db));
        sqlite3_close(db);
        return (1);
    }

    rc = sqlite3_exec(db, "PRAGMA busy_timeout = 60000;", NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    int doesTableExist = checkIfTableExist(db, pLayout->name);
    if (isDebug)
        cout << "doesTableExist=" << doesTableExist << endl;
    if (doesTableExist == 2) {
        return 1;
    }

    try {
        if (doesTableExist == 0 && isAppendMode) {
            createTable(db, *pLayout, isDebug);
        } else

        if (doesTableExist == 0 && isDeleteMode) {
            createTable(db, *pLayout, isDebug);
        } else

        if (doesTableExist == 1 && isDeleteMode) {
            deleteTable(db, *pLayout, isDebug);
            createTable(db, *pLayout, isDebug);
        } else if (doesTableExist == 1 && isAppendMode) {
        } else {
            createTable(db, *pLayout, isDebug);
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
    //rc = sqlite3_exec(db, "PRAGMA journal_mode = WAL;", NULL, 0, &zErrMsg);
    //if (rc != SQLITE_OK) {
    //    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    //    sqlite3_free(zErrMsg);
    //    return 1;
    //}

    // Switching off journal
    rc = sqlite3_exec(db, "PRAGMA journal_mode = OFF;", NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    }

    if (pragmaList.length() > 0) {
        if (isDebug)
            cout << "Applying pragmaList=" << pragmaList << endl;
        rc = sqlite3_exec(db, pragmaList.c_str(), NULL, 0, &zErrMsg);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error running pragmaList. SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            return 1;
        }
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
    if (isDebug) {
        cout << "insertBindQry=" << insertBindQry << endl;
    }
    rc = sqlite3_prepare_v2(db, insertBindQry.c_str(), insertBindQry.length(), &sqlStmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Prepare query=%s\n", insertBindQry.c_str());
        fprintf(stderr, "SQL error %d: preparing failed\n", rc);
        return 1;
    }
#endif

    long recordCounter = 0;
    int fieldCounter = 0;
    char *lineStr = NULL;
    long commitCounter = 0;
    char * line = NULL;
    size_t len = 0;
    Buffer buffer(inStream, readBufferSize, fieldBufferSize);
    int parseRet = 0;
    while (true) {

        parseRet = parseDelimRecord(*pLayout, &buffer, sqlStmt);
        if (parseRet < 0) {
            if (parseRet == -1)
                break;
            else if (parseRet == -3) {
                fprintf(stderr, "Error occurred at record number=%ld", recordCounter);
                fprintf(stderr, "Try increasing using -f <N>\n");
                return -1;
            }
        }

#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
        sqlite3_step(sqlStmt);
        sqlite3_reset(sqlStmt);
        if (commitCounter == commitAfter) {
            commitCounter = 0;
            sqlite3_exec(db, "END TRANSACTION;", NULL, 0, NULL);
            sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, 0, NULL);
        }
#    else
        rc = sqlite3_step(sqlStmt);
        if (rc == SQLITE_DONE) {
            rc = sqlite3_reset(sqlStmt);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "Error occurred at record number=%ld", recordCounter);
                fprintf(stderr, "SQL error %d: reset failed\n", rc);
                return 1;
            }
        } else {
            fprintf(stderr, "Error occurred at record number=%ld", recordCounter);
            fprintf(stderr, "SQL error %d: step failed\n", rc);
            return 1;
        }
        if (commitCounter == commitAfter) {
            commitCounter = 0;
            rc = sqlite3_exec(db, "END TRANSACTION;", NULL, 0, &zErrMsg);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "Error occurred at record number=%ld", recordCounter);
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
                return 1;
            }
            rc = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, 0, &zErrMsg);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "Error occurred at record number=%ld", recordCounter);
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
                return 1;
            }
        }
#    endif
#endif
        commitCounter++;
        recordCounter++;
    }

#ifndef DISABLE_SQL_CODE
    rc = sqlite3_exec(db, "END TRANSACTION;", NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    long insertTimeInSecs = time(NULL) - cInsertStartTime;
    if (isShowStats) {
        printf("Inserted %ld records in %ld seconds with %.2f opts/sec \n", recordCounter,
                insertTimeInSecs, ((double) recordCounter / insertTimeInSecs));
    }

    rc = sqlite3_finalize(sqlStmt);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: reset failed\n", rc);
    }

    vector<Index> indexList = pLayout->indexList;

    for (int i = 0; i < indexList.size(); i++) {
        time_t cIndexStartTime;
        cIndexStartTime = time(NULL);

        createIndex(db, pLayout->name, pLayout->indexList[i], isDebug);
        long indexTimeInSecs = time(NULL) - cIndexStartTime;
        if (isShowStats) {
            printf("Indexed %ld records in %ld seconds with %.2f opts/sec \n", recordCounter,
                    indexTimeInSecs, ((double) recordCounter / indexTimeInSecs));
        }
    }

    sqlite3_close(db);
#endif
    long timeInSecs = time(NULL) - cStartTime;
    if (isShowStats) {
        printf("Imported %ld records in %ld seconds with %.2f opts/sec \n", recordCounter,
                timeInSecs, ((double) recordCounter / timeInSecs));
        printf("DB file is at: %s\n", dbFileName.c_str());
    }
    printf("TableCreated=%s\n", pLayout->name.c_str());
    printf("RecordInserted=%ld\n", recordCounter);
    delete pLayout;
    if (fp != NULL) {
        fclose(fp);
    }
}

