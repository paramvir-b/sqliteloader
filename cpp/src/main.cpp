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
    int isSkip;
    string name;
    char type;
    int length;
    string format;
    int pivotYear;
    string *missingValue;
    int endOffSet;
    Field() :
            layoutPtr(NULL), isSkip(0), type('S'), length(0), format(""), pivotYear(
                    -1), missingValue(
            NULL), endOffSet(0) {
    }

    friend ostream& operator<<(ostream &outStream, Field &field) {
        cout << "[ isSkip=" << field.isSkip << " name=" << field.name
                << ", type=" << field.type << ", length=" << field.length
                << ", format=" << field.format << ", pivotYear="
                << field.pivotYear << ", missingValue="
                << (field.missingValue == NULL ?
                        "NULL" : field.missingValue->c_str()) << ", endOffSet="
                << field.endOffSet << " ]";
        return outStream;
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
        outStream << "[ " << "name=" << indexColumn.name << ", query="
                << indexColumn.query << " ]";
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
        outStream << "[ " << "name=" << index.name << ", whereClause="
                << index.whereClause;
        outStream << ", indexColumnList(" << index.indexColumnList.size()
                << ")=[ ";
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

    Layout(const int fieldListSize) :
            type('D'), separator('\t') {
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
    int year = getLastTwoDigits(ptm->tm_year + 1900);

    // TODO WE CAN COMPUTE THIS ONCE FOR PIVOT YEAR
    int low = pivotYear - 50;

    int t;
    if (low >= 0) {
        t = low % 100;
    } else {
        t = 99 + ((low + 1) % 100);
    }

    year += low + ((year < t) ? 100 : 0) - t;
    ptm->tm_year = year - 1900;

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

int parseDelimRecord(Layout &layout, void *pInfo, char *lineStr, int lineStrLen,
        sqlite3_stmt *sqlStmt) {
#define MAX_DATE_STRING_LEN 20
    static char datestring[MAX_DATE_STRING_LEN];
    DelimFileData *pDelimInfo = (DelimFileData *) pInfo;
//char separator = pDelimInfo->separator;
    char separator = layout.separator;
#ifndef DISABLE_SQL_CODE
    int rc;
#endif
    struct tm tm;

    int indexInLine = 0;
    int fieldListCount = layout.fieldListLen;
    for (int fieldCounter = 0, bindFieldCounter = 0;
            fieldCounter < fieldListCount; fieldCounter++) {
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

        if (field.isSkip == 1) {
            indexInLine += fieldLength + 1;
            continue;
        }
        bindFieldCounter++;
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
            sqlite3_bind_null(sqlStmt, bindFieldCounter);
#    else
            rc = sqlite3_bind_null(sqlStmt, bindFieldCounter);
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
            sqlite3_bind_text(sqlStmt, bindFieldCounter,
                    fieldStart + nonSpaceStartIndex,
                    nonSpaceEndIndex - nonSpaceStartIndex,
                    SQLITE_TRANSIENT);
#    else
            rc = sqlite3_bind_text(sqlStmt, bindFieldCounter,
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
            INIT_TM(tm)
            if (strptime(fieldStart, field.format.c_str(), &tm) != NULL) {
                if (field.pivotYear != -1) {
                    fixYear(field.pivotYear, &tm);
                }
                strftime(datestring, MAX_DATE_STRING_LEN, "%Y-%m-%d", &tm);
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                sqlite3_bind_text(sqlStmt, bindFieldCounter,
                        datestring,
                        10,
                        SQLITE_TRANSIENT);
#    else
                rc = sqlite3_bind_text(sqlStmt, bindFieldCounter, datestring,
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
            INIT_TM(tm)
            if (strptime(fieldStart, field.format.c_str(), &tm) != NULL) {
                if (field.pivotYear != -1) {
                    fixYear(field.pivotYear, &tm);
                }
                strftime(datestring, MAX_DATE_STRING_LEN, "%Y-%m-%dT%H:%M:%S",
                        &tm);
#ifndef DISABLE_SQL_CODE
#    ifndef ENABLE_SQL_CHECKS
                sqlite3_bind_text(sqlStmt, bindFieldCounter,
                        datestring,
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

Layout * parseLayout(string layoutFileName, string argTableName,
        bool isRetainCase) {
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
    int indexListCount =
            jIndexList != NULL ? cJSON_GetArraySize(jIndexList) : 0;

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
        throw string(
                "Table name not specified either in layout or as argument");
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

    // Parse index part of layout
    char str[20];
    for (int i = 0; i < indexListCount; i++) {
        cJSON *jIndex = cJSON_GetArrayItem(jIndexList, i);
        string preferredIndexName =
                cJSON_GetObjectItem(jIndex, "name") != NULL ?
                        cJSON_GetObjectItem(jIndex, "name")->valuestring : "";
        string isUniqueStr =
                cJSON_GetObjectItem(jIndex, "isUnique") != NULL ?
                        cJSON_GetObjectItem(jIndex, "isUnique")->valuestring :
                        "";
        bool isUnique = false;
        isUnique = strcmp(isUniqueStr.c_str(), "true") == 0;
        string whereClause =
                cJSON_GetObjectItem(jIndex, "where") != NULL ?
                        cJSON_GetObjectItem(jIndex, "where")->valuestring : "";
        cJSON *jColumnList = cJSON_GetObjectItem(jIndex, "columnList");
        int columnListLen = cJSON_GetArraySize(jColumnList);

        Index *idx = new Index(isUnique, whereClause);

        string genIndexName = "IDX_" + pLayout->name;
        for (int j = 0; j < columnListLen; j++) {
            cJSON *jIndexColumn = cJSON_GetArrayItem(jColumnList, i);
            string columnName =
                    cJSON_GetObjectItem(jIndexColumn, "name") != NULL ?
                            cJSON_GetObjectItem(jIndexColumn, "name")->valuestring :
                            "";
            string query =
                    cJSON_GetObjectItem(jIndexColumn, "query") != NULL ?
                            cJSON_GetObjectItem(jIndexColumn, "query")->valuestring :
                            "";
            if (columnName.length() < 1) {
                char str[20];
                sprintf(str, "%d", (j + 1));
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
            pLayout->fieldList[i].name = to_lower_copy(
                    jsonFieldName->valuestring);
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
                e += pLayout->fieldList[i].name;
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
                e += pLayout->fieldList[i].name;
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
        Field &field = layout.fieldList[i];
        if (field.isSkip != 1) {

            createTableQry += field.name;

            if (field.type == 'S' || field.type == 'D' || field.type == 'T') {
                createTableQry += " TEXT";
            }

            if (field.type == 'I') {
                createTableQry += " INTEGER";
            }

            if (field.type == 'R') {
                createTableQry += " REAL";
            }

            if (fieldCounter < layout.fieldListLen - 1) {
                createTableQry += ", ";
            }
        }
        if (i > 0)
            field.endOffSet = layout.fieldList[i - 1].endOffSet
                    + layout.fieldList[i].length;
        else
            field.endOffSet = field.length;
        recordLen += field.length;
        fieldCounter++;
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
            columnQry += "'" + field.name + "'";
            valueQry += "?";
            if (fieldCounter < layout.fieldListLen - 1) {
                columnQry += ", ";
                valueQry += ", ";
            }
        }
        fieldCounter++;
    }
    columnQry += ")";
    valueQry += ")";
    string insertBindQry;
    insertBindQry = "INSERT INTO '" + layout.name + "'" + columnQry + valueQry
            + ";";
    return insertBindQry;
}

int checkIfTableExist(sqlite3 *db, string tableNameToCheck) {
    int doesTableExist = 0;

#ifndef DISABLE_SQL_CODE
    int rc;
    sqlite3_stmt *sqlStmt;
    tableNameToCheck = to_lower_copy(tableNameToCheck);
    string queryStr =
            "SELECT count(name) FROM sqlite_master WHERE type='table' AND lower(name)='"
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

    string delQuery = "DROP table '" + layout.name + "'";
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

int createIndex(sqlite3 *db, const string tableName, const Index& index,
        const bool isDebug) {
#ifndef DISABLE_SQL_CODE
    int rc;
    char *zErrMsg = 0;

    string indexQry = "CREATE";

    if (index.isUnique) {
        indexQry += " UNIQUE";
    }
    indexQry += " INDEX '" + index.name + "' ON '" + tableName + "' (";

    int indexColListLen = index.indexColumnList.size();
    for (int i = 0; i < indexColListLen; i++) {
        IndexColumn ic = index.indexColumnList[i];
        indexQry += "'";
        indexQry += ic.name;
        indexQry += "'";
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
    string sqliteVersion = string(sqlite3_libversion());
    return "\nUsing Sqlite Version: " + sqliteVersion
            + "\n\nLayout definition:"
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
                    "\n   missingValue: If this is the value then it will be replaced with null in db"
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
    parser.add_option("-c").dest("c").metavar("<N>").set_default("10000").help(
            "Number of records after which commit operation is called on the data base");
    parser.add_option("-d").dest("d").set_default("0").action("store_true").help(
            "Delete if table already exists");
    parser.add_option("-a").dest("a").set_default("0").action("store_true").help(
            "Append to existing table if it does exist");
    parser.add_option("-v").dest("v").set_default("0").action("store_true").help(
            "Debug mode");
    parser.add_option("-p").dest("p").metavar("<comma-separated-pragma-list>").help(
            "Comma separated pragma which ran before creation of DB.");

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
    string pragmaList = options["p"];

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
        pLayout = parseLayout(layoutFileName, argTableName, false);
    } catch (string& e) {
        cout << "\nError Occurred: " << e << endl;
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
        fprintf(stderr, "Can't open database file: %s\n", dbFileName.c_str());
        fprintf(stderr, "Can't open database sqlite error: %s\n",
                sqlite3_errmsg(db));
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

    if (pragmaList.length() > 0) {
        if (isDebug)
            cout << "Applying pragmaList=" << pragmaList << endl;
        rc = sqlite3_exec(db, pragmaList.c_str(), NULL, 0, &zErrMsg);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error running pragmaList. SQL error: %s\n",
                    zErrMsg);
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
    long commitCounter = 0;
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
                fprintf(stderr, "SQL error %d: reset failed\n", rc);
                return 1;
            }
        } else {
            fprintf(stderr, "SQL error %d: step failed\n", rc);
            return 1;
        }
        if (commitCounter == commitAfter) {
            commitCounter = 0;
            rc = sqlite3_exec(db, "END TRANSACTION;", NULL, 0, &zErrMsg);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
                return 1;
            }
            rc = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, 0, &zErrMsg);
            if (rc != SQLITE_OK) {
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
    if (isDebug) {
        printf("Inserted %ld records in %ld seconds with %.2f opts/sec \n",
                recordCounter, insertTimeInSecs,
                ((double) recordCounter / insertTimeInSecs));
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
        if (isDebug) {
            printf("Indexed %ld records in %ld seconds with %.2f opts/sec \n",
                    recordCounter, indexTimeInSecs,
                    ((double) recordCounter / indexTimeInSecs));
        }
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
    if (isDebug) {
        printf("Imported %ld records in %ld seconds with %.2f opts/sec \n",
                recordCounter, timeInSecs,
                ((double) recordCounter / timeInSecs));
        printf("DB file is at: %s\n", dbFileName.c_str());
    }
    delete pLayout;
    inFile.close();
}

