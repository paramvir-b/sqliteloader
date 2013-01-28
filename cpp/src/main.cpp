/*
 * The code is available under LGPL3 license. License can be found at:
 * http://www.gnu.org/licenses/lgpl-3.0.txt 
 * The code will soon be available on github at
 * https://github.com/paramvir-b
 */

#include <iostream>
#include <fstream>
#include <string.h>
#include <stdlib.h>

#include "sqlite3.h"
#include "cJSON.h"
#include "OptionParser.h"

using namespace std;
using optparse::OptionParser;

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
}

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

int main(int argc, char **argv) {

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
    vector<string> args = parser.args();

    string layoutFileName = options["l"];
    string inputFileName = options["i"];
    long commitAfter = atol(options["c"].c_str());
    bool isDebug = atoi(options["d"].c_str()) == 1? true : false;

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
    string tableName = "t";
    int recordLen = 0;
    int fieldCounter = 0;
    int fieldListCount = cJSON_GetArraySize(fieldList);
    string createTableQry;
    createTableQry = "CREATE TABLE " + tableName + " (";
    string insertBindQry;
    insertBindQry = "INSERT INTO " + tableName + " VALUES (";
    for (int i=0; i<fieldListCount; i++) {
        cJSON *field=cJSON_GetArrayItem(fieldList,i);
        if(isDebug) cout<<cJSON_Print(field)<<endl;

        if(cJSON_GetObjectItem(field, "name") == NULL) {
            if(isDebug) cout<<"Name cannot be empty"<<endl;
            return 1;
        }

        if(cJSON_GetObjectItem(field, "length") == NULL) {
            if(isDebug) cout<<"Length cannot be empty"<<endl;
            return 1;
        }

        createTableQry += string(cJSON_GetObjectItem(field, "name")->valuestring);
        char * type = (char *)"text";
        if(cJSON_GetObjectItem(field, "type") != NULL)
            type = cJSON_GetObjectItem(field, "type")->valuestring;

        if(strcmp(type, "text") == 0)
            createTableQry += " TEXT";

        if(strcmp(type, "integer") == 0)
            createTableQry += " INTEGER";

        if(strcmp(type, "real") == 0)
            createTableQry += " REAL";

        insertBindQry += "?";
        if(fieldCounter < fieldListCount - 1) {
            insertBindQry += ", ";
            createTableQry += ", ";
        }

        recordLen += cJSON_GetObjectItem(field, "length")->valueint;
        fieldCounter++;
    }
    insertBindQry += ");";
    createTableQry += ");";
    if(isDebug) cout<<"createTableQry="<<createTableQry<<endl;
    if(isDebug) cout<<"insertBindQry="<<insertBindQry<<endl;

    ifstream inFile;
    inFile.open(inputFileName.c_str());

    string dbFileName = inputFileName + ".db";
    remove(dbFileName.c_str());

    int rc;
    sqlite3 *db;
    char *zErrMsg = 0;

    rc = sqlite3_open(dbFileName.c_str(), &db);
    if(rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return(1);
    }
    //sqlconn = sqlite3.connect(dbFileName);
    //cursor = sqlconn.cursor();
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


    string line;

    long recordCounter = 0;
    while(getline(inFile, line)) {
        // if(isDebug) cout<<"line="<<line<<endl;
        // string insertQry;
        // insertQry = "INSERT INTO " + tableName + " VALUES (";

        int index = 0;
        int fieldCounter = 0;
        for (int i=0; i<fieldListCount; i++) {
            cJSON *field=cJSON_GetArrayItem(fieldList,i);
            int fieldLength = cJSON_GetObjectItem(field, "length")->valueint;
            string value = line.substr(index, fieldLength);
            // if(isDebug) cout<<"valueRaw="<<value<<endl;

            char * type = (char *)"text";
            if(cJSON_GetObjectItem(field, "type") != NULL)
                type = cJSON_GetObjectItem(field, "type")->valuestring;

            //if(strcmp(type, "text") == 0) {
            if(type[0] == 't') {
                value = trim(value);
                // value = replaceAll(value, "'", "''");
                if(value.length() > 0) {
                    rc = sqlite3_bind_text(sqlStmt, fieldCounter + 1, value.c_str(), value.length(), SQLITE_TRANSIENT);
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: text binding failed\n", rc);
                        return 1;
                    }
                    // insertQry += "'" + value + "'";
                }
                else {
                    rc = sqlite3_bind_null(sqlStmt, fieldCounter + 1);
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: text null binding failed\n", rc);
                        return 1;
                    }
                    // insertQry += " NULL";
                }
            }

            //if(strcmp(type, "integer") == 0) {
            if(type[0] == 'i') {
                value = trim(value);
                value = trim(value, "0");
                if(value.length() > 0) {
                    rc = sqlite3_bind_int64(sqlStmt, fieldCounter + 1, atol(value.c_str()));
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: int binding failed value=%s\n", rc, value.c_str());
                        return 1;
                    }
                    // insertQry += value;
                }
                else {
                    rc = sqlite3_bind_null(sqlStmt, fieldCounter + 1);
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: int null binding failed\n", rc);
                        return 1;
                    }
                    // insertQry += " NULL";
                }
            }

            //if(strcmp(type, "real") == 0) {
            if(type[0] == 'r') {
                value = trim(value);
                value = trim(value, "0");
                if(value.length() > 0) {
                    rc = sqlite3_bind_double(sqlStmt, fieldCounter + 1, atof(value.c_str()));
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: double binding failed value=%s\n", rc, value.c_str());
                        return 1;
                    }
                    // insertQry += value;
                }
                else {
                    rc = sqlite3_bind_null(sqlStmt, fieldCounter + 1);
                    if(rc != SQLITE_OK) {
                        fprintf(stderr, "SQL error %d: double null binding failed\n", rc);
                        return 1;
                    }
                    // insertQry += " NULL";
                }
            }
            // if(isDebug) cout<<"valueAfter="<<value<<endl;

            if(fieldCounter < fieldListCount - 1)
                // insertQry += ", ";

            index += fieldLength;
            fieldCounter++;
        }

        // insertQry += ");";
        /*

        rc = sqlite3_exec(db, insertQry.c_str(), NULL, 0, &zErrMsg);
        if(rc != SQLITE_OK) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }
        */

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

        // if(isDebug) cout<<"insertQry="<<insertQry<<endl;
        recordCounter++;
        if(recordCounter % commitAfter == 0) {
            // if(isDebug) cout<<"Committing at recordCounter="<<recordCounter<<endl;
            rc = sqlite3_exec(db, "COMMIT;", NULL, 0, &zErrMsg);
            if(rc != SQLITE_OK) {
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
            }

            rc = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, 0, &zErrMsg);
            if(rc != SQLITE_OK) {
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
            }

            // sqlconn.commit();
        }
    }

    rc = sqlite3_finalize(sqlStmt);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error %d: reset failed\n", rc);
    }

    rc = sqlite3_exec(db, "COMMIT;", NULL, 0, &zErrMsg);
    if(rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }

    sqlite3_close(db);
    inFile.close();
    cJSON_Delete(root);
}

