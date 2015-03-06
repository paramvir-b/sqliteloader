#!/bin/bash

. common.sh

WORK_DIR=./out
LOAD_DIR=./load

while getopts "fc:b:p:h" opt;
do
    case $opt in
        f) forceCreate="true"
            ;;
        c) NUM_COUNT=$OPTARG
            ;;
        b) READ_BUFFER_SIZE=$OPTARG
            ;;
        p) PRAGMA_VALUES="$OPTARG"
            ;;
        h) usage
            exit 1
            ;;
        ?)
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))  #This tells getopts to move on to the next argument.

usage() {
    echo "Usage: $0 [-f] [-c <N>] [-b <N>] [-p <comma-separated-pragmas>]"
    echo "-f      Force create input csv files"
    echo "-c      Number of times sample input files be repeated"
    echo "-b      Read buffer size to be passed to sqliteloader. Refer to -b of sqliteloader"
    echo "-p      Pragma values passed to sqliteloader. Refer to -p of sqliteloader"
}

if [[ $NUM_COUNT != "" && $NUM_COUNT != [0-9]* ]]; then
    echo "-c must contain only numbers"
    exit 1
fi

if [[ $READ_BUFFER_SIZE != "" && $READ_BUFFER_SIZE != [0-9]* ]]; then
    echo "-b must contain only numbers"
    exit 1
fi

forceCreate=${forceCreate:-false}
NUM_COUNT=${NUM_COUNT:-19}

echo forceCreate=$forceCreate
echo NUM_COUNT=$NUM_COUNT
echo READ_BUFFER_SIZE=$READ_BUFFER_SIZE
echo PRAGMA_VALUES=$PRAGMA_VALUES

rm -rf $WORK_DIR
mkdir -p $WORK_DIR
if (( $? != 0 )); then
    echo "Unable to run test as not able to create work dir: $WORK_DIR"
    exit 1;
fi

if [[ ! -e $LOAD_DIR ]]; then
    mkdir -p $LOAD_DIR
    if (( $? != 0 )); then
        echo "Unable to run test as not able to create load dir: $LOAD_DIR"
        exit 1;
    fi
fi

loadFile=$LOAD_DIR/in1_big_no_skip_${NUM_COUNT}.csv
time create_load_test_file $WORK_DIR in1_big_no_skip.csv $loadFile $NUM_COUNT $forceCreate
run_sqlite_loader $WORK_DIR in1_big_full_no_skip_layout.json $loadFile "" "$READ_BUFFER_SIZE" "$PRAGMA_VALUES"

echo "Import using sqlite3"
time sqlite3 << !
.echo on
.open $WORK_DIR/in1_big_no_skip_import_${NUM_COUNT}.db
CREATE TABLE 't' ( miss1 TEXT, rid1 INTEGER, rid2 INTEGER, rid3 INTEGER, rid4 INTEGER, rid5 INTEGER, rid6 INTEGER, rid7 INTEGER, rid8 INTEGER, name1 TEXT, name2 TEXT, name3 TEXT, name4 TEXT, name5 TEXT, name6 TEXT, name7 TEXT, name8 TEXT, balance1 REAL, balance2 REAL, balance3 REAL, balance4 REAL, balance5 REAL, balance6 REAL, balance7 REAL, balance8 REAL, date1 TEXT, date2 TEXT, time1 TEXT, time2 TEXT, date3 TEXT, date4 TEXT, time3 TEXT, time4 TEXT);
.separator "\t"
.import $loadFile t
!
