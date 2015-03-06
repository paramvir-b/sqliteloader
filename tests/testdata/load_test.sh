#!/bin/bash
. common.sh

run_for_type() {
    typeset workDir=$1
    typeset loadDir=$2
    typeset type=$3
    typeset num=$4
    typeset readBufferSize=$5
    typeset pragmaValues="$6"
    typeset loadFile=$loadDir/in_load_${type}_${num}.csv

    run_sqlite_loader $workDir in_load_${type}_template_layout.json in_load_${type}_template.csv in_load_${type}_template.exp_csv "$readBufferSize" "$pragmaValues"
    time create_load_test_file $workDir in_load_${type}_template.csv $loadFile $num $forceCreate
    time run_sqlite_loader $workDir in_load_${type}_template_layout.json $loadFile "" "$readBufferSize" "$pragmaValues"
}

usage() {
    echo "Usage: $0 [-f] [-c <N>] [-b <N>] [-p <comma-separated-pragmas>]"
    echo "-f      Force create input csv files"
    echo "-c      Number of times sample input files be repeated"
    echo "-b      Read buffer size to be passed to sqliteloader. Refer to -b of sqliteloader"
    echo "-p      Pragma values passed to sqliteloader. Refer to -p of sqliteloader"
}

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

run_sqlite_loader $WORK_DIR in1_big_full_layout.json in1_big.csv in1_big_exp.csv "$READ_BUFFER_SIZE" "$PRAGMA_VALUES"
run_sqlite_loader $WORK_DIR in1_big_full_no_skip_layout.json in1_big_no_skip.csv in1_big_no_skip_exp.csv "$READ_BUFFER_SIZE" "$PRAGMA_VALUES"
run_for_type $WORK_DIR $LOAD_DIR "text" $NUM_COUNT "$READ_BUFFER_SIZE" "$PRAGMA_VALUES" 
run_for_type $WORK_DIR $LOAD_DIR "integer" $NUM_COUNT "$READ_BUFFER_SIZE" "$PRAGMA_VALUES"
run_for_type $WORK_DIR $LOAD_DIR "real" $NUM_COUNT "$READ_BUFFER_SIZE" "$PRAGMA_VALUES"
run_for_type $WORK_DIR $LOAD_DIR "date" $NUM_COUNT "$READ_BUFFER_SIZE" "$PRAGMA_VALUES"
run_for_type $WORK_DIR $LOAD_DIR "date_epoch" $NUM_COUNT "$READ_BUFFER_SIZE" "$PRAGMA_VALUES"
run_for_type $WORK_DIR $LOAD_DIR "date_time" $NUM_COUNT "$READ_BUFFER_SIZE" "$PRAGMA_VALUES"
run_for_type $WORK_DIR $LOAD_DIR "date_pivot" $NUM_COUNT "$READ_BUFFER_SIZE" "$PRAGMA_VALUES"


#time create_load_test_file in_load_template.csv in_load.csv 10 $forceCreate

#rm in_load.db; time ./sqliteloader -l in_load_full_layout.json -i in_load.csv -o in_load.db

#printf "select rid11,name11,balance11,date111,date211,time111,time211,date311 from 'inl' limit 10;" | sqlite3 in_load.db
