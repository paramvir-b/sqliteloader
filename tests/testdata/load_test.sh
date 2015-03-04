#!/bin/bash

create_load_test_file() {
    typeset workDir=$1
    typeset tfn=$2
    typeset ofn=$3
    typeset num=$4
    typeset fc=$5
    typeset tf1=$workDir/t1.out
    typeset tf2=$workDir/t2.out

    if [[ $fc == "true" ]]; then
        echo "Removing $ofn file"
        rm $ofn;
    fi

    if [[ -e $ofn ]]; then
        echo "Skipping as $ofn file already exists"
        return;
    fi

    echo "Creating $ofn"
    > $ofn
    > $tf2
    cp $tfn $tf1
    for i in $(seq 1 $num);
    do
        cat $tf1 $tf1 >> $tf2
        mv $tf2 $tf1
    done

    mv $tf1 $ofn
    echo "Done creating $ofn"
}

run_sqlite_loader() {
    typeset workDir=$1
    typeset layoutFile=$2
    typeset inputFile=$3
    typeset expOutputFile=$4
    typeset readBufferSize=$5
    typeset pragmaValues="$6"
    typeset inputOnlyFileName=${inputFile##*/}
    typeset outputFile=$workDir/${inputOnlyFileName%%.*}.db
    typeset actOutputFile=$workDir/${inputOnlyFileName%%.*}.act_csv
    
    if [[ ! -e $inputFile ]]; then
        echo "Input file '"$inputFile"' is not accessible"
        exit 1;
    fi

    if [[ ! -e ../../cpp/bin/sqliteloader ]]; then
        echo "Sqliteloader is missing. Try compiliing it"
        exit 1;
    else
        if [[ ! -e ./sqliteloader ]]; then
            ln -s ../../cpp/bin/sqliteloader sqliteloader
        fi
    fi

    echo outputFile=$outputFile

    rm -f $outputFile;
    if [[ $pragmaValues == "" ]]; then
        echo "time ./sqliteloader -v -l $layoutFile -i $inputFile -t t -o $outputFile ${readBufferSize:+-b $readBufferSize}"
        time ./sqliteloader -v -s -l $layoutFile -i $inputFile -t t -o $outputFile ${readBufferSize:+-b $readBufferSize}
    else
        echo "time ./sqliteloader -v -l $layoutFile -i $inputFile -t t -o $outputFile ${readBufferSize:+-b $readBufferSize} -p $pragmaValues"
        time ./sqliteloader -v -s -l $layoutFile -i $inputFile -t t -o $outputFile ${readBufferSize:+-b $readBufferSize} -p "$pragmaValues"
    fi

    if [[ -e $expOutputFile ]]; then
        echo "Comparing $outputFile against expected file : $expOutputFile"
        printf "select * from t;" | sqlite3 $outputFile > $actOutputFile
        echo diff $expOutputFile $actOutputFile
        diff $expOutputFile $actOutputFile
        rc=$?
        if (( rc == 0 )); then
            echo "No diff found"
        else
            echo "Diff found"
            exit 1
        fi
    else
        echo "Nothing to compare against."
    fi
    echo "Output at: $outputFile "
}

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
