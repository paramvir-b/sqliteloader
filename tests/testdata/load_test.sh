#!/bin/bash

create_load_test_file() {
    typeset tfn=$1
    typeset ofn=$2
    typeset num=$3
    typeset fc=$4
    typeset tf1="t1.out"
    typeset tf2="t2.out"

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
 #   for i in {1..10000}
    for i in $(seq 1 $num);
    do
        cat $tfn >> $ofn
    done
    echo "Done creating $ofn"
}

run_sqlite_loader() {
    typeset workDir=$1
    typeset layoutFile=$2
    typeset inputFile=$3
    typeset expOutputFile=$4
    typeset inputOnlyFileName=${inputFile##*/}
    typeset outputFile=$workDir/${inputOnlyFileName%%.*}.db
    typeset actOutputFile=$workDir/${inputOnlyFileName%%.*}.act_csv
    
    if [[ ! -e $inputFile ]]; then
        echo "Input file '"$inputFile"' is not accessible"
        exit 1;
    fi

    echo outputFile=$outputFile

    rm -f $outputFile;
    echo time ./sqliteloader -l $layoutFile -i $inputFile -t t -o $outputFile
    time ./sqliteloader -l $layoutFile -i $inputFile -t t -o $outputFile

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

    run_sqlite_loader $workDir in_load_${type}_template_layout.json in_load_${type}_template.csv in_load_${type}_template.exp_csv
    time create_load_test_file in_load_${type}_template.csv $loadDir/in_load_${type}.csv $num $forceCreate
    time run_sqlite_loader $workDir in_load_${type}_template_layout.json $loadDir/in_load_${type}.csv
}

WORK_DIR=./out
LOAD_DIR=./load
forceCreate=$1

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

NUM_COUNT=100000
run_for_type $WORK_DIR $LOAD_DIR "text" $NUM_COUNT 
run_for_type $WORK_DIR $LOAD_DIR "integer" $NUM_COUNT
run_for_type $WORK_DIR $LOAD_DIR "real" $NUM_COUNT
run_for_type $WORK_DIR $LOAD_DIR "date" $NUM_COUNT
run_for_type $WORK_DIR $LOAD_DIR "date_pivot" $NUM_COUNT
#time create_load_test_file in_load_template.csv in_load.csv 10 $forceCreate

#rm in_load.db; time ./sqliteloader -l in_load_full_layout.json -i in_load.csv -o in_load.db

#printf "select rid11,name11,balance11,date111,date211,time111,time211,date311 from 'inl' limit 10;" | sqlite3 in_load.db
