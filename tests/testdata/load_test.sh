#!/bin/bash

create_load_test_file() {
    typeset tfn=$1
    typeset ofn=$2
    typeset num=$3
    typeset fc=$4
    typeset tf1="t1.out"
    typeset tf2="t2.out"

    if [[ $fc == "true" ]]; then
        rm $ofn;
    fi

    if [[ -e $ofn ]]; then
        echo "Skipping as $ofn file already exists"
        return;
    fi

    > $ofn
    for i in {1..100000}
    do
        cat $tfn >> $ofn
    done
}

forceCreate=$1
time create_load_test_file in_load_template.csv in_load.csv 10 $forceCreate

rm in_load.db; time ./sqliteloader -l in_load_full_layout.json -i in_load.csv -o in_load.db

printf "select rid11,name11,balance11,date111,date211,time111,time211,date311 from 'inl' limit 10;" | sqlite3 in_load.db
