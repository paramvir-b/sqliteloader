
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
    typeset createTableName="$4"
    typeset expTableName="$5"
    typeset expOutputFile=$6
    typeset readBufferSize=$7
    typeset pragmaValues="$8"
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

    if [[ $createTableName != "" ]]; then
        typeset createTableNameCmd="-t $createTableName"
    fi
    if [[ $expTableName == "" ]]; then
        expTableName="$createTableName"
    fi

    echo outputFile=$outputFile

    rm -f $outputFile;
    if [[ $pragmaValues == "" ]]; then
        echo "time ./sqliteloader -v -l $layoutFile -i $inputFile $createTableNameCmd -o $outputFile ${readBufferSize:+-b $readBufferSize}"
        time ./sqliteloader -v -s -l $layoutFile -i $inputFile $createTableNameCmd -o $outputFile ${readBufferSize:+-b $readBufferSize}
        rc=$?
        if (( rc != 0 )); then
            "sqliteloader command failed"
            exit 1
        fi
    else
        echo "time ./sqliteloader -v -l $layoutFile -i $inputFile $createTableNameCmd -o $outputFile ${readBufferSize:+-b $readBufferSize} -p $pragmaValues"
        time ./sqliteloader -v -s -l $layoutFile -i $inputFile $createTableNameCmd -o $outputFile ${readBufferSize:+-b $readBufferSize} -p "$pragmaValues"
        rc=$?
        if (( rc != 0 )); then
            "sqliteloader command faild"
            exit 1
        fi
    fi

    if [[ -e $expOutputFile ]]; then
        echo "Comparing $outputFile against expected file : $expOutputFile with expTableName=$expTableName"
        printf "select * from \"$expTableName\";\n"
        printf "select * from \"$expTableName\";" | sqlite3 $outputFile > $actOutputFile
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
