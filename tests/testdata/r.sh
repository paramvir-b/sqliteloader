cnt=19
#head -$cnt in1.csv
rm in1.db; time head -$cnt in1.csv | ./sqliteloader -l in1_full_layout.json -o in1.db

cols="rid1, name1, balance1, date1, date2, time1, time2, date3, date4, time3, time4"
printf ".header on\n.mode column\nselect $cols from 'in';" | sqlite3 in1.db
#printf ".mode line\nselect $cols from 'in';" | sqlite3 in1.db

echo
printf "select $cols from 'in';" | sqlite3 in1.db > act_in1.csv

head -$cnt exp_in1.csv > t.csv
diff t.csv act_in1.csv
