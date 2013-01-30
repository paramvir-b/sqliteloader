Sample Record Info
==================

x.dat
100,000 records
Size: TODO

x.dat.db
100,000 records
Size: TODO

RESULTS
=======

No optimization
---------------
real    3m22.42s
user    0m12.04s
sys     0m21.81s

real    3m20.48s
user    0m11.91s
sys     0m21.50s

Sync Off
--------

real    1m16.42s
user    0m10.93s
sys     0m11.95s

real    1m16.27s
user    0m10.94s
sys     0m11.96s

Transaction Off
---------------

real    0m13.80s
user    0m8.55s
sys     0m0.08s

real    0m14.13s
user    0m8.57s
sys     0m0.08s

Prepared statement
------------------

real    0m8.99s
user    0m5.51s
sys     0m0.07s

real    0m9.06s
user    0m5.50s
sys     0m0.07s


5,000,000
1.9G
Imported 5000000 records in 105 seconds with 47619.05 opts/sec

Journal File
--------------

real    1m44.96s
user    1m0.02s
sys     0m4.17s

Journal Memory
--------------

Imported 5000000 records in 103 seconds with 48543.69 opts/sec

real    1m42.61s
user    0m59.06s
sys     0m4.25s

Journal Off
-----------

Imported 5000000 records in 102 seconds with 49019.61 opts/sec

real    1m42.73s
user    0m59.37s
sys     0m4.22s

Create Index
------------
Imported 5000000 records in 123 seconds with 40650.41 opts/sec

real    2m2.82s
user    1m10.15s
sys     0m5.46s

Imported 5000000 records in 123 seconds with 40650.41 opts/sec

real    2m3.16s
user    1m9.87s
sys     0m5.48s


247,705,965
93G Flat file
39G Un-indexed
43G Indexed on one field

10m for just parsing without sqlite calls

Using 3.7.5 sqlite lib
Tue Jan 29 22:39:24 CST 2013
Inserted 247705965 records in 5323 seconds with 46535.03 opts/sec
Indexed 247705965 records in 1239 seconds with 199924.10 opts/sec
Imported 247705965 records in 6562 seconds with 37748.55 opts/sec

real    1h49m21.66s
user    59m12.86s
sys     6m11.90s
Wed Jan 30 00:28:46 CST 2013

Using 3.7.15.2 lib
Wed Jan 30 02:21:18 CST 2013
Inserted 247705965 records in 4575 seconds with 54143.38 opts/sec
Indexed 247705965 records in 1184 seconds with 209211.12 opts/sec
Imported 247705965 records in 5759 seconds with 43011.98 opts/sec

real    1h35m59.05s
user    49m45.98s
sys     6m46.44s
Wed Jan 30 03:57:17 CST 2013

Wed Jan 30 08:30:56 CST 2013
Inserted 247705965 records in 6626 seconds with 37383.94 opts/sec
Indexed 247705965 records in 1200 seconds with 206421.64 opts/sec
Imported 247705965 records in 7829 seconds with 31639.54 opts/sec

real    2h10m29.04s
user    50m13.50s
sys     6m15.70s
Wed Jan 30 10:41:25 CST 2013

Wed Jan 30 11:32:51 CST 2013
Inserted 247705965 records in 5087 seconds with 48693.92 opts/sec
Indexed 247705965 records in 1186 seconds with 208858.32 opts/sec
Imported 247705965 records in 6273 seconds with 39487.64 opts/sec

real    1h44m32.52s
user    53m44.55s
sys     6m46.27s
Wed Jan 30 13:17:24 CST 2013

