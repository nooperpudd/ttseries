TT-series
=========

High performance engine to store Time-series data in Redis.

|travis| |appveyor| |codecov| |codacy| |requirements| |docs| |pypi| |status| |pyversion|


TT-series is based on redis sorted sets to store the time-series data, `Sorted set` store scores with
unique numbers under a single key, but it has a weakness to store records, only unique members are allowed
and trying to record a time-series entry with the same value as a previous will result in only updating the score.
So TT-series provide a solution to solve that problem.

TT series normally can support redis version > 3.0, and will support *redis 5.0* in the future.


Tips
----

**Max Store series length**

For 32 bit Redis on a 32 bit platform redis sorted sets can support maximum 2**32-1 members,
and for 64 bit redis on a 64 bit platform can support maximum 2*64-1 members.
But large amount of data would cause more CPU activity, so better keep a balance with length of records is
very important.

**Only Support Python 3.6**

Because python3.6 changed the dictionary implementation for better performance,
so in Python3.6 dictionaries are insertion ordered.

links: https://stackoverflow.com/questions/39980323/are-dictionaries-ordered-in-python-3-6

Install
-------

    pip install ttseries



Documentation
=============

Features
--------

    1. Support Data Serializer

    2. Support Data Compression

    3. In-order to void update previous records, Support Hash Set Time-series storage format.

    4. Support Numpy Ndarray data

    5. Support **max length** to auto to trim records



TT-series provide three implementation to support different kinds of time-series data type.

    - **RedisSimpleTimeSeries** : Normally only base on Sorted sets to store records, previous records will impact the new

    inserting records which are **NOT** unique numbers.

    - **RedisHashTimeSeries**: use Redis Sorted sets with Hashes to store time-series data, User don't need to consider the
    data repeatability with records, but sorted sets with hashes would take some extra memories to store the keys.

    - **RedisNumpyTimeSeries**: base on Redis Sorted sets to store records, support `numpy.ndarray` data type format
    to serializer data.



Usage
-----

**Serializer Data**:

Tt-series use `Msgpack` to serializer data, because compare with other data serializer solutions,
Msgpack provide a better performance solution to store data. If user don't want to use **Msgpack** to serializer data, just
inherit from `ttseries.BaseSerializer` class to implement the supported data serializers.


Prepare data records:

.. sourcecode:: python

    from datetime import datetime


    now = datetime.now()
    timestamp = now.timestamp()

    series_data = []

    for i in range(1000):
        series_data.append((timestamp+i,i))


Redis Client:

.. sourcecode:: python
    from redis import StrictRedis
    client = StrictRedis()


RedisSimpleTimeSeries && RedisHashTimeSeries && RedisNumpyTimeSeries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Three series data implementation provide the same functions and methods, in the usage will
provide the difference in the methods.

`Add records`

.. sourcecode:: python
    from ttseries import RedisSimpleTimeSeries

    simple_series = RedisSimpleTimeSeries(client=client)

    key = "TEST:SIMPLE"

    simple_series.add_many(key, series_data)



`count length`

get the length of the records or need just get the length from timestamp span.

.. sourcecode:: python

    # get the records length
    simple_series.length(key)

    # result: ...: 1000

    # get the records length from start timestamp and end timestamp
    simple_series.count(key, from_timestamp=timestamp, end_timestamp=timestamp+10)

    # result: ...: 11


`trim records`

trim the records as the ASC.

.. sourcecode:: python

    simple_series.trim(key,10) # trim 10 length of records


`delete timestamp span`

delete timestamp provide delete key or delete records from start timestamp to end timestamp
.. sourcecode:: python

    simple_series.delete(key) # delete key with all records

    simple_series.delete(key, start_timestamp=timestamp) # delete key form start timestamp

`Get Slice`

Get slice form records provide start timestamp and end timestamp with **ASC** or **DESC** ordered.

**Default Ordered**: **ASC**

If user want to get the timestamp great than (>) or less than (<) which not including the timestamp record.

just use `(timestamp` which support `<timestamp` or `>timestamp` sign format like this.

.. sourcecode:: python

    # get series data from start timestamp ordered as ASC.

    simple_series.get_slice(key, start_timestamp=timestamp, acs=True)

    # get series data from great than start timestamp order as ASC
    simple_series.get_slice(key, start_timestamp="("+str(timestamp), asc=True)

    # get series data from start timestamp and limit the numbers with 500
    time_series.get_slice(key,start_timestamp=timestamp,limit=500)


`iter`

yield item from records.

.. sourcecode:: python

    for item in simple_series.iter(key):
        print(item)



**RedisNumpyTimeSeries**

Numpy array support provide numpy.dtype or just arrays with data.

Use **numpy.dtype** to create records. must provide **timestamp column name** and  **dtype** parameters.

.. sourcecode:: python

    import numpy as np
    from ttseries import RedisNumpyTimeSeries

    dtype = [("timestamp","float64"),("value","i")]

    array = np.array(series_data,dtype=dtype)

    np_series = RedisNumpyTimeSeries(client=client, dtype=dtype,timestamp_column_name="timestamp")


Or just numpy array without dtype, but must provide **timestamp column index** parameter.

.. sourcecode:: python

    array = np.array(series_data)

    np_series = RedisNumpyTimeSeries(client=client, ,timestamp_column_index=0)



Benchmark
=========

just run `make benchmark-init`, after then start `make benchmark-test`.

---------------------------

Ubuntu Server 16.04.1 LTS 64

Linux VM-141-82-ubuntu 4.4.0-127-generic
CUP info
    processor	: 0
    vendor_id	: GenuineIntel
    cpu family	: 6
    model		: 79
    model name	: Intel(R) Xeon(R) CPU E5-26xx v4
    stepping	: 1
    microcode	: 0x1
    cpu MHz		: 2394.446
    cache size	: 4096 KB
    physical id	: 0
    siblings	: 1
    core id		: 0
    cpu cores	: 1
    apicid		: 0
    initial apicid	: 0
    fpu		: yes
    fpu_exception	: yes
    cpuid level	: 13
    wp		: yes
    flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss ht syscall nx lm constant_tsc rep_good nopl eagerfpu pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch kaiser bmi1 avx2 bmi2 rdseed adx xsaveopt
    bugs		: cpu_meltdown spectre_v1 spectre_v2 spec_store_bypass
    bogomips	: 4788.89
    clflush size	: 64
    cache_alignment	: 64
    address sizes	: 40 bits physical, 48 bits virtual


Memory info

    MemTotal:        1917168 kB
    MemFree:          605964 kB
    MemAvailable:    1608148 kB
    Buffers:          235512 kB
    Cached:           874660 kB
    SwapCached:            0 kB
    Active:           796924 kB
    Inactive:         371160 kB
    Active(anon):      60900 kB
    Inactive(anon):    21652 kB

Add Large amount of records benchmark tests.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


        ------------------------------------------------------------------------------------------------------ benchmark 'RedisHashTimeSeries': 6 tests ------------------------------------------------------------------------------------------------------
        Name (time in ms)                                              Min                   Max                  Mean             StdDev                Median                 IQR            Outliers       OPS            Rounds  Iterations
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        test_get_hash_timeseries_without_serializer[1000]           4.2381 (1.0)          7.2087 (1.0)          4.4481 (1.0)       0.2812 (1.0)          4.4062 (1.0)        0.1195 (1.0)          5;10  224.8141 (1.0)         221           1
        test_add_hash_timeseries_without_serializer[1000]          18.4634 (4.36)        21.9601 (3.05)        19.6037 (4.41)      0.8631 (3.07)        19.3947 (4.40)       0.7153 (5.99)         14;8   51.0108 (0.23)         53           1
        test_get_hash_timeseries_without_serializer[10000]         45.4610 (10.73)       54.1898 (7.52)        46.2645 (10.40)     1.8184 (6.47)        45.7754 (10.39)      0.3032 (2.54)          1;3   21.6148 (0.10)         22           1
        test_add_hash_timeseries_without_serializer[10000]        151.6246 (35.78)      158.4621 (21.98)      153.7811 (34.57)     2.4860 (8.84)       153.1871 (34.77)      3.2599 (27.28)         1;0    6.5028 (0.03)          7           1
        test_get_hash_timeseries_without_serializer[100000]       464.9249 (109.70)     482.7335 (66.97)      471.9193 (106.09)    7.3656 (26.19)      468.1012 (106.24)    10.8657 (90.92)         1;0    2.1190 (0.01)          5           1
        test_add_hash_timeseries_without_serializer[100000]     1,613.1363 (380.63)   1,796.1478 (249.16)   1,719.1033 (386.48)   81.0188 (288.08)   1,745.1521 (396.06)   143.0141 (>1000.0)       1;0    0.5817 (0.00)          5           1
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        ---------------------------------------------------------------------------------------------------- benchmark 'RedisNumpyTimeSeries': 6 tests -----------------------------------------------------------------------------------------------------
        Name (time in ms)                                            Min                   Max                  Mean              StdDev                Median                 IQR            Outliers       OPS            Rounds  Iterations
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        test_get_numpy_timeseries_serializer[1000]                4.3686 (1.0)          6.7763 (1.0)          4.6957 (1.0)        0.3250 (1.0)          4.5929 (1.0)        0.2431 (1.0)         25;19  212.9587 (1.0)         209           1
        test_add_numpy_timeseries_serializer[1000]               28.7861 (6.59)        50.0243 (7.38)        30.3772 (6.47)       3.6462 (11.22)       29.3694 (6.39)       0.8888 (3.66)          1;6   32.9194 (0.15)         34           1
        test_get_numpy_timeseries_serializer[10000]              45.4824 (10.41)       64.3312 (9.49)        49.9695 (10.64)      4.9119 (15.11)       48.3781 (10.53)      5.4138 (22.27)         2;1   20.0122 (0.09)         16           1
        test_add_numpy_timeseries_serializer[10000]             268.5749 (61.48)      275.9462 (40.72)      271.8456 (57.89)      2.9689 (9.13)       271.7817 (59.17)      4.7371 (19.48)         2;0    3.6786 (0.02)          5           1
        test_get_numpy_timeseries_serializer[100000]            470.8673 (107.78)     510.9136 (75.40)      489.5806 (104.26)    17.0964 (52.60)      492.4769 (107.23)    29.5768 (121.64)        2;0    2.0426 (0.01)          5           1
        test_add_numpy_timeseries_serializer[100000]          2,844.0156 (651.01)   3,251.2273 (479.80)   3,011.1005 (641.24)   170.2551 (523.86)   3,047.0660 (663.43)   263.1282 (>1000.0)       1;0    0.3321 (0.00)          5           1
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        --------------------------------------------------------------------------------------------------- benchmark ' RedisNumpyTimeSeries support numpy_dtype': 6 tests --------------------------------------------------------------------------------------------------
        Name (time in ms)                                             Min                   Max                  Mean              StdDev                Median                 IQR            Outliers       OPS            Rounds  Iterations
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        test_get_numpy_dtype_timeseries_serializer[1000]           4.4843 (1.0)          8.3673 (1.0)          4.9945 (1.0)        0.5078 (1.0)          4.8608 (1.0)        0.3034 (1.0)         18;17  200.2198 (1.0)         203           1
        test_add_numpy_dtype_timeseries_serializer[1000]          31.2367 (6.97)        36.3806 (4.35)        32.0748 (6.42)       0.9122 (1.80)        32.0078 (6.58)       0.7530 (2.48)          1;1   31.1771 (0.16)         31           1
        test_get_numpy_dtype_timeseries_serializer[10000]         47.7206 (10.64)       59.9121 (7.16)        51.3988 (10.29)      3.3596 (6.62)        50.6238 (10.41)      5.8274 (19.21)         5;0   19.4557 (0.10)         18           1
        test_add_numpy_dtype_timeseries_serializer[10000]        297.2527 (66.29)      304.0626 (36.34)      300.8233 (60.23)      3.3319 (6.56)       302.0702 (62.14)      6.3487 (20.93)         2;0    3.3242 (0.02)          5           1
        test_get_numpy_dtype_timeseries_serializer[100000]       458.7863 (102.31)     518.0780 (61.92)      473.4387 (94.79)     25.0700 (49.37)      464.3583 (95.53)     17.3442 (57.17)         1;1    2.1122 (0.01)          5           1
        test_add_numpy_dtype_timeseries_serializer[100000]     3,107.9100 (693.07)   3,389.3925 (405.07)   3,240.0696 (648.73)   108.2738 (213.23)   3,214.6302 (661.34)   155.5410 (512.67)        2;0    0.3086 (0.00)          5           1
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        ------------------------------------------------------------------------------------------------- benchmark 'RedisSimpleTimeSeries': 6 tests --------------------------------------------------------------------------------------------------
        Name (time in ms)                                        Min                   Max                  Mean             StdDev                Median                IQR            Outliers       OPS            Rounds  Iterations
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        test_get_simple_timeseries_serializer[1000]           4.3650 (1.0)          8.3641 (1.0)          4.7913 (1.0)       0.4306 (1.0)          4.6877 (1.0)       0.2422 (1.0)         22;24  208.7101 (1.0)         200           1
        test_simple_timeseries_serializer[1000]              11.3507 (2.60)        14.9411 (1.79)        11.8664 (2.48)      0.5057 (1.17)        11.6784 (2.49)      0.5205 (2.15)         10;3   84.2716 (0.40)         81           1
        test_get_simple_timeseries_serializer[10000]         46.5695 (10.67)       84.8253 (10.14)       53.4132 (11.15)     8.8485 (20.55)       49.8491 (10.63)     6.2105 (25.64)         2;2   18.7219 (0.09)         20           1
        test_simple_timeseries_serializer[10000]             97.0772 (22.24)      102.6981 (12.28)       98.6118 (20.58)     1.6252 (3.77)        98.2531 (20.96)     1.0087 (4.16)          1;1   10.1408 (0.05)         10           1
        test_get_simple_timeseries_serializer[100000]       479.6096 (109.88)     502.6342 (60.09)      489.0946 (102.08)    9.5128 (22.09)      487.0854 (103.91)   15.3937 (63.55)         1;0    2.0446 (0.01)          5           1
        test_simple_timeseries_serializer[100000]         1,085.5771 (248.70)   1,126.5896 (134.69)   1,106.5186 (230.94)   18.0360 (41.88)    1,099.1654 (234.48)   30.7080 (126.78)        2;0    0.9037 (0.00)          5           1
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        ------------------------------------------------------------------------------------------------------------------- benchmark 'RedisSimpleTimeSeries without serializer': 6 tests -------------------------------------------------------------------------------------------------------------------
        Name (time in us)                                                         Min                       Max                      Mean                  StdDev                    Median                     IQR            Outliers          OPS            Rounds  Iterations
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        test_iter_simple_timeseries_dumpy_serializer[10000]                   72.5270 (1.0)            229.6760 (1.62)            79.9218 (1.03)          10.5865 (1.62)            75.5030 (1.01)           6.6620 (4.49)      766;719  12,512.2378 (0.97)       7738           1
        test_iter_simple_timeseries_dumpy_serializer[1000]                    72.9570 (1.01)           142.1290 (1.0)             77.4991 (1.0)            6.5444 (1.0)             75.1095 (1.0)            2.0490 (1.38)     739;1475  12,903.3775 (1.0)        8132           1
        test_iter_simple_timeseries_dumpy_serializer[100000]                  73.8380 (1.02)           188.7950 (1.33)            77.9637 (1.01)           7.2878 (1.11)            75.7760 (1.01)           1.4840 (1.0)       295;748  12,826.4871 (0.99)       4436           1
        test_get_simple_timeseries_dumpy_serializer[10000]                    95.5740 (1.32)           531.1120 (3.74)           103.7966 (1.34)          13.8005 (2.11)            98.9130 (1.32)           6.4195 (4.33)      479;593   9,634.2310 (0.75)       5919           1
        test_get_simple_timeseries_dumpy_serializer[1000]                     96.1660 (1.33)           347.8859 (2.45)           103.5577 (1.34)          14.1824 (2.17)            98.6720 (1.31)           6.2768 (4.23)      449;572   9,656.4493 (0.75)       5965           1
        test_get_simple_timeseries_dumpy_serializer[100000]                   96.7310 (1.33)         1,858.3750 (13.08)          104.2567 (1.35)          33.3117 (5.09)            99.3050 (1.32)           5.1175 (3.45)      107;386   9,591.7085 (0.74)       3507           1
        test_add_simple_timeseries_without_serializer[1000]                9,486.0930 (130.79)      11,444.4020 (80.52)        9,842.7142 (127.00)       301.5482 (46.08)        9,788.1840 (130.32)       352.2820 (237.39)       17;2     101.5980 (0.01)         78           1
        test_add_simple_timeseries_without_serializer[10000]              92,445.7930 (>1000.0)     97,025.0480 (682.65)      94,254.4075 (>1000.0)    1,716.1013 (262.22)      93,785.5180 (>1000.0)    2,740.3620 (>1000.0)       4;0      10.6096 (0.00)         11           1
        test_add_simple_timeseries_without_serializer[100000]            945,746.0330 (>1000.0)    954,236.1020 (>1000.0)    950,219.0136 (>1000.0)    3,910.6886 (597.56)     951,603.1720 (>1000.0)    7,174.0500 (>1000.0)       2;0       1.0524 (0.00)          5           1
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


TODO
----

1. Support Redis 5.0

2. Support compress data

3. Support get slice chunk array data

Author
======

- Winton Wang

Donate
======


Contact
=======

Email: 365504029@qq.com


Reference
---------

    links: https://www.infoq.com/articles/redis-time-series


.. _Sorted set: https://github.com/agiliq/merchant/


.. |travis| image:: https://travis-ci.org/nooperpudd/ttseries.svg?branch=master
    :target: https://travis-ci.org/nooperpudd/ttseries

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/ntlhwaagr5dqh341/branch/master?svg=true
    :target: https://ci.appveyor.com/project/nooperpudd/ttseries

.. |codecov| image:: https://codecov.io/gh/nooperpudd/ttseries/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/nooperpudd/ttseries

.. |codacy| image:: https://api.codacy.com/project/badge/Grade/154fe60c6d2b4e59b8ee18baa56ad0a9
    :target: https://www.codacy.com/app/nooperpudd/ttseries?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=nooperpudd/ttseries&amp;utm_campaign=Badge_Grade

.. |pypi| image:: https://img.shields.io/pypi/v/ttseries.svg
    :target: https://pypi.python.org/pypi/ttseries

.. |status| image:: https://img.shields.io/pypi/status/ttseries.svg
    :target: https://pypi.python.org/pypi/ttseries

.. |pyversion| image:: https://img.shields.io/pypi/pyversions/ttseries.svg
    :target: https://pypi.python.org/pypi/ttseries

.. |requirements| image:: https://requires.io/github/nooperpudd/ttseries/requirements.svg?branch=master
    :target: https://requires.io/github/nooperpudd/ttseries/requirements/?branch=master

.. |docs| image:: https://readthedocs.org/projects/ttseries/badge/?version=latest
    :target: http://ttseries.readthedocs.io/en/latest/?badge=latest

