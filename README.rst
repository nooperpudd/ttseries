TTseries
========

High-performance engine to store Time-series data in Redis.

|travis| |appveyor| |codecov| |codacy| |requirements| |docs| |pypi| |status| |pyversion|


Install
=======

    pip install ttseries --upgrade


Documentation
=============

TT-series is based on redis sorted sets to store the time-series data,
Redis sorted sets can support maximum 2**32-1 members, more than 4 billion of
numbers per set.


Usage
=====

Redis Sorted sets have the data consistency principle,
For elements with the same timestamp or different timestamps
with the same data, but for the time-series data storage principle,
if the repeated data with different timestamps to store in redis
sorted sets, one element have been add to the sorted sets,
 but duplicated timestamp can't add to the sorted sets.

`RedisHashTimeSeries`


`RedisSimpleTimeSeries`

`RedisNumpyTimeSeries`

.. sourcecode:: python

    from datetime import datetime

    now = datetime.now()
    timestamp = now.timestamp()

    series_data = []

    for i in range(1000):
        series_data.append((timestamp+i,i))



.. sourcecode:: python

    from ttseries import RedisHashTimeSeries
    import redis

    client = redis.StrictRedis()
    time_series = RedisHashTimeSeries(client=client)

    key = "AAPL:TICK"
    time_series.add_many(key,series_data)


.. sourcecode:: python
    count = time_series.length(key)


.. sourcecode:: python

   records = time_series.get_slice(key,start_timestamp=timestamp,limit=500)



.. sourcecode:: python

    import numpy as np

    dtype = [("timestamp","float64"),("value","i")]

    array = np.array(series_data)

.. sourcecode:: python

    array = np.array(series_data,dtype=dtype)



Benchmark
=========

    add many function benchmark test

    1. add 1000 records
        `RedisHashTimeSeries`

        `RedisSimpleTimeSeries`

        `RedisNumpyTimeSeries`
    2. add 10000 records

         `RedisHashTimeSeries`

        `RedisSimpleTimeSeries`

        `RedisNumpyTimeSeries`

    3. add 100000 records

         `RedisHashTimeSeries`

        `RedisSimpleTimeSeries`

        `RedisNumpyTimeSeries`


    get slice function benchmark test


    1. get 1000 records

          `RedisHashTimeSeries`

        `RedisSimpleTimeSeries`

        `RedisNumpyTimeSeries`

    2. get 10000 records

          `RedisHashTimeSeries`

        `RedisSimpleTimeSeries`

        `RedisNumpyTimeSeries`


    3. get 100000 records

          `RedisHashTimeSeries`

        `RedisSimpleTimeSeries`

        `RedisNumpyTimeSeries`




``


Author
======

- Winton Wang

Donate
======


Contact
=======

Email: 365504029@qq.com





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

