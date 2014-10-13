# flume-regexp-interceptor

Flume interceptor that create new headers from other headers by regular expressions.

## Example

E.g. there are too tables in hadoop - `first`  and `second`.
Flume watches spool directory and uploads files to hdfs to destination `first` and `second` respectively.
Interceptor helps you determine table name.


Config example:

    agent.sources = src
    agent.sources.src.type = spooldir
    agent.sources.src.channels = channel
    agent.sources.src.spoolDir = /var/spool/flume
    agent.sources.src.fileHeader = true
    agent.sources.src.basenameHeader = true
    agent.sources.src.deletePolicy = immediate
    agent.sources.src.interceptors = table_detector
    agent.sources.src.interceptors.table_detector.type = org.apache.hadoop.flume.interceptor.HeaderRegexInterceptor$Builder
    agent.sources.src.interceptors.table_detector.header = basename
    agent.sources.src.interceptors.table_detector.regex = ^(?<table>[a-zA-Z\-]+)_.*$
    agent.sources.src.interceptors.table_detector.group.table = tablename

Interceptor get value of header `basename` and try to parse it with regex.
Then it adds new header `tablename` with value of named group `table`.
