# Flume Adapter

Redis Source
-------

For example, a redis source for agent named a1:
```
# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe the source
a1.sources.r1.type = com.asiainfo.ocdp.flume.source.redis.RedisSource
a1.sources.r1.redis.schema = A1,A2,A3
a1.sources.r1.redis.keyPrefix = siteposition:
a1.sources.r1.redis.interval = 5000
a1.sources.r1.redis.flumeBatchSize = 6
a1.sources.r1.redis.host = host1
a1.sources.r1.redis.port = 6379

# Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.fileType=DataStream
a1.sinks.k1.hdfs.path = /flume/events/redis
a1.sinks.k1.hdfs.filePrefix = events
a1.sinks.k1.hdfs.fileSuffix = .log
#2G
a1.sinks.k1.hdfs.rollSize = 2048000000
a1.sinks.k1.hdfs.batchSize = 1000
a1.sinks.k1.hdfs.rollInterval = 0
a1.sinks.k1.hdfs.rollCount = 0
a1.sinks.k1.hdfs.idleTimeout=21
#must set minBlockReplicas to 1
a1.sinks.k1.hdfs.minBlockReplicas=1
a1.sinks.k1.hdfs.callTimeout=300000

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 100000
a1.channels.c1.transactionCapacity = 10000

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```