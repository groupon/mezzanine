# Mezzanine
**Mezzanine is a library built on Spark Streaming used to consume data from Kafka and store it into Hadoop.**

This library was built to replace the batch-based model of Kafka consumption, where jobs would be launched periodically to consume and persist large amounts of data at a time. Mezzanine contains logic for transforming, partitioning, and compacting the consumed Kafka data to persist them in HDFS. It was built with [Baryon](https://github.com/groupon/baryon) to handle the Kafka consumption, but Mezzanine can still be used as library with other methods for consuming from Kafka.

See the [wiki](https://github.com/groupon/mezzanine/wiki) for a full guide to using Mezzanine.

## Quick Start
To use this library, add the dependency to `mezzanine` in your project:
```xml
<dependency>
    <groupId>com.groupon.dse</groupId>
    <artifactId>mezzanine</artifactId>
    <version>1.0</version>
</dependency>
```

Include Baryon to use the `MezzaninePlugin` that comes with Mezzanine:
```xml
<dependency>
    <groupId>com.groupon.dse</groupId>
    <artifactId>baryon</artifactId>
    <version>1.0</version>
</dependency>
```

