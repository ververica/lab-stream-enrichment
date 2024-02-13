# Stream Enrichment Lab Solutions

Enrichment Function classes are located in the package `come.ververica.enrich.solution`.
To use these Function classes in the job class, copy the two lines of each solution into your 
[StreamEnrichmentJobExercise](src/main/java/com/ververica/enrich/StreamEnrichmentJobExercise.java)

```
String enrichMethod = "perRecordLookupSync"
enrichedStream = orderStream.map(new EnrichOrderSyncFunction(dbClientParameter));
```

```
String enrichMethod = "perRecordLookupSyncCache"
enrichedStream = orderStream.map(new EnrichOrderSyncWithCacheFunction(dbClientParameter,10000));
```

```
String enrichMethod = "perRecordLookupAsync"
enrichedStream = AsyncDataStream.unorderedWait(
        orderStream,
        new EnrichOrderAsyncFunction(dbClientParameter),
        1000, TimeUnit.MILLISECONDS, 100
);
```

```
String enrichMethod = "perRecordLookupAsyncCache"
enrichedStream = AsyncDataStream.unorderedWait(
        orderStream,
        new EnrichOrderAsyncWithCacheFunction(dbClientParameter,10000),
        1000, TimeUnit.MILLISECONDS, 100
);
```

```
String enrichMethod = "loadEntireDBThenEnrich"
enrichedStream = orderStream.map(new LoadEntireDBThenEnrichOrderFunction(dbClientParameter));
```

```
String enrichMethod = "loadCustomPartitionThenEnrich"
enrichedStream = orderStream.partitionCustom(new LoadCustomPartitionThenEnrichOrderFunction.CustomerIdPartitioner(),
                    order -> order.getCustomerId() )
            .map(new LoadCustomPartitionThenEnrichOrderFunction(dbClientParameter));
```

```
String enrichMethod = "loadKeyGroupPartitionThenEnrich"
enrichedStream = keyedOrderStream.map(new LoadKeyGroupPartitionThenEnrichOrderFunction(dbClientParameter));
```

```
String enrichMethod = "periodicLoadKeyGroupPartitionThenEnrich"
enrichedStream = keyedOrderStream.process(new PeriodicLoadThenEnrichOrderFunction(dbClientParameter, "PT1M"));
```

```
String enrichMethod = "streamingJoinOnProcessingTime"
enrichedStream = keyedOrderStream.connect(keyedCustomerChangeLogStream)
            .process(new OrderCustomerCoProcessFunction());
```

```
String enrichMethod = "streamingJoinOnProcessingTimeWait"
enrichedStream = keyedOrderStream.connect(keyedCustomerChangeLogStream)
            .process(new OrderCustomerCoProcessWaitFunction("PT10S"));
```

```
String enrichMethod = "streamingJoinOnEventTime"
enrichedStream = keyedOrderStream.connect(keyedCustomerChangeLogStream)
            .process(new OrderCustomerCoProcessOnEventTimeFunction());

```

See also [StreamEnrichmentJob](src/main/java/com/ververica/enrich/solution/StreamEnrichmentJob.java) where all
enrichment methods are included. This class accepts two additional main arguments. For example,

    --local false \
    --enrich-method periodicLoadKeyGroupPartitionThenEnrich

Exactly which enrichment method is used is determined by the passed command line parameters.



