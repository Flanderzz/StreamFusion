package tech.streamfusion.compat;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;

public record LookupAsyncOptions(
    int asyncBufferCapacity,
    long asyncTimeout,
    boolean keyOrdered,
    AsyncDataStream.OutputMode asyncOutputMode) {}
