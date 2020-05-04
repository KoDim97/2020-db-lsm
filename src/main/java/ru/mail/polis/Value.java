package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Optional;

public class Value implements Comparable<Value>{
    private final long timestamp;
    private final Optional<ByteBuffer> data;

    Value(final long timestamp, final ByteBuffer data) {
        this.timestamp = timestamp;
        this.data = Optional.of(data);
    }

    Value(final long timestamp) {
        this.timestamp = timestamp;
        this.data = Optional.empty();
    }

    boolean isTombstone() {
        return data.isEmpty();
    }

    @NotNull
    ByteBuffer getData() {
        return data.orElse(ByteBuffer.allocate(0));
    }

    @NotNull
    long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return Long.compare(timestamp, o.timestamp);
    }
}