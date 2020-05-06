package ru.mail.polis.kodim97;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {
    private final long timestamp;
    private final ByteBuffer data;

    Value(final long timestamp, @NotNull final ByteBuffer data) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.data = data;
    }

    Value(final long timestamp) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.data = null;
    }

    boolean isTombstone() {
        return data == null;
    }

    ByteBuffer getData() {
        assert !isTombstone();
        return data.asReadOnlyBuffer();
    }

    long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}
