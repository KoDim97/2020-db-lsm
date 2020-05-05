package ru.mail.polis.kodim97;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable implements Table {
    
    private static final int LONG_BYTES = 8;
    
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();

    private int curSizeInBytes;

    public MemTable() {
        this.curSizeInBytes = 0;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(entry -> new Cell(entry.getKey(), entry.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value val = map.get(key);
        if (val == null) {
            curSizeInBytes += key.remaining() + value.remaining() + LONG_BYTES;
        } else {
            curSizeInBytes += value.remaining() - val.getData().remaining();
        }
        map.put(key.duplicate(), new Value(System.currentTimeMillis(), value.duplicate()));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value value = map.get(key);
        if (value == null) {
            curSizeInBytes += key.remaining() + LONG_BYTES;
        } else if (!value.isTombstone()) {
            curSizeInBytes -= value.getData().remaining();
        }
        map.put(key.duplicate(), new Value(System.currentTimeMillis()));
    }

    @Override
    public long getSizeInByte() {
        return curSizeInBytes;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void close() {
        map.clear();
        curSizeInBytes = 0;
    }
}
