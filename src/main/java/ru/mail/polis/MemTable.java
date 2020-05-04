package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable implements Table {

    private final static int INT_BYTES = 8;

    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();

    private long curSizeInBytes;

    public MemTable() {
        this.curSizeInBytes = 0;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(entry -> new Cell(entry.getKey(), entry.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        map.put(key, new Value(System.currentTimeMillis(), value));
        curSizeInBytes = key.remaining() + value.remaining() + INT_BYTES;
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        Value value = map.get(key);
        if (value != null && !value.isTombstone()) {
            curSizeInBytes -= value.getData().remaining();
        } else {
            curSizeInBytes += key.remaining() + INT_BYTES;
        }
        map.put(key, new Value(System.currentTimeMillis()));
    }

    public long getSizeInByte() {
        return curSizeInBytes;
    }

    @Override
    public int size() {
        return map.size();
    }
}