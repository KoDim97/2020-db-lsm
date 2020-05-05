package ru.mail.polis.kodim97;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class DAOImpl implements DAO {

    private final SortedMap<ByteBuffer, ByteBuffer> map;

    DAOImpl() {
         map = new TreeMap<>();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(element -> Record.of(element.getKey(), element.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }
}
