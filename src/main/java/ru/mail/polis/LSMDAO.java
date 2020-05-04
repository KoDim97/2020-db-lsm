package ru.mail.polis;

import com.google.common.collect.Iterators;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Stream;


public class LSMDAO implements DAO {

    private static final String FILE_POSTFIX = ".dat";
    private static final String TEMP_FILE_POSTFIX = ".tmp";

    @NonNull
    private final File storage;
    private final long flushThreshold;

    private Table memtable;
    private final NavigableMap<Integer, Table> ssTables;

    private int generation = 0;

    public LSMDAO(
            @NotNull final File storage,
            final long flushThreshold) throws IOException {
        this.storage = storage;
        this.flushThreshold = flushThreshold;
        this.memtable = new MemTable();
        this.ssTables = new TreeMap<>();
        try (final Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(file -> file.toString().endsWith(FILE_POSTFIX))
                    .forEach(file -> {
                        try {
                            final String fileName = file.toString();
                            final int gen = Integer.parseInt(fileName.substring(0, fileName.indexOf(FILE_POSTFIX)));
                            generation = Math.max(gen, generation);
                            ssTables.put(gen, new SSTable(file.toFile()));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        iters.add(memtable.iterator(from));
        ssTables.descendingMap().values().forEach(ssTable -> {
            try {
                iters.add(ssTable.iterator(from));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        final Iterator<Cell> mergedElements = Iterators.mergeSorted(iters, Cell.COMPARATOR);
        final Iterator<Cell> freshElements = Iters.collapseEquals(mergedElements, Cell::getKey);
        final Iterator<Cell> aliveElements = Iterators.filter(freshElements, element -> !element.getValue().isTombstone());

        return Iterators.transform(aliveElements, element -> Record.of(element.getKey(), element.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memtable.upsert(key, value);
        if (memtable.getSizeInByte() >= flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memtable.remove(key);
        if (memtable.getSizeInByte() >= flushThreshold) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (memtable.size() > 0) {
            flush();
        }
    }

    private void flush() throws IOException {
        String tmpName = generation + TEMP_FILE_POSTFIX;
        final File file = new File(storage, generation + TEMP_FILE_POSTFIX);
        SSTable.serialize(file, memtable.iterator(ByteBuffer.allocate(0)), memtable.size());
        final File dst = new File(storage, generation + FILE_POSTFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ++generation;
        ssTables.put(generation, new SSTable(dst));
        memtable = new MemTable();
    }
}