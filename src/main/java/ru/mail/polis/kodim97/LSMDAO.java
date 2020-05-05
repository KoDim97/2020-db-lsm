package ru.mail.polis.kodim97;

import com.google.common.collect.Iterators;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Stream;


public class LSMDAO implements DAO {

    private static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private static final String FILE_POSTFIX = ".dat";
    private static final String TEMP_FILE_POSTFIX = ".tmp";

    @NonNull
    private final File storage;
    private final int flushThreshold;

    private MemTable memtable;
    private final NavigableMap<Integer, Table> ssTables;

    private int generation = 0;

    public LSMDAO(
            @NotNull final File storage,
            final int flushThreshold) throws IOException {
        this.storage = storage;
        this.flushThreshold = flushThreshold;
        this.memtable = new MemTable();
        this.ssTables = new TreeMap<>();
        try (final Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(file -> !file.toFile().isDirectory() && file.toString().endsWith(FILE_POSTFIX))
                    .forEach(file -> {
                        final String fileName = file.getFileName().toString();
                        try {
                            final int gen = Integer.parseInt(fileName.substring(0, fileName.indexOf(FILE_POSTFIX)));
                            generation = Math.max(gen, generation);
                            ssTables.put(gen, new SSTable(file.toFile()));
                        } catch (IOException e) {
                            //log
                        }
                    });
            generation++;
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
                //log
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
        if (memtable.getSizeInByte() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memtable.remove(key);
        if (memtable.getSizeInByte() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (memtable.size() > 0) {
            flush();
        }
        ssTables.values().forEach(Table::close);
    }

    private void flush() throws IOException {
        final File file = new File(storage, generation + TEMP_FILE_POSTFIX);
        SSTable.serialize(file, memtable.iterator(EMPTY_BUFFER), memtable.size());
        final File dst = new File(storage, generation + FILE_POSTFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ++generation;
        ssTables.put(generation, new SSTable(dst));
        memtable.close();
    }
}