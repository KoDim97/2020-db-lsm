package ru.mail.polis.kodim97;

import com.google.common.collect.Iterators;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class LsmDAO implements DAO {

    private static final Logger logger = LoggerFactory.getLogger(LsmDAO.class);

    private static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private static final String FILE_POSTFIX = ".dat";
    private static final String TEMP_FILE_POSTFIX = ".tmp";

    @NonNull
    private final File storage;
    private final int flushThreshold;

    private MemTable memtable;
    private final NavigableMap<Integer, Table> ssTables;

    private int generation;

    /**
     * LSM DAO implementation.
     * @param storage - the directory where SSTables stored.
     * @param flushThreshold - amount of bytes that need to flush current memory table.
     */
    public LsmDAO(
            @NotNull final File storage,
            final int flushThreshold) throws IOException {
        this.storage = storage;
        this.flushThreshold = flushThreshold;
        this.memtable = new MemTable();
        this.ssTables = new TreeMap<>();
        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(file -> !file.toFile().isDirectory() && file.toString().endsWith(FILE_POSTFIX))
                    .forEach(file -> {
                        final String fileName = file.getFileName().toString();
                        try {
                            final int gen = Integer.parseInt(fileName.substring(0, fileName.indexOf(FILE_POSTFIX)));
                            generation = Math.max(gen, generation);
                            ssTables.put(gen, new SSTable(file.toFile()));
                        } catch (IOException e) {
                            logger.info("Something went wrong in SSTable ctor", e);
                        } catch (NumberFormatException e) {
                            logger.info("Unexpected name of SSTable file", e);
                        }
                    });
            ++generation;
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Cell> freshElements = freshCellIterator(from);
        final Iterator<Cell> aliveElements = Iterators
                .filter(freshElements, element -> !element.getValue().isTombstone());

        return Iterators.transform(aliveElements, element -> Record.of(element.getKey(), element.getValue().getData()));
    }

    private Iterator<Cell> freshCellIterator(@NotNull final ByteBuffer from) {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        iters.add(memtable.iterator(from));
        ssTables.descendingMap().values().forEach(ssTable -> {
            try {
                iters.add(ssTable.iterator(from));
            } catch (IOException e) {
                logger.info("Something went wrong when in freshCellIterator");
            }
        });

        final Iterator<Cell> mergedElements = Iterators.mergeSorted(
                iters,
                Cell.COMPARATOR
        );

        return Iters.collapseEquals(mergedElements, Cell::getKey);
    }

    private File serialize(final Iterator<Cell> iterator) throws IOException {
        final File file = new File(storage, generation + TEMP_FILE_POSTFIX);
        file.createNewFile();
        SSTable.serialize(file, iterator);
        final String newFileName = generation + FILE_POSTFIX;
        final File dst = new File(storage, newFileName);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        return dst;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memtable.upsert(key, value);
        if (memtable.getSizeInByte() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
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
        for (final Table table : ssTables.values()) {
            table.close();
        }
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Cell> freshElements = freshCellIterator(EMPTY_BUFFER);
        final File dst = serialize(freshElements);

        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(f -> !f.getFileName().toFile().toString().equals(dst.getName()))
                    .forEach(f -> {
                        try {
                            Files.delete(f);
                        } catch (IOException e) {
                            logger.info("Unable to delete file: " + f.getFileName().toFile().toString(), e);
                        }
                    });
        }
    }

    private void flush() throws IOException {
        final File dst = serialize(memtable.iterator(EMPTY_BUFFER));
        ++generation;
        ssTables.put(generation, new SSTable(dst));
        memtable.close();
    }
}
