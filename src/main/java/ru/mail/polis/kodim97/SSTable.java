package ru.mail.polis.kodim97;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

public class SSTable implements Table {

    private static final Logger logger = Logger.getLogger(LsmDAO.class.getName());

    private static final int INT_BYTES = 4;
    private static final int LONG_BYTES = 8;

    private final FileChannel fileChannel;

    private final int numOfElements;
    private final int shiftToOffsetsArray;

    SSTable(@NotNull final File file) throws IOException {
        fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final int fileSize = (int)fileChannel.size();

        final ByteBuffer offsetBuf = ByteBuffer.allocate(INT_BYTES);
        fileChannel.read(offsetBuf, fileSize - INT_BYTES);
        numOfElements = offsetBuf.flip().getInt();

        shiftToOffsetsArray = fileSize - INT_BYTES * (1 + numOfElements);
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new SSTableIterator(from);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException("SSTable doesn't provide upsert operations!");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("SSTable doesn't provide remove operations!");
    }

    @Override
    public long getSizeInByte() {
        throw new UnsupportedOperationException("SSTable doesn't provide getSizeInByte operations!");
    }

    @Override
    public int size() {
        return numOfElements;
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
        } catch (IOException e) {
            logger.warning("The error happened when the file channel was closed");
        }
    }

    static void serialize(
            final File file,
            final Iterator<Cell> elementsIterator,
            final int size) throws IOException {

        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {

            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (elementsIterator.hasNext()) {
                final Cell cell = elementsIterator.next();
                final ByteBuffer key = cell.getKey();
                final Value value = cell.getValue();
                final int keySize = key.remaining();
                offsets.add(offset);
                offset += keySize + INT_BYTES * 2 + LONG_BYTES;

                fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(keySize).flip());

                fileChannel.write(key);

                fileChannel.write(ByteBuffer.allocate(Long.BYTES).putLong(value.getTimestamp()).flip());

                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(-1).flip());
                } else {
                    final ByteBuffer valueBuffer = value.getData();
                    final int valueSize = valueBuffer.remaining();
                    fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(valueSize).flip());
                    fileChannel.write(valueBuffer);
                    offset += valueSize;
                }
            }

            for (final Integer i : offsets) {
                fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(i).flip());
            }
            fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(size).flip());
        }
    }

    private int getOffset(final int position) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(buffer,shiftToOffsetsArray + position * Integer.BYTES);
        return buffer.flip().getInt();
    }

    private ByteBuffer getKey(final int position) throws IOException {
        final int keyLengthOffset = getOffset(position);

        final ByteBuffer keySizeBuf = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(keySizeBuf, keyLengthOffset);

        final ByteBuffer keyBuf = ByteBuffer.allocate(keySizeBuf.flip().getInt());
        fileChannel.read(keyBuf,keyLengthOffset + INT_BYTES);

        return keyBuf.flip();
    }

    private int getPosition(final ByteBuffer key) throws IOException {
        int left = 0;
        int right = numOfElements - 1;
        while (left <= right) {
            final int mid = (left + right) / 2;
            final ByteBuffer midValue = getKey(mid);
            final int cmp = midValue.compareTo(key);

            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private Cell get(final int position) throws IOException {

        int elementOffset = getOffset(position);

        final ByteBuffer key = getKey(position);

        elementOffset += Integer.BYTES + key.remaining();
        final ByteBuffer timestampBuf = ByteBuffer.allocate(LONG_BYTES);
        fileChannel.read(timestampBuf, elementOffset);

        final ByteBuffer valueSizeBuf = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(valueSizeBuf, elementOffset + Long.BYTES);
        final int valueSize = valueSizeBuf.flip().getInt();

        final Value value;
        if (valueSize == -1) {
            value = new Value(timestampBuf.flip().getLong());
        } else {
            final ByteBuffer valueBuf = ByteBuffer.allocate(valueSize);
            fileChannel.read(valueBuf, elementOffset + LONG_BYTES + INT_BYTES);
            valueBuf.flip();
            value = new Value(timestampBuf.flip().getLong(), valueBuf);
        }

        return new Cell(key, value);
    }

    class SSTableIterator implements Iterator<Cell> {

        private int position;

        public SSTableIterator(final ByteBuffer from) {
            try {
                position = getPosition(from.rewind());
            } catch (IOException e) {
                logger.info("Iterator cannot get 'from' position in SStable");
            }
        }

        @Override
        public boolean hasNext() {
            return position < numOfElements;
        }

        @Override
        public Cell next() {
            try {
                return get(position++);
            } catch (IOException e) {
                logger.info("Iterator cannot get a cell in SStable");
                throw new RuntimeException(e);
            }
        }
    }
}
