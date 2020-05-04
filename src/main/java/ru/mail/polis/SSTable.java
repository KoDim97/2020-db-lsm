package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SSTable implements Table {

    private static final int INT_BYTES = 4;
    private static final int LONG_BYTES = 8;

    private ByteBuffer elements;
    private final FileChannel fileChannel;

    private final int numOfElements;
    private final int shiftToOffsetsArray;


    SSTable(@NotNull final File file) throws IOException {
        fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final int fileSize = (int)fileChannel.size();

        final ByteBuffer byteBuffer = ByteBuffer.allocate(fileSize);
        fileChannel.read(byteBuffer);

        final int numOfElementsPosition = fileSize - INT_BYTES;

        ByteBuffer offsetBuf = ByteBuffer.allocate(INT_BYTES);
        fileChannel.read(offsetBuf, numOfElementsPosition);
        numOfElements = offsetBuf.flip().getInt();

        shiftToOffsetsArray = fileSize - INT_BYTES * (1 + numOfElements);
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return new SSTableIterator(from);
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        throw new UnsupportedEncodingException("SSTable doesn't provide upsert operations!");
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
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

    static void serialize(
            final File file,
            final Iterator<Cell> elementsIterator,
            int size) throws IOException {

        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {

            ByteBuffer offsetArrAndSizeBuffer = ByteBuffer.allocate(INT_BYTES * (size + 1));
            int offset = 0;
            while (elementsIterator.hasNext()) {
                final Cell cell = elementsIterator.next();
                final ByteBuffer key = cell.getKey();
                final Value value = cell.getValue();
                final int keySize = key.remaining();
                offsetArrAndSizeBuffer.putInt(offset);
                offset += keySize + INT_BYTES * 2 + LONG_BYTES;

                fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(keySize).rewind());
                fileChannel.write(key.duplicate());
                fileChannel.write(ByteBuffer.allocate(LONG_BYTES).putLong(value.getTimestamp()));
                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(-1).rewind());
                } else {
                    final ByteBuffer valueBuffer = value.getData();
                    final int valueSize = valueBuffer.remaining();
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(valueSize).rewind());
                    fileChannel.write(valueBuffer.duplicate());
                    offset += valueSize;
                }
            }
            fileChannel.write(offsetArrAndSizeBuffer.putInt(size).rewind());
        }
    }

    private ByteBuffer getPositionElement(final int position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(INT_BYTES);
        fileChannel.read(buffer,shiftToOffsetsArray + position * INT_BYTES);
        final int keyLengthOffset = buffer.flip().getInt();
        buffer.clear();

        fileChannel.read(buffer, keyLengthOffset);
        final int keySize = buffer.flip().getInt();

        final int keyOffset = keyLengthOffset + INT_BYTES;
        final ByteBuffer keyBuf = ByteBuffer.allocate(keySize);
        fileChannel.read(keyBuf,keyOffset);

        return keyBuf.flip();
    }

    private int getPosition(final ByteBuffer key) throws IOException {
        int left = 0;
        int right = numOfElements - 1;
        while (left <= right) {
            final int mid = (left + right) >>> 1;
            final ByteBuffer midValue = getPositionElement(mid);
            final int cmp = midValue.compareTo(key);

            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return numOfElements + 1;
    }

    private Cell get(final int position) throws IOException {
        final int lengthSizeOffset = shiftToOffsetsArray + position * INT_BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(INT_BYTES);
        fileChannel.read(buffer, lengthSizeOffset);
        final int elementOffset = buffer.flip().getInt();
        buffer.clear();

        fileChannel.read(buffer, elementOffset);
        final int keySize = buffer.flip().getInt();
        final int keyOffset = elementOffset + INT_BYTES;
        buffer.clear();


        final ByteBuffer key = ByteBuffer.allocate(keySize);
        fileChannel.read(key, keyOffset);
        key.flip();

        final int timestampOffset = keyOffset + keySize;
        ByteBuffer timestampBuf = ByteBuffer.allocate(LONG_BYTES);
        fileChannel.read(timestampBuf, timestampOffset);
        final long timestamp = timestampBuf.flip().getLong();

        final int valueSizeOffset = timestampOffset + LONG_BYTES;
        fileChannel.read(buffer, valueSizeOffset);
        final int valueSize = buffer.flip().getInt();

        final Value value;
        if (valueSize == -1) {
            value = new Value(timestamp);
        } else {
            final int valueOffset = timestampOffset + LONG_BYTES + INT_BYTES;
            ByteBuffer valueBuf = ByteBuffer.allocate(valueSize);
            fileChannel.read(valueBuf, valueOffset);
            valueBuf.flip();
            value = new Value(timestamp, valueBuf);
        }

        return new Cell(key, value);
    }

    class SSTableIterator implements Iterator<Cell> {

        private int position;

        public SSTableIterator(final ByteBuffer from) {
            try {
                position = getPosition(from.rewind());
            } catch (IOException e) {
                e.printStackTrace();
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
                throw new NoSuchElementException();
            }
        }
    }
}