package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTable implements Table {

    private static final int INT_BYTES = 4;
    private static final int LONG_BYTES = 8;

    private int[] offsetArr;
    private final ByteBuffer elements;
    
    private final int elementsSize;
    private final int numOfElements;

    SSTable(@NotNull final File file) throws IOException {
        try (FileChannel fileChannel =  FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            final int fileSize = (int)fileChannel.size();

            final ByteBuffer byteBuffer = ByteBuffer.allocate(fileSize);
            fileChannel.read(byteBuffer);

            final int numOfElementsPosition = fileSize - INT_BYTES;
            numOfElements = byteBuffer.getInt(numOfElementsPosition);

            final int offsetArrPosition = fileSize - INT_BYTES - INT_BYTES * numOfElements;
            IntBuffer offsetArrBuffer = byteBuffer
                    .duplicate()
                    .position(offsetArrPosition)
                    .limit(numOfElementsPosition)
                    .slice()
                    .asIntBuffer();

            offsetArr = new int[numOfElements];
            for (int i = 0; i < numOfElements; ++i) {
                offsetArr[i] = offsetArrBuffer.get(i);
            }


            elements = byteBuffer
                    .duplicate()
                    .position(0)
                    .limit(offsetArrPosition)
                    .slice()
                    .asReadOnlyBuffer();

            elementsSize = offsetArrPosition;
        }
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
            int amount) throws IOException {

        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {

            ByteBuffer offsetArrAndSizeBuffer = ByteBuffer.allocate(INT_BYTES * (amount + 1));

            while (elementsIterator.hasNext()) {
                final Cell cell = elementsIterator.next();
                final ByteBuffer key = cell.getKey();
                final Value value = cell.getValue();
                final int keySize = key.remaining();
                int offset = keySize + INT_BYTES * 2 + LONG_BYTES;

                fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(keySize));
                fileChannel.write(key);
                fileChannel.write(ByteBuffer.allocate(LONG_BYTES).putLong(value.getTimestamp()));
                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(-1));
                } else {
                    final int valueSize = value.getData().remaining();
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(valueSize));
                    fileChannel.write(value.getData());
                    offset += valueSize;
                }
                offsetArrAndSizeBuffer.putInt(offset);
            }
            offsetArrAndSizeBuffer.putInt(amount);
            fileChannel.write(offsetArrAndSizeBuffer);
        }
    }

    private ByteBuffer getByteBufferPosition(final int position) {
        final int elementOffset = offsetArr[position];
        final int keySize = elements.getInt(elementOffset);
        final int keyOffset = elementOffset + INT_BYTES;
        return elements
                .position(keyOffset)
                .limit(keyOffset + keySize)
                .slice();
    }

    private int getPosition(final ByteBuffer key) {
        int left = 0;
        int right = numOfElements - 1;
        while (left <= right) {
            final int mid = (left + right) >>> 1;
            ByteBuffer midValue = getByteBufferPosition(mid);
            final int cmp = midValue.compareTo(key);

            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }

        return -1;
    }

    private Cell get(final int position) {
        final int elementOffset = offsetArr[position];
        final int keySize = elements.getInt(elementOffset);
        final int keyOffset = elementOffset + INT_BYTES;
        final int timestampOffset = keyOffset + keySize;

        final ByteBuffer key = elements
                .position(keyOffset)
                .limit(timestampOffset)
                .slice();

        final long timestamp = elements.get(timestampOffset);
        final int valueSize = elements.getInt(timestampOffset + LONG_BYTES);

        final Value value;
        if (valueSize == -1) {
            value = new Value(timestamp);
        } else {
            final int valueOffset = timestampOffset + LONG_BYTES + INT_BYTES;
            final ByteBuffer valyeByteBuffer = elements
                    .position(valueOffset)
                    .limit(valueOffset + valueSize)
                    .slice();
            value = new Value(timestamp, valyeByteBuffer);
        }
        return new Cell(key, value);
    }

    class SSTableIterator implements Iterator<Cell> {

        private int position;

        public SSTableIterator(final ByteBuffer from) {
            position = getPosition(from);
        }

        @Override
        public boolean hasNext() {
            return position < elementsSize;
        }

        @Override
        public Cell next() {
            return get(position);
        }
    }
}