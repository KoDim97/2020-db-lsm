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
    private ByteBuffer elements;
    private final FileChannel fileChannel;
    
    private final int elementsSize;
    private final int numOfElements;

    SSTable(@NotNull final File file) throws IOException {
        fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final int fileSize = (int)fileChannel.size();

        final ByteBuffer byteBuffer = ByteBuffer.allocate(fileSize);
        fileChannel.read(byteBuffer);

        final int numOfElementsPosition = fileSize - INT_BYTES;
        numOfElements = byteBuffer.getInt(numOfElementsPosition);

        final int offsetArrPosition = fileSize - INT_BYTES - INT_BYTES * numOfElements;
        IntBuffer offsetArrBuffer = byteBuffer
                .rewind()
                .position(offsetArrPosition)
                .limit(numOfElementsPosition)
                .slice()
                .asIntBuffer();

        offsetArr = new int[numOfElements];
        for (int i = 0; i < numOfElements; ++i) {
            offsetArr[i] = offsetArrBuffer.get(i);
        }

        elements = byteBuffer
                .rewind()
                .position(0)
                .limit(offsetArrPosition)
                .slice()
                .asReadOnlyBuffer();

        elementsSize = offsetArrPosition;
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
                fileChannel.write(key);
                fileChannel.write(ByteBuffer.allocate(LONG_BYTES).putLong(value.getTimestamp()));
                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(-1).rewind());
                } else {
                    final ByteBuffer valueBuffer = value.getData();
                    final int valueSize = valueBuffer.remaining();
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(valueSize).rewind());
                    fileChannel.write(valueBuffer);
                    offset += valueSize;
                }
            }
            fileChannel.write(offsetArrAndSizeBuffer.putInt(size).rewind());
        }
    }

    private ByteBuffer getByteBufferPosition(final int position) {
        final int elementOffset = offsetArr[position];
        final int keySize = elements.getInt(elementOffset);
        final int keyOffset = elementOffset + INT_BYTES;
        return elements
                .duplicate()
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
        return elementsSize + 1;
    }

    private Cell get(final int position) {
        final int elementOffset = offsetArr[position];
        final int keySize = elements.getInt(elementOffset);
        final int keyOffset = elementOffset + INT_BYTES;
        final int timestampOffset = keyOffset + keySize;

        final ByteBuffer key = elements
                .duplicate()
                .position(keyOffset)
                .limit(timestampOffset)
                .slice();

        final long timestamp = elements.getLong(timestampOffset);
        final int valueSize = elements.getInt(timestampOffset + LONG_BYTES);

        final Value value;
        if (valueSize == -1) {
            value = new Value(timestamp);
        } else {
            final int valueOffset = timestampOffset + LONG_BYTES + INT_BYTES;
            final ByteBuffer valyeByteBuffer = elements
                    .duplicate()
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
            position = getPosition(from.rewind());
        }

        @Override
        public boolean hasNext() {
            return position < offsetArr.length;
        }

        @Override
        public Cell next() {
            return get(position++);
        }
    }
}