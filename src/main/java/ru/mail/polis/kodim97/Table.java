package ru.mail.polis.kodim97;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {

    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException;

    void remove(@NotNull ByteBuffer key);

    long getSizeInByte();

    int size();

    void close() throws IOException;
}
