package ru.mail.polis.kodim97;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cell {

    static final Comparator<Cell> COMPARATOR = Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue);

    private final ByteBuffer key;
    private final Value value;

    public Cell(@NotNull final ByteBuffer key, @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public Value getValue() {
        return value;
    }
}
