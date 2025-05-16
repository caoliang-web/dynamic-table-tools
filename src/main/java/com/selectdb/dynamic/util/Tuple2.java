package com.selectdb.dynamic.util;

import java.io.Serializable;
import java.util.Objects;

public class Tuple2<T0, T1> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Field 0 of the tuple. */
    public T0 f0;
    /** Field 1 of the tuple. */
    public T1 f1;

    /**
     * Creates a new tuple with 2 fields.
     */
    public Tuple2() {}

    /**
     * Creates a new tuple with the given 2 fields.
     *
     * @param value0 The value for field 0
     * @param value1 The value for field 1
     */
    public Tuple2(T0 value0, T1 value1) {
        this.f0 = value0;
        this.f1 = value1;
    }

    /**
     * Gets the value of field 0.
     *
     * @return The field's value.
     */
    public T0 getField0() {
        return f0;
    }

    /**
     * Sets the value of field 0.
     *
     * @param value The new value of field 0.
     */
    public void setField0(T0 value) {
        this.f0 = value;
    }

    /**
     * Gets the value of field 1.
     *
     * @return The field's value.
     */
    public T1 getField1() {
        return f1;
    }

    /**
     * Sets the value of field 1.
     *
     * @param value The new value of field 1.
     */
    public void setField1(T1 value) {
        this.f1 = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple2<?, ?> that = (Tuple2<?, ?>) o;
        return Objects.equals(f0, that.f0) && Objects.equals(f1, that.f1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f0, f1);
    }

    @Override
    public String toString() {
        return "(" + f0 + "," + f1 + ")";
    }

    /**
     * Creates a new tuple with the given 2 fields.
     * This is a static utility method that mirrors the constructor.
     *
     * @param value0 The value for field 0
     * @param value1 The value for field 1
     * @param <T0> The type of field 0
     * @param <T1> The type of field 1
     * @return A new tuple.
     */
    public static <T0, T1> Tuple2<T0, T1> of(T0 value0, T1 value1) {
        return new Tuple2<>(value0, value1);
    }
}