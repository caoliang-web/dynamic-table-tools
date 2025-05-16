package com.selectdb.dynamic.util;



import javax.annotation.Nullable;

import static java.lang.String.format;

public class Preconditions {

    public static void checkArgument(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean condition, @Nullable String errorMessageTemplate, @Nullable Object... errorMessageArgs) {
        if (!condition) {
            throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static <T> T checkNotNull(@Nullable T reference) {
        if (reference == null) {
            throw new NullPointerException();
        } else {
            return reference;
        }
    }

    public static <T> T checkNotNull(@Nullable T reference, @Nullable String errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        } else {
            return reference;
        }
    }

    public static boolean isNullOrWhitespaceOnly(String str) {
        if (str != null && str.length() != 0) {
            int len = str.length();

            for(int i = 0; i < len; ++i) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

    public static void checkState(boolean condition, @Nullable Object errorMessage) {
        if (!condition) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }
}
