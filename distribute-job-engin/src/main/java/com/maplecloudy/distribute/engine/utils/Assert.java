package com.maplecloudy.distribute.engine.utils;


/**
 * Assertion utility used for validating arguments.
 */
public abstract class Assert {

    public static void hasText(CharSequence sequence, String message) {
        if (!StringUtils.hasText(sequence)) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void hasText(CharSequence sequence) {
        hasText(sequence, "[Assertion failed] - this CharSequence argument must have text; it must not be null, empty, or blank");
    }

    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void notNull(Object object) {
        notNull(object, "[Assertion failed] - this argument is required; it must not be null");
    }

    public static void isTrue(Boolean object, String message) {
        if (!Boolean.TRUE.equals(object)) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void isTrue(Boolean object) {
        isTrue(object, "[Assertion failed] - this argument must be true");
    }
}
