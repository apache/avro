package com.commercehub.gradle.plugin.avro;

/**
 * Utility methods for working with {@link String}s.
 */
class Strings {
    /**
     * Not intended for instantiation.
     */
    private Strings() { }

    /**
     * Checks if a {@link String} is empty ({@code ""}) or {@code null}.
     *
     * @param str the String to check, may be {@code null}
     * @return true if the String is empty or {@code null}
     */
    static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /**
     * Checks if a {@link String} is not empty ({@code ""}) and not {@code null}.
     *
     * @param str the String to check, may be {@code null}
     * @return true if the String is not empty and not {@code null}
     */
    static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    /**
     * Requires that a {@link String} is not empty ({@code ""}) and not {@code null}.
     * If the requirement is violated, an {@link IllegalArgumentException} will be thrown.
     *
     * @param str the String to check, may be {@code null}
     * @param message the message to include in
     * @return the String, if the requirement was not violated
     * @throws IllegalArgumentException if the requirement was violated
     */
    @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
    static String requireNotEmpty(String str, String message) {
        if (isEmpty(str)) {
            throw new IllegalArgumentException(message);
        }
        return str;
    }
}
