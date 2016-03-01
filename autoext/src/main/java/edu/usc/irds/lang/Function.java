package edu.usc.irds.lang;

/**
 * Backport of JDK8's Function
 *
 */
public interface Function<T, R> {
    R apply(T obj);
}
