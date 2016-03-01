package edu.usc.irds.lang;

/**
 *
 * Back port of JDK8's BiFunction
 *
 */
public interface BiFunction<T, U, R> {
    R apply(T obj1, U ibj2);
}
