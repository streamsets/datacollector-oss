/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.util;

import com.google.common.base.Throwables;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class LambdaUtil {

  /**
   * This is custom variant of supplier that adds support for exceptions.
   */
  public interface ExceptionSupplier<T> {
    T get() throws Exception;
  }

  /**
   * Runs a Supplier within the context of a specified ClassLoader.
   *
   * @param classLoader the ClassLoader to run the Supplier.
   * @param supplier the Supplier to run within the context of a specified ClassLoader.
   */
   public static <T> T withClassLoader(
      ClassLoader classLoader,
      ExceptionSupplier<T> supplier
   ) {
     try {
       return withClassLoaderInternal(classLoader, supplier);
     } catch (Exception e) {
       Throwables.propagate(e);
     }

     return null;
   }

  /**
   * Runs a Supplier within the context of a specified ClassLoader.
   *
   * @param classLoader the ClassLoader to run the Supplier.
   * @param supplier the Supplier to run within the context of a specified ClassLoader.
   */
   public static <T, E1 extends Exception> T withClassLoader(
      ClassLoader classLoader,
      Class<E1> e1,
      ExceptionSupplier<T> supplier
   ) throws E1 {
     try {
       return withClassLoaderInternal(classLoader, supplier);
     } catch (Exception e) {
       Throwables.propagateIfPossible(e, e1);
       Throwables.propagate(e);
     }

     return null;
   }

  /**
   * Runs a Supplier within the context of a specified ClassLoader and in priviledged mode.
   *
   * @param classLoader the ClassLoader to run the Supplier.
   * @param supplier the Supplier to run within the context of a specified ClassLoader.
   */
  public static <T> T privilegedWithClassLoader(
      ClassLoader classLoader,
      ExceptionSupplier<T> supplier
  ) {
    try {
      return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> withClassLoaderInternal(classLoader, supplier));
    } catch (PrivilegedActionException e) {
      Throwables.propagate(e);
    }

    return null;
  }

  /**
   * Runs a Supplier within the context of a specified ClassLoader and in priviledged mode.
   *
   * @param classLoader the ClassLoader to run the Supplier.
   * @param e1 Exception class that should be propagated as-is
   * @param supplier the Supplier to run within the context of a specified ClassLoader.
   */
  public static <T, E1 extends Exception> T privilegedWithClassLoader(
      ClassLoader classLoader,
      Class<E1> e1,
      ExceptionSupplier<T> supplier
  ) throws E1 {
    try {
      return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> withClassLoaderInternal(classLoader, supplier));
    } catch (PrivilegedActionException e) {
      Throwables.propagateIfPossible(e.getCause(), e1);
      Throwables.propagate(e);
    }

    return null;
  }

  /**
   * Runs a Supplier within the context of a specified ClassLoader and in priviledged mode.
   *
   * @param classLoader the ClassLoader to run the Supplier.
   * @param e1 Exception class that should be propagated as-is
   * @param e2 Exception class that should be propagated as-is
   * @param supplier the Supplier to run within the context of a specified ClassLoader.
   */
  public static <T, E1 extends Exception, E2 extends Exception> T privilegedWithClassLoader(
      ClassLoader classLoader,
      Class<E1> e1,
      Class<E2> e2,
      ExceptionSupplier<T> supplier
  ) throws E1, E2 {
    try {
      return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> withClassLoaderInternal(classLoader, supplier));
    } catch (PrivilegedActionException e) {
      Throwables.propagateIfPossible(e.getCause(), e1, e2);
      Throwables.propagate(e);
    }

    return null;
  }

  /**
   * Internal version of the wrapping function that will simply propagate all exceptions up.
   */
  private static <T> T withClassLoaderInternal(
      ClassLoader classLoader,
      ExceptionSupplier<T> supplier
  ) throws Exception {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(classLoader);
      return supplier.get();
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

}
