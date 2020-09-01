/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.lib.security.http.aster;

/**
 * Utility class with a command pattern invocation that sets a specific classloader as the
 * current thread context classloader.
 * <p/>
 * We have a {@code LambdaUtil} class doing this in the container module, we just don't want
 * to bring container as provided dependency here.
 */
public class ClassLoaderUtils {

  /**
   * Interface to wrap the code to invoke within the  context of a specific classloader.
   */
  public interface SupplierWithException<T, E extends Exception> {
    T get() throws E;
  }

  /**
   * Invokes the {@link SupplierWithException} with the given classloader as the current
   * thread context classloader. The original classloader is restored after the invocation.
   */
  public static <T, E extends Exception> T withOwnClassLoader(
      ClassLoader classLoader,
      SupplierWithException<T, E> supplier
  ) throws E {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(classLoader);
      return supplier.get();
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

}
