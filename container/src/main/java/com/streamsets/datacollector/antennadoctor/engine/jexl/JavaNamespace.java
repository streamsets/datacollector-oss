/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.antennadoctor.engine.jexl;

/**
 * Set of static methods helpful for testing various aspects of Java stack.
 */
public class JavaNamespace {

  /**
   * Return true if and only if, given class can be loaded in given classloader.
   */
  public static boolean hasClass(ClassLoader cl, String name) {
    try {
      Class x = cl.loadClass(name);
      return x != null;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

}
