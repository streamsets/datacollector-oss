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
package com.streamsets.pipeline.lib.util;

public class ThreadUtil {

  private ThreadUtil() {}

  /**
   * <p>
   *   Puts the current thread to sleep for the specified number of milliseconds.
   * </p>
   * <p>
   *   If the thread was interrupted before the sleep method is called, the sleep method wont sleep.
   * </p>
   * @param milliseconds number of milliseconds to sleep.
   *
   * @return <code>true</code> if the thread slept uninterrupted for the specified milliseconds, <code>false</code> if
   * it was interrupted.
   */
  public static boolean sleep(long milliseconds) {
    //checking if we got pre-interrupted.
    boolean interrupted = Thread.interrupted();
    if (!interrupted) {
      try {
        Thread.sleep(milliseconds);
      } catch (InterruptedException ex) {
        interrupted = true;
        // clearing the interrupt flag
        Thread.interrupted();
      }
    }
    return !interrupted;
  }

}
