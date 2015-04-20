/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

public class ThreadUtil {

  /**
   * Puts the current thread to sleep for the specified number of milliseconds.
   * <p/>
   * If the thread was interrupted before the sleep method is called, the sleep method wont sleep.
   * <p/>
   * @param millisecs number of milliseconds to sleep.
   *
   * @return <code>true</code> if the thread slept uninterrupted for the specified milliseconds, <code>false</code> if
   * it was interrupted.
   */
  public static boolean sleep(long millisecs) {
    //checking if we got pre-interrupted.
    boolean interrupted = Thread.interrupted();
    if (!interrupted) {
      try {
        Thread.sleep(millisecs);
      } catch (InterruptedException ex) {
        interrupted = true;
        // clearing the interrupt flag
        Thread.interrupted();
      }
    }
    return !interrupted;
  }

}
