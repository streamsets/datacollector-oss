/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

/**
 * If implemented, the pipeline will call the errorNotification method when the pipeline encounters
 * an error. The errorNotification method should not throw an exception of any kind.
 */
public interface ErrorListener {

  void errorNotification(Throwable throwable);

}
