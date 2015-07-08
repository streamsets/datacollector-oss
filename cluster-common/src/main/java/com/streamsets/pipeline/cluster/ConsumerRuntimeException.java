/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

/**
 * Thrown by the producer when the consumer has indicated it
 * encountered an error.
 */
public class ConsumerRuntimeException extends RuntimeException {

  public ConsumerRuntimeException(String msg, Throwable throwable) {
    super(msg, throwable);
  }
}
