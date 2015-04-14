/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.http;

@SuppressWarnings("serial")
public class ServerNotYetRunningException extends Exception {
 public ServerNotYetRunningException(String message) {
    super(message);
  }
}
