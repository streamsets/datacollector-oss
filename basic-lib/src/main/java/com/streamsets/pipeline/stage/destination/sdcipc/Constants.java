/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.sdcipc;

public interface Constants {
  String X_SDC_APPLICATION_ID_HEADER = "X-SDC-APPLICATION-ID";
  String X_SDC_COMPRESSION_HEADER = "X-SDC-COMPRESSION";
  String SNAPPY_COMPRESSION = "snappy";
  String CONTENT_TYPE_HEADER = "Content-Type";
  String APPLICATION_BINARY = "application/binary";

  String SSL_CERTIFICATE = "SunX509";
  String[] SSL_ENABLED_PROTOCOLS = {"TLSv1"};

  String PING_PATH = "/ping";

  String IPC_PATH = "/ipc/v1";
}
