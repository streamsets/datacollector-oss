package com.streamsets.pipeline.lib.parser.netflow;

import java.io.IOException;

public class InvalidFlowVersionException extends IOException {
  public InvalidFlowVersionException(int version) {
    super("Invalid version: " + version);
  }
}
