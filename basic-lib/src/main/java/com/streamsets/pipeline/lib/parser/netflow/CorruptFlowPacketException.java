package com.streamsets.pipeline.lib.parser.netflow;

import java.io.IOException;

public class CorruptFlowPacketException extends IOException {
  public CorruptFlowPacketException(String msg) {
    super(msg);
  }
}
