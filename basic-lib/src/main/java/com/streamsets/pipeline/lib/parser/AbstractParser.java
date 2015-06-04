/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public abstract class AbstractParser {
  protected final Stage.Context context;

  public AbstractParser(Stage.Context context) {
    this.context = context;
  }

  public abstract List<Record> parse(ByteBuf buf, InetSocketAddress recipient, InetSocketAddress sender)
    throws OnRecordErrorException;
}
