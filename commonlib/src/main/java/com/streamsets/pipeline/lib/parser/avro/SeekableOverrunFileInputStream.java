/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.avro;

import com.streamsets.pipeline.lib.io.OverrunInputStream;
import org.apache.avro.file.SeekableInput;

import java.io.FileInputStream;
import java.io.IOException;

public class SeekableOverrunFileInputStream extends OverrunInputStream implements SeekableInput {

  private final FileInputStream in;

  public SeekableOverrunFileInputStream(FileInputStream in, int readLimit, boolean enabled) {
    super(in, readLimit, enabled);
    this.in = in;
  }

  @Override
  public void seek(long l) throws IOException {
    in.getChannel().position(l);
  }

  @Override
  public long tell() throws IOException {
    return in.getChannel().position();
  }

  @Override
  public long length() throws IOException {
    return in.getChannel().size();
  }
}
