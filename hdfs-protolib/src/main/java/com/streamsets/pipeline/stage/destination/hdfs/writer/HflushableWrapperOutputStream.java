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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import org.apache.commons.io.output.ProxyOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * A Wrapper Output stream for {@link FSDataOutputStream} which will call {@link FSDataOutputStream#hflush()} in
 * addition to {@link FSDataOutputStream#flush()} when {@link #flush()} is called
 *
 * Rest of the method calls are delegated to {@link FSDataOutputStream} by extending {@link org.apache.commons.io.output.ProxyOutputStream}
 */
public final class HflushableWrapperOutputStream extends ProxyOutputStream {
  FSDataOutputStream os;

  HflushableWrapperOutputStream(FSDataOutputStream os) {
    super(os);
    this.os = os;
  }

  @Override
  public void flush() throws IOException {
    super.flush();
    os.hflush();
  }
}
