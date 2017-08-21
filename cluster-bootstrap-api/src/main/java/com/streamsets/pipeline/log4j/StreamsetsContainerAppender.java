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
package com.streamsets.pipeline.log4j;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.FileAppender;

public class StreamsetsContainerAppender extends FileAppender
  implements Flushable
{
  private String containerLogDir;
  private String containerLogFile;

  @Override
  public synchronized void activateOptions() {
    try {
      containerLogDir = System.getProperty("yarn.app.container.log.dir");
      if (containerLogDir == null) {
        System.err.println("WARN: yarn app container log dir was not set, writing to stderr");
        setWriter(createWriter(new SystemErrStream()));
      } else {
        containerLogFile = System.getProperty("hadoop.root.logfile", "syslog");
        setFile(new File(this.containerLogDir, containerLogFile).toString());
        setAppend(true);
        super.activateOptions();
      }
    } catch (Throwable throwable) {
      // we've hit some nasty issues where an exception is thrown in this method
      // and eaten by the JVM when running in yarn since yarn tries to log
      // uncaught exceptions which cannot be done if this method throws exception
      throwable.printStackTrace(System.err);
      System.err.flush();
      if (throwable instanceof Error) {
        throw (Error)throwable;
      } else if (throwable instanceof  RuntimeException) {
        throw (RuntimeException)throwable;
      } else {
        throw new RuntimeException(throwable);
      }
    }
  }

  @Override
  public void flush() {
    if (qw != null) {
      qw.flush();
    }
  }

  private static class SystemErrStream extends OutputStream {
    public SystemErrStream() {
    }

    @Override
    public void close() {
    }

    @Override
    public void flush() {
      System.err.flush();
    }

    @Override
    public void write(final byte[] b) throws IOException {
      System.err.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len)
      throws IOException {
      System.err.write(b, off, len);
    }

    @Override
    public void write(final int b) throws IOException {
      System.err.write(b);
    }
  }
}
