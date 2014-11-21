/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.container.Utils;
import jersey.repackaged.com.google.common.base.Preconditions;
import org.apache.commons.io.input.ProxyInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Just because InputStream.skip() does not work as a seek
 */
public class PositionableInputStream extends ProxyInputStream {

  public PositionableInputStream(InputStream proxy, long initialPosition) throws IOException {
    super(proxy);
    Preconditions.checkArgument(initialPosition >= 0, "initialPosition must be greater than zero");
    byte[] arr = new byte[4096];
    long reminder = initialPosition;
    boolean eof = false;
    while (!eof && reminder > 0) {
      int toRead = (int) Math.min(arr.length, reminder);
      int read = read(arr, 0, toRead);
      if (read >= 0) {
        reminder -= read;
      } else {
        eof = true;
      }
    }
    if (eof) {
      throw new IOException(Utils.format("Reached end of inputStream at '{}' before reaching position '{}'",
                                         initialPosition - reminder, initialPosition));
    }
  }

}
