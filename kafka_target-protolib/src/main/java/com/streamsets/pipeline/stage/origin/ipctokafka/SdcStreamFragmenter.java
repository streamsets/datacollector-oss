/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

//Using commons-io ByteArrayOutputStream on purpose

public class SdcStreamFragmenter {

  //10100000
  static final byte BASE_MAGIC_NUMBER = (byte) 0xa0;
  //10100001
  static final byte JSON1_MAGIC_NUMBER = BASE_MAGIC_NUMBER | (byte) 0x01;

  static boolean copy(InputStream input, OutputStream output, int limit) throws IOException {
    byte[] buffer = new byte[8024];
    boolean eof = false;
    while (limit > 0 && !eof) {
      int readLimit = Math.min(buffer.length, limit);
      int read = input.read(buffer, 0, readLimit);
      eof = (read == -1);
      if (!eof) {
        output.write(buffer, 0, read);
        limit -= read;
      }
    }
    return eof;
  }

  static int findEndOfLastLineBeforeLimit(byte[] buffer, int limit) {
    for (int i = limit - 1; i > 0; i--) {
      // as we are going backwards, this will handle \r\n EOLs as well without producing extra EOLs
      // and if a buffer ends in \r, the last line will be kept as incomplete until the next chunk.
      if (buffer[i] == '\n') {
        return i + 1; // including EOL character
      }
    }
    return -1;
  }

  // max array size will be limit + 1 (because the magic byte is being added)
  static byte[] extract(InputStream is, ByteArrayOutputStream overflowBuffer, int limit) throws IOException {
    // the inputstream we get has been already stripped of the magic byte if first call
    byte[] message;
    if (copy(is, overflowBuffer, limit - overflowBuffer.size())) {
      // got rest of payload without exceeding the max message size
      if (overflowBuffer.size() == 0) {
        // there is no more payload
        message = null;
      } else {
        // extract the rest payload and prefix it with the magic byte
        byte[] data = overflowBuffer.toByteArray();
        message = new byte[data.length + 1];
        message[0] = JSON1_MAGIC_NUMBER;
        System.arraycopy(data, 0, message, 1, data.length);
        overflowBuffer.reset();
      }
    } else {
      // got partial payload, exceeded the max message size
      byte[] data = overflowBuffer.toByteArray();
      // find last full record in partial payload
      int lastEOL = findEndOfLastLineBeforeLimit(data, limit);
      if (lastEOL == -1) {
        throw new IOException(Utils.format("Maximum message size '{}' exceeded", limit));
      }
      // extract payload up to last EOL and prefix with the magic byte
      message = new byte[lastEOL + 1];
      message[0] = JSON1_MAGIC_NUMBER;
      System.arraycopy(data, 0, message, 1, lastEOL);

      // put back in the stream buffer the portion of the payload that did not make it to the message
      overflowBuffer.reset();
      overflowBuffer.write(data, lastEOL, data.length - lastEOL);
    }
    return message;
  }

  public static List<byte[]> fragment(InputStream is, int fragmentSize, int maxSize) throws IOException {
    List<byte[]> list = new ArrayList<>();
    int size = 0;
    int magicByte = is.read();
    if (magicByte == -1) {
      throw new IOException("Request has no data");
    } else if ((((byte)magicByte) & JSON1_MAGIC_NUMBER) != JSON1_MAGIC_NUMBER) {
      throw new IOException(Utils.format("Data is not JSON1, unsupported magic byte '{}'", magicByte));
    } else {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(fragmentSize);
      byte[] message = extract(is, baos, fragmentSize - 2); // to account for the magic byte and \r\n EOLs
      while (message != null) {
        size += message.length;
        if (size > maxSize) {
          throw new IOException(Utils.format("Maximum data size '{}' exceeded", maxSize));
        }
        list.add(message);
        message = extract(is, baos, fragmentSize - 2);
      }
    }
    return list;
  }

}
