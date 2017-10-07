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
package com.streamsets.pipeline.lib.parser.net.syslog;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.ext.io.CountingReader;
import com.streamsets.pipeline.lib.parser.net.BaseNetworkMessageDataParser;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public class SyslogDataParser extends BaseNetworkMessageDataParser {

  private final SyslogDecoder syslogDecoder;

  public SyslogDataParser(
      Stage.Context context,
      String readerId,
      CountingReader reader,
      long readerOffset,
      int maxObjectLen,
      Charset charset
  ) {
    super(context, readerId, reader, readerOffset, maxObjectLen, charset);
    syslogDecoder = new SyslogDecoder(charset);
  }

  @Override
  protected String getTypeName() {
    return "syslog";
  }

  @Override
  protected void performDecode(ByteBuf byteBuf) throws OnRecordErrorException {
    syslogDecoder.decodeStandaloneBuffer(byteBuf, decodedMessages, null, null);
  }

}
