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
package com.streamsets.pipeline.lib.parser.net;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.Errors;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.lib.util.ExceptionUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.time.Clock;
import java.util.List;

/**
 * <p>An implementation of {@link ByteToMessageDecoder} that delegates parsing logic</p>
 * <p><em>NOTE: </em> this class assumes that the {@link ByteBuf} supplied by the earlier handler is complete ,
 * and it has already taken care of properly framing the buffer and we are not waiting on any more input.  In
 * other words, it does not check the writerIndex nor does it follow the
 * {@link io.netty.handler.codec.ReplayingDecoder} paradigm.</p>
 *
 * <p>This class lives in commonlib (as opposed to net-commonlib) because of its dependency on
 * {@link DataParserFactory}</p>
 */
public class DataFormatParserDecoder extends ByteToMessageDecoder {
  private final DataParserFactory parserFactory;
  private final Stage.Context context;
  private long lastChannelStart;
  private long totalRecordCount;

  public DataFormatParserDecoder(DataParserFactory parserFactory, Stage.Context context) {
    this.parserFactory = parserFactory;
    this.context = context;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
    this.lastChannelStart = Clock.systemUTC().millis();
  }

  @Override
  protected void decode(
      ChannelHandlerContext ctx, ByteBuf in, List<Object> out
  ) throws Exception {
    final int numBytes = in.readableBytes();
    final byte[] bytes = new byte[numBytes];
    in.readBytes(bytes);

    DataParser parser = parserFactory.getParser(generateRecordId(), bytes);

    Record record;
    try {
      while ((record = parser.parse()) != null) {
        out.add(record);
      }
    } catch (RecoverableDataParserException e) {
      // allow to return
    } catch (Exception e) {
      ExceptionUtils.throwUndeclared(new OnRecordErrorException(Errors.DATA_PARSER_04, e.getMessage(), e));
    } finally {
      parser.close();
    }
  }

  private String generateRecordId() {
    return String.format(
        "DataFormatParserDecoder_%s_%d-%d",
        context.getPipelineId(),
        lastChannelStart,
        totalRecordCount++
    );
  }
}
