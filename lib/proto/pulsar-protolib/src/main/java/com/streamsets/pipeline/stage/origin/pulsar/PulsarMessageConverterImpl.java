/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.pulsar;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.lib.pulsar.config.PulsarErrors;
import com.streamsets.pipeline.stage.origin.lib.MessageConfig;
import com.streamsets.pipeline.support.service.ServicesUtil;
import org.apache.pulsar.client.api.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PulsarMessageConverterImpl implements PulsarMessageConverter {

  private MessageConfig messageConfig;

  public PulsarMessageConverterImpl(MessageConfig messageConfig) {
    this.messageConfig = messageConfig;
  }

  @Override
  public List<Stage.ConfigIssue> init(Source.Context context) {
    return Collections.emptyList();
  }

  @Override
  public int convert(BatchMaker batchMaker, Source.Context context, String messageId, Message message)
      throws StageException {
    byte[] payload = message.getData();
    int count = 0;
    if (payload.length > 0) {
      try {
        for (Record record : ServicesUtil.parseAll(context, context, messageConfig.produceSingleRecordPerMessage,
            messageId, payload)) {
          Map<String, String> messageProperties = message.getProperties();
          messageProperties.forEach((key, value) -> record.getHeader().setAttribute(key, value == null ? "" : value));
          batchMaker.addRecord(record);
          ++count;
        }
      } catch (StageException e) {
        handleException(context, messageId, e);
      }
    }
    return count;
  }

  private void handleException(Source.Context context, String messageId, Exception e)
      throws StageException {
    switch (context.getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        context.reportError(PulsarErrors.PULSAR_09, messageId, e.toString(), e);
        break;
      case STOP_PIPELINE:
        if (e instanceof StageException) {
          throw (StageException) e;
        } else {
          throw new StageException(PulsarErrors.PULSAR_09, messageId, e.toString(), e);
        }
      default:
        throw new IllegalStateException(Utils.format("Unknown On Error Value '{}'",
            context.getOnErrorRecord(), e));
    }
  }

}
