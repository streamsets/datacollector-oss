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
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.jms.config.JmsErrors;
import com.streamsets.pipeline.lib.jms.config.JmsGroups;
import com.streamsets.pipeline.stage.origin.lib.DataFormatParser;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.pipeline.stage.origin.lib.MessageConfig;
import com.streamsets.pipeline.stage.origin.lib.ParserErrors;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class JmsMessageConverterImpl implements JmsMessageConverter {

  private final DataFormatParser parser;

  public JmsMessageConverterImpl(DataFormat dataFormat, DataParserFormatConfig dataFormatConfig, MessageConfig messageConfig) {
    this.parser = new DataFormatParser(JmsGroups.JMS.name(), dataFormat, dataFormatConfig, messageConfig);
  }

  @Override
  public List<Stage.ConfigIssue> init(Source.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    issues.addAll(parser.init(context));
    return issues;
  }

  @Override
  public int convert(BatchMaker batchMaker, Source.Context context, String messageId, Message message)
    throws StageException {
    byte[] payload = null;
    if (message instanceof TextMessage) {
      TextMessage textMessage = (TextMessage) message;
      try {
        payload = textMessage.getText().getBytes(parser.getCharset());
      } catch (JMSException ex) {
        Record record = context.createRecord(messageId);
        record.set(Field.create(attemptSerializationUnderErrorCondition(messageId, message, ex)));
        handleException(context, messageId, ex, record);
      }
    } else if (message instanceof BytesMessage) {
      BytesMessage bytesMessage = (BytesMessage) message;
      try {
        long length = bytesMessage.getBodyLength();
        if (length > 0L) {
          if (length > Integer.MAX_VALUE) {
            throw new JMSException("Unable to process message " + "of size "
              + length);
          }
          payload = new byte[(int) length];
          int count = bytesMessage.readBytes(payload);
          if (count != length) {
            throw new JMSException("Unable to read full message. " +
              "Read " + count + " of total " + length);
          }
        }
      } catch (JMSException ex) {
        Record record = context.createRecord(messageId);
        record.set(Field.create(attemptSerializationUnderErrorCondition(messageId, message, ex)));
        handleException(context, messageId, ex, record);
      }
//      TODO handle ObjectMessage's which are Java Serialized objects
//    } else if (message instanceof ObjectMessage) {
//      ObjectMessage objectMessage = (ObjectMessage)message;
//      try {
//        Object object = objectMessage.getObject();
//        if(object != null) {
//          ByteArrayOutputStream bos = new ByteArrayOutputStream();
//          try (ObjectOutput out = new ObjectOutputStream(bos)) {
//            out.writeObject(object);
//            payload = bos.toByteArray();
//          }
//        }
//      } catch (JMSException | IOException ex) {
//        handleException(context, messageId, ex);
//      }
    } else {
      StageException ex = new StageException(JmsErrors.JMS_10, message.getClass().getName());
      Record record = context.createRecord(messageId);
      record.set(Field.create(attemptSerializationUnderErrorCondition(messageId, message, ex)));
      handleException(context, messageId, ex, record);
    }
    int count = 0;
    if (payload != null) {
      try {
        for (Record record : parser.parse(context, messageId, payload)) {
          Enumeration propertyNames = message.getPropertyNames();
          while (propertyNames.hasMoreElements()) {
            String name = String.valueOf(propertyNames.nextElement());
            String value = message.getStringProperty(name);
            record.getHeader().setAttribute(name, value == null ? "" : value);
          }
          batchMaker.addRecord(record);
          count++;
        }
      } catch (JMSException | StageException ex) {
        Record record = context.createRecord(messageId);
        record.set(Field.create(payload));
        handleException(context, messageId, ex, record);
      }
    }
    return count;
  }

  private byte[] attemptSerializationUnderErrorCondition(String messageId, Message message, Exception originalEx)
  throws StageException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(message);
      return bos.toByteArray();
    } catch (IOException e) {
      throw new StageException(ParserErrors.PARSER_08, originalEx.toString(), e.toString(),
        messageId, message.getClass().getName(), e);
    } finally {
      if (out != null) {
        try { out.close(); } catch (IOException e) {}
      }
      if (bos != null) {
        try { bos.close(); } catch (IOException e) {}
      }
    }
  }

  private void handleException(Source.Context context, String messageId, Exception ex, Record record)
    throws StageException {
    switch (context.getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        context.reportError(ParserErrors.PARSER_03, messageId, ex.toString(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(ParserErrors.PARSER_03, messageId, ex.toString(), ex);
        }
      default:
        throw new IllegalStateException(Utils.format("Unknown On Error Value '{}'",
          context.getOnErrorRecord(), ex));
    }
  }
}
