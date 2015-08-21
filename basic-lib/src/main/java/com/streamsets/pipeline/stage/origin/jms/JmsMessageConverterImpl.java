/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatParser;
import com.streamsets.pipeline.stage.origin.lib.MessageConfig;
import com.streamsets.pipeline.stage.origin.lib.ParserErrors;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class JmsMessageConverterImpl implements JmsMessageConverter {

  private final DataFormatConfig dataFormatConfig;
  private final DataFormatParser parser;

  public JmsMessageConverterImpl(DataFormatConfig dataFormatConfig, MessageConfig messageConfig) {
    this.dataFormatConfig = dataFormatConfig;
    this.parser = new DataFormatParser(JmsGroups.JMS.name(), dataFormatConfig, messageConfig);
  }

  public List<Stage.ConfigIssue> init(Source.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    issues.addAll(parser.init(context));
    return issues;
  }

  @Override
  public int convert(BatchMaker batchMaker, Source.Context context, String messsageId, Message message) throws StageException {
    byte[] payload = null;
    if (message instanceof TextMessage) {
      TextMessage textMessage = (TextMessage) message;
      try {
        payload = textMessage.getText().getBytes(parser.getCharset());
      } catch (JMSException ex) {
        handleException(context, messsageId, ex);
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
        handleException(context, messsageId, ex);
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
//        handleException(context, messsageId, ex);
//      }
    } else {
      handleException(context, messsageId, new StageException(JmsErrors.JMS_10, message.getClass().getName()));
    }
    int count = 0;
    if (payload != null) {
      try {
        for (Record record : parser.parse(context, messsageId, payload)) {
          Enumeration propertyNames = message.getPropertyNames();
          while (propertyNames.hasMoreElements()) {
            String name = String.valueOf(propertyNames.nextElement());
            String value = message.getStringProperty(name);
            record.getHeader().setAttribute(name, value);
          }
          batchMaker.addRecord(record);
          count++;
        }
      } catch (JMSException ex) {
        handleException(context, messsageId, ex);
      }
    }
    return count;
  }

  private void handleException(Source.Context context, String messageId, Exception ex) throws StageException {
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
