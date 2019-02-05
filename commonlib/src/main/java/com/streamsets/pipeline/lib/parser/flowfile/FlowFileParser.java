/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.parser.flowfile;

import org.apache.nifi.util.FlowFileUnpackager;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.util.Map;
import java.util.LinkedList;
import java.util.Queue;

public class FlowFileParser extends AbstractDataParser {
  private static final Logger LOG = LoggerFactory.getLogger(FlowFileParser.class);

  private final ProtoConfigurableEntity.Context context;
  private final String readerId;
  private final InputStream in;
  private final FlowFileUnpackager unpacker;
  private final DataParserFactory contentParserFactory;
  private boolean done = false;

  // Initialize per FlowFile
  private final Queue<ContentParserAndAttributes> contentQueue;

  static class ContentParserAndAttributes {
    DataParser contentParser;
    Map<String, String> attributes;

    ContentParserAndAttributes(DataParser parser, Map<String, String> attr) {
      this.contentParser = parser;
      this.attributes = attr;
    }
  }

  public FlowFileParser(
      ProtoConfigurableEntity.Context context,
      String id,
      InputStream in,
      FlowFileUnpackager unpacker,
      DataParserFactory contentParserFactory
  ) {
    this.context = context;
    this.readerId = id;
    this.in = new DataInputStream(in);
    this.unpacker = unpacker;
    this.contentParserFactory = contentParserFactory;
    contentQueue = new LinkedList<>();
  }

  /**
   * Building a FlowFile from DataInputStream.
   * There will be three status when this method is called.
   * 1. First time. Parse from the beginning of inputStream
   * 2. End of parsing. No more data left in the inputStream.
   * 3. Middle of parsing the contents. There might be multiple records inside of one FlowFile content.
   * @return
   * @throws IOException
   * @throws DataParserException
   */
  @Override
  public Record parse() throws IOException, DataParserException {
    if (done) {  // Done parsing FlowFile content
      return null;
    }

    // First time calling parse
    if (contentQueue.isEmpty()) {
      try {
        unpackFlowFile(); // need initial loading

        while(unpacker.hasMoreData()) {
          unpackFlowFile();
        }
      } catch (EOFException ex) {
        // This does not happen but just in case, do logging and finish processing this FlowFile
        done = true;
        LOG.debug("End of parsing FlowFile. {}", ex.getMessage());
        return null;
      } catch (IOException | DataParserException ex) {
        done = true;
        throw new DataParserException(Errors.FLOWFILE_PARSER_01, ex.getMessage(), ex);
      }
    }

    // parsed inputstream but still no content.
    if (contentQueue.isEmpty()) {
      // No data found in the inputStream
      done = true;
      return null;
    } else {
      LOG.trace("Number of files in queue: {}", contentQueue.size());
    }

    Record newRecord = getRecordFromFlowFileContent();
    if (newRecord == null) {
      // remove this FlowFile content from the queue and move on to the next one.
      ContentParserAndAttributes ended = contentQueue.poll();
      ended.contentParser.close();
      newRecord = getRecordFromFlowFileContent();
    }
    return newRecord;
  }

  private void unpackFlowFile() throws IOException, DataParserException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Map<String, String> attributes = unpacker.unpackageFlowFile(in, baos);
    byte[] contentByte = baos.toByteArray();
    // Pass the bytes to inner parser
    DataParser contentParser = contentParserFactory.getParser(readerId, contentByte);
    contentQueue.add(new ContentParserAndAttributes(contentParser, attributes));
    baos.close();
  }

  private Record getRecordFromFlowFileContent() throws DataParserException {
    if (contentQueue.isEmpty()) {
      return null;
    }
    ContentParserAndAttributes parsedContent = contentQueue.peek();

    try {
      Record contentRecord = parsedContent.contentParser.parse();
      // At the end of this FlowFile content. Might move on to the next FlowFile
      if (contentRecord == null) {
        LOG.trace("End of FlowFile: {}", parsedContent.attributes.get("filename"));
        return null;
      }
      Record newRecord = context.createRecord(readerId + ":" + parsedContent.contentParser.getOffset());
      newRecord.set(contentRecord.get());

      // Put headers from content record to parent record. For example, Avro data format has avroSchema in header
      contentRecord.getHeader().getAttributeNames().stream().forEach(attr ->
          newRecord.getHeader().setAttribute(attr, contentRecord.getHeader().getAttribute(attr))
      );
      // Put FlowFile attributes in parent record headers
      parsedContent.attributes.forEach((k, v) -> newRecord.getHeader().setAttribute(k, v));
      return newRecord;
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new DataParserException(Errors.FLOWFILE_PARSER_01, ex.getMessage(), ex);
    }
  }

  @Override
  public String getOffset() {
    LOG.debug("Offset is not supported");
    return "";  // Currently not supported
  }

  @Override
  public void close() throws IOException {
    if (!contentQueue.isEmpty()) {
      contentQueue.forEach(elem -> {
        try {
          elem.contentParser.close();
        } catch (IOException ex) {
          LOG.warn("Error while closing parser: {}", ex.getMessage(), ex);
        }
      });
    }
    if (in != null) {
      in.close();
    }
  }
}

