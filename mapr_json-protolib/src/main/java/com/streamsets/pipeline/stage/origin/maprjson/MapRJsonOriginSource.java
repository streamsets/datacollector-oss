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
package com.streamsets.pipeline.stage.origin.maprjson;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.exceptions.DBException;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.Value;
import org.ojai.store.QueryCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.streamsets.pipeline.stage.origin.lib.DataFormatParser.DATA_FORMAT_CONFIG_PREFIX;

public class MapRJsonOriginSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(MapRJsonOriginSource.class);
  private static final String MAPR_ID = "_id";

  private Table table;
  private MapRJsonOriginConfigBean conf;
  private DataParserFactory parserFactory;
  private DocumentStream documentStream = null;
  private Iterator<Document> iter;
  private long noMoreRecordsTime = 0;
  private Base64 b64 = new Base64();
  private Map<String, Value.Type> jsonDataTypes = new HashMap<>();
  private int binaryColumnWidth = 0;

  public MapRJsonOriginSource(MapRJsonOriginConfigBean mapRJsonOriginConfigBean) {

    conf = mapRJsonOriginConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (StringUtils.isEmpty(conf.tableName)) {
      issues.add(getContext().createConfigIssue(Groups.MAPR_JSON_ORIGIN.name(),
          "tableName",
          Errors.MAPR_JSON_ORIGIN_01
      ));
    }

    DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();
    if (!dataFormatConfig.init(getContext(),
        DataFormat.JSON,
        Groups.MAPR_JSON_ORIGIN.getLabel(),
        DATA_FORMAT_CONFIG_PREFIX,
        issues
    )) {
      issues.add(getContext().createConfigIssue(Groups.MAPR_JSON_ORIGIN.name(),
          "Initialize dataFormatConfig",
          Errors.MAPR_JSON_ORIGIN_11
      ));
    }
    parserFactory = dataFormatConfig.getParserFactory();

    getTable(issues);
    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {

    final int batchSize = Math.min(maxBatchSize, conf.basic.maxBatchSize);

    // waiting for new data?
    if (noMoreRecordsTime > 0) {
      takeANap();
    }

    // need to get a document to find the data types.
    if (jsonDataTypes.isEmpty() && !gatherJsonDataTypes()) {
      noMoreRecordsTime = System.currentTimeMillis();
      return "";
    }

    if (documentStream == null) {
      try {
        documentStream = createDocumentStream(lastSourceOffset);
        iter = documentStream.iterator();

      } catch (StageException ex) {
        LOG.error(Errors.MAPR_JSON_ORIGIN_03.getMessage(), lastSourceOffset, ex);
        throw new OnRecordErrorException(Errors.MAPR_JSON_ORIGIN_03, lastSourceOffset, ex);
      }
    }

    String endOffset = "";
    for (int i = 0; i < batchSize; i++) {
      Document document;
      Record record;

      if (iter.hasNext()) {
        try {
          document = iter.next();
        } catch (NoSuchElementException ex) {
          LOG.error(Errors.MAPR_JSON_ORIGIN_10.getMessage(), ex);
          throw new StageException(Errors.MAPR_JSON_ORIGIN_10, ex);
        }

        endOffset = getDocumentKey(document);
        record = createRecordFromJSON(lastSourceOffset, document);
        batchMaker.addRecord(record);

      } else {
        documentStream.close();
        documentStream = null;
        noMoreRecordsTime = System.currentTimeMillis();
        break;
      }
    }

    if (lastSourceOffset == null) {
      lastSourceOffset = "";
    }

    return StringUtils.isEmpty(endOffset) ? lastSourceOffset : endOffset;
  }

  private boolean gatherJsonDataTypes() throws StageException {

    try (DocumentStream docs = table.find()) {

      Iterator<Document> it = docs.iterator();
      if (it.hasNext()) {
        Document doc = it.next();

        jsonDataTypes.clear();

        Map<String, Object> m = doc.asMap();
        Set<String> names = m.keySet();

        for (String s : names) {
          jsonDataTypes.put(s, doc.getValue(s).getType());
        }

        if (jsonDataTypes.get(MAPR_ID) == Value.Type.BINARY) {
          ByteBuffer bb = doc.getValue(MAPR_ID).getBinary();
          binaryColumnWidth = bb.capacity();
        }
      } else {
        return false;
      }

    } catch (DBException ex) {
      LOG.error(Errors.MAPR_JSON_ORIGIN_12.getMessage(), ex);
      throw new OnRecordErrorException(Errors.MAPR_JSON_ORIGIN_12, ex);
    }
    return true;
  }

  private Record createRecordFromJSON(String lastSourceOffset, Document document) throws StageException {
    Record record;
    DataParser parser;

    try {
      if (jsonDataTypes.get(MAPR_ID) == Value.Type.BINARY) {
        String str;
        ByteBuffer bb = document.getIdBinary();

        switch (bb.array().length) {
          case 8:
            Long l = Bytes.toLong(bb.array());
            str = l.toString();
            break;
          case 4:
            Integer i = Bytes.toInt(bb.array());
            str = i.toString();
            break;

          case 2:
            Short s = Bytes.toShort(bb.array());
            str = s.toString();
            break;

          default:
            str = Bytes.toString(bb.array());
            break;
        }
        document.set(MAPR_ID, str);
      }

      if ((parser = parserFactory.getParser(lastSourceOffset, document.asJsonString())) == null) {
        LOG.error(Errors.MAPR_JSON_ORIGIN_05.getMessage());
        throw new OnRecordErrorException(Errors.MAPR_JSON_ORIGIN_05);
      }
    } catch (DataParserException ex) {
      LOG.error(Errors.MAPR_JSON_ORIGIN_05.getMessage(), ex);
      throw new OnRecordErrorException(Errors.MAPR_JSON_ORIGIN_05, ex);
    }

    try {
      record = parser.parse();
    } catch (IOException | DataParserException ex) {
      LOG.error(Errors.MAPR_JSON_ORIGIN_04.getMessage(), ex);
      throw new OnRecordErrorException(Errors.MAPR_JSON_ORIGIN_04, ex);
    }
    return record;

  }

  private String getDocumentKey(Document document) {

    String ans = "";

    if (jsonDataTypes.get(MAPR_ID) == Value.Type.BINARY) {
      ByteBuffer bb = document.getBinary(MAPR_ID);
      ans = new String(b64.encode(bb.array()));

    } else if (jsonDataTypes.get(MAPR_ID) == Value.Type.STRING) {
      ans = document.getString(MAPR_ID);

    }
    return ans;
  }

  private void takeANap() throws StageException {

    long latency = Math.abs(System.currentTimeMillis() - noMoreRecordsTime);
    long napTime;
    if (latency > 0 && latency < conf.basic.maxWaitTime) {
      napTime = Math.min(conf.basic.maxWaitTime, conf.basic.maxWaitTime - latency);
    } else {
      napTime = conf.basic.maxWaitTime;
    }

    noMoreRecordsTime = 0;

    if (napTime > 0) {
      try {
        Thread.sleep(napTime);
      } catch (InterruptedException ex) {
        LOG.error("Thread.sleep() failed ", ex.getMessage(), ex);
        Thread.currentThread().interrupt();
        throw new StageException(Errors.MAPR_JSON_ORIGIN_07, ex);
      }
    }
  }

  private void getTable(List<ConfigIssue> issues) {
    try {
      table = MapRDB.getTable(conf.tableName);
    } catch (DBException ex) {
      LOG.error(Errors.MAPR_JSON_ORIGIN_06.getMessage(), conf.tableName, ex);
      issues.add(getContext().createConfigIssue(Groups.MAPR_JSON_ORIGIN.name(),
          conf.tableName,
          Errors.MAPR_JSON_ORIGIN_06,
          ex
      ));
    }
  }

  private DocumentStream createDocumentStream(String lastSourceOffset) throws StageException {

    String start = "";
    byte[] byteVal;

    boolean starting = true;

    if (StringUtils.isEmpty(lastSourceOffset)) {
      if (jsonDataTypes.get(MAPR_ID) == Value.Type.BINARY && !conf.startValue.isEmpty()) {
        starting = true;
        start = convertStartValueToBinary();

      } else if (jsonDataTypes.get(MAPR_ID) == Value.Type.STRING) {
        starting = true;
        start = conf.startValue;
      }
    } else {
      starting = false;
      start = lastSourceOffset;
    }

    if (jsonDataTypes.get(MAPR_ID) == Value.Type.BINARY) {
      try {
        byteVal = b64.decode(start);
      } catch (DBException ex) {
        LOG.error(Errors.MAPR_JSON_ORIGIN_08.getMessage(), start, ex);
        throw new OnRecordErrorException(Errors.MAPR_JSON_ORIGIN_08, start, ex);
      }

      try {
        QueryCondition condition;
        QueryCondition.Op qcOp = conditionOp(starting);

        condition = MapRDB.newCondition().is(MAPR_ID, qcOp, ByteBuffer.wrap(byteVal)).build();
        documentStream = table.find(condition);

      } catch (DBException ex) {
        LOG.error(Errors.MAPR_JSON_ORIGIN_08.getMessage(), start, ex);
        throw new StageException(Errors.MAPR_JSON_ORIGIN_08, start, ex);
      }

    } else {

      try {
        QueryCondition condition;
        QueryCondition.Op qcOp = conditionOp(starting);

        condition = MapRDB.newCondition().is(MAPR_ID, qcOp, start).build();
        documentStream = table.find(condition);

      } catch (DBException ex) {
        LOG.error(Errors.MAPR_JSON_ORIGIN_08.getMessage(), start, ex);
        throw new StageException(Errors.MAPR_JSON_ORIGIN_08, start, ex);
      }

    }

    return documentStream;
  }

  private QueryCondition.Op conditionOp(boolean starting) {
    if (starting) {
      return QueryCondition.Op.GREATER_OR_EQUAL;
    } else {
      return QueryCondition.Op.GREATER;
    }

  }

  private String convertStartValueToBinary() throws OnRecordErrorException {
    try {
      switch (binaryColumnWidth) {
        case 8:
          return new String(b64.encode(Bytes.toBytes(Long.parseLong(conf.startValue))));

        case 4:
          return new String(b64.encode(Bytes.toBytes(Integer.parseInt(conf.startValue))));

        case 2:
          return new String(b64.encode(Bytes.toBytes(Short.parseShort(conf.startValue))));

        default:
          return new String(b64.encode(Bytes.toBytes(conf.startValue)));
      }
    } catch (NumberFormatException ex) {
      LOG.error(Errors.MAPR_JSON_ORIGIN_13.getMessage(), binaryColumnWidth, conf.startValue, ex);
      throw new OnRecordErrorException(Errors.MAPR_JSON_ORIGIN_13, binaryColumnWidth, conf.startValue, ex);
    }

  }

  @Override
  public void destroy() {
    try {
      if (documentStream != null) {
        documentStream.close();
        documentStream = null;
      }

      if (table != null) {
        table.close();
        table = null;
      }

    } catch (DBException ex) {
      LOG.error(Errors.MAPR_JSON_ORIGIN_09.getMessage(), conf.tableName, ex);
    }
  }

}
