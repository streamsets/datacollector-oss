/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.lib.salesforce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.soap.partner.PartnerConnection;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.jetty.client.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class EventRecordCreator extends ForceRecordCreatorImpl {
  private static final Logger LOG = LoggerFactory.getLogger(PlatformEventRecordCreator.class);
  private static final String UNEXPECTED_TYPE = "Unexpected type: ";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String DEFAULT = "default";
  public static final String TYPE = "type";
  public static final String FIELDS = "fields";
  public static final String STRING = "string";

  private final DateTimeFormatter datetimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]X");
  private final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
  private final ForceConfigBean conf;
  private HttpClient httpClient;

  protected Schema schema;
  protected final Stage.Context context;

  public EventRecordCreator(Stage.Context context, ForceConfigBean conf) {
    this.context = context;
    this.conf = conf;
  }

  @NotNull
  void createRecordFields(Record record, LinkedHashMap<String, Field> map, Map<String, Object> payload) {
    // TODO - maybe just call createField with schema and map?
    for (Map.Entry<String, Object> entry : payload.entrySet()) {
      String key = entry.getKey();
      Object val = entry.getValue();
      Schema.Field field = schema.getField(key);

      map.put(key, createField(field.schema(), field.doc(), val));
    }

    record.set(Field.createListMap(map));
  }

  @Override
  public void init() throws StageException {
    super.init();

    try {
      httpClient = new HttpClient(ForceUtils.makeSslContextFactory(conf));
      if (conf.connection.useProxy) {
        ForceUtils.setProxy(httpClient, conf);
      }
      httpClient.start();
    } catch (Exception e) {
      throw new StageException(Errors.FORCE_34, e);
    }
  }

  @Override
  public void destroy() {
    try {
      httpClient.stop();
    } catch (Exception e) {
      LOG.error("Exception stopping HttpClient", e);
    }

    super.destroy();
  }

  private Schema getSchemaMetadata(PartnerConnection partnerConnection, String schemaId) throws StageException {
    String soapEndpoint = partnerConnection.getConfig().getServiceEndpoint();
    String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("services/Soap/"));

    // Need to explicitly request expanded schema due to change in Salesforce Summer '18 - SDC-9169
    String path = "/services/data/v43.0/event/eventSchema/" + schemaId + "?payloadFormat=EXPANDED";
    try {
      String json = httpClient.newRequest(restEndpoint + path)
          .header("Authorization", "OAuth " + partnerConnection.getConfig().getSessionId())
          .send()
          .getContentAsString();

      // Don't parse the expanded schema directly as
      // (1) It contains more than we need
      // (2) Avro schema parser doesn't like 'expanded-record' type
      Map<String, Object> expandedSchema = (Map<String, Object>)(OBJECT_MAPPER.readValue(json, Object.class));
      List<Object> fields1 = (List<Object>)expandedSchema.get(FIELDS);
      Map<String, Object> field1 = (Map<String, Object>)fields1.get(0);
      Map<String, Object> type1 = (Map<String, Object>)field1.get(TYPE);
      List<Object> fields2 = (List<Object>)type1.get(FIELDS);
      Map<String, Object> field2 = (Map<String, Object>)fields2.get(1);
      Map<String, Object> type2 = (Map<String, Object>)field2.get(TYPE);

      // Salesforce schema is not legal Avro!
      removeInvalidDefaults(type2);

      return getParser().parse(OBJECT_MAPPER.writeValueAsString(type2));
    } catch (InterruptedException | TimeoutException | ExecutionException | IOException e ) {
      throw new StageException(Errors.FORCE_46, schemaId, e);
    }
  }

  // Clean up invalid defaults in the schema
  // Need to do this, otherwise Avro spews errors to System.err!
  private void removeInvalidDefaults(Map<String, Object> node) {
    if (node.containsKey(DEFAULT) && STRING.equals(node.get(TYPE))) {
      node.remove(DEFAULT);
    }
    for (Map.Entry<String, Object> e : node.entrySet()) {
      Object val = e.getValue();
      if (val instanceof List) {
        for (Object obj : (List<Object>)e.getValue()) {
          if (obj instanceof Map) {
            removeInvalidDefaults((Map<String, Object>)obj);
          }
        }
      } else if (val instanceof Map) {
        removeInvalidDefaults((Map<String, Object>)val);
      }
    }
  }

  @NotNull
  protected Schema.Parser getParser() {
    return new Schema.Parser();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Record createRecord(String sourceId, Object source) throws StageException {
    Pair<PartnerConnection,Map<String, Object>> pair = (Pair<PartnerConnection,Map<String, Object>>)source;
    PartnerConnection partnerConnection = pair.getLeft();
    Map<String, Object> data = pair.getRight();

    // Get new schema if necessary
    String schemaId = (String)data.get("schema");
    if (schema == null || !schemaId.equals(schema.getProp("uuid"))) {
      schema = getSchemaMetadata(partnerConnection, schemaId);
    }

    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    Map<String, Object> payload = (Map<String, Object>) data.get("payload");

    Record record = createRecord(sourceId, map, payload);

    return record;
  }

  @NotNull
  abstract Record createRecord(String sourceId, LinkedHashMap<String, Field> map, Map<String, Object> payload);

  private Field createField(Schema schema, String doc, Object val) throws StageException {
    String fieldType = schema.getType().getName();

    if ("union".equals(fieldType)) {
      // We need to look at the possible types, and the value, to determine what the value is
      for (Schema s : schema.getTypes()) {
        String t = s.getType().getName();
        if ("record".equals(t)) {
          if (val instanceof Map) {
            schema = s;
            fieldType = t;
            break;
          }
        } else if (!("null".equals(t))) {
          schema = s;
          fieldType = t;
        }
      }
    }

    if ("boolean".equals(fieldType)) {
      return  Field.create(Field.Type.BOOLEAN, val);
    } else if ("int".equals(fieldType)) {
      return  Field.create(Field.Type.INTEGER, val);
    } else if ("long".equals(fieldType)) {
      return  Field.create(Field.Type.LONG, val);
    } else if ("float".equals(fieldType)) {
      return  Field.create(Field.Type.FLOAT, val);
    } else if ("double".equals(fieldType)) {
      return  Field.create(Field.Type.DOUBLE, val);
    } else if ("bytes".equals(fieldType)) {
      return  Field.create(Field.Type.BYTE_ARRAY, Base64.getDecoder().decode((String)val));
    } else if ("enum".equals(fieldType)) {
      return Field.create(Field.Type.STRING, val);
    } else if (STRING.equals(fieldType)) {
      // Dates and Datetimes are encoded as strings
      if (doc != null) {
        if (doc.endsWith(":DateTime")) {
          // DateTime in format "2020-02-07T17:38:13[.000]Z" - milliseconds are optional!
          Date date = (val != null) ?
              Date.from(LocalDateTime.parse((String)val, datetimeFormat).atZone(ZoneId.of("UTC")).toInstant()) :
              null;
          return Field.create(Field.Type.DATETIME, date);
        } else if (doc.endsWith(":DateOnly") || doc.endsWith(":Birthday") || doc.endsWith(":DueDate")) {
          // Date in format "2020-02-07"
          Date date = (val != null) ?
              Date.from(LocalDate.parse((String)val, dateFormat).atStartOfDay().atZone(ZoneId.of("UTC")).toInstant()) :
              null;
          return Field.create(Field.Type.DATE, date);
        }
      }
      return Field.create(Field.Type.STRING, val);
    } else if ("record".equals(fieldType)) {
      Map<String, Field> map = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry: ((Map<String, Object>)val).entrySet()) {
        String key = entry.getKey();
        Schema.Field field = schema.getField(key);
        map.put(entry.getKey(), createField(field.schema(), field.doc(), entry.getValue()));
      }
      return Field.create(Field.Type.MAP, map);
    } else if ("array".equals(fieldType)) {
      List<Field> list = new ArrayList<>();
      for (Object obj: (List<Object>)val) {
        list.add(createField(schema.getElementType(), schema.getElementType().getDoc(), obj));
      }
      return Field.create(Field.Type.LIST, list);
    } else {
      throw new StageException(
          Errors.FORCE_04,
          UNEXPECTED_TYPE + fieldType
      );
    }
  }
}
