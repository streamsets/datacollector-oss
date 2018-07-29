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
package com.streamsets.pipeline.lib.salesforce;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.XMLEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BulkRecordCreator extends SobjectRecordCreator {
  private static final String RECORDS = "records";
  private static final String TYPE = "type";
  private static final QName XSI_TYPE = new QName("http://www.w3.org/2001/XMLSchema-instance", "type");
  private static final String S_OBJECT = "sObject";

  // Hide the superclass config with a more specific one
  protected final ForceSourceConfigBean conf;

  public BulkRecordCreator(Stage.Context context, ForceSourceConfigBean conf, String sobjectType) {
    super(context, conf, sobjectType);
    this.conf = conf;
  }

  public String createRecord(Object source, BatchMaker batchMaker) throws StageException {
    XMLEventReader reader = (XMLEventReader)source;
    String nextSourceOffset = null;

    try {
      // Pull the root map from the reader
      Field field = pullMap(reader);

      // Get the offset from the record
      Object o = getIgnoreCase(field.getValueAsMap(), conf.offsetColumn);
      if (o == null || !(o instanceof String)) {
        throw new StageException(Errors.FORCE_22, conf.offsetColumn);
      }
      String offset = fixOffset(conf.offsetColumn, (String)o);

      nextSourceOffset = RECORD_ID_OFFSET_PREFIX + offset;
      final String sourceId = conf.soqlQuery + "::" + offset;

      Record record = context.createRecord(sourceId);
      record.set(field);
      record.getHeader().setAttribute(SOBJECT_TYPE_ATTRIBUTE, sobjectType);

      batchMaker.addRecord(record);

      return nextSourceOffset;
    } catch (XMLStreamException e) {
      throw new StageException(Errors.FORCE_37, e);
    }

  }

  // When pullMap is called, the caller should have consumed the opening tag for the record
  private Field pullMap(XMLEventReader reader) throws StageException, XMLStreamException {
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    String type = null;

    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (event.isStartElement()) {
        if (event.asStartElement().getName().getLocalPart().equals(TYPE)) {
          // Move to content
          event = reader.nextEvent();
          type = event.asCharacters().getData().toLowerCase();
          // Consume closing tag
          reader.nextEvent();
        } else {
          String fieldName = event.asStartElement().getName().getLocalPart();
          Attribute attr = event.asStartElement().getAttributeByName(XSI_TYPE);
          if (attr != null && attr.getValue().equals(S_OBJECT)) {
            // Element is a nested record
            map.put(fieldName, pullMap(reader));
          } else {
            event = reader.nextEvent();
            if (event.isCharacters()) {
              // Element is a field value
              String fieldValue = event.asCharacters().getData();
              if (type == null) {
                throw new StageException(Errors.FORCE_38);
              }
              com.sforce.soap.partner.Field sfdcField = getFieldMetadata(type, fieldName);

              Field field = createField(fieldValue, sfdcField);
              if (conf.createSalesforceNsHeaders) {
                setHeadersOnField(field, getFieldMetadata(type, fieldName));
              }

              map.put(fieldName, field);

              // Consume closing tag
              reader.nextEvent();
            } else if (event.isStartElement()) {
              // Element is a nested list of records
              // Advance over <done>, <queryLocator> to record list
              while (!(event.isStartElement() && event.asStartElement().getName().getLocalPart().equals(RECORDS))) {
                event = reader.nextEvent();
              }

              // Read record list
              List<Field> recordList = new ArrayList<>();
              while (event.isStartElement() && event.asStartElement().getName().getLocalPart().equals(RECORDS)) {
                recordList.add(pullMap(reader));
                event = reader.nextEvent();
              }
              map.put(fieldName, Field.create(recordList));
            }
          }
        }
      } else if (event.isEndElement()) {
        // Done with record
        return Field.createListMap(map);
      }
    }

    throw new StageException(Errors.FORCE_39);
  }

  private Object getIgnoreCase(Map<String, Field> map, String offsetColumn) {
    for (Map.Entry<String, Field> entry : map.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(offsetColumn)) {
        return entry.getValue().getValue();
      }
    }

    return null;
  }

  // SDC-9731 will refactor record creators so we won't have this dummy implementation
  @Override
  public Record createRecord(String sourceId, Object source) throws StageException {
    return null;
  }
}
