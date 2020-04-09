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

public class BulkRecordCreator extends SobjectRecordCreator {
  private static final String RECORDS = "records";
  private static final String TYPE = "type";
  private static final QName XSI_TYPE = new QName("http://www.w3.org/2001/XMLSchema-instance", "type");
  private static final String S_OBJECT = "sObject";

  public BulkRecordCreator(Stage.Context context, ForceInputConfigBean conf, String sobjectType) {
    super(context, conf, sobjectType);
  }

  public Record createRecord(String sourceId, Object source) throws StageException {
    LinkedHashMap<String, Field> fields = (LinkedHashMap<String, Field>)source;

    Record record = context.createRecord(sourceId);
    record.set(Field.createListMap(fields));
    record.getHeader().setAttribute(SOBJECT_TYPE_ATTRIBUTE, sobjectType);

    return record;
  }

  // When getFields is called, the caller should have consumed the opening tag for the record
  public LinkedHashMap<String, Field> getFields(XMLEventReader reader) throws StageException, XMLStreamException {
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    String type = null;
    String fieldValue;

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
            map.put(fieldName, Field.createListMap(getFields(reader)));
          } else {
            event = reader.nextEvent();
            fieldValue = null;
            switch (event.getEventType()) {
              case XMLEvent.START_ELEMENT:
                // Element is a nested list of records
                // Advance over <done>, <queryLocator> to record list
                while (!(event.isStartElement() && event.asStartElement().getName().getLocalPart().equals(RECORDS))) {
                  event = reader.nextEvent();
                }

                // Read record list
                List<Field> recordList = new ArrayList<>();
                while (event.isStartElement() && event.asStartElement().getName().getLocalPart().equals(RECORDS)) {
                  recordList.add(Field.createListMap(getFields(reader)));
                  event = reader.nextEvent();
                }
                map.put(fieldName, Field.create(recordList));
                break;
              case XMLEvent.CHARACTERS:
                // Element is a field value
                fieldValue = event.asCharacters().getData();
                // Consume closing tag
                reader.nextEvent();
                // Intentional fall through to next case!
              case XMLEvent.END_ELEMENT:
                // Create the SDC field
                if (type == null) {
                  throw new StageException(Errors.FORCE_38);
                }
                // Is this a relationship to another object?
                com.sforce.soap.partner.Field sfdcField = metadataCache.get(type).getFieldFromRelationship(fieldName);
                if (sfdcField != null) {
                  // See if we already added fields from the related record
                  if (map.get(fieldName) != null) {
                    // We already created this node - don't overwrite it!
                    sfdcField = null;
                  }
                } else {
                  sfdcField = getFieldMetadata(type, fieldName);
                }

                if (sfdcField != null) {
                  Field field = createField(fieldValue, sfdcField);
                  if (conf.createSalesforceNsHeaders) {
                    setHeadersOnField(field, getFieldMetadata(type, fieldName));
                  }

                  map.put(fieldName, field);
                }
                break;
              default:
                throw new StageException(Errors.FORCE_41, event.getEventType());
            }
          }
        }
      } else if (event.isEndElement()) {
        // Done with record
        return map;
      }
    }

    throw new StageException(Errors.FORCE_39);
  }
}
