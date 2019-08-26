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
package com.streamsets.pipeline.stage.processor.xmlflattener;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import com.streamsets.pipeline.lib.util.CommonError;
import com.streamsets.pipeline.stage.common.DataFormatErrors;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;

public class XMLFlatteningProcessor extends SingleLaneRecordProcessor {

  private final String fieldPath;
  private final String recordDelimiter;
  private final boolean ignoreAttrs;
  private final boolean ignoreNamespace;
  private final String fieldDelimiter;
  private final String outputField;
  private final String attrDelimiter;
  private final boolean keepExistingFields;
  private final boolean newFieldsOverwrite;
  private final DocumentBuilderFactory factory;
  private ErrorRecordHandler errorRecordHandler;

  private int flattenerPerRecordCount = 0;

  public XMLFlatteningProcessor(
      String fieldPath,
      boolean keepExistingFields,
      boolean newFieldsOverwrite,
      String outputField,
      String recordDelimiter,
      String fieldDelimiter,
      String attrDelimiter,
      boolean ignoreAttrs,
      boolean ignoreNamespace
  ) {
    super();
    this.fieldPath = fieldPath;
    this.keepExistingFields = keepExistingFields;
    this.newFieldsOverwrite = newFieldsOverwrite;
    this.outputField = outputField;
    this.recordDelimiter = recordDelimiter;
    this.fieldDelimiter = fieldDelimiter;
    this.attrDelimiter = attrDelimiter;
    this.ignoreAttrs = ignoreAttrs;
    this.ignoreNamespace = ignoreNamespace;
    factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
  }

  @Override
  protected List<ConfigIssue> init() {
    super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    List<ConfigIssue> issues = new ArrayList<>();
    String invalidCharsRegex = ".*[\\]\\[\\'\\\"\\/]+.*";

    if (this.fieldDelimiter.matches(invalidCharsRegex)) {
      issues.add(getContext().createConfigIssue(Groups.XML.name(), "fieldDelimiter", Errors.XMLF_02, fieldDelimiter));
    }

    if (this.attrDelimiter.matches(invalidCharsRegex)) {
      issues.add(getContext().createConfigIssue(Groups.XML.name(), "attrDelimiter", Errors.XMLF_01, attrDelimiter));
    }

    return issues;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      process(records.next(), singleLaneBatchMaker);
    }
  }

  @Override
  public void process(Record record, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    if (keepExistingFields && !record.get().getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
      String errorValue;
      if (record.get().getType() == Field.Type.LIST) {
        errorValue = record.get().getValueAsList().toString();
      } else {
        errorValue = record.get().toString();
      }
      throw new OnRecordErrorException(
          CommonError.CMN_0100, record.get().getType().toString(),
          errorValue, record.toString());
    }

    Field original = record.get(fieldPath);
    String xmlData = null;
    if (original != null) {
      try {
        if (original.getType() != Field.Type.STRING) {
          throw new OnRecordErrorException(
              CommonError.CMN_0100, original.getType().toString(), original.getValue().toString(), record.toString());
        }

        xmlData = record.get(fieldPath).getValueAsString().trim();
        Document doc = factory.newDocumentBuilder().parse(new ByteArrayInputStream(xmlData.getBytes()));
        doc.getDocumentElement().normalize();
        Element root = doc.getDocumentElement();
        // If we are have a delimiter we are in a record, so just add everything as fields.
        boolean currentlyInRecord = false;
        Record newR = null;
        String prefix = null;
        List<Record> results = new LinkedList<>();
        if (recordDelimiter == null || recordDelimiter.isEmpty() || root.getNodeName().equals(recordDelimiter)) {
          currentlyInRecord = true;
          //Increment record count.
          flattenerPerRecordCount++;
          if (keepExistingFields) {
            newR = getContext().cloneRecord(record, String.valueOf(flattenerPerRecordCount));
          } else {
            newR = getContext().createRecord(record, String.valueOf(flattenerPerRecordCount));
            newR.set(Field.create(new HashMap<String, Field>()));
          }
          ensureOutputFieldExists(newR);
          prefix = root.getTagName();
          addAttrs(newR, root, prefix);
          results.add(newR);
        }
        processXML(newR, currentlyInRecord, new XMLNode(root, prefix), results, record);
        for (Record result : results) {
          singleLaneBatchMaker.addRecord(result);
        }
      } catch (OnRecordErrorException ex) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                ex.getErrorCode(),
                ex.getParams()
            )
        );
      } catch (Exception ex) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                DataFormatErrors.DATA_FORMAT_303,
                xmlData,
                ex
            )
        );
      } finally {
        //Reset record count
        flattenerPerRecordCount = 0;
      }
    }
  }

  private void processXML(
      Record record,
      boolean currentlyInRecord,
      XMLNode current,
      List<Record> recordList,
      Record originalRecord
  ) throws DOMException {
    NodeList nodeList = current.element.getChildNodes();
    // Preprocess the child Nodes to avoid having to do it each time we hit a new element.
    if (currentlyInRecord) {
      Set<String> nodes = new HashSet<>();
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node next = nodeList.item(i);
        if (next.getNodeType() == Node.ELEMENT_NODE) {
          Element e = (Element)next;
          // We already saw the same name once before. Add it to the current element's map.
          if (nodes.contains(e.getTagName()) && !current.nodeCounters.containsKey(e.getTagName())) {
            current.nodeCounters.put(e.getTagName(), 0);
          } else {
            nodes.add(e.getTagName());
          }
        }
      }
    }
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node next = nodeList.item(i);
      // Text node - add it as a field, if we are currently in a record
      if (currentlyInRecord && next.getNodeType() == Node.TEXT_NODE) {
        String text = ((Text) next).getWholeText();
        if (text.trim().isEmpty()) {
          continue;
        }
        // If we don't need to keep existing fields, just write
        // If we need to keep existing fields, overwrite only if the original field does not exist
        // If we need to keep existing fields, and the current record has the path, overwrite only if newFieldsOverwrite
        if (!keepExistingFields || !record.has(getPathPrefix() + current.prefix) || newFieldsOverwrite) {
          ensureOutputFieldExists(record);
          record.set(getPathPrefix() + current.prefix, Field.create(text));
        }
      } else if (next.getNodeType() == Node.ELEMENT_NODE) {
        Element element = (Element) next;
        String tagName = element.getTagName();
        String elementPrefix;

        // If we are not in a record and this is the delimiter, then create a new record to fill in.
        if (!currentlyInRecord && element.getNodeName().equals(recordDelimiter)) {
          // Create a new record
          elementPrefix = tagName;

          //Increment record count
          flattenerPerRecordCount++;

          Record newR;
          //Add the node index as sourceRecordIdPostFix for newly created/cloned record.
          if (keepExistingFields) {
            newR = getContext().cloneRecord(originalRecord, String.valueOf(flattenerPerRecordCount));
          } else {
            newR = getContext().createRecord(originalRecord, String.valueOf(flattenerPerRecordCount));
            newR.set(Field.create(new HashMap<String, Field>()));
          }
          ensureOutputFieldExists(newR);
          recordList.add(newR);
          addAttrs(newR, element, elementPrefix);
          processXML(newR, true, new XMLNode(element, tagName), recordList, record);
        } else { // This is not a record delimiter, or we are currently already in a record
          // Check if this is the first time we are seeing this prefix?
          // The map tracks the next index to append. If the map does not have the key, don't add an index.
          elementPrefix = current.prefix + fieldDelimiter + tagName;
          if (current.nodeCounters.containsKey(tagName)) {
            Integer nextCount = current.nodeCounters.get(tagName);
            elementPrefix =  elementPrefix + "(" + nextCount.toString() + ")";
            current.nodeCounters.put(tagName, nextCount + 1);
          }
          if (currentlyInRecord) {
            addAttrs(record, element, elementPrefix);
          }
          XMLNode nextNode = new XMLNode(element, elementPrefix);
          processXML(record, currentlyInRecord, nextNode, recordList, record);
        }
      }
    }
  }

  private void addAttrs(Record record, Element element, String elementPrefix) {
    if (!ignoreAttrs) {
      NamedNodeMap attrs = element.getAttributes();
      for (int j = 0; j < attrs.getLength(); j++) {
        Node attr = attrs.item(j);
        String attrName = attr.getNodeName();
        if (attrName.equals("xmlns")) { //handled separately.
          continue;
        }
        record.set(getPathPrefix() + elementPrefix + attrDelimiter + attr.getNodeName(), Field.create(attr.getNodeValue()));
      }
    }
    if (!ignoreNamespace) {
      String namespaceURI = element.getNamespaceURI();
      if (namespaceURI != null) {
        record.set(getPathPrefix() + elementPrefix + attrDelimiter + "xmlns", Field.create(namespaceURI));
      }
    }
  }

  private void ensureOutputFieldExists(Record record) {
    // when keepExistingFields is set to false, the outputField is hidden.. so any existing value in the outputField should not be used
    if(StringUtils.isEmpty(outputField) || !keepExistingFields) {
      return;
    }

    if(!record.has("/" + outputField)) {
      record.set("/" + outputField, Field.create(Field.Type.MAP, Collections.emptyMap()));
    }
  }

  private String getPathPrefix() {
    // when keepExistingFields is set to false, the outputField is hidden.. so any existing value in the outputField should not be used
    if(StringUtils.isEmpty(outputField) || !keepExistingFields) {
      return  "/";
    }

    return "/" + outputField + "/";
  }

  private class XMLNode {
    final Element element;
    final String prefix;
    Map<String, Integer> nodeCounters = new HashMap<>();

    public XMLNode(Element element, String prefix) {
      this.element = element;
      this.prefix = prefix;
    }
  }
}
