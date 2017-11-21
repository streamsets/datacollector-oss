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

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.JsonUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import soql.SOQLParser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class SobjectRecordCreator extends ForceRecordCreatorImpl {
  private static final Logger LOG = LoggerFactory.getLogger(SobjectRecordCreator.class);

  private static final int MAX_METADATA_TYPES = 100;
  private static final String UNEXPECTED_TYPE = "Unexpected type: ";

  private static final String WILDCARD_SELECT_QUERY = "^SELECT\\s*\\*\\s*FROM\\s*.*";
  private static final Pattern WILDCARD_SELECT_PATTERN = Pattern.compile(WILDCARD_SELECT_QUERY, Pattern.DOTALL);
  private static final int METADATA_DEPTH = 5;

  private static final List<String> BOOLEAN_TYPES = Arrays.asList(
      "boolean",
      "checkbox"
  );
  private static final List<String> STRING_TYPES = Arrays.asList(
      "combobox",
      "email",
      "encryptedstring",
      "id",
      "multipicklist",
      "phone",
      "picklist",
      "reference",
      "string",
      "textarea",
      "time",
      "url"
  );
  private static final List<String> DECIMAL_TYPES = Arrays.asList(
      "currency",
      "double",
      "percent"
  );
  private static final List<String> INT_TYPES = Collections.singletonList("int");
  private static final List<String> BINARY_TYPES = Collections.singletonList("base64");
  private static final List<String> BYTE_TYPES = Collections.singletonList("byte");
  private static final List<String> DATETIME_TYPES = Collections.singletonList("datetime");
  private static final List<String> DATE_TYPES = Collections.singletonList("date");

  private static final TimeZone TZ = TimeZone.getTimeZone("GMT");

  private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss");
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  final String sobjectType;
  protected final ForceInputConfigBean conf;

  Map<String, ObjectMetadata> metadataCache;
  final Stage.Context context;

  class ObjectMetadata {
    Map<String, Field> nameToField;
    Map<String, Field> relationshipToField;

    ObjectMetadata(
        Map<String, Field> fieldMap, Map<String, Field> relationshipMap
    ) {
      this.nameToField = fieldMap;
      this.relationshipToField = relationshipMap;
    }
  }

  SobjectRecordCreator(Stage.Context context, ForceInputConfigBean conf, String sobjectType) {
    datetimeFormat.setTimeZone(TZ);
    dateFormat.setTimeZone(TZ);

    this.context = context;
    this.conf = conf;
    this.sobjectType = sobjectType;
  }

  SobjectRecordCreator(SobjectRecordCreator recordCreator) {
    this(recordCreator.context, recordCreator.conf, recordCreator.sobjectType);
    this.metadataCache = recordCreator.metadataCache;
  }

  public boolean metadataCacheExists() {
    return metadataCache != null;
  }

  public void clearMetadataCache() {
    metadataCache = null;
  }

  public void buildMetadataCacheFromQuery(PartnerConnection partnerConnection, String query) throws StageException {
    LOG.debug("Getting metadata for sobjectType {} - query is {}", sobjectType, query);

    // We make a list of reference paths that are in the query
    // Each path is a list of (SObjectName, FieldName) pairs
    List<List<Pair<String, String>>> references = new LinkedList<>();

    metadataCache = new LinkedHashMap<>();

    if (query != null) {
      SOQLParser.StatementContext statementContext = ForceUtils.getStatementContext(query);

      for (SOQLParser.FieldListContext flc : statementContext.fieldList()) {
        for (SOQLParser.FieldElementContext fec : flc.fieldElement()) {
          String fieldName = fec.getText();
          String[] pathElements = fieldName.split("\\.");
          // Handle references
          extractReferences(references, pathElements);
        }
      }
    }

    try {
      getAllReferences(partnerConnection, metadataCache, references, new String[]{sobjectType}, METADATA_DEPTH);
    } catch (ConnectionException e) {
      throw new StageException(Errors.FORCE_21, sobjectType, e);
    }
  }

  public void buildMetadataCacheFromFieldList(PartnerConnection partnerConnection, String fieldList) throws StageException {
    LOG.debug("Getting metadata for sobjectType {} - field list is {}", sobjectType, fieldList);

    // We make a list of reference paths that are in the query
    // Each path is a list of (SObjectName, FieldName) pairs
    List<List<Pair<String, String>>> references = new LinkedList<>();

    metadataCache = new LinkedHashMap<>();

    for (String fieldName : fieldList.split("\\s*,\\s*")) {
      String[] pathElements = fieldName.split("\\.");
      // Handle references
      extractReferences(references, pathElements);
    }

    try {
      getAllReferences(partnerConnection, metadataCache, references, new String[]{sobjectType}, METADATA_DEPTH);
    } catch (ConnectionException e) {
      throw new StageException(Errors.FORCE_21, sobjectType, e);
    }
  }

  private void extractReferences(List<List<Pair<String, String>>> references, String[] pathElements) {
    if (pathElements.length > 1) {
      // LinkedList since we'll be removing elements from the path as we get their metadata
      List<Pair<String, String>> path = new LinkedList<>();
      // Last element in the list is the field itself; we don't need it
      for (int i = 0; i < pathElements.length - 1; i++) {
        path.add(Pair.of(path.isEmpty() ? sobjectType.toLowerCase() : null, pathElements[i].toLowerCase()));
      }
      references.add(path);
    }
  }

  public void buildMetadataCache(PartnerConnection partnerConnection) throws StageException {
    buildMetadataCacheFromQuery(partnerConnection, null);
  }

    // Recurse through the tree of referenced types, building a metadata query for each level
  // Salesforce constrains the depth of the tree to 5, so we don't need to worry about
  // infinite recursion
  private void getAllReferences(
      PartnerConnection partnerConnection,
      Map<String, ObjectMetadata> metadataMap,
      List<List<Pair<String, String>>> references,
      String[] allTypes,
      int depth
  ) throws ConnectionException {
    if (depth < 0) {
      return;
    }

    List<String> next = new ArrayList<>();

    for (int typeIndex = 0; typeIndex < allTypes.length; typeIndex += MAX_METADATA_TYPES) {
      int copyTo = Math.min(typeIndex + MAX_METADATA_TYPES, allTypes.length);
      String[] types = Arrays.copyOfRange(allTypes, typeIndex, copyTo);

      for (DescribeSObjectResult result : partnerConnection.describeSObjects(types)) {
        Map<String, Field> fieldMap = new LinkedHashMap<>();
        Map<String, Field> relationshipMap = new LinkedHashMap<>();
        for (Field field : result.getFields()) {
          fieldMap.put(field.getName().toLowerCase(), field);
          String relationshipName = field.getRelationshipName();
          if (relationshipName != null) {
            relationshipMap.put(relationshipName.toLowerCase(), field);
          }
        }
        metadataMap.put(result.getName().toLowerCase(), new ObjectMetadata(fieldMap, relationshipMap));
      }

      for (List<Pair<String, String>> path : references) {
        // Top field name in the path should be in the metadata now
        if (!path.isEmpty()) {
          Pair<String, String> top = path.get(0);
          Field field = metadataMap.get(top.getLeft()).relationshipToField.get(top.getRight());
          Set<String> sobjectNames = metadataMap.keySet();
          for (String ref : field.getReferenceTo()) {
            ref = ref.toLowerCase();
            if (!sobjectNames.contains(ref) && !next.contains(ref)) {
              next.add(ref);
            }
            if (path.size() > 1) {
              path.set(1, Pair.of(ref, path.get(1).getRight()));
            }
          }
          path.remove(0);
        }
      }
    }

    if (!next.isEmpty()) {
      getAllReferences(partnerConnection, metadataMap, references, next.toArray(new String[0]), depth - 1);
    }
  }

  public boolean queryHasWildcard(String query) {
    Matcher m = WILDCARD_SELECT_PATTERN.matcher(query.toUpperCase());
    return m.matches();
  }

  public String expandWildcard(String query) {
    return query.replaceFirst("\\*", expandWildcard());
  }

  public String expandWildcard() {
    StringBuilder fieldsString = new StringBuilder();
    for (Field field : metadataCache.get(sobjectType.toLowerCase()).nameToField.values()) {
      String typeName = field.getType().name();
      if ("address".equals(typeName) || "location".equals(typeName)) {
        // Skip compound fields of address or geolocation type since they are returned
        // with null values by the SOAP API and not supported at all by the Bulk API
        continue;
      }
      if (fieldsString.length() > 0){
        fieldsString.append(',');
      }
      fieldsString.append(field.getName());
    }

    return fieldsString.toString();
  }

  com.streamsets.pipeline.api.Field createField(Object val, Field sfdcField) throws
      StageException {
    return createField(val, DataType.USE_SALESFORCE_TYPE, sfdcField);
  }

  com.streamsets.pipeline.api.Field createField(
      Object val,
      DataType userSpecifiedType,
      Field sfdcField
  ) throws StageException {
    String sfdcType = sfdcField.getType().toString();
    if (userSpecifiedType != DataType.USE_SALESFORCE_TYPE) {
      return com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.valueOf(userSpecifiedType.getLabel()), val);
    } else {
      if(val instanceof Map) {
        // Fields like Fiscal on Opportunity show up as Maps from Streaming API
        try {
          return JsonUtil.jsonToField(val);
        } catch (IOException e) {
          throw new StageException(Errors.FORCE_04, "Error parsing data", e);
        }
      } else if (BOOLEAN_TYPES.contains(sfdcType)) {
        return com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.BOOLEAN, val);
      } else if (BYTE_TYPES.contains(sfdcType)) {
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.BYTE, val);
      } else if (INT_TYPES.contains(sfdcType)) {
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.INTEGER, val);
      } else if (DECIMAL_TYPES.contains(sfdcType)) {
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.DECIMAL, val);
      } else if (STRING_TYPES.contains(sfdcType)) {
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.STRING, val);
      } else if (BINARY_TYPES.contains(sfdcType)) {
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.BYTE_ARRAY, val);
      } else if (DATETIME_TYPES.contains(sfdcType)) {
        if (val != null && !(val instanceof String)) {
          throw new StageException(
              Errors.FORCE_04,
              UNEXPECTED_TYPE + val.getClass().getName()
          );
        }
        String strVal = (String)val;
        try {
          return com.streamsets.pipeline.api.Field.createDatetime((strVal != null) ? datetimeFormat.parse(strVal) : null);
        } catch (ParseException e) {
          throw new StageException(Errors.FORCE_04, "Error parsing date", e);
        }
      } else if (DATE_TYPES.contains(sfdcType)) {
        if (val != null && !(val instanceof String)) {
          throw new StageException(
              Errors.FORCE_04,
              UNEXPECTED_TYPE + val.getClass().getName()
          );
        }
        String strVal = (String)val;
        try {
          return com.streamsets.pipeline.api.Field.createDatetime((strVal != null) ? dateFormat.parse(strVal) : null);
        } catch (ParseException e) {
          throw new StageException(Errors.FORCE_04, "Error parsing date", e);
        }
      } else {
        throw new StageException(
            Errors.FORCE_04,
            UNEXPECTED_TYPE + sfdcType
        );
      }
    }
  }

  void setHeadersOnField(com.streamsets.pipeline.api.Field field, Field sfdcField) {
    Map<String, String> headerMap = getHeadersForField(sfdcField);
    for (Map.Entry<String, String> entry : headerMap.entrySet()) {
      field.setAttribute(entry.getKey(), entry.getValue());
    }
  }

  private Map<String, String> getHeadersForField(Field sfdcField) {
    Map<String, String> attributeMap = new HashMap<>();

    if (sfdcField == null) {
      return attributeMap;
    }
    String type = sfdcField.getType().toString();
    attributeMap.put(conf.salesforceNsHeaderPrefix + "salesforceType", type);
    if (STRING_TYPES.contains(type)) {
      attributeMap.put(conf.salesforceNsHeaderPrefix + "length", Integer.toString(sfdcField.getLength()));
    } else if (DECIMAL_TYPES.contains(type) ||
        "currency".equals(type) ||
        "percent".equals(type)) {
      attributeMap.put(conf.salesforceNsHeaderPrefix + "precision", Integer.toString(sfdcField.getPrecision()));
      attributeMap.put(conf.salesforceNsHeaderPrefix + "scale", Integer.toString(sfdcField.getScale()));
    } else if (INT_TYPES.contains(type)) {
      attributeMap.put(conf.salesforceNsHeaderPrefix + "digits", Integer.toString(sfdcField.getDigits()));
    }

    return attributeMap;
  }
}
