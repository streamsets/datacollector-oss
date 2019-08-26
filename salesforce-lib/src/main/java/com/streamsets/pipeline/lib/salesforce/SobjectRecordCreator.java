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

import com.sforce.soap.partner.ChildRelationship;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.bind.XmlObject;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.JsonUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import soql.SOQLParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.GregorianCalendar;
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

  protected static final String UNEXPECTED_TYPE = "Unexpected type: ";

  public static final List<String> DECIMAL_TYPES = Arrays.asList(
      "currency",
      "double",
      "percent"
  );
  private static final List<String> INT_TYPES = Collections.singletonList("int");
  private static final List<String> BINARY_TYPES = Collections.singletonList("base64");
  private static final List<String> BYTE_TYPES = Collections.singletonList("byte");
  private static final List<String> DATETIME_TYPES = Collections.singletonList("datetime");
  private static final List<String> DATE_TYPES = Collections.singletonList("date");
  private static final String ANYTYPE = "anyType";

  private static final BigDecimal MAX_OFFSET_INT = new BigDecimal(Integer.MAX_VALUE);
  protected static final String RECORD_ID_OFFSET_PREFIX = "recordId:";

  private static final TimeZone TZ = TimeZone.getTimeZone("GMT");
  private static final String NAME = "Name";
  private static final String COUNT = "count()";
  private static final String ERROR_PARSING_DATA = "Error parsing data";

  private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss");
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  final String sobjectType;
  protected final ForceInputConfigBean conf;

  Map<String, ObjectMetadata> metadataCache;
  final Stage.Context context;
  private boolean countQuery = false;

  @NotNull
  protected com.streamsets.pipeline.api.Field getField(SoapRecordCreator.XmlType xmlType, Object val, DataType userSpecifiedType) throws StageException {
    if (userSpecifiedType != DataType.USE_SALESFORCE_TYPE) {
      return com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.valueOf(userSpecifiedType.getLabel()), val);
    }

    com.streamsets.pipeline.api.Field field;
    if (xmlType == null) {  // String data does not contain an XML type!
      field = com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.STRING, (val == null) ? null : val.toString());
    } else {
      switch (xmlType) {
        case DATE_TIME:
          field = com.streamsets.pipeline.api.Field.create(xmlType.getFieldType(), (val == null) ? null : ((GregorianCalendar)val).getTime());
          break;
        case DATE:
        case INT:
        case DOUBLE:
          field = com.streamsets.pipeline.api.Field.create(xmlType.getFieldType(), val);
          break;
        default:
          throw new StageException(Errors.FORCE_04, UNEXPECTED_TYPE + xmlType);
      }
    }

    return field;
  }

  class ObjectMetadata {
    Map<String, Field> nameToField;
    Map<String, Field> relationshipToField;
    Map<String, String> childRelationships;

    ObjectMetadata(
        Map<String, Field> fieldMap,
        Map<String, Field> relationshipMap,
        Map<String, String> childRelationships
    ) {
      this.nameToField = fieldMap;
      this.relationshipToField = relationshipMap;
      this.childRelationships = childRelationships;
    }

    Field getFieldFromName(String name) {
      return nameToField.get(name.toLowerCase());
    }

    Field getFieldFromRelationship(String relationshipName) {
      return relationshipToField.get(relationshipName.toLowerCase());
    }

    String getChildSObjectType(String relationshipName) {
      return childRelationships.get(relationshipName.toLowerCase());
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

  public String getSobjectType() {
    return sobjectType;
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

    // Prepopulate the metadata cache with the sobject type, since we know we'll need it
    // and we can use it to figure out relationship types for subqueries
    try {
      getAllReferences(partnerConnection, metadataCache, Collections.emptyList(), new String[]{sobjectType}, 1);
    } catch (ConnectionException e) {
      throw new StageException(Errors.FORCE_21, sobjectType, e);
    }

    if (query != null) {
      SOQLParser.StatementContext statementContext = ForceUtils.getStatementContext(query);

      // Old-style COUNT() query
      countQuery = COUNT.equalsIgnoreCase(statementContext.fieldList(0).getText());

      for (SOQLParser.FieldListContext flc : statementContext.fieldList()) {
        getReferencesFromFieldList(partnerConnection, metadataCache, sobjectType, references, flc);
      }
    }
  }

  public boolean isCountQuery() {
    return countQuery;
  }

  private void getReferencesFromFieldList(
      PartnerConnection partnerConnection,
      Map<String, ObjectMetadata> metadataMap,
      String objectType,
      List<List<Pair<String, String>>> references,
      SOQLParser.FieldListContext flc
  ) throws StageException {
    List<String> objectTypes = new ArrayList<>();

    objectTypes.add(objectType);
    for (SOQLParser.FieldElementContext fec : flc.fieldElement()) {
      SOQLParser.SubqueryContext subquery = fec.subquery();
      SOQLParser.TypeOfClauseContext typeofClause = fec.typeOfClause();
      if (subquery != null) {
        // Recurse into subqueries
        getReferencesFromFieldList(partnerConnection,
            metadataMap,
            metadataMap.get(objectType).getChildSObjectType(subquery.objectList().getText()),
            references,
            subquery.fieldList());
      } else if (typeofClause != null) {
        for (SOQLParser.WhenThenClauseContext whenThen : typeofClause.whenThenClause()) {
          String whenObjectType = whenThen.whenObjectType().getText();
          objectTypes.add(whenObjectType);
          for (SOQLParser.FieldNameContext fieldName : whenThen.whenFieldList().fieldName()) {
            String[] pathElements = fieldName.getText().split("\\.");
            // Handle references
            extractReferences(whenObjectType, references, pathElements);
          }
        }
      } else {
        String fieldName = fec.getText();
        String[] pathElements = fieldName.split("\\.");
        // Handle references
        extractReferences(objectType, references, pathElements);
      }
    }

    try {
      getAllReferences(partnerConnection, metadataMap, references, objectTypes.toArray(new String[0]), METADATA_DEPTH);
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
      extractReferences(sobjectType, references, pathElements);
    }

    try {
      getAllReferences(partnerConnection, metadataCache, references, new String[]{sobjectType}, METADATA_DEPTH);
    } catch (ConnectionException e) {
      throw new StageException(Errors.FORCE_21, sobjectType, e);
    }
  }

  private void extractReferences(String objectType, List<List<Pair<String, String>>> references, String[] pathElements) {
    if (pathElements.length > 1) {
      // LinkedList since we'll be removing elements from the path as we get their metadata
      List<Pair<String, String>> path = new LinkedList<>();
      // Last element in the list is the field itself; we don't need it
      for (int i = 0; i < pathElements.length - 1; i++) {
        // Ignore redundant reference to object being queried - SDC-9067
        if (!(i == 0 && objectType.equalsIgnoreCase(pathElements[i]))) {
          path.add(Pair.of(path.isEmpty() ? objectType.toLowerCase() : null, pathElements[i].toLowerCase()));
        }
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

      // Special case - we prepopulate the cache with the root sobject type - don't repeat
      // ourselves
      if (types.length > 1 || !metadataMap.containsKey(types[0])) {
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
          Map<String, String> childRelationships = new LinkedHashMap<>();
          for (ChildRelationship child : result.getChildRelationships()) {
            if (child.getRelationshipName() != null) {
              childRelationships.put(child.getRelationshipName().toLowerCase(),
                  child.getChildSObject().toLowerCase());
            }
          }
          metadataMap.put(result.getName().toLowerCase(), new ObjectMetadata(fieldMap,
              relationshipMap, childRelationships));
        }
      }

      if (references != null) {
        for (List<Pair<String, String>> path : references) {
          // Top field name in the path should be in the metadata now
          if (!path.isEmpty()) {
            Pair<String, String> top = path.get(0);
            Field field = metadataMap.get(top.getLeft()).getFieldFromRelationship(top.getRight());
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
            // SDC-10422 Polymorphic references have an implicit reference to the Name object type
            if (field.isPolymorphicForeignKey()) {
              next.add(NAME);
            }
            path.remove(0);
          }
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
    return createField(null, val, DataType.USE_SALESFORCE_TYPE, sfdcField);
  }

  com.streamsets.pipeline.api.Field createField(
      SoapRecordCreator.XmlType xmlType,
      Object val,
      DataType userSpecifiedType,
      Field sfdcField
  ) throws StageException {
    String sfdcType = sfdcField.getType().toString();
    if (userSpecifiedType != DataType.USE_SALESFORCE_TYPE) {
      return com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.valueOf(userSpecifiedType.getLabel()), val);
    } else {
      if (val instanceof Map) {
        // Fields like Fiscal on Opportunity show up as Maps from Streaming API
        try {
          return JsonUtil.jsonToField(val);
        } catch (IOException e) {
          throw new StageException(Errors.FORCE_04, ERROR_PARSING_DATA, e);
        }
      } else if (ANYTYPE.equals(sfdcType)) {
        // anyType can be String, boolean etc
        try {
          // xmlType gives us a hint, if it is present
          if (xmlType != null) {
            return getField(xmlType, val, userSpecifiedType);
          } else {
            return JsonUtil.jsonToField(val);
          }
        } catch (IOException e) {
          throw new StageException(Errors.FORCE_04, ERROR_PARSING_DATA, e);
        }
      } else if (BOOLEAN_TYPES.contains(sfdcType)) {
        return com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.BOOLEAN, val);
      } else if (BYTE_TYPES.contains(sfdcType)) {
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.BYTE, val);
      } else if (INT_TYPES.contains(sfdcType)) {
        if (val != null && val.toString().contains(".")) {
          // SDC-12343 - schema says int, but Salesforce can give you a float!
          switch (conf.mismatchedTypesOption) {
            case PRESERVE_DATA:
              // Keep the data, coerce the type to decimal
              return com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.DECIMAL, val);
            case TRUNCATE_DATA:
              // Simply chop the data in half at the decimal point
              val = val.toString().split("\\.")[0];
              break;
            case ROUND_DATA:
              val = (new BigDecimal(val.toString())).setScale(sfdcField.getScale(), RoundingMode.HALF_UP).intValueExact();
              break;
          }
        }
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.INTEGER, val);
      } else if (DECIMAL_TYPES.contains(sfdcType)) {
        // Salesforce can return a string value with greater scale than that defined in the schema - see SDC-10152
        // Ensure that the created BigDecimal value matches the Salesforce schema
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.DECIMAL,
            (val == null)
                ? null
                : (new BigDecimal(val.toString())).setScale(sfdcField.getScale(), RoundingMode.HALF_UP));
      } else if (STRING_TYPES.contains(sfdcType)) {
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.STRING, val);
      } else if (BINARY_TYPES.contains(sfdcType)) {
        return  com.streamsets.pipeline.api.Field.create(com.streamsets.pipeline.api.Field.Type.BYTE_ARRAY,
            Base64.getDecoder().decode((String)val));
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

  public com.sforce.soap.partner.Field getFieldMetadata(String objectType, String fieldName) {
    return metadataCache.get(objectType.toLowerCase()).getFieldFromName(fieldName.toLowerCase());
  }

  public boolean objectTypeIsCached(String objectType) {
    return (metadataCache != null && metadataCache.get(objectType.toLowerCase()) != null);
  }

  public void addObjectTypeToCache(PartnerConnection partnerConnection, String objectType) throws ConnectionException {
    if (metadataCache == null) {
      metadataCache = new LinkedHashMap<>();
    }
    getAllReferences(partnerConnection, metadataCache, null, new String[]{objectType}, 1);
  }

  // SDC-9731 will remove the duplicate method from ForceSource
  // SDC-9078 - coerce scientific notation away when decimal field scale is zero
  // since Salesforce doesn't like scientific notation in queries
  protected String fixOffset(String offsetColumn, String offset) {
    com.sforce.soap.partner.Field sfdcField = getFieldMetadata(sobjectType, offsetColumn);
    if (SobjectRecordCreator.DECIMAL_TYPES.contains(sfdcField.getType().toString())
        && offset.contains("E")) {
      BigDecimal val = new BigDecimal(offset);
      offset = val.toPlainString();
      if (val.compareTo(MAX_OFFSET_INT) > 0 && !offset.contains(".")) {
        // We need the ".0" suffix since Salesforce doesn't like integer
        // bigger than 2147483647
        offset += ".0";
      }
    }
    return offset;
  }

}
