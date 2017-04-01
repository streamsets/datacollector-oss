/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.sforce.ws.bind.XmlObject;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ForceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ForceUtils.class);

  private static final String SOBJECT_TYPE_FROM_QUERY = "^SELECT.*FROM\\s*(\\S*)\\b.*";
  private static final String WILDCARD_SELECT_QUERY = "^SELECT\\s*\\*\\s*FROM\\s*.*";
  public static final Pattern WILDCARD_SELECT_PATTERN = Pattern.compile(WILDCARD_SELECT_QUERY, Pattern.DOTALL);
  private static Pattern sObjectFromQueryPattern = Pattern.compile(SOBJECT_TYPE_FROM_QUERY, Pattern.DOTALL);
  private static SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss");
  private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  private static TimeZone TZ = TimeZone.getTimeZone("GMT");
  private static List<String> BOOLEAN_TYPES = Arrays.asList("boolean", "checkbox");
  private static List<String> STRING_TYPES = Arrays.asList(
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
  private static List<String> DECIMAL_TYPES = Arrays.asList(
      "currency",
      "double",
      "percent"
  );
  private static List<String> INT_TYPES = Collections.singletonList("int");
  private static List<String> BINARY_TYPES = Collections.singletonList("base64");
  private static List<String> BYTE_TYPES = Collections.singletonList("byte");
  private static List<String> DATETIME_TYPES = Collections.singletonList("datetime");
  private static List<String> DATE_TYPES = Collections.singletonList("date");

  static {
    DATETIME_FORMAT.setTimeZone(TZ);
    DATE_FORMAT.setTimeZone(TZ);
  }

  public static String getExceptionCode(Throwable th) {
    return (th instanceof ApiFault) ? ((ApiFault) th).getExceptionCode().name() : "";
  }

  public static String getExceptionMessage(Throwable th) {
    return (th instanceof ApiFault) ? ((ApiFault) th).getExceptionMessage() : th.getMessage();
  }

  private static void setProxyConfig(ForceConfigBean conf, ConnectorConfig config) {
    if (conf.useProxy) {
      config.setProxy(conf.proxyHostname, conf.proxyPort);
      if (conf.useProxyCredentials) {
        config.setProxyUsername(conf.proxyUsername);
        config.setProxyPassword(conf.proxyPassword);
      }
    }
  }

  public static ConnectorConfig getPartnerConfig(ForceConfigBean conf, SessionRenewer sessionRenewer) {
    ConnectorConfig config = new ConnectorConfig();

    config.setUsername(conf.username);
    config.setPassword(conf.password);
    config.setAuthEndpoint("https://"+conf.authEndpoint+"/services/Soap/u/"+conf.apiVersion);
    config.setCompression(conf.useCompression);
    config.setTraceMessage(conf.showTrace);
    config.setSessionRenewer(sessionRenewer);

    setProxyConfig(conf, config);

    return config;
  }

  public static BulkConnection getBulkConnection(ConnectorConfig partnerConfig, ForceConfigBean conf) throws ConnectionException,
      AsyncApiException {
    // When PartnerConnection is instantiated, a login is implicitly
    // executed and, if successful,
    // a valid session is stored in the ConnectorConfig instance.
    // Use this key to initialize a BulkConnection:
    ConnectorConfig config = new ConnectorConfig();
    config.setSessionId(partnerConfig.getSessionId());

    // The endpoint for the Bulk API service is the same as for the normal
    // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
    String soapEndpoint = partnerConfig.getServiceEndpoint();
    String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
        + "async/" + conf.apiVersion;
    config.setRestEndpoint(restEndpoint);
    config.setCompression(conf.useCompression);
    config.setTraceMessage(conf.showTrace);
    config.setSessionRenewer(partnerConfig.getSessionRenewer());

    setProxyConfig(conf, config);

    return new BulkConnection(config);
  }

  public static Field createField(Object val, com.sforce.soap.partner.Field sfdcField) throws StageException {
    return createField(val, DataType.USE_SALESFORCE_TYPE, sfdcField);
  }

  public static Field createField(Object val, DataType userSpecifiedType, com.sforce.soap.partner.Field sfdcField) throws StageException {
    String sfdcType = sfdcField.getType().toString();
    if (userSpecifiedType != DataType.USE_SALESFORCE_TYPE) {
      return Field.create(Field.Type.valueOf(userSpecifiedType.getLabel()), val);
    } else {
      if (val != null && !(val instanceof String)) {
        throw new StageException(
            Errors.FORCE_04,
            "Unexpected type: " + val.getClass().getName()
        );
      }
      String strVal = (String)val;
      if (BOOLEAN_TYPES.contains(sfdcType)) {
        return Field.create(Field.Type.BOOLEAN, (strVal != null) ? Boolean.valueOf(strVal) : null);
      } else if (BYTE_TYPES.contains(sfdcType)) {
        return  Field.create(Field.Type.BYTE, (strVal != null) ? Byte.valueOf(strVal) : null);
      } else if (INT_TYPES.contains(sfdcType)) {
        return  Field.create(Field.Type.INTEGER, (strVal != null) ? Integer.valueOf(strVal) : null);
      } else if (DECIMAL_TYPES.contains(sfdcType)) {
        return  Field.create(Field.Type.DECIMAL, (strVal != null) ? new BigDecimal(strVal) : null);
      } else if (STRING_TYPES.contains(sfdcType)) {
        return  Field.create(Field.Type.STRING, strVal);
      } else if (BINARY_TYPES.contains(sfdcType)) {
        return  Field.create(Field.Type.BYTE_ARRAY, (strVal != null) ? Base64.decodeBase64(strVal) : null);
      } else if (DATETIME_TYPES.contains(sfdcType)) {
        try {
          return Field.createDatetime((strVal != null) ? DATETIME_FORMAT.parse(strVal) : null);
        } catch (ParseException e) {
          throw new StageException(Errors.FORCE_04, "Error parsing date", e);
        }
      } else if (DATE_TYPES.contains(sfdcType)) {
        try {
          return Field.createDatetime((strVal != null) ? DATE_FORMAT.parse(strVal) : null);
        } catch (ParseException e) {
          throw new StageException(Errors.FORCE_04, "Error parsing date", e);
        }
      } else {
        throw new StageException(
            Errors.FORCE_04,
            "Unexpected type: " + sfdcType
        );
      }
    }
  }

  public static LinkedHashMap<String, Field> addFields(
      XmlObject parent,
      Map<String, Map<String, com.sforce.soap.partner.Field>> metadataMap,
      boolean createSalesforceNsHeaders,
      String salesforceNsHeaderPrefix
  ) throws StageException {
      return addFields(parent, metadataMap, createSalesforceNsHeaders, salesforceNsHeaderPrefix, null);
  }

  public static LinkedHashMap<String, Field> addFields(
      XmlObject parent,
      Map<String, Map<String, com.sforce.soap.partner.Field>> metadataMap,
      boolean createSalesforceNsHeaders,
      String salesforceNsHeaderPrefix,
      Map<String, DataType> columnsToTypes
  ) throws StageException {
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();

    Iterator<XmlObject> iter = parent.getChildren();
    String type = null;
    while (iter.hasNext()) {
      XmlObject obj = iter.next();

      String key = obj.getName().getLocalPart();
      if ("type".equals(key)) {
        // Housekeeping field
        type = obj.getValue().toString().toLowerCase();
        continue;
      }

      if (obj.hasChildren()) {
        map.put(key, Field.createListMap(addFields(obj, metadataMap, createSalesforceNsHeaders, salesforceNsHeaderPrefix, columnsToTypes)));
      } else {
        Object val = obj.getValue();
        if ("Id".equalsIgnoreCase(key) && null == val) {
          // Get a null Id if you don't include it in the SELECT
          continue;
        }
        if (type == null) {
          throw new StageException(
              Errors.FORCE_04,
              "No type information for " + obj.getName().getLocalPart() +
                  ". Specify component fields of compound fields, e.g. Location__Latitude__s or BillingStreet"
          );
        }
        com.sforce.soap.partner.Field sfdcField = metadataMap.get(type).get(key.toLowerCase());
        Field field = null;
        if (sfdcField == null) {
          // null relationship
          field = Field.createListMap(new LinkedHashMap<>());
        } else {
          DataType dataType = (columnsToTypes != null) ? columnsToTypes.get(key.toLowerCase()) : null;
          field = ForceUtils.createField(val, (dataType == null ? DataType.USE_SALESFORCE_TYPE : dataType), sfdcField);
        }
        if (createSalesforceNsHeaders) {
          ForceUtils.setHeadersOnField(field, sfdcField, salesforceNsHeaderPrefix);
        }
        map.put(key, field);
      }
    }

    return map;
  }

  public static void setHeadersOnField(Field field, com.sforce.soap.partner.Field sfdcField, String salesforceNsHeaderPrefix) {
    Map<String, String> headerMap = getHeadersForField(sfdcField, salesforceNsHeaderPrefix);
    for (String key : headerMap.keySet()) {
      field.setAttribute(key, headerMap.get(key));
    }
  }

  public static Map<String, String> getHeadersForField(com.sforce.soap.partner.Field sfdcField,
      String salesforceNsHeaderPrefix) {
    Map<String, String> attributeMap = new HashMap<>();

    if (sfdcField == null) {
      return attributeMap;
    }
    String type = sfdcField.getType().toString();
    attributeMap.put(salesforceNsHeaderPrefix + "salesforceType", type);
    if (STRING_TYPES.contains(type)) {
      attributeMap.put(salesforceNsHeaderPrefix + "length", Integer.toString(sfdcField.getLength()));
    } else if (DECIMAL_TYPES.contains(type) ||
        "currency".equals(type) ||
        "percent".equals(type)) {
      attributeMap.put(salesforceNsHeaderPrefix + "precision", Integer.toString(sfdcField.getPrecision()));
      attributeMap.put(salesforceNsHeaderPrefix + "scale", Integer.toString(sfdcField.getScale()));
    } else if (INT_TYPES.contains(type)) {
      attributeMap.put(salesforceNsHeaderPrefix + "digits", Integer.toString(sfdcField.getDigits()));
    }

    return attributeMap;
  }

  // Recurse through the tree of referenced types, building a metadata query for each level
  // Salesforce constrains the depth of the tree to 5, so we don't need to worry about
  // infinite recursion
  public static void getAllReferences(
      PartnerConnection partnerConnection,
      Map<String, Map<String, com.sforce.soap.partner.Field>> metadataMap,
      String[] types
  ) throws ConnectionException {
    List<String> next = new ArrayList<>();

    for (DescribeSObjectResult result : partnerConnection.describeSObjects(types)) {
      Map<String, com.sforce.soap.partner.Field> fieldMap = new LinkedHashMap<>();
      com.sforce.soap.partner.Field[] fields = result.getFields();
      for (int i = 0; i < fields.length; i++) {
        com.sforce.soap.partner.Field field = fields[i];
        fieldMap.put(field.getName().toLowerCase(), field);
      }

      metadataMap.put(result.getName().toLowerCase(), fieldMap);

      Set<String> sobjectNames = metadataMap.keySet();
      for (com.sforce.soap.partner.Field field : fieldMap.values()) {
        for (String ref : field.getReferenceTo()) {
          ref = ref.toLowerCase();
          if (!sobjectNames.contains(ref) && !next.contains(ref)) {
            next.add(ref);
          }
        }
      }
    }

    if (next.size() > 0) {
      getAllReferences(partnerConnection, metadataMap, next.toArray(new String[0]));
    }
  }

  public static Map<String, Map<String, com.sforce.soap.partner.Field>> getMetadataMap(
      PartnerConnection partnerConnection,
      String sobjectType
  ) throws ConnectionException {
    Map<String, Map<String, com.sforce.soap.partner.Field>> metadataMap = new LinkedHashMap<>();
    getAllReferences(partnerConnection, metadataMap, new String[]{sobjectType});
    return metadataMap;
  }

  public static String getSobjectTypeFromQuery(String query) {
    Matcher m = sObjectFromQueryPattern.matcher(query);
    if (m.matches()) {
      return m.group(1).toLowerCase();
    }
    return null;
  }

  public static String expandWildcard(
      String query,
      String sobjectType,
      Map<String, Map<String, com.sforce.soap.partner.Field>> metadataMap
  ) {
    Matcher m = ForceUtils.WILDCARD_SELECT_PATTERN.matcher(query);
    if (m.matches()) {
      // Query is SELECT * FROM... - substitute in list of field names
      StringBuilder fieldsString = new StringBuilder();
      for (com.sforce.soap.partner.Field field : metadataMap.get(sobjectType.toLowerCase()).values()) {
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
      query = query.replaceFirst("\\*", fieldsString.toString());
    }
    return query;
  }

  public static int getOperationFromRecord(Record record,
      SalesforceOperationType defaultOp,
      UnsupportedOperationAction unsupportedAction,
      List<OnRecordErrorException> errorRecords) {
    String op = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
    int opCode = -1; // unsupported
    // Check if the operation code from header attribute is valid
    if (op != null && !op.isEmpty()) {
      try {
        opCode = SalesforceOperationType.convertToIntCode(op);
      } catch (NumberFormatException | UnsupportedOperationException ex) {
        LOG.debug(
            "Operation obtained from record is not supported. Handle by UnsupportedOpertaionAction {}. {}",
            unsupportedAction.getLabel(),
            ex
        );
        switch (unsupportedAction) {
          case DISCARD:
            LOG.debug("Discarding record with unsupported operation {}", op);
            break;
          case SEND_TO_ERROR:
            LOG.debug("Sending record to error due to unsupported operation {}", op);
            errorRecords.add(new OnRecordErrorException(record, Errors.FORCE_23, op));
            break;
          case USE_DEFAULT:
            opCode = defaultOp.code;
            break;
          default: //unknown action
            LOG.debug("Sending record to error due to unknown operation {}", op);
        }
      }
    } else {
      opCode = defaultOp.code;
    }
    return opCode;
  }
}
