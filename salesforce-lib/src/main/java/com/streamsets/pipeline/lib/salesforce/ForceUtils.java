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
import com.streamsets.pipeline.api.Field;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ForceUtils {
  private static final String SOBJECT_TYPE_FROM_QUERY = "^SELECT.*FROM\\s*(\\S*)\\b.*";
  private static Pattern sObjectFromQueryPattern = Pattern.compile(SOBJECT_TYPE_FROM_QUERY, Pattern.DOTALL);

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

  public static Field createField(Object val) {
    return createField(val, DataType.USE_SALESFORCE_TYPE);
  }

  public static Field createField(Object val, DataType userSpecifiedType) {
    if (userSpecifiedType != DataType.USE_SALESFORCE_TYPE) {
      return Field.create(Field.Type.valueOf(userSpecifiedType.getLabel()), val);
    } else {
      if (val instanceof Boolean) {
        return Field.create((Boolean)val);
      } else if (val instanceof Character) {
        return  Field.create((Character)val);
      } else if (val instanceof Byte) {
        return  Field.create((Byte)val);
      } else if (val instanceof Short) {
        return  Field.create((Short)val);
      } else if (val instanceof Integer) {
        return  Field.create((Integer)val);
      } else if (val instanceof Long) {
        return  Field.create((Long)val);
      } else if (val instanceof Float) {
        return  Field.create((Float)val);
      } else if (val instanceof Double) {
        return  Field.create((Double)val);
      } else if (val instanceof BigDecimal) {
        return  Field.create((BigDecimal)val);
      } else if (val instanceof String) {
        return  Field.create((String)val);
      } else if (val instanceof byte[]) {
        return  Field.create((byte[])val);
      } else if (val instanceof Date) {
        return  Field.createDatetime((Date)val);
      } else if (val == null) {
        return  Field.create(Field.Type.STRING, null);
      } else {
        return null;
      }
    }
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

    String type = sfdcField.getType().toString();
    attributeMap.put(salesforceNsHeaderPrefix + "salesforceType", type);
    if ("string".equals(type) || "textarea".equals(type)) {
      attributeMap.put(salesforceNsHeaderPrefix + "length", Integer.toString(sfdcField.getLength()));
    } else if ("double".equals(type)) {
      attributeMap.put(salesforceNsHeaderPrefix + "precision", Integer.toString(sfdcField.getPrecision()));
      attributeMap.put(salesforceNsHeaderPrefix + "scale", Integer.toString(sfdcField.getScale()));
    } else if ("int".equals(type)) {
      attributeMap.put(salesforceNsHeaderPrefix + "digits", Integer.toString(sfdcField.getDigits()));
    }

    return attributeMap;
  }

  public static Map<String, com.sforce.soap.partner.Field> getFieldMap(
      PartnerConnection partnerConnection,
      String sobjectType
  ) throws ConnectionException {
    DescribeSObjectResult[] results = partnerConnection.describeSObjects(new String[]{sobjectType});

    Map<String, com.sforce.soap.partner.Field> fieldMap = new LinkedHashMap<>();
    com.sforce.soap.partner.Field[] fields = results[0].getFields();
    for (int i = 0; i < fields.length; i++) {
      com.sforce.soap.partner.Field field = fields[i];
      String typeName = field.getType().name();
      if ("address".equals(typeName) || "location".equals(typeName)) {
        // Skip compound fields of address or geolocation type since they are returned
        // with null values by the SOAP API and not supported at all by the Bulk API
        continue;
      }
      fieldMap.put(field.getName(), field);
    }

    return fieldMap;
  }

  public static String getSobjectTypeFromQuery(String query) {
    Matcher m = sObjectFromQueryPattern.matcher(query);
    if (m.matches()) {
      return m.group(1);
    }
    return null;
  }
}
