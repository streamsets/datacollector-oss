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
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import com.streamsets.pipeline.api.Field;

import java.math.BigDecimal;
import java.util.Date;

public class ForceUtils {
  public static String getExceptionCode(Throwable th) {
    return (th instanceof ApiFault) ? ((ApiFault) th).getExceptionCode().name() : "";
  }

  public static String getExceptionMessage(Throwable th) {
    return (th instanceof ApiFault) ? ((ApiFault) th).getExceptionMessage() : th.getMessage();
  }

  public static ConnectorConfig getPartnerConfig(ForceConfigBean conf, SessionRenewer sessionRenewer) {
    ConnectorConfig partnerConfig = new ConnectorConfig();

    partnerConfig.setUsername(conf.username);
    partnerConfig.setPassword(conf.password);
    partnerConfig.setAuthEndpoint("https://"+conf.authEndpoint+"/services/Soap/u/"+conf.apiVersion);
    partnerConfig.setCompression(conf.useCompression);
    partnerConfig.setTraceMessage(conf.showTrace);
    partnerConfig.setSessionRenewer(sessionRenewer);

    return partnerConfig;
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
}
