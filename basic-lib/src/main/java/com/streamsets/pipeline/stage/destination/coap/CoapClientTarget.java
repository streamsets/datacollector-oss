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
package com.streamsets.pipeline.stage.destination.coap;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.coap.Errors;
import com.streamsets.pipeline.lib.coap.Groups;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.http.HttpClientCommon;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

public class CoapClientTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(CoapClientTarget.class);
  private static final String CONF_RESOURCE_URL = "conf.resourceUrl";
  private final CoapClientTargetConfig conf;
  private DataGeneratorFactory generatorFactory;
  private ErrorRecordHandler errorRecordHandler;
  private CoapClient coapClient;

  CoapClientTarget(CoapClientTargetConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    if(issues.size() == 0) {
      conf.dataGeneratorFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.COAP.name(),
          HttpClientCommon.DATA_FORMAT_CONFIG_PREFIX,
          issues
      );
      generatorFactory = conf.dataGeneratorFormatConfig.getDataGeneratorFactory();
      try {
        coapClient = getCoapClient();
        if (!coapClient.ping()) {
          issues.add(getContext().createConfigIssue(
              Groups.COAP.toString(),
              CONF_RESOURCE_URL,
              Errors.COAP_03,
              "Ping to CoAP URL failed"
          ));
        }
      } catch (URISyntaxException e) {
        issues.add(getContext().createConfigIssue(
            Groups.COAP.toString(),
            CONF_RESOURCE_URL,
            Errors.COAP_03,
            e.getMessage())
        );
      }
    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      Record record = records.next();
      CoapResponse response = null;
      HttpMethod method = conf.coapMethod;
      int contentType = getContentType();
      try {
        if (method == HttpMethod.POST || method == HttpMethod.PUT) {
          ByteArrayOutputStream byteBufferOutputStream = new ByteArrayOutputStream();
          try (DataGenerator dataGenerator = generatorFactory.getGenerator(byteBufferOutputStream)) {
            dataGenerator.write(record);
            dataGenerator.flush();
            if (conf.dataFormat == DataFormat.TEXT) {
              response = coapClient.post(byteBufferOutputStream.toString(), contentType);
            } else {
              response = coapClient.post(byteBufferOutputStream.toByteArray(), contentType);
            }
          }
        } else if (method == HttpMethod.GET ) {
          response = coapClient.get();
        } else if (method == HttpMethod.DELETE ) {
          response = coapClient.delete();
        }

        if (response == null) {
          errorRecordHandler.onError(
              new OnRecordErrorException(
                  record,
                  Errors.COAP_02
              )
          );
        } else if (!response.isSuccess()) {
          errorRecordHandler.onError(
              new OnRecordErrorException(
                  record,
                  Errors.COAP_00,
                  response.getCode(),
                  response.getResponseText()
              )
          );
        }

      } catch (Exception ex) {
        LOG.error(Errors.COAP_01.getMessage(), ex.toString(), ex);
        throw new OnRecordErrorException(record, Errors.COAP_01, ex.toString());
      }
    }
  }

  private CoapClient getCoapClient() throws URISyntaxException {
    URI coapURI = new URI(conf.resourceUrl);
    CoapClient coapClient = new CoapClient(coapURI);
    coapClient.setTimeout(conf.connectTimeoutMillis);
    if (conf.requestType == RequestType.NONCONFIRMABLE) {
      coapClient.useNONs();
    } else if (conf.requestType == RequestType.CONFIRMABLE) {
      coapClient.useCONs();
    }
    // TODO: coaps (DTLS Support) - https://issues.streamsets.com/browse/SDC-5893
    return coapClient;
  }

  private int getContentType() {
    switch (conf.dataFormat) {
      case TEXT:
        return MediaTypeRegistry.TEXT_PLAIN;
      case BINARY:
        return MediaTypeRegistry.APPLICATION_OCTET_STREAM;
      case JSON:
      case SDC_JSON:
      default:
        return MediaTypeRegistry.APPLICATION_JSON;
    }
  }

  @Override
  public void destroy() {
    super.destroy();
  }
}
