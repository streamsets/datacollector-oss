/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.elasticsearch.api.ElasticSearchFactory;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import org.apache.http.client.fluent.Request;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticSearchTarget extends BaseTarget {

  private static final Pattern URI_PATTERN = Pattern.compile("\\S+:(\\d+)");
  private static final Pattern SHIELD_USER_PATTERN = Pattern.compile("\\S+:\\S+");
  private static final Pattern VERSION_NUMBER_PATTERN = Pattern.compile(".*\"number\":\"([^\"]*)\".*");
  private final ElasticSearchConfigBean conf;
  private ELEval timeDriverEval;
  private TimeZone timeZone;
  private Date batchTime;
  private ELEval indexEval;
  private ELEval typeEval;
  private ELEval docIdEval;
  private DataGeneratorFactory generatorFactory;
  private Client elasticClient;

  public ElasticSearchTarget(ElasticSearchConfigBean conf) {
    this.conf = conf;
    this.timeZone = TimeZone.getTimeZone(conf.timeZoneID);
  }

  private void validateEL(ELEval elEval, String elStr, String config, ErrorCode parseError, ErrorCode evalError,
      List<ConfigIssue> issues) {
    ELVars vars = getContext().createELVars();
    RecordEL.setRecordInContext(vars, getContext().createRecord("validateConfigs"));
    TimeEL.setCalendarInContext(vars, Calendar.getInstance());
    try {
      getContext().parseEL(elStr);
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(Groups.ELASTIC_SEARCH.name(), config, parseError, ex.toString(), ex));
      return;
    }
    try {
      elEval.eval(vars, elStr, String.class);
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(Groups.ELASTIC_SEARCH.name(), config, evalError, ex.toString(), ex));
    }
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    indexEval = getContext().createELEval("indexTemplate");
    typeEval = getContext().createELEval("typeTemplate");
    docIdEval = getContext().createELEval("docIdTemplate");
    timeDriverEval = getContext().createELEval("timeDriver");

    //validate timeDriver
    try {
      getRecordTime(getContext().createRecord("validateTimeDriver"));
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(
          Groups.ELASTIC_SEARCH.name(),
          "timeDriverEval",
          Errors.ELASTICSEARCH_18,
          ex.toString(),
          ex
      ));
    }

    validateEL(
        indexEval,
        conf.indexTemplate,
        ElasticSearchConfigBean.CONF_PREFIX + "indexTemplate",
        Errors.ELASTICSEARCH_00,
        Errors.ELASTICSEARCH_01,
        issues
    );
    validateEL(
        typeEval,
        conf.typeTemplate,
        ElasticSearchConfigBean.CONF_PREFIX + "typeTemplate",
        Errors.ELASTICSEARCH_02,
        Errors.ELASTICSEARCH_03,
        issues
    );
    if (conf.docIdTemplate != null && !conf.docIdTemplate.isEmpty()) {
      validateEL(
          typeEval,
          conf.docIdTemplate,
          ElasticSearchConfigBean.CONF_PREFIX + "docIdTemplate",
          Errors.ELASTICSEARCH_04,
          Errors.ELASTICSEARCH_05,
          issues
      );
    }

    if (conf.clusterName == null || conf.clusterName.isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ELASTIC_SEARCH.name(),
              ElasticSearchConfigBean.CONF_PREFIX + "clusterName",
              Errors.ELASTICSEARCH_06
          )
      );
    }
    if (conf.uris == null || conf.uris.isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ELASTIC_SEARCH.name(),
              ElasticSearchConfigBean.CONF_PREFIX + "uris",
              Errors.ELASTICSEARCH_07
          )
      );
    } else {
      for (String uri : conf.uris) {
        validateUri(uri, issues, ElasticSearchConfigBean.CONF_PREFIX + "uris");
      }
    }

    if (conf.upsert) {
      if (conf.docIdTemplate == null || conf.docIdTemplate.isEmpty()) {
        issues.add(
            getContext().createConfigIssue(
                Groups.ELASTIC_SEARCH.name(),
                ElasticSearchConfigBean.CONF_PREFIX + "upsert",
                Errors.ELASTICSEARCH_19
            )
        );
      }
    }

    if (conf.useShield) {
      if (!SHIELD_USER_PATTERN.matcher(conf.shieldConfigBean.shieldUser).matches()) {
        issues.add(
            getContext().createConfigIssue(
                Groups.SHIELD.name(),
                ShieldConfigBean.CONF_PREFIX + "shieldUser",
                Errors.ELASTICSEARCH_20,
                conf.shieldConfigBean.shieldUser
            )
        );
      }
    }

    if (!issues.isEmpty()) {
      return issues;
    }

    // By default, try to use the 1st cluster uri w/ port 9200 as http endpoint.
    // If this doesn't work, validation will fail and ask the user provide an alternative uri.
    if (conf.httpUri == null || conf.httpUri.isEmpty() || "hostname:port".equals(conf.httpUri)) {
      conf.httpUri = conf.uris.get(0).split(":")[0] + ":9200";
    }
    validateUri(conf.httpUri, issues, ElasticSearchConfigBean.CONF_PREFIX + "httpUri");

    if (!issues.isEmpty()) {
      return issues;
    }

    try {
      String response = Request
          .Get("http://" + conf.httpUri + "?pretty=false")
          .execute()
          .returnContent()
          .asString();
      Matcher matcher = VERSION_NUMBER_PATTERN.matcher(response);

      if (!matcher.matches()) {
        issues.add(
            getContext().createConfigIssue(
                Groups.ELASTIC_SEARCH.name(),
                null,
                Errors.ELASTICSEARCH_12,
                response
            )
        );
      } else {
        Version clientVersion = Version.CURRENT;
        Version clusterVersion = Version.fromString(matcher.group(1));

        // The major and minor version numbers must match between the client and cluster.
        if (clientVersion.major != clusterVersion.major || clientVersion.minor != clusterVersion.minor) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.ELASTIC_SEARCH.name(),
                  null,
                  Errors.ELASTICSEARCH_13,
                  clientVersion,
                  clusterVersion
              )
          );
        }
      }
    } catch (IOException e) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ELASTIC_SEARCH.name(),
              ElasticSearchConfigBean.CONF_PREFIX + "httpUri",
              Errors.ELASTICSEARCH_11,
              e.toString(),
              e
          )
      );
    }

    if (!issues.isEmpty()) {
      return issues;
    }

    try {
      elasticClient = ElasticSearchFactory.client(
          conf.clusterName,
          conf.uris,
          conf.clientSniff,
          conf.configs,
          conf.useShield,
          conf.shieldConfigBean.shieldUser,
          conf.shieldConfigBean.shieldTransportSsl,
          conf.shieldConfigBean.sslKeystorePath,
          conf.shieldConfigBean.sslKeystorePassword,
          conf.shieldConfigBean.sslTruststorePath,
          conf.shieldConfigBean.sslTruststorePassword,
          conf.useFound
      );
      elasticClient.admin().cluster().health(new ClusterHealthRequest());
    } catch (RuntimeException|UnknownHostException ex) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ELASTIC_SEARCH.name(),
              null,
              Errors.ELASTICSEARCH_08,
              ex.toString(),
              ex
          )
      );
    }

    if (!issues.isEmpty()) {
      return issues;
    }

    generatorFactory = new DataGeneratorFactoryBuilder(getContext(), DataGeneratorFormat.JSON)
        .setMode(JsonMode.MULTIPLE_OBJECTS)
        .setCharset(Charset.forName(conf.charset))
        .build();

    return issues;
  }

  @Override
  public void destroy() {
    if (elasticClient != null) {
      elasticClient.close();
    }
    super.destroy();
  }

  @VisibleForTesting
  Date getRecordTime(Record record) throws ELEvalException {
    ELVars variables = getContext().createELVars();
    TimeNowEL.setTimeNowInContext(variables, getBatchTime());
    RecordEL.setRecordInContext(variables, record);
    return timeDriverEval.eval(variables, conf.timeDriver, Date.class);
  }

  @VisibleForTesting
  String getRecordIndex(ELVars elVars, Record record) throws ELEvalException {
    Date date = getRecordTime(record);
    if (date != null) {
      Calendar calendar = Calendar.getInstance(timeZone);
      calendar.setTime(date);
      TimeEL.setCalendarInContext(elVars, calendar);
    }
    return indexEval.eval(elVars, conf.indexTemplate, String.class);
  }

  @Override
  public void write(final Batch batch) throws StageException {
    setBatchTime();
    ELVars elVars = getContext().createELVars();
    TimeNowEL.setTimeNowInContext(elVars, getBatchTime());
    Iterator<Record> it = batch.getRecords();

    BulkRequestBuilder bulkRequest = elasticClient.prepareBulk();
    boolean atLeastOne = false;

    //we need to keep the records in order of appearance in case we have indexing errors
    //and error handling is TO_ERROR
    List<Record> records = new ArrayList<>();

    while (it.hasNext()) {
      atLeastOne = true;
      Record record = it.next();

      records.add(record);

      try {
        RecordEL.setRecordInContext(elVars, record);
        String index = getRecordIndex(elVars, record);
        String type = typeEval.eval(elVars, conf.typeTemplate, String.class);
        String id = null;
        if (conf.docIdTemplate != null && !conf.docIdTemplate.isEmpty()) {
          id = docIdEval.eval(elVars, conf.docIdTemplate, String.class);
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataGenerator generator = generatorFactory.getGenerator(baos);
        generator.write(record);
        generator.close();
        String json = new String(baos.toByteArray(), StandardCharsets.UTF_8);

        IndexRequest insert = elasticClient.prepareIndex(index, type, id)
            .setContentType(XContentType.JSON)
            .setSource(json)
            .request();
        if (conf.upsert) {
          // Upsert cannot be processed without the id. Bulk process does not read document content
          // but only headers and then pass content to the right shard. To extract the right shard,
          // Elasticsearch needs to know the id without parsing the body itself.
          Utils.checkNotNull(id, "Document ID");
          UpdateRequest upsert = elasticClient.prepareUpdate(index, type, id)
              .setDoc(json)
              .setUpsert(insert)
              .request();
          bulkRequest.add(upsert);
        } else {
          bulkRequest.add(insert);
        }
      } catch (IOException ex) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().toError(record, ex);
            break;
          case STOP_PIPELINE:
            throw new StageException(Errors.ELASTICSEARCH_15, record.getHeader().getSourceId(), ex.toString(), ex);
          default:
            throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                                                         getContext().getOnErrorRecord(), ex));
        }
      }
    }
    if (atLeastOne) {
      BulkResponse bulkResponse = bulkRequest.execute().actionGet();
      if (bulkResponse.hasFailures()) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            for (BulkItemResponse item : bulkResponse.getItems()) {
              if (item.isFailed()) {
                Record record = records.get(item.getItemId());
                getContext().toError(record, Errors.ELASTICSEARCH_16, item.getFailureMessage());
              }
            }
            break;
          case STOP_PIPELINE:
            String msg = bulkResponse.buildFailureMessage();
            if (msg != null && msg.length() > 100) {
              msg = msg.substring(0, 100) + " ...";
            }
            throw new StageException(Errors.ELASTICSEARCH_17, msg);
          default:
            throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                                                         getContext().getOnErrorRecord()));
        }
      }
    }
  }

  protected Date setBatchTime() {
    batchTime = new Date();
    return batchTime;
  }

  protected Date getBatchTime() {
    return batchTime;
  }

  private void validateUri(String uri, List<ConfigIssue> issues, String configName) {
    Matcher matcher = URI_PATTERN.matcher(uri);
    if (uri.startsWith("http://") || uri.startsWith("https://") || !matcher.matches()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ELASTIC_SEARCH.name(),
              configName,
              Errors.ELASTICSEARCH_09,
              uri
          )
      );
    } else {
      int port = Integer.valueOf(matcher.group(1));
      if (port < 0 || port > 65535) {
        issues.add(
            getContext().createConfigIssue(
                Groups.ELASTIC_SEARCH.name(),
                configName,
                Errors.ELASTICSEARCH_10,
                port
            )
        );
      }
    }
  }
}
