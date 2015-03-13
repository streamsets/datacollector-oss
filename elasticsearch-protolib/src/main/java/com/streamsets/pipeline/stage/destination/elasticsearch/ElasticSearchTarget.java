/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.google.common.collect.ImmutableList;
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
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ElasticSearchTarget extends BaseTarget {

  private final String clusterName;
  private final List<String> uris;
  private final Map<String, String> configs;
  private final String indexTemplate;
  private final String typeTemplate;
  private final String docIdTemplate;

  public ElasticSearchTarget(String clusterName, List<String> uris,
      Map<String, String> configs, String indexTemplate, String typeTemplate, String docIdTemplate) {
    this.clusterName = clusterName;
    this.uris = uris;
    this.configs = configs;
    this.indexTemplate = indexTemplate;
    this.typeTemplate = typeTemplate;
    this.docIdTemplate = docIdTemplate;
  }

  @Override
  public List<ELEval> getELEvals(ELContext elContext) {
    return ImmutableList.of(
      ElUtil.createIndexEval(elContext),
      ElUtil.createTypeEval(elContext),
      ElUtil.createDocIdEval(elContext)
    );
  }

  private Date batchTime;
  private ELEval indexEval;
  private ELEval typeEval;
  private ELEval docIdEval;
  private CharDataGeneratorFactory generatorFactory;
  private Client elasticClient;

  private void validateEL(ELEval elEval, String elStr, String config, ErrorCode parseError, ErrorCode evalError,
      List<ConfigIssue> issues) {
    ELVars vars = getContext().createELVars();
    RecordEL.setRecordInContext(vars, getContext().createRecord("validateConfigs"));
    boolean parsed = false;
    try {
      getContext().parseEL(elStr);
      parsed = true;
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(Groups.ELASTIC_SEARCH.name(), config, parseError, ex.getMessage(), ex));
    }
    if (parsed) {
      try {
        elEval.eval(vars, elStr, String.class);
      } catch (ELEvalException ex) {
        issues
            .add(getContext().createConfigIssue(Groups.ELASTIC_SEARCH.name(), config, evalError, ex.getMessage(), ex));
      }
    }
  }
  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();

    indexEval = ElUtil.createIndexEval(getContext());
    typeEval = ElUtil.createTypeEval(getContext());
    docIdEval = ElUtil.createDocIdEval(getContext());

    validateEL(indexEval, indexTemplate, "indexTemplate", Errors.ELASTICSEARCH_00, Errors.ELASTICSEARCH_01, issues);
    validateEL(typeEval, typeTemplate, "typeTemplate", Errors.ELASTICSEARCH_02, Errors.ELASTICSEARCH_03, issues);
    if (docIdTemplate != null && !docIdTemplate.isEmpty()) {
      validateEL(typeEval, docIdTemplate, "docIdTemplate", Errors.ELASTICSEARCH_04, Errors.ELASTICSEARCH_05, issues);
    }

    boolean clusterInfo = true;
    if (clusterName == null || clusterName.isEmpty()) {
      clusterInfo = false;
      issues.add(getContext().createConfigIssue(Groups.ELASTIC_SEARCH.name(), "clusterName", Errors.ELASTICSEARCH_06));
    }
    if (uris == null || uris.isEmpty()) {
      clusterInfo = false;
      issues.add(getContext().createConfigIssue(Groups.ELASTIC_SEARCH.name(), "uris", Errors.ELASTICSEARCH_07));
    } else {
      for (String uri : uris) {
        if (!uri.contains(":")) {
          clusterInfo = false;
          issues.add(getContext().createConfigIssue(Groups.ELASTIC_SEARCH.name(), "uris", Errors.ELASTICSEARCH_09, uri));
        }
      }
    }

    if (clusterInfo) {
      try {
        elasticClient = getElasticClient();
        elasticClient.admin().cluster().health(new ClusterHealthRequest());
      } catch (RuntimeException ex) {
        issues.add(getContext().createConfigIssue(Groups.ELASTIC_SEARCH.name(), null, Errors.ELASTICSEARCH_08,
                                                  ex.getMessage(), ex));
      }
    }

    return issues;
  }

  @Override
  protected void init() throws StageException {
    super.init();
    generatorFactory = new CharDataGeneratorFactory.Builder(getContext(), CharDataGeneratorFactory.Format.JSON)
        .setMode(JsonMode.MULTIPLE_OBJECTS).build();
  }

  private Client getElasticClient() {
    Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put(configs).build();
    InetSocketTransportAddress[] elasticAddresses = new InetSocketTransportAddress[uris.size()];
    for (int i = 0; i < uris.size(); i++) {
      String uri = uris.get(i);
      String[] parts = uri.split(":");
      elasticAddresses[i] = new InetSocketTransportAddress(parts[0], Integer.parseInt(parts[1]));
    }
    return getElasticClient(settings, elasticAddresses);
  }

  protected Client getElasticClient(Settings settings, TransportAddress[] addresses) {
    return new TransportClient(settings).addTransportAddresses(addresses);
  }

  @Override
  public void destroy() {
    if (elasticClient != null) {
      elasticClient.close();
    }
    super.destroy();
  }

  @Override
  public void write(final Batch batch) throws StageException {
    StringWriter writer = new StringWriter();
    setBatchTime();
    ELVars elVars = getContext().createELVars();
    TimeEL.setTimeNowInContext(elVars, getBatchTime());
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
        String index = indexEval.eval(elVars, indexTemplate, String.class);
        String type = typeEval.eval(elVars, typeTemplate, String.class);
        String id = null;
        if (docIdTemplate != null && !docIdTemplate.isEmpty()) {
          id = docIdEval.eval(elVars, docIdTemplate, String.class);
        }
        writer.getBuffer().setLength(0);
        DataGenerator generator = generatorFactory.getGenerator(writer);
        generator.write(record);
        generator.close();
        String json = writer.toString();
        bulkRequest.add(elasticClient.prepareIndex(index, type, id).setContentType(XContentType.JSON).setSource(json));
      } catch (Exception ex) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().toError(record, ex);
            break;
          case STOP_PIPELINE:
            throw new StageException(Errors.ELASTICSEARCH_10, record.getHeader().getSourceId(), ex.getMessage(), ex);
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
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
                getContext().toError(record, Errors.ELASTICSEARCH_11, item.getFailureMessage());
              }
            }
            break;
          case STOP_PIPELINE:
            String msg = bulkResponse.buildFailureMessage();
            if (msg != null && msg.length() > 100) {
              msg = msg.substring(0, 100);
            }
            throw new StageException(Errors.ELASTICSEARCH_12, msg);
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
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

}
