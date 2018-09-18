/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.couchbase;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.QueryExecutionException;
import com.couchbase.client.java.error.subdoc.PathNotFoundException;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlMetrics;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.subdoc.AsyncLookupInBuilder;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.couchbase.CouchbaseConnector;
import com.streamsets.pipeline.lib.couchbase.Errors;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.streamsets.pipeline.lib.util.JsonUtil.jsonToField;

public class CouchbaseProcessor extends BaseProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseProcessor.class);
  private final CouchbaseProcessorConfig config;
  private ErrorRecordHandler errorRecordHandler;
  private ConcurrentHashMap<Record, ErrorRecord> errors;
  private CouchbaseConnector connector = null;

  private ELVars elVars;

  public CouchbaseProcessor(CouchbaseProcessorConfig config) { this.config = config; }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    errors = new ConcurrentHashMap<>();

    elVars = getContext().createELVars();

    // Attempts to connect to Couchbase, returning any connection-time issues
    connector = CouchbaseConnector.getInstance(config, issues, getContext());

    return issues;
  }

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {
    List<Record> records = new ArrayList<>();
    List<Record> successes;

    batch.getRecords().forEachRemaining(records::add);

    errors.clear();

    if (config.lookupType == LookupType.KV) {
      successes = processKV(records);
    } else {
      successes = processN1QL(records);
    }

    successes.forEach(record -> batchMaker.addRecord(record));

    for(Map.Entry<Record, ErrorRecord> e : errors.entrySet()) {
      Record record = e.getKey();
      Throwable ex = e.getValue().throwable;
      Errors error = e.getValue().error;
      boolean passable = e.getValue().passable;

      if (config.missingValueOperation == MissingValueType.PASS) {
        if (passable ||
            ex instanceof DocumentDoesNotExistException ||
            ex.getCause() instanceof DocumentDoesNotExistException ||
            ex instanceof PathNotFoundException ||
            ex.getCause() instanceof PathNotFoundException ||
            ex instanceof QueryExecutionException) {
          batchMaker.addRecord(record);
          continue;
        }
      }

      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          errorRecordHandler.onError(new OnRecordErrorException(record, error, ex));
          break;
        case STOP_PIPELINE:
          errorRecordHandler.onError(records, new StageException(error));
          break;
        default:
          throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
              getContext().getOnErrorRecord()
          ));
      }
    }
  }

  private List<Record> processKV(List<Record> records) {

    return Observable.from(records)
        .observeOn(connector.getScheduler())
        .flatMap(record -> {
          RecordEL.setRecordInContext(elVars, record);

          String key;

          try {
            key = getContext().createELEval("documentKeyEL").eval(elVars, config.documentKeyEL, String.class);
          } catch (ELEvalException e) {
            return handleError(record, Errors.COUCHBASE_16, e, false);
          }

          if (key.isEmpty()) {
            return handleError(record, Errors.COUCHBASE_07, true);
          }

          if (config.useSubdoc) {
            AsyncLookupInBuilder builder = connector.bucket().lookupIn(key);

            int num_subdoc = 0;
            for(SubdocMappingConfig subdocConfig: config.subdocMappingConfigs) {
              if (!subdocConfig.subdocPath.isEmpty() && !subdocConfig.sdcField.isEmpty()) {
                builder.get(subdocConfig.subdocPath);
                num_subdoc++;
              }
            }

            if(num_subdoc == 0) {
              return handleError(record, Errors.COUCHBASE_37, false);
            }

            return builder.execute()
                .timeout(config.couchbase.kvTimeout, TimeUnit.MILLISECONDS)
                .flatMap(frag -> setFragmentInRecord(record, frag))
                .onErrorResumeNext(throwable -> handleError(record, Errors.COUCHBASE_20, throwable, false));
          } else {
            return connector.bucket()
                .get(key)
                .defaultIfEmpty(JsonDocument.create(key))
                .timeout(config.couchbase.kvTimeout, TimeUnit.MILLISECONDS)
                .flatMap(doc -> setDocumentInRecord(record, doc))
                .onErrorResumeNext(throwable -> handleError(record, Errors.COUCHBASE_21, throwable, false));
          }
        })
        .toList()
        .toBlocking()
        .single();
  }

  private List<Record> processN1QL(List<Record> records) {

    return Observable.from(records)
        .observeOn(connector.getScheduler())
        .flatMap(record -> {
          RecordEL.setRecordInContext(elVars, record);

          String n1qlQuery;
          try {
            n1qlQuery = getContext().createELEval("n1qlQueryEL").eval(elVars, config.n1qlQueryEL, String.class);
          } catch (ELEvalException e) {
            LOG.debug("N1QL query is unparsable");
            return handleError(record, Errors.COUCHBASE_22, e, false);
          }

          N1qlParams params = N1qlParams.build()
              .adhoc(!config.n1qlPrepare)
              .serverSideTimeout(config.n1qlTimeout, TimeUnit.MILLISECONDS)
              .disableMetrics(false)
              .pretty(false);

            return connector.bucket()
                .query(N1qlQuery.simple(n1qlQuery, params))
                .timeout(config.n1qlTimeout, TimeUnit.MILLISECONDS)
                .flatMap(result -> result.errors()
                        .flatMap(e -> {
                          LOG.debug("N1QL error: {}", e);
                          handleError(record, Errors.COUCHBASE_23, new RuntimeException(e.toString()), false);
                          return Observable.<N1qlMetrics>empty();
                        })
                        .switchIfEmpty(result.info())
                    .flatMap(info -> {
                      if(info.resultCount() == 0) {
                        LOG.debug("No results returned for query: {}", n1qlQuery);
                        handleError(record, Errors.COUCHBASE_24, new RuntimeException(n1qlQuery), true);
                      }
                      return Observable.<AsyncN1qlQueryRow>empty();
                    })
                    .switchIfEmpty(result.rows()))
                .flatMap(row -> setN1QLRowInRecord(record, row))
                .onErrorResumeNext(throwable -> handleError(record, Errors.COUCHBASE_23, throwable, false));
        })
        .toList()
        .toBlocking()
        .single();
  }

  /**
   * Aggregates errors that occur during the processing of a batch.
   *
   * @param record the record being written
   * @param error the error encountered while writing the record
   * @param ex the exception encountered while writing the record
   * @param passable whether the record can be optionally passed down the pipeline
   * @return an empty observable
   */
  private <T> Observable<T> handleError(Record record, Errors error, Throwable ex, boolean passable) {
    errors.put(record, new ErrorRecord(error, ex, passable));
    return Observable.empty();
  }

  /**
   * Aggregates errors that occur during the processing of a batch.
   *
   * @param record the record being written
   * @param error the error encountered while writing the record
   * @param passable whether the record can be optionally passed down the pipeline
   * @return an empty observable
   */
  private <T> Observable<T> handleError(Record record, Errors error, boolean passable) {
    return handleError(record, error, new RuntimeException(), passable);
  }

  /**
   * Writes sub-document lookup values to the record
   *
   * @param record the record being processed
   * @param frag the sub-document fragment from Couchbase
   * @return an Observable of the updated record
   */
  private Observable<Record> setFragmentInRecord(Record record, DocumentFragment<Lookup> frag) {
    if(frag.content(0) == null) {
      LOG.debug("Sub-document path not found");
      return handleError(record, Errors.COUCHBASE_25, true);
    }

    for(SubdocMappingConfig subdocMapping : config.subdocMappingConfigs) {
     Object fragJson = frag.content(subdocMapping.subdocPath);

      if(fragJson == null) {
        return handleError(record, Errors.COUCHBASE_25, true);
      }

      try {
        record.set(subdocMapping.sdcField, jsonToField(fragJson));
        record.getHeader().setAttribute(config.CAS_HEADER_ATTRIBUTE, String.valueOf(frag.cas()));
      } catch (IOException e) {
        try {
          record.set(subdocMapping.sdcField, jsonToField(JsonObject.fromJson(fragJson.toString()).toMap()));
          record.getHeader().setAttribute(config.CAS_HEADER_ATTRIBUTE, String.valueOf(frag.cas()));
        } catch (IOException ex) {
          return handleError(record, Errors.COUCHBASE_19, ex, false);
        }
      }
    }

    return Observable.just(record);
  }

  /**
   * Writes full document lookup values to the record
   *
   * @param record the record being processed
   * @param doc the JsonDocument from Couchbase
   * @return an Observable of the updated record
   */
  private Observable<Record> setDocumentInRecord(Record record, JsonDocument doc) {
    if(doc.content() == null) {
      LOG.debug("Document does not exist: {}", doc.id());
      return handleError(record, Errors.COUCHBASE_26, true);
    }

    try {
      record.set(config.outputField, jsonToField(doc.content().toMap()));
      record.getHeader().setAttribute(config.CAS_HEADER_ATTRIBUTE, String.valueOf(doc.cas()));
      return Observable.just(record);
    } catch (IOException e) {
      LOG.debug("Unable to set KV lookup in record for: {}", doc.id());
      return handleError(record, Errors.COUCHBASE_19, e, false);
    }
  }

  /**
   * Writes N1QL query result rows to the record
   *
   * @param record the record being processed
   * @param row the current row from the N1QL query result
   * @return an Observable of the updated record
   */
  private Observable<Record> setN1QLRowInRecord(Record record, AsyncN1qlQueryRow row) {

    for(N1QLMappingConfig n1qlMapping : config.n1qlMappingConfigs) {
      if (config.multipleValueOperation == MultipleValueType.FIRST && record.get(n1qlMapping.sdcField) != null) {
        LOG.debug("Only populating output field with first record. Skipping additional result.");
        return Observable.empty();
      }

      Object property = row.value().get(n1qlMapping.property);

      if (property == null) {
        LOG.debug("Requested property not returned: {}", n1qlMapping.property);
        return handleError(record, Errors.COUCHBASE_27, true);
      }

      try {
        record.set(n1qlMapping.sdcField, jsonToField(property));
      } catch (IOException e) {
        try {
          record.set(n1qlMapping.sdcField, jsonToField(JsonObject.fromJson(property.toString()).toMap()));
        } catch (IOException ex) {
          LOG.debug("Unable to set N1QL property in record");
          return handleError(record, Errors.COUCHBASE_19, ex, false);
        }
      }
    }
    return Observable.just(record);
  }

  /**
   * Structure to hold pipeline error, exception pairs that occur during the processing of a batch
   */
  public static class ErrorRecord {
    Errors error;
    Throwable throwable;
    boolean passable;

    ErrorRecord(Errors error, Throwable throwable, boolean passable) {
      this.error = error;
      this.throwable = throwable;
      this.passable = passable;
    }
  }

  @Override
  public void destroy() {
    if (connector != null) {
      connector.close();
    }
    super.destroy();
  }
}