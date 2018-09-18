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
package com.streamsets.pipeline.stage.destination.couchbase;

import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.ByteArrayDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.AsyncMutateInBuilder;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.SubdocOptionsBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.lib.couchbase.CouchbaseConnector;
import com.streamsets.pipeline.lib.couchbase.Errors;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CouchbaseTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseTarget.class);
  private ErrorRecordHandler errorRecordHandler;
  private ConcurrentHashMap<Record, ErrorRecord> errors;

  private final CouchbaseTargetConfig config;

  private CouchbaseConnector connector = null;
  private DataGeneratorFactory generatorFactory;

  private ELVars elVars;
  private ELEval documentKeyELEval;
  private ELEval documentTtlELEval;
  private ELEval subdocOperationELEval;
  private ELEval subdocPathELEval;

  CouchbaseTarget(CouchbaseTargetConfig config) {
    this.config = config;
  }

  @VisibleForTesting
  CouchbaseTarget(CouchbaseTargetConfig config, CouchbaseConnector connector) {
    this.config = config;
    this.connector = connector;
  }

  @Override
  protected List<ConfigIssue> init() {
    // Initialize reused resources
    List<ConfigIssue> issues = super.init();

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    errors = new ConcurrentHashMap<>();

    elVars = getContext().createELVars();

    if(config.documentKeyEL == null || config.documentKeyEL.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.DOCUMENT.name(), "config.documentKeyEL", Errors.COUCHBASE_07));
    }

    if(config.documentTtlEL == null || config.documentTtlEL.isEmpty()) {
      config.documentTtlEL = "0";
    }

    documentKeyELEval = getContext().createELEval("documentKeyEL");
    documentTtlELEval = getContext().createELEval("documentTtlEL");
    subdocOperationELEval = getContext().createELEval("subdocOperationEL");
    subdocPathELEval = getContext().createELEval("subdocPathEL");

    if(config.dataFormat == null) {
      issues.add(getContext().createConfigIssue(Groups.DATA_FORMAT.name(), "config.dataFormat", Errors.COUCHBASE_28));
      return issues;
    }

    config.dataFormatConfig = new DataGeneratorFormatConfig();

    config.dataFormatConfig.init(
        getContext(),
        config.dataFormat,
        Groups.DATA_FORMAT.name(),
        "config.dataFormatConfig",
        issues
    );

    config.dataFormatConfig.jsonMode = JsonMode.MULTIPLE_OBJECTS;

    generatorFactory = config.dataFormatConfig.getDataGeneratorFactory();

    if(connector == null) {
      // Attempts to connect to Couchbase, returning any connection-time issues
      connector = CouchbaseConnector.getInstance(config, issues, getContext());
    }

    return issues;
  }

  @Override
  public void write(final Batch batch) throws StageException {
    List<Record> records = new ArrayList<>();
    batch.getRecords().forEachRemaining(records::add);

    errors.clear();

    Observable.from(records)
        .observeOn(connector.getScheduler())
        .flatMap(record -> {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();

          try (DataGenerator generator = generatorFactory.getGenerator(baos)) {
            generator.write(record);

          } catch (IOException | DataGeneratorException e) {
            return handleError(record, Errors.COUCHBASE_19, e);
          }

          RecordEL.setRecordInContext(elVars, record);
          TimeNowEL.setTimeNowInContext(elVars, new Date());

          String key;
          int ttl;
          long cas = 0;
          String subdocPath = "";

          try {
            key = documentKeyELEval.eval(elVars, config.documentKeyEL, String.class);
          } catch (ELEvalException e) {
            return handleError(record, Errors.COUCHBASE_16, e);
          }

          try {
            ttl = documentTtlELEval.eval(elVars, config.documentTtlEL, int.class);
          } catch (ELEvalException e) {
            return handleError(record, Errors.COUCHBASE_17, e);
          }

          if(ttl < 0) {
            return handleError(record, Errors.COUCHBASE_36, new RuntimeException());
          }

          if(config.subdocPathEL != null && !config.subdocPathEL.isEmpty()) {
            try {
              subdocPath = subdocPathELEval.eval(elVars, config.subdocPathEL, String.class);
            } catch (ELEvalException e) {
              return handleError(record, Errors.COUCHBASE_18, e);
            }
          }

          if (key.isEmpty()) {
            return handleError(record, Errors.COUCHBASE_07, new RuntimeException());
          }

          if (config.useCas && record.getHeader().getAttribute(config.CAS_HEADER_ATTRIBUTE) != null) {
            try {
              cas = Long.parseLong(record.getHeader().getAttribute(config.CAS_HEADER_ATTRIBUTE));
            } catch (Exception e) {
              return handleError(record, Errors.COUCHBASE_15, e);
            }
          }

          if (subdocPath.isEmpty()) {
            return writeDoc(key, ttl, cas, baos, record)
                .onErrorResumeNext(e -> handleError(record, Errors.COUCHBASE_10, e));
          } else {
            return writeSubdoc(key, ttl, cas, baos, subdocPath, record)
                .onErrorResumeNext(e -> handleError(record, Errors.COUCHBASE_11, e));
          }
        })
        .toList()
        .toBlocking()
        .single();

    for(Map.Entry<Record, ErrorRecord> error : errors.entrySet()) {
      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          errorRecordHandler.onError(new OnRecordErrorException(error.getKey(), error.getValue().error, error.getValue().throwable));
          break;
        case STOP_PIPELINE:
          errorRecordHandler.onError(records, new StageException(error.getValue().error, error.getValue().throwable));
          break;
        default:
          throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
              getContext().getOnErrorRecord()
          ));
      }
    }
  }

  /**
   * Aggregates errors that occur during the processing of a batch.
   *
   * @param record the record being written
   * @param error the error encountered while writing the record
   * @param ex the exception encountered while writing the record
   * @return an empty observable
   */
  private <T> Observable<T> handleError(Record record, Errors error, Throwable ex) {
    errors.put(record, new ErrorRecord(error, ex));
    return Observable.empty();
  }

  /**
   * Evaluates the sdc.operation.type header for a record and returns the equivalent Couchbase write operation type.
   *
   * @param record the record being evaluated
   * @param key the document key, used for logging
   * @return the equivalent WriteOperationType or the default write operation if sdc.operation.type is not configured.
   */
  private WriteOperationType getOperationFromHeader(Record record, String key) {

    String op = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
    if (op == null || op.isEmpty()) {
      return config.defaultWriteOperation;
    }

    int opCode;

    try {
      opCode = Integer.parseInt(op);
    } catch(NumberFormatException e) {
        LOG.debug("Unparsable CDC operation. Sending record to error.");
        handleError(record, Errors.COUCHBASE_08, e);
        return null;
      }

    switch (opCode) {
      case OperationType.INSERT_CODE:
        return WriteOperationType.INSERT;
      case OperationType.UPDATE_CODE:
        return WriteOperationType.REPLACE;
      case OperationType.UPSERT_CODE:
        return WriteOperationType.UPSERT;
      case OperationType.DELETE_CODE:
        return WriteOperationType.DELETE;
      default:
        switch (config.unsupportedOperation) {
          case DISCARD:
            LOG.debug("Unsupported CDC operation for key: {}. Discarding record per configuration.", key);
            return null;
          case TOERROR:
            LOG.debug("Unsupported CDC operation for key: {}. Sending record to error configuration.", key);
            handleError(record, Errors.COUCHBASE_09, new RuntimeException());
            return null;
          default:
            LOG.debug("Unsupported CDC operation for key: {}. Using default write operation per configuration.", key);
            return config.defaultWriteOperation;
        }
    }
  }

  /**
   * Executes a document write operation.
   *
   * @param key the document key
   * @param ttl the document expiry ttl
   * @param cas the compare-and-swap (CAS) value to apply to the write
   * @param baos the raw document content
   * @param record the record being written
   * @return an observable for the document write or an empty observable on error
   */
  private Observable<AbstractDocument> writeDoc(String key, int ttl, long cas, ByteArrayOutputStream baos, Record record) {

    WriteOperationType opType = getOperationFromHeader(record, key);

    if(opType == null) {
      return Observable.empty();
    }

    AbstractDocument doc;

    if(config.dataFormat == DataFormat.JSON) {
      try {
         doc = JsonDocument.create(key, ttl, JsonObject.fromJson(baos.toString(config.dataFormatConfig.charset)), cas);
      } catch(Exception e) {
        return handleError(record, Errors.COUCHBASE_10, e);
      }
    } else {
       doc = ByteArrayDocument.create(key, ttl, baos.toByteArray(), cas);
    }

    switch (opType) {
      case DELETE: {
          LOG.debug("DELETE key: {}, TTL: {}, CAS: {}", key, ttl, cas);
          return connector.bucket().remove(doc, config.persistTo, config.replicateTo)
              .timeout(config.couchbase.kvTimeout, TimeUnit.MILLISECONDS);
      }
      case INSERT: {
          LOG.debug("INSERT key: {}, TTL: {}, CAS: {}", key, ttl, cas);
          return connector.bucket().insert(doc, config.persistTo, config.replicateTo)
              .timeout(config.couchbase.kvTimeout, TimeUnit.MILLISECONDS);
      }
      case REPLACE: {
          LOG.debug("REPLACE key: {}, TTL: {}, CAS: {}", key, ttl, cas);
          return connector.bucket().replace(doc, config.persistTo, config.replicateTo)
              .timeout(config.couchbase.kvTimeout, TimeUnit.MILLISECONDS);
      }
      case UPSERT: {
          LOG.debug("UPSERT key: {}, TTL: {}, CAS: {}", key, ttl, cas);
          return connector.bucket().upsert(doc, config.persistTo, config.replicateTo)
              .timeout(config.couchbase.kvTimeout, TimeUnit.MILLISECONDS);
      }
      default:
        return Observable.empty();
    }
  }

  /**
   * Executes a sub-document write operation.
   *
   * @param key the document key
   * @param ttl the document expiry ttl
   * @param cas the compare-and-swap (CAS) value to apply to the write
   * @param baos the raw document content
   * @param subdocPath the path within the parent document in which to write the subdoc
   * @param record the record being written
   * @return an observable for the subdoc mutation or an empty observable on error
   */
  private Observable<DocumentFragment<Mutation>> writeSubdoc(String key, int ttl, long cas, ByteArrayOutputStream baos,
      String subdocPath, Record record) {

    JsonObject frag;

    try {
      frag = JsonObject.fromJson(baos.toString(config.dataFormatConfig.charset));
    }
    catch (IOException e) {
      return handleError(record, Errors.COUCHBASE_12, e);
    }

    AsyncMutateInBuilder mutation = connector.bucket().mutateIn(key);
    SubdocOptionsBuilder options = new SubdocOptionsBuilder().createPath(true);
    String subdocOpType;

    try {
      subdocOpType = subdocOperationELEval.eval(elVars, config.subdocOperationEL, String.class);
    } catch (ELEvalException e) {
      return handleError(record, Errors.COUCHBASE_13, e);
    }

    if(config.allowSubdoc && !subdocOpType.isEmpty()) {
      switch (subdocOpType.toUpperCase()) {
        case "DELETE":
          LOG.debug("Sub-document DELETE key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
          return buildSubdocMutation(mutation.remove(subdocPath, options), ttl, cas, false);

        case "INSERT":
          LOG.debug("Sub-document INSERT key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
          return buildSubdocMutation(mutation.insert(subdocPath, frag, options), ttl, cas, true);

        case "REPLACE":
          LOG.debug("Sub-document REPLACE key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
          return buildSubdocMutation(mutation.replace(subdocPath, frag), ttl, cas,false);

        case "UPSERT":
          LOG.debug("Sub-document UPSERT key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
          return buildSubdocMutation(mutation.upsert(subdocPath, frag, options), ttl, cas,true);

        case "ARRAY_PREPEND":
          LOG.debug("Sub-document ARRAY_PREPEND key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
          return buildSubdocMutation(mutation.arrayPrepend(subdocPath, frag, options), ttl, cas,true);

        case "ARRAY_APPEND":
          LOG.debug("Sub-document ARRAY_APPEND key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
          return buildSubdocMutation(mutation.arrayAppend(subdocPath, frag, options), ttl, cas, true);

        case "ARRAY_ADD_UNIQUE":
          LOG.debug("Sub-document ARRAY_ADD_UNIQUE key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
          return buildSubdocMutation(mutation.arrayAddUnique(subdocPath, frag, options), ttl, cas, true);

        default:
          switch (config.unsupportedOperation) {
            case DISCARD:
              LOG.debug("Unsupported sub-document operation: {} for key: {}. Discarding record per configuration.", subdocOpType, key);
              return Observable.empty();
            case TOERROR:
              LOG.debug("Unsupported sub-document operation: {} for key: {}. Sending record to error per configuration.", subdocOpType, key);
              return handleError(record, Errors.COUCHBASE_14, new RuntimeException());
            default:
              LOG.debug("Unsupported sub-document operation: {} for key: {}. Using default write operation per configuration.", subdocOpType, key);
              // Fall through
              // Inherit the CDC or default operation
          }
      }
    }

    WriteOperationType opType = getOperationFromHeader(record, key);

    if(opType == null) {
      return Observable.empty();
    }

    switch (opType) {
      case DELETE:
        LOG.debug("Sub-document DELETE key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
        return buildSubdocMutation(mutation.remove(subdocPath, options), ttl, cas,false);

      case INSERT:
        LOG.debug("Sub-document INSERT key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
        return buildSubdocMutation(mutation.insert(subdocPath, frag, options), ttl, cas, true);

      case REPLACE:
        LOG.debug("Sub-document REPLACE key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
        return buildSubdocMutation(mutation.replace(subdocPath, frag), ttl, cas, false);

      case UPSERT:
        LOG.debug("Sub-document UPSERT key: {}, sub-document path: {}, TTL: {}, CAS: {}", key, subdocPath, ttl, cas);
        return buildSubdocMutation(mutation.upsert(subdocPath, frag, options), ttl, cas, true);

      default:
        return Observable.empty();
    }
  }

  /**
   * Applies standard options to sub-document mutations
   *
   * @param mutation the mutation to be augmented
   * @param ttl the time to live to be set on the parent document
   * @param cas the compare-and-swap (CAS) value to apply to the write
   * @param upsertDoc defines whether the parent document should be created if it does not exist
   * @return an augmented mutation
   */
  private Observable<DocumentFragment<Mutation>> buildSubdocMutation(AsyncMutateInBuilder mutation, int ttl, long cas,
      boolean upsertDoc) {

    return mutation
        .upsertDocument(upsertDoc)
        .withExpiry(ttl)
        .withCas(cas)
        .withDurability(config.persistTo, config.replicateTo)
        .execute()
        .timeout(config.couchbase.kvTimeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Structure to hold pipeline error, exception pairs that occur during the processing of a batch
   */
  public static class ErrorRecord {
    Errors error;
    Throwable throwable;

    ErrorRecord(Errors error, Throwable throwable) {
      this.error = error;
      this.throwable = throwable;
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