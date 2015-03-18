/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.impl.Utils;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class ScriptingProcessor extends SingleLaneProcessor {

  // to hide all Header implementation methods from script
  public static class HeaderWrapper implements Record.Header {
    private Record.Header header;

    private HeaderWrapper(Record.Header header) {
      this.header = header;
    }
    @Override
    public String getStageCreator() {
      return header.getStageCreator();
    }

    @Override
    public String getSourceId() {
      return header.getSourceId();
    }

    @Override
    public String getTrackingId() {
      return header.getTrackingId();
    }

    @Override
    public String getPreviousTrackingId() {
      return header.getPreviousTrackingId();
    }

    @Override
    public String getStagesPath() {
      return header.getStagesPath();
    }

    @Override
    public byte[] getRaw() {
      return header.getRaw();
    }

    @Override
    public String getRawMimeType() {
      return header.getRawMimeType();
    }

    @Override
    public Set<String> getAttributeNames() {
      return header.getAttributeNames();
    }

    @Override
    public String getAttribute(String name) {
      return header.getAttribute(name);
    }

    @Override
    public void setAttribute(String name, String value) {
      header.setAttribute(name, value);
    }

    @Override
    public void deleteAttribute(String name) {
      header.deleteAttribute(name);
    }

    @Override
    public String getErrorDataCollectorId() {
      return header.getErrorDataCollectorId();
    }

    @Override
    public String getErrorPipelineName() {
      return header.getErrorPipelineName();
    }

    @Override
    public String getErrorCode() {
      return header.getErrorCode();
    }

    @Override
    public String getErrorMessage() {
      return header.getErrorMessage();
    }

    @Override
    public String getErrorStage() {
      return header.getErrorStage();
    }

    @Override
    public long getErrorTimestamp() {
      return header.getErrorTimestamp();
    }
  }

  // to hide all Record implementation methods from script
  public static class RecordWrapper implements Record {
    private Record record;
    private Header header;

    public RecordWrapper(Record record) {
      this.record = record;
    }

    private Record getRecord() {
      return record;
    }

    @Override
    public Header getHeader() {
      if (header == null) {
        header = new HeaderWrapper(record.getHeader());
      }
      return header;
    }

    @Override
    public Field get() {
      return record.get();
    }

    @Override
    public Field set(Field field) {
      return record.set(field);
    }

    @Override
    public Field get(String fieldPath) {
      return record.get(fieldPath);
    }

    @Override
    public Field delete(String fieldPath) {
      return record.delete(fieldPath);
    }

    @Override
    public boolean has(String fieldPath) {
      return record.has(fieldPath);
    }

    @Override
    public Set<String> getFieldPaths() {
      return record.getFieldPaths();
    }

    @Override
    public Field set(String fieldPath, Field newField) {
      return record.set(fieldPath, newField);
    }
  }

  public interface Out {
    public void write(RecordWrapper record);
  }

  // to hide all other methods of batchMaker
  public class OutImpl implements Out {
    private SingleLaneBatchMaker batchMaker;

    private void set(SingleLaneBatchMaker batchMaker) {
      this.batchMaker = batchMaker;
    }

    public void write(RecordWrapper record) {
      batchMaker.addRecord(record.getRecord());
    }
  }

  public interface Err {
    public void write(RecordWrapper record, String errMsg);
  }

  // to hide all other methods of Stage.Context
  public class ErrImpl implements Err{
    public void write(RecordWrapper record, String errMsg) {
      getContext().toError(record.getRecord(), errMsg);
    }
  }

  private final String scriptingEngineName;
  private final String scriptConfigGroup;
  private final String scriptConfigName;
  private final ProcessingMode processingMode;
  private final  String script;
  private ScriptEngine engine;
  private OutImpl out;
  private ErrImpl err;

  public ScriptingProcessor(String scriptingEngineName, String scriptConfigGroup, String scriptConfigName,
      ProcessingMode processingMode, String script) {
    this.scriptingEngineName = scriptingEngineName;
    this.scriptConfigGroup = scriptConfigGroup;
    this.scriptConfigName = scriptConfigName;
    this.processingMode = processingMode;
    this.script = prepareScript(script);
  }

  protected String prepareScript(String script) {
    return script;
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
    try {
      engine = new ScriptEngineManager(getClass().getClassLoader()).getEngineByName(scriptingEngineName);
      if (engine == null) {
        issues.add(getContext().createConfigIssue(null, null, Errors.SCRIPTING_00, scriptingEngineName));
      }
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(null, null, Errors.SCRIPTING_01, scriptingEngineName, ex.getMessage(),
                                                ex));
    }

    if (script.trim().isEmpty()) {
      issues.add(getContext().createConfigIssue(scriptConfigGroup, scriptConfigName, Errors.SCRIPTING_02));
    }

    // we cannot verify the script is syntactically correct as there is no way to differentiate
    // that from an exception due to a script execution :(

    return issues;
  }

  @Override
  protected void init() throws StageException {
    super.init();
    err = new ErrImpl();
    out = new OutImpl();
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    out.set(singleLaneBatchMaker);
    switch (processingMode) {
      case RECORD: {
        List<RecordWrapper> records = new ArrayList<>();
        records.add(null);
        Iterator<Record> it = batch.getRecords();
        while (it.hasNext()) {
          records.set(0, new RecordWrapper(it.next()));
          runScript(records, out, err);
        }
        break;
      }
      case BATCH: {
        List<RecordWrapper> records = new ArrayList<>();
        Iterator<Record> it = batch.getRecords();
        while (it.hasNext()) {
          records.add(new RecordWrapper(it.next()));
        }
        runScript(records, out, err);
        break;
      }
    }
  }

  private void runScript(List<RecordWrapper> records, OutImpl out, ErrImpl err) throws StageException {
    SimpleBindings bindings = new SimpleBindings();
    bindings.put("records", records);
    bindings.put("out", out);
    bindings.put("err", err);
    try {
      runScript(bindings);
    } catch (ScriptException ex) {
      switch (processingMode) {
        case RECORD:
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().toError(records.get(0).getRecord(), Errors.SCRIPTING_05, ex.getMessage(), ex);
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.SCRIPTING_05, ex.getMessage(), ex);
            default:
              throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                           getContext().getOnErrorRecord(), ex));
          }
          break;
        case BATCH:
          throw new StageException(Errors.SCRIPTING_06, ex.getMessage(), ex);
        default:
          throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                       getContext().getOnErrorRecord(), ex));
      }
    }

  }

  private void runScript(SimpleBindings bindings) throws ScriptException {
    engine.eval(script, bindings);
  }

}
