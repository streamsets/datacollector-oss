/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.solr;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Configuration;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.solr.api.Errors;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestSolrTarget {

  @Test
  public void testSolrTargetValidateRecordSolrFieldsPathNonBlank() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    configBean.recordSolrFieldsPath = "NON_BLANK";
    SolrTarget target = getSpiedTarget(configBean);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateRecordSolrFieldsPath(issues);
    assertTrue(issues.isEmpty());
  }

  @Test
  public void testSolrTargetValidateRecordsSolrFieldsPathBlank() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    configBean.recordSolrFieldsPath = "";
    SolrTarget target = getSpiedTarget(configBean);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateRecordSolrFieldsPath(issues);
    assertEquals(1, issues.size());
    FakeConfigIssue issue = (FakeConfigIssue) issues.get(0);
    assertEquals(Errors.SOLR_11, issue.getErrorCode());
  }

  @Test
  public void testSolrTargetValidateSolrURIValidURI() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    configBean.solrURI = "non/empty:123/field";
    SolrTarget target = getSpiedTarget(configBean);
    // Avoid null pointers
    PowerMockito.doReturn(true).when(target).inSingleNode();
    PowerMockito.doReturn(false).when(target).inCloud();
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateSolrURI(issues);
    assertTrue(issues.isEmpty());
  }

  @Test
  public void testSolrTargetValidateSolrURIInvalidURISingleNode() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    SolrTarget target = getSpiedTarget(configBean);
    // Avoid null pointers
    PowerMockito.doReturn(true).when(target).inSingleNode();
    PowerMockito.doReturn(false).when(target).inCloud();
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateSolrURI(issues);
    assertEquals(1, issues.size());
    FakeConfigIssue issue = (FakeConfigIssue) issues.get(0);
    assertEquals(Errors.SOLR_00, issue.getErrorCode());
  }

  @Test
  public void testSolrTargetValidateSolrURIInvalidURICloud() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    SolrTarget target = getSpiedTarget(configBean);
    // Avoid null pointers
    PowerMockito.doReturn(false).when(target).inSingleNode();
    PowerMockito.doReturn(true).when(target).inCloud();
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateSolrURI(issues);
    assertEquals(1, issues.size());
    FakeConfigIssue issue = (FakeConfigIssue) issues.get(0);
    assertEquals(Errors.SOLR_01, issue.getErrorCode());
  }

  @Test
  public void testSolrTargetValidateFieldsNamesMapNullList() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    SolrTarget target = getSpiedTarget(configBean);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateFieldsNamesMap(issues);
    assertEquals(1, issues.size());
    FakeConfigIssue issue = (FakeConfigIssue) issues.get(0);
    assertEquals(Errors.SOLR_02, issue.getErrorCode());
  }

  @Test
  public void testSolrTargetValidateFieldsNamesMapEmpty() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    configBean.fieldNamesMap = new ArrayList<>();
    SolrTarget target = getSpiedTarget(configBean);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateFieldsNamesMap(issues);
    assertEquals(1, issues.size());
    FakeConfigIssue issue = (FakeConfigIssue) issues.get(0);
    assertEquals(Errors.SOLR_02, issue.getErrorCode());
  }

  @Test
  public void testSolrTargetValidateFieldsNamesEmptyListAndNotMapped() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    configBean.fieldNamesMap = new ArrayList<>();
    configBean.fieldsAlreadyMappedInRecord = false;
    SolrTarget target = getSpiedTarget(configBean);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateFieldsNamesMap(issues);
    assertEquals(1, issues.size());
    FakeConfigIssue issue = (FakeConfigIssue) issues.get(0);
    assertEquals(Errors.SOLR_02, issue.getErrorCode());
  }

  @Test
  public void testSolrTargetValidateFieldsNamesValidInput() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    configBean.fieldNamesMap = new ArrayList<SolrFieldMappingConfig>() {
      {
        add(null);
      }
    };
    configBean.fieldsAlreadyMappedInRecord = true;
    SolrTarget target = getSpiedTarget(configBean);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateFieldsNamesMap(issues);
    assertTrue(issues.isEmpty());
  }

  @Test
  public void testSolrTargetValidateConnectionTimeoutNegativeValue() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    configBean.connectionTimeout = -1;
    SolrTarget target = getSpiedTarget(configBean);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateConnectionTimeout(issues);
    assertEquals(1, issues.size());
    FakeConfigIssue issue = (FakeConfigIssue) issues.get(0);
    assertEquals(Errors.SOLR_14, issue.getErrorCode());
  }

  @Test
  public void testSolrTargetValidateSocketTimeoutNegativeValue() throws Exception {
    SolrConfigBean configBean = new SolrConfigBean();
    configBean.socketTimeout = -1;
    SolrTarget target = getSpiedTarget(configBean);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    target.validateSocketConnection(issues);
    assertEquals(1, issues.size());
    FakeConfigIssue issue = (FakeConfigIssue) issues.get(0);
    assertEquals(Errors.SOLR_15, issue.getErrorCode());
  }

  private SolrTarget getSpiedTarget(SolrConfigBean configBean) throws Exception {
    SolrTarget target = new SolrTarget(configBean);
    SolrTarget spiedTarget = PowerMockito.spy(target);
    PowerMockito.when(spiedTarget, "getContext").thenReturn(new FakeContext());
    return spiedTarget;
  }

  private static class FakeConfigIssue implements ConfigIssue {

    private ErrorCode errorCode;

    FakeConfigIssue(ErrorCode errorCode) {
      this.errorCode = errorCode;
    }

    ErrorCode getErrorCode() {
      return errorCode;
    }

  }

  private static class FakeContext implements Target.Context {

    public ConfigIssue createConfigIssue(String groupName, String configName, ErrorCode errorCode,
        Object... objects) {
      return new FakeConfigIssue(errorCode);
    }

    @Override
    public void complete(Record record) {

    }

    @Override
    public void complete(Collection<Record> collection) {

    }

    @Override
    public String getEnvironmentVersion() {
      return null;
    }

    @Override
    public ExecutionMode getExecutionMode() {
      return null;
    }

    @Override
    public long getPipelineMaxMemory() {
      return 0;
    }

    @Override
    public boolean isPreview() {
      return false;
    }

    @Override
    public Stage.UserContext getUserContext() {
      return null;
    }

    @Override
    public Stage.Info getStageInfo() {
      return null;
    }

    @Override
    public List<Stage.Info> getPipelineInfo() {
      return null;
    }

    @Override
    public void reportError(Exception e) {

    }

    @Override
    public void reportError(String s) {

    }

    @Override
    public void reportError(ErrorCode errorCode, Object... objects) {

    }

    @Override
    public OnRecordError getOnErrorRecord() {
      return null;
    }

    @Override
    public long getLastBatchTime() {
      return 0;
    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public boolean isErrorStage() {
      return false;
    }

    @Override
    public EventRecord createEventRecord(String s, int i, String s1) {
      return null;
    }

    @Override
    public LineageEvent createLineageEvent(LineageEventType lineageEventType) {
      return null;
    }

    @Override
    public void publishLineageEvent(LineageEvent lineageEvent) {

    }

    @Override
    public String getSdcId() {
      return null;
    }

    @Override
    public void finishPipeline() {

    }

    @Override
    public void finishPipeline(boolean b) {

    }

    @Override
    public String getPipelineId() {
      return null;
    }

    @Override
    public Map<String, Object> getStageRunnerSharedMap() {
      return null;
    }

    @Override
    public <T> T getService(Class<? extends T> aClass) {
      return null;
    }

    @Override
    public String getConfig(String s) {
      return null;
    }

    @Override
    public Configuration getConfiguration() {
      return null;
    }

    @Override
    public int getRunnerId() {
      return 0;
    }

    @Override
    public int getRunnerCount() {
      return 0;
    }

    @Override
    public String getResourcesDirectory() {
      return null;
    }

    @Override
    public Map<String, Object> getPipelineConstants() {
      return null;
    }

    @Override
    public Record createRecord(String s) {
      return null;
    }

    @Override
    public Record createRecord(String s, byte[] bytes, String s1) {
      return null;
    }

    @Override
    public MetricRegistry getMetrics() {
      return null;
    }

    @Override
    public Timer createTimer(String s) {
      return null;
    }

    @Override
    public Timer getTimer(String s) {
      return null;
    }

    @Override
    public Meter createMeter(String s) {
      return null;
    }

    @Override
    public Meter getMeter(String s) {
      return null;
    }

    @Override
    public Counter createCounter(String s) {
      return null;
    }

    @Override
    public Counter getCounter(String s) {
      return null;
    }

    @Override
    public Histogram createHistogram(String s) {
      return null;
    }

    @Override
    public Histogram getHistogram(String s) {
      return null;
    }

    @Override
    public Gauge<Map<String, Object>> createGauge(String s) {
      return null;
    }

    @Override
    public Gauge<Map<String, Object>> createGauge(String s, Comparator<String> comparator) {
      return null;
    }

    @Override
    public Gauge<Map<String, Object>> getGauge(String s) {
      return null;
    }

    @Override
    public void parseEL(String s) throws ELEvalException {

    }

    @Override
    public ELVars createELVars() {
      return null;
    }

    @Override
    public ELEval createELEval(String s) {
      return null;
    }

    @Override
    public ELEval createELEval(String s, Class<?>... classes) {
      return null;
    }

    @Override
    public void toError(Record record, Exception e) {

    }

    @Override
    public void toError(Record record, String s) {

    }

    @Override
    public void toError(Record record, ErrorCode errorCode, Object... objects) {

    }

    @Override
    public void toEvent(EventRecord eventRecord) {

    }

    @Override
    public void toSourceResponse(Record record) {

    }
  }
}