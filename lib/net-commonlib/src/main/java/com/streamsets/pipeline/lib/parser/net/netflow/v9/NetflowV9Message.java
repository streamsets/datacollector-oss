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
package com.streamsets.pipeline.lib.parser.net.netflow.v9;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.net.netflow.BaseNetflowMessage;
import com.streamsets.pipeline.lib.parser.net.netflow.OutputValuesMode;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class NetflowV9Message extends BaseNetflowMessage {

  public static final String FIELD_VERSION = "version";
  public static final String FIELD_FLOW_RECORD_COUNT = "flowRecordCount";

  public static final String FIELD_SYS_UPTIME_MS = "sysUptimeMs";
  public static final String FIELD_UNIX_SECONDS = "unixSeconds";
  public static final String FIELD_SEQUENCE_NUMBER = "sequenceNumber";
  public static final String FIELD_SOURCE_ID = "sourceId";
  public static final String FIELD_SOURCE_ID_RAW = "sourceIdRaw";

  public static final String FIELD_FLOW_KIND = "flowKind";
  public static final String FIELD_PACKET_HEADER = "packetHeader";
  public static final String FIELD_RAW_VALUES = "rawValues";
  public static final String FIELD_INTERPRETED_VALUES = "values";
  public static final String FIELD_FLOW_TEMPLATE_ID = "flowTemplateId";

  public static final String FIELD_SENDER = "sender";
  public static final String FIELD_RECIPIENT = "recipient";

  private OutputValuesMode outputValuesMode;
  private int flowRecordCount = 0;
  private long systemUptimeMs = 0;
  private long unixSeconds = 0;
  private long sequenceNumber = 0;
  private byte[] sourceIdBytes;
  private long sourceId = 0;
  private FlowKind flowKind;
  private InetSocketAddress sender;
  private InetSocketAddress recipient;

  private final List<NetflowV9Field> fields = new ArrayList<>();
  private int flowTemplateId;

  public int getFlowRecordCount() {
    return flowRecordCount;
  }

  public void setFlowRecordCount(int flowRecordCount) {
    this.flowRecordCount = flowRecordCount;
  }

  public long getSystemUptimeMs() {
    return systemUptimeMs;
  }

  public void setSystemUptimeMs(long systemUptimeMs) {
    this.systemUptimeMs = systemUptimeMs;
  }

  public long getUnixSeconds() {
    return unixSeconds;
  }

  public void setUnixSeconds(long unixSeconds) {
    this.unixSeconds = unixSeconds;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  public void setSequenceNumber(long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  public byte[] getSourceIdBytes() {
    return sourceIdBytes;
  }

  public void setSourceIdBytes(byte[] sourceIdBytes) {
    this.sourceIdBytes = sourceIdBytes;
  }

  public long getSourceId() {
    return sourceId;
  }

  public void setSourceId(long sourceId) {
    this.sourceId = sourceId;
  }

  public OutputValuesMode getOutputValuesMode() {
    return outputValuesMode;
  }

  public void setOutputValuesMode(OutputValuesMode outputValuesMode) {
    this.outputValuesMode = outputValuesMode;
  }

  public FlowKind getFlowKind() {
    return flowKind;
  }

  public void setFlowKind(FlowKind flowKind) {
    this.flowKind = flowKind;
  }

  public InetSocketAddress getSender() {
    return sender;
  }

  public void setSender(InetSocketAddress sender) {
    this.sender = sender;
  }

  public InetSocketAddress getRecipient() {
    return recipient;
  }

  public void setRecipient(InetSocketAddress recipient) {
    this.recipient = recipient;
  }

  @Override
  public void populateRecord(Record record) {
    LinkedHashMap<String, Field> rootMap = new LinkedHashMap<>();
    rootMap.put(FIELD_FLOW_KIND, Field.create(getFlowKind().name()));

    if (getSender() != null) {
      rootMap.put(FIELD_SENDER, Field.create(getSender().toString()));
    }
    if (getRecipient() != null) {
      rootMap.put(FIELD_RECIPIENT, Field.create(getRecipient().toString()));
    }

    LinkedHashMap<String, Field> headerFields = new LinkedHashMap<>();
    headerFields.put(FIELD_VERSION, Field.create(getNetflowVersion()));
    headerFields.put(FIELD_FLOW_RECORD_COUNT, Field.create(getFlowRecordCount()));
    headerFields.put(FIELD_SYS_UPTIME_MS, Field.create(getSystemUptimeMs()));
    headerFields.put(FIELD_UNIX_SECONDS, Field.create(getUnixSeconds()));
    headerFields.put(FIELD_SEQUENCE_NUMBER, Field.create(getSequenceNumber()));
    headerFields.put(FIELD_SOURCE_ID, Field.create(getSourceId()));
    headerFields.put(FIELD_SOURCE_ID_RAW, Field.create(getSourceIdBytes()));
    rootMap.put(FIELD_PACKET_HEADER, Field.createListMap(headerFields));
    rootMap.put(FIELD_FLOW_TEMPLATE_ID, Field.create(getFlowTemplateId()));

    switch (outputValuesMode) {
      case RAW_AND_INTERPRETED:
        rootMap.put(FIELD_RAW_VALUES, Field.createListMap(createFieldsMap(true)));
        rootMap.put(FIELD_INTERPRETED_VALUES, Field.createListMap(createFieldsMap(false)));
        break;
      case RAW_ONLY:
        rootMap.put(FIELD_RAW_VALUES, Field.createListMap(createFieldsMap(true)));
        break;
      case INTERPRETED_ONLY:
        rootMap.put(FIELD_INTERPRETED_VALUES, Field.createListMap(createFieldsMap(false)));
        break;
    }

    record.set(Field.createListMap(rootMap));
  }

  public LinkedHashMap<String, Field> createFieldsMap(boolean rawValues) {
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    getFields().forEach(field -> fields.put(
        field.getSdcFieldName(),
        rawValues ? Field.create(field.getRawValue()) : field.getInterpretedValueField()
    ));
    return fields;
  }

  public List<NetflowV9Field> getFields() {
    return fields;
  }

  public void setFields(List<NetflowV9Field> fields) {
    if (fields != null) {
      this.fields.addAll(fields);
    }
  }

  public int getFlowTemplateId() {
    return flowTemplateId;
  }

  public void setFlowTemplateId(int flowTemplateId) {
    this.flowTemplateId = flowTemplateId;
  }

  @Override
  public int getNetflowVersion() {
    return 9;
  }
}
