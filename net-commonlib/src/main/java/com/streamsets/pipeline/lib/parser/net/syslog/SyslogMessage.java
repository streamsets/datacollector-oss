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
package com.streamsets.pipeline.lib.parser.net.syslog;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.net.MessageToRecord;

import java.util.HashMap;
import java.util.Map;

public class SyslogMessage implements MessageToRecord {
  public static final String FIELD_SYSLOG_FACILITY = "facility";
  public static final String FIELD_SYSLOG_SEVERITY = "severity";
  public static final String FIELD_SYSLOG_PRIORITY = "priority";
  public static final String FIELD_SYSLOG_VERSION = "version";
  public static final String FIELD_TIMESTAMP = "timestamp";
  public static final String FIELD_HOST = "host";
  public static final String FIELD_REMAINING = "remaining";
  public static final String FIELD_RAW = "raw";
  public static final String FIELD_RECEIVER_PORT = "receiverPort";
  public static final String FIELD_RECEIVER_ADDR = "receiverAddr";
  public static final String FIELD_SENDER_PORT = "senderPort";
  public static final String FIELD_SENDER_ADDR = "senderAddr";

  private String senderHost;
  private String senderAddress;
  private int senderPort;
  private String receiverHost;
  private String receiverAddress;
  private int receiverPort;
  private String rawMessage;
  private int priority;
  private int facility;
  private int severity;
  private int syslogVersion;
  private long timestamp;
  private String host;
  private String remainingMessage;

  public String getSenderHost() {
    return senderHost;
  }

  public void setSenderHost(String senderHost) {
    this.senderHost = senderHost;
  }

  public String getSenderAddress() {
    return senderAddress;
  }

  public void setSenderAddress(String senderAddress) {
    this.senderAddress = senderAddress;
  }

  public int getSenderPort() {
    return senderPort;
  }

  public void setSenderPort(int senderPort) {
    this.senderPort = senderPort;
  }

  public String getReceiverHost() {
    return receiverHost;
  }

  public void setReceiverHost(String receiverHost) {
    this.receiverHost = receiverHost;
  }

  public String getReceiverAddress() {
    return receiverAddress;
  }

  public void setReceiverAddress(String receiverAddress) {
    this.receiverAddress = receiverAddress;
  }

  public int getReceiverPort() {
    return receiverPort;
  }

  public void setReceiverPort(int receiverPort) {
    this.receiverPort = receiverPort;
  }

  public String getRawMessage() {
    return rawMessage;
  }

  public void setRawMessage(String rawMessage) {
    this.rawMessage = rawMessage;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public int getFacility() {
    return facility;
  }

  public void setFacility(int facility) {
    this.facility = facility;
  }

  public int getSeverity() {
    return severity;
  }

  public void setSeverity(int severity) {
    this.severity = severity;
  }

  public int getSyslogVersion() {
    return syslogVersion;
  }

  public void setSyslogVersion(int syslogVersion) {
    this.syslogVersion = syslogVersion;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getRemainingMessage() {
    return remainingMessage;
  }

  public void setRemainingMessage(String remainingMessage) {
    this.remainingMessage = remainingMessage;
  }

  @Override
  public void populateRecord(Record record) {
    Map<String, Field> fields = new HashMap<>();
    fields.put(FIELD_SENDER_ADDR, Field.create(getSenderAddress()));
    fields.put(FIELD_SENDER_PORT, Field.create(getSenderPort()));
    fields.put(FIELD_RAW, Field.create(getRawMessage()));
    fields.put(FIELD_SYSLOG_PRIORITY, Field.create(getPriority()));
    fields.put(FIELD_SYSLOG_FACILITY, Field.create(getFacility()));
    fields.put(FIELD_SYSLOG_SEVERITY, Field.create(getSeverity()));
    fields.put(FIELD_SYSLOG_VERSION, Field.create(getSyslogVersion()));
    fields.put(FIELD_TIMESTAMP, Field.create(getTimestamp()));
    fields.put(FIELD_HOST, Field.create(getHost()));
    fields.put(FIELD_REMAINING, Field.create(getRemainingMessage()));
    fields.put(FIELD_RECEIVER_ADDR, Field.create(getReceiverAddress()));
    fields.put(FIELD_RECEIVER_PORT, Field.create(getReceiverPort()));
    record.set(Field.create(fields));
  }
}
