/**
 * Code was adapted from:
 * https://github.com/apache/flume/blob/trunk/flume-ng-core/src/main/java/org/apache/flume/source/SyslogParser.java
 *
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
package com.streamsets.pipeline.lib.parser.syslog;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.AbstractParser;
import io.netty.buffer.ByteBuf;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class SyslogParser extends AbstractParser {

  static final String SYSLOG_FACILITY = "facility";
  static final String SYSLOG_SEVERITY = "severity";
  static final String SYSLOG_PRIORITY = "priority";
  static final String SYSLOG_VERSION = "version";
  static final String TIMESTAMP = "timestamp";
  static final String HOST = "host";
  static final String REMAINING = "remaining";
  static final String RAW = "raw";
  static final String RECEIVER_PORT = "receiverPort";
  static final String RECEIVER_ADDR = "receiverAddr";
  static final String SENDER_PORT = "senderPort";
  static final String SENDER_ADDR = "senderAddr";
  private static final Field EMPTY_STRING = Field.create("");
  private static final Field SYSLOG_VERSION1 = Field.create(1);
  private static final Pattern TWO_SPACES = Pattern.compile("  ");
  private static final DateTimeFormatter rfc3164Format =
    DateTimeFormat.forPattern("MMM d HH:mm:ss").withZoneUTC();

  private static final String timePat = "yyyy-MM-dd'T'HH:mm:ss";
  private static final int RFC3164_LEN = 15;
  private static final int RFC5424_PREFIX_LEN = 19;
  private final DateTimeFormatter timeParser;
  private final Charset charset;
  private final LoadingCache<String, Long> timestampCache;
  private long recordId;

  public SyslogParser(Stage.Context context, Charset charset) {
    super(context);
    this.charset = charset;
    timeParser = DateTimeFormat.forPattern(timePat).withZoneUTC();
    timestampCache = CacheBuilder.newBuilder().maximumSize(1000).build(
      new CacheLoader<String, Long>() {
        @Override
        public Long load(String key) {
          return timeParser.parseMillis(key);
        }
      });
  }

  @Override
  public List<Record> parse(ByteBuf buf, InetSocketAddress recipient, InetSocketAddress sender)
    throws OnRecordErrorException {
    Map<String, Field> fields = new HashMap<>();
    String senderHost = sender.getHostString();
    if (senderHost == null) {
      senderHost = sender.toString();
    }
    // ipv6 needs to be enclosed in brackets if port specified.
    if (senderHost.contains(":")) {
      senderHost = "[" + senderHost + "]";
    }
    Field senderAddr = Field.create(senderHost + ":" + sender.getPort());
    fields.put(SENDER_ADDR, senderAddr);
    fields.put(SENDER_PORT, Field.create(sender.getPort()));
    final String recordIdentifer = senderAddr.getValueAsString() + "::" + recordId++;
    final String msg = buf.toString(charset);
    int msgLen = msg.length();
    int curPos = 0;
    fields.put(RAW, Field.create(msg));
    if (msg.charAt(curPos) != '<') {
      throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_01, "cannot find open bracket '<'", msg);
    }
    int endBracketPos = msg.indexOf('>');
    if (endBracketPos <= 0 || endBracketPos > 6) {
      throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_01, "cannot find end bracket '>'", msg);
    }
    String priority = msg.substring(1, endBracketPos);
    int pri;
    try {
      pri = Integer.parseInt(priority);
    } catch (NumberFormatException nfe) {
      throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_01, nfe, msg, nfe);
    }
    int facility = pri / 8;
    int severity = pri % 8;

    // Remember priority
    fields.put(SYSLOG_PRIORITY, Field.create(priority));

    // put fac / sev into header
    fields.put(SYSLOG_FACILITY, Field.create(facility));
    fields.put(SYSLOG_SEVERITY, Field.create(severity));

    if (msgLen <= endBracketPos + 1) {
      throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_02, msg);
    }
    // update parsing position
    curPos = endBracketPos + 1;

    // remember version string
    if (msgLen > curPos + 2 && "1 ".equals(msg.substring(curPos, curPos + 2))) {
      // this is curious, I guess the code above matches 1 exactly because
      // there has not been another version.
      fields.put(SYSLOG_VERSION, SYSLOG_VERSION1);
      curPos += 2;
    }

    // now parse timestamp (handle different varieties)
    long ts;
    String tsString;
    char dateStartChar = msg.charAt(curPos);

    // no timestamp specified; use relay current time
    if (dateStartChar == '-') {
      ts = System.currentTimeMillis();
      if (msgLen <= curPos + 2) {
        throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_03, msg);
      }
      curPos += 2; // assume we skip past a space to get to the hostname
      // rfc3164 timestamp
    } else if (dateStartChar >= 'A' && dateStartChar <= 'Z') {
      if (msgLen <= curPos + RFC3164_LEN) {
        throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_04, msg);
      }
      tsString = msg.substring(curPos, curPos + RFC3164_LEN);
      ts = parseRfc3164Time(recordIdentifer, msg, tsString);
      curPos += RFC3164_LEN + 1;
      // rfc 5424 timestamp
    } else {
      int nextSpace = msg.indexOf(' ', curPos);
      if (nextSpace == -1) {
        throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_04, msg);
      }
      tsString = msg.substring(curPos, nextSpace);
      ts = parseRfc5424Date(recordIdentifer, tsString);
      curPos = nextSpace + 1;
    }
    fields.put(TIMESTAMP, Field.create(ts));
    // parse out hostname
    int nextSpace = msg.indexOf(' ', curPos);
    if (nextSpace == -1) {
      throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_03, msg);
    }
    fields.put(HOST, Field.create(msg.substring(curPos, nextSpace)));
    if (msgLen > nextSpace + 1) {
      curPos = nextSpace + 1;
      fields.put(REMAINING, Field.create(msg.substring(curPos)));
    } else {
      fields.put(REMAINING, EMPTY_STRING);
    }
    String receiverHost = recipient.getHostString();
    if (receiverHost == null) {
      receiverHost = recipient.toString();
    }
    // ipv6 needs to be enclosed in brackets if port specified.
    if (receiverHost.contains(":")) {
      receiverHost = "[" + receiverHost + "]";
    }
    Field receiverAddr = Field.create(receiverHost + ":" + recipient.getPort());
    fields.put(RECEIVER_ADDR, receiverAddr);
    fields.put(RECEIVER_PORT, Field.create(recipient.getPort()));
    Record record = context.createRecord(senderAddr.getValueAsString() + "::" + recordId++);
    record.set(Field.create(fields));
    return Arrays.asList(record);
  }

  /**
   * Parse date in RFC 5424 format. Uses an LRU cache to speed up parsing for
   * multiple messages that occur in the same second.
   * @param msg
   * @return Typical (for Java) milliseconds since UNIX epoch
   */
  protected long parseRfc5424Date(String recordIdentifer, String msg) throws OnRecordErrorException {

    boolean includesTimezone = true;
    long ts;
    int curPos = 0;
    int msgLen = msg.length();
    if (msgLen <= RFC5424_PREFIX_LEN) {
      throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_09, msg);
    }
    String timestampPrefix = msg.substring(curPos, RFC5424_PREFIX_LEN);
    try {
      ts = timestampCache.get(timestampPrefix);
    } catch (ExecutionException ex) {
      Throwable cause = Throwables.getRootCause(ex);
      if (cause instanceof IllegalArgumentException) {
        throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_05, cause, timestampPrefix, cause);
      } else {
        // I don't believe this will ever occur
        throw new IllegalStateException(Utils.format(Errors.SYSLOG_05.getMessage(), cause, timestampPrefix),
          cause);
      }
    }
    curPos += RFC5424_PREFIX_LEN;
    // look for the optional fractional seconds
    if (msg.charAt(curPos) == '.') {
      // figure out how many numeric digits
      boolean foundEnd = false;
      int endMillisPos = curPos + 1;
      if (msgLen <= endMillisPos) {
        throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_06, msg);
      }
      // FIXME: TODO: ensure we handle all bad formatting cases
      while (!foundEnd && endMillisPos < msgLen) {
        char curDigit = msg.charAt(endMillisPos);
        if (curDigit >= '0' && curDigit <= '9') {
          endMillisPos++;
        } else {
          foundEnd = true;
        }
      }
      includesTimezone = foundEnd;
      if (!includesTimezone) {
        endMillisPos--;
      }
      // if they had a valid fractional second, append it rounded to millis
      final int fractionalPositions = endMillisPos - (curPos + 1);
      if (fractionalPositions > 0) {
        long milliseconds = Long.parseLong(msg.substring(curPos + 1, endMillisPos));
        if (fractionalPositions > 3) {
          milliseconds /= Math.pow(10, (fractionalPositions - 3));
        } else if (fractionalPositions < 3) {
          milliseconds *= Math.pow(10, (3 - fractionalPositions));
        }
        ts += milliseconds;
      } else {
        throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_07, msg);
      }
      curPos = endMillisPos;
    }
    // look for timezone
    if (includesTimezone) {
      char tzFirst = msg.charAt(curPos);
      // UTC
      if (tzFirst == 'Z') {
        // no-op
      } else if (tzFirst == '+' || tzFirst == '-') {
        if (msgLen <= curPos + 5) {
          throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_08, msg);
        }
        int polarity;
        if (tzFirst == '+') {
          polarity = +1;
        } else {
          polarity = -1;
        }

        char[] h = new char[5];
        for (int i = 0; i < 5; i++) {
          h[i] = msg.charAt(curPos + 1 + i);
        }

        if (h[0] >= '0' && h[0] <= '9'
          && h[1] >= '0' && h[1] <= '9'
          && h[2] == ':'
          && h[3] >= '0' && h[3] <= '9'
          && h[4] >= '0' && h[4] <= '9') {
          try {
            int hourOffset = Integer.parseInt(msg.substring(curPos + 1, curPos + 3));
            int minOffset = Integer.parseInt(msg.substring(curPos + 4, curPos + 6));
            ts -= polarity * ((hourOffset * 60L) + minOffset) * 60000L;
          } catch (NumberFormatException nfe) {
            throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_08, msg, nfe);
          }
        } else {
          throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_08, msg);
        }
      }
    }
    return ts;
  }

  /**
   * Parse the RFC3164 date format. This is trickier than it sounds because this
   * format does not specify a year so we get weird edge cases at year
   * boundaries. This implementation tries to "do what I mean".
   * @param ts RFC3164-compatible timestamp to be parsed
   * @return Typical (for Java) milliseconds since the UNIX epoch
   */
  protected long parseRfc3164Time(String recordIdentifer, String msg, String ts) throws OnRecordErrorException {
    DateTime now = DateTime.now();
    int year = now.getYear();
    ts = TWO_SPACES.matcher(ts).replaceFirst(" ");
    DateTime date;
    try {
      date = rfc3164Format.parseDateTime(ts);
    } catch (IllegalArgumentException e) {
      throw throwOnRecordErrorException(recordIdentifer, msg, Errors.SYSLOG_10, ts, e);
    }
    // try to deal with boundary cases, i.e. new year's eve.
    // rfc3164 dates are really dumb.
    // NB: cannot handle replaying of old logs or going back to the future
    DateTime fixed = date.withYear(year);
    // flume clock is ahead or there is some latency, and the year rolled
    if (fixed.isAfter(now) && fixed.minusMonths(1).isAfter(now)) {
      fixed = date.withYear(year - 1);
      // flume clock is behind and the year rolled
    } else if (fixed.isBefore(now) && fixed.plusMonths(1).isBefore(now)) {
      fixed = date.withYear(year + 1);
    }
    date = fixed;
    return date.getMillis();
  }

  private OnRecordErrorException throwOnRecordErrorException(String recordIdentifer, String msg,
                                                             ErrorCode errorCode, Object... params)
  throws OnRecordErrorException {
    Record record = context.createRecord(recordIdentifer);
    record.set(Field.create(msg));
    throw new OnRecordErrorException(record, errorCode, params);
  }
}
