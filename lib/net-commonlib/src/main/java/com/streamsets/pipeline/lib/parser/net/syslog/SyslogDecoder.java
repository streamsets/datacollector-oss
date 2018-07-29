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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class SyslogDecoder extends ByteToMessageDecoder {

  private static final Pattern TWO_SPACES = Pattern.compile("  ");
  private static final DateTimeFormatter rfc3164Format =
      DateTimeFormatter.ofPattern("MMM d HH:mm:ss", Locale.US);

  public static final String RFC5424_TS_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
  private static final int RFC3164_LEN = 15;
  private static final int RFC5424_PREFIX_LEN = 19;
  private final LoadingCache<String, Long> timestampCache;
  private final Charset charset;

  private final Clock clock;

  public SyslogDecoder(Charset charset) {
    this(charset, Clock.systemUTC());
  }

  public SyslogDecoder(Charset charset, Clock clock) {
    this.charset = charset;
    this.clock = clock;
    timestampCache = buildTimestampCache(DateTimeFormatter.ofPattern(RFC5424_TS_PATTERN, Locale.US));
  }

  public static LoadingCache<String, Long> buildTimestampCache(DateTimeFormatter timeParser) {
    return CacheBuilder.newBuilder().maximumSize(1000).build(
        new CacheLoader<String, Long>() {
          @Override
          public Long load(String key) {
            return LocalDateTime.from(timeParser.parse(key)).toInstant(ZoneOffset.UTC).toEpochMilli();
          }
        }
    );
  }

  public void decodeStandaloneBuffer(
      ByteBuf buf,
      List<SyslogMessage> resultMessages,
      InetSocketAddress sender,
      InetSocketAddress recipient
  ) throws OnRecordErrorException {

    final List<Object> results = new LinkedList<>();
    decode(null, buf, results, recipient, sender);
    for (Object result : results) {
      if (result instanceof SyslogMessage) {
        resultMessages.add((SyslogMessage) result);
      } else {
        throw new IllegalStateException(String.format(
            "Found unexpected object type in results: %s",
            result.getClass().getName())
        );
      }
    }
  }

  @Override
  protected void decode(
      ChannelHandlerContext ctx, ByteBuf in, List<Object> out
  ) throws Exception {
    decode(ctx, in, out, null, null);
  }

  public void decode(
      ChannelHandlerContext ctx,
      ByteBuf buf,
      List<Object> out,
      InetSocketAddress recipient,
      InetSocketAddress sender
  ) throws OnRecordErrorException {
    if (ctx != null) {
      if (sender == null) {
        SocketAddress socketAddress = ctx.channel().remoteAddress();
        if (socketAddress instanceof InetSocketAddress) {
          sender = (InetSocketAddress) socketAddress;
        }
      }
      if (recipient == null) {
        SocketAddress socketAddress = ctx.channel().localAddress();
        if (socketAddress instanceof InetSocketAddress) {
          recipient = (InetSocketAddress) socketAddress;
        }
      }
    }

    final SyslogMessage syslogMsg = new SyslogMessage();
    if (sender != null) {
      String senderHost = resolveHostAddressString(sender);
      syslogMsg.setSenderHost(senderHost);
      syslogMsg.setSenderAddress(resolveAddressString(senderHost, sender));
      syslogMsg.setSenderPort(sender.getPort());
    }
    final String msg = buf.toString(charset);
    int msgLen = msg.length();
    int curPos = 0;
    syslogMsg.setRawMessage(msg);
    if (msg.charAt(curPos) != '<') {
      throw new OnRecordErrorException(Errors.SYSLOG_01, "cannot find open bracket '<'", msg);
    }
    int endBracketPos = msg.indexOf('>');
    if (endBracketPos <= 0 || endBracketPos > 6) {
      throw new OnRecordErrorException(Errors.SYSLOG_01, "cannot find end bracket '>'", msg);
    }
    String priority = msg.substring(1, endBracketPos);
    int pri;
    try {
      pri = Integer.parseInt(priority);
    } catch (NumberFormatException nfe) {
      throw new OnRecordErrorException(Errors.SYSLOG_01, nfe, msg, nfe);
    }
    int facility = pri / 8;
    int severity = pri % 8;

    // Remember priority
    syslogMsg.setPriority(pri);

    // put fac / sev into header
    syslogMsg.setFacility(facility);
    syslogMsg.setSeverity(severity);

    if (msgLen <= endBracketPos + 1) {
      throw new OnRecordErrorException(Errors.SYSLOG_02, msg);
    }
    // update parsing position
    curPos = endBracketPos + 1;

    // remember version string
    if (msgLen > curPos + 2 && "1 ".equals(msg.substring(curPos, curPos + 2))) {
      // this is curious, I guess the code above matches 1 exactly because
      // there has not been another version.
      syslogMsg.setSyslogVersion(1);
      curPos += 2;
    }

    // now parse timestamp (handle different varieties)
    long ts;
    String tsString;
    char dateStartChar = msg.charAt(curPos);
    while (dateStartChar == ' ' && curPos < msgLen-1) {
      // consume any spaces immediately after PRI
      dateStartChar = msg.charAt(++curPos);
    }

    // no timestamp specified; use relay current time
    if (dateStartChar == '-') {
      ts = clock.millis();
      if (msgLen <= curPos + 2) {
        throw new OnRecordErrorException(Errors.SYSLOG_03, msg);
      }
      curPos += 2; // assume we skip past a space to get to the hostname
      // rfc3164 timestamp
    } else if (dateStartChar >= 'A' && dateStartChar <= 'Z') {
      if (msgLen <= curPos + RFC3164_LEN) {
        throw new OnRecordErrorException(Errors.SYSLOG_04, msg);
      }
      tsString = msg.substring(curPos, curPos + RFC3164_LEN);
      ts = parseRfc3164Time(tsString);
      curPos += RFC3164_LEN + 1;
      // rfc 5424 timestamp
    } else {
      int nextSpace = msg.indexOf(' ', curPos);
      if (nextSpace == -1) {
        throw new OnRecordErrorException(Errors.SYSLOG_04, msg);
      }
      tsString = msg.substring(curPos, nextSpace);
      ts = parseRfc5424Date(tsString);
      curPos = nextSpace + 1;
    }
    syslogMsg.setTimestamp(ts);
    // parse out hostname
    int nextSpace = msg.indexOf(' ', curPos);
    if (nextSpace == -1) {
      throw new OnRecordErrorException(Errors.SYSLOG_03, msg);
    }
    String host = msg.substring(curPos, nextSpace);
    syslogMsg.setHost(host);
    if (msgLen > nextSpace + 1) {
      curPos = nextSpace + 1;
      syslogMsg.setRemainingMessage(msg.substring(curPos));
    } else {
      syslogMsg.setRemainingMessage("");
    }
    if (recipient != null) {
      String receiverHost = resolveHostAddressString(recipient);
      syslogMsg.setReceiverHost(receiverHost);
      syslogMsg.setReceiverAddress(resolveAddressString(receiverHost, recipient));
      syslogMsg.setReceiverPort(recipient.getPort());
    }
    out.add(syslogMsg);
    // consume the buffer that was just read (as an entire String)
    buf.skipBytes(buf.readableBytes());
  }

  public static String resolveHostAddressString(InetSocketAddress address) {
    String receiverHost = address.getHostString();
    if (receiverHost == null) {
      receiverHost = address.toString();
    }
    // ipv6 needs to be enclosed in brackets if port specified.
    if (receiverHost.contains(":")) {
      receiverHost = "[" + receiverHost + "]";
    }
    return receiverHost;
  }

  public static String resolveAddressString(String hostName, InetSocketAddress address) {
    if (hostName == null || address == null) {
      return null;
    }
    return hostName + ":" + address.getPort();
  }

  protected long parseRfc5424Date(String ts) throws OnRecordErrorException {
    return parseRfc5424Date(timestampCache, ts);
  }

  /**
   * Parse date in RFC 5424 format. Uses an LRU cache to speed up parsing for
   * multiple messages that occur in the same second.
   * @param tsStr
   * @return Typical (for Java) milliseconds since UNIX epoch
   */
  public static long parseRfc5424Date(LoadingCache<String, Long> cache, String tsStr) throws OnRecordErrorException {

    boolean includesTimezone = true;
    long ts;
    int curPos = 0;
    int msgLen = tsStr.length();
    if (msgLen <= RFC5424_PREFIX_LEN) {
      throw new OnRecordErrorException(Errors.SYSLOG_09, tsStr);
    }
    String timestampPrefix = tsStr.substring(curPos, RFC5424_PREFIX_LEN);
    try {
      ts = cache.get(timestampPrefix);
    } catch (ExecutionException ex) {
      Throwable cause = Throwables.getRootCause(ex);
      if (cause instanceof IllegalArgumentException) {
        throw new OnRecordErrorException(Errors.SYSLOG_05, cause, timestampPrefix, cause);
      } else {
        // I don't believe this will ever occur
        throw new IllegalStateException(
            Utils.format(Errors.SYSLOG_05.getMessage(), cause, timestampPrefix),
            cause);
      }
    }
    curPos += RFC5424_PREFIX_LEN;
    // look for the optional fractional seconds
    if (tsStr.charAt(curPos) == '.') {
      // figure out how many numeric digits
      boolean foundEnd = false;
      int endMillisPos = curPos + 1;
      if (msgLen <= endMillisPos) {
        throw new OnRecordErrorException(Errors.SYSLOG_06, tsStr);
      }
      // FIXME: TODO: ensure we handle all bad formatting cases
      while (!foundEnd && endMillisPos < msgLen) {
        char curDigit = tsStr.charAt(endMillisPos);
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
        long milliseconds = Long.parseLong(tsStr.substring(curPos + 1, endMillisPos));
        if (fractionalPositions > 3) {
          milliseconds /= Math.pow(10, (fractionalPositions - 3));
        } else if (fractionalPositions < 3) {
          milliseconds *= Math.pow(10, (3 - fractionalPositions));
        }
        ts += milliseconds;
      } else {
        throw new OnRecordErrorException(Errors.SYSLOG_07, tsStr);
      }
      curPos = endMillisPos;
    }
    // look for timezone
    if (includesTimezone) {
      char tzFirst = tsStr.charAt(curPos);
      // UTC
      if (tzFirst == 'Z') {
        // no-op
      } else if (tzFirst == '+' || tzFirst == '-') {
        if (msgLen <= curPos + 5) {
          throw new OnRecordErrorException(Errors.SYSLOG_08, tsStr);
        }
        int polarity;
        if (tzFirst == '+') {
          polarity = +1;
        } else {
          polarity = -1;
        }

        char[] h = new char[5];
        for (int i = 0; i < 5; i++) {
          h[i] = tsStr.charAt(curPos + 1 + i);
        }

        if (h[0] >= '0' && h[0] <= '9'
            && h[1] >= '0' && h[1] <= '9'
            && h[2] == ':'
            && h[3] >= '0' && h[3] <= '9'
            && h[4] >= '0' && h[4] <= '9') {
          try {
            int hourOffset = Integer.parseInt(tsStr.substring(curPos + 1, curPos + 3));
            int minOffset = Integer.parseInt(tsStr.substring(curPos + 4, curPos + 6));
            ts -= polarity * ((hourOffset * 60L) + minOffset) * 60000L;
          } catch (NumberFormatException nfe) {
            throw new OnRecordErrorException(Errors.SYSLOG_08, tsStr, nfe);
          }
        } else {
          throw new OnRecordErrorException(Errors.SYSLOG_08, tsStr);
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
  public static long parseRfc3164Time(String ts) throws OnRecordErrorException {
    LocalDateTime now = LocalDateTime.now();
    int year = now.getYear();
    ts = TWO_SPACES.matcher(ts).replaceFirst(" ");
    LocalDateTime date;
    try {
      MonthDay monthDay = MonthDay.parse(ts, rfc3164Format);
      LocalTime time = LocalTime.parse(ts, rfc3164Format);
      // this is overly complicated because of the way Java 8 Time API works, as compared to Joda
      // essentially, we just want to pull year out of "now" and set all other fields based on
      // what was parsed
      date = now;
      // zero out millis since we aren't actually parsing those
      date = date.with(ChronoField.MILLI_OF_SECOND, 0);
      // set month and day of month from parsed
      date = date.withMonth(monthDay.getMonthValue()).withDayOfMonth(monthDay.getDayOfMonth());
      // set time fields from parsed
      date = date.withHour(time.getHour()).withMinute(time.getMinute()).withSecond(time.getSecond());
    } catch (DateTimeParseException e) {
      throw new OnRecordErrorException(Errors.SYSLOG_10, ts, e);
    }
    // The RFC3164 is a bit weird date format - it contains day and month, but no year. So we have to somehow guess
    // the year. The current logic is to provide a sliding window - going 11 months to the past and 1 month to the
    // future. If the message is outside of this window, it will have incorrectly guessed year. We go 11 months to the
    // past as we're expecting that more messages will be from the past (syslog usually contains historical data).
    LocalDateTime fixed = date;
    if (fixed.isAfter(now) && fixed.minusMonths(1).isAfter(now)) {
      fixed = date.withYear(year - 1);
    } else if (fixed.isBefore(now) && fixed.plusMonths(11).isBefore(now)) {
      fixed = date.withYear(year + 1);
    }
    date = fixed;
    return date.toInstant(ZoneOffset.UTC).toEpochMilli();
  }
}
