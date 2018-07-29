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

import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.base.OnRecordErrorException;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SyslogBaseTestClass {

  private final Clock clock;

  protected SyslogBaseTestClass() {
    clock = Clock.fixed(Clock.systemUTC().instant(), ZoneId.of("UTC"));
  }

  private final LoadingCache<String, Long> rfc5424TsCache = SyslogDecoder.buildTimestampCache(
      DateTimeFormatter.ofPattern(SyslogDecoder.RFC5424_TS_PATTERN, Locale.US)
  );

  protected Clock getSystemClock() {
    return clock;
  }

  protected List<String> getTestMessageStrings() {
    List<String> messages = Lists.newArrayList();

    // supported examples from RFC 3164
    messages.add("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for " +
        "lonvick on /dev/pts/8");
    messages.add("<13>Feb  5 17:32:18 10.0.0.99 Use the BFG!");
    messages.add("<165>Aug 24 05:34:00 CST 1987 mymachine myproc[10]: %% " +
        "It's time to make the do-nuts.  %%  Ingredients: Mix=OK, Jelly=OK # " +
        "Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Transport: " +
        "Conveyer1=OK, Conveyer2=OK # %%");
    messages.add("<0>Oct 22 10:52:12 scapegoat 1990 Oct 22 10:52:01 TZ-6 " +
        "scapegoat.dmz.example.org 10.1.2.3 sched[0]: That's All Folks!");

    // supported examples from RFC 5424
    messages.add("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - " +
        "ID47 - BOM'su root' failed for lonvick on\n /dev/pts/8");
    messages.add("<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc " +
        "8710 - - %% It's time to make the do-nuts.");

    // non-standard (but common) messages (RFC3339 dates, no version digit)
    messages.add("<13>2003-08-24T05:14:15Z localhost snarf?");
    messages.add("<13>2012-08-16T14:34:03-08:00 127.0.0.1 test shnap!");

    // multi-line messages
    // adapted from above
    messages.add("<42>1 2003-08-24T05:14:15.000003-07:00 \n192.0.2.1 \nmyproc " +
        "8710 - - %% It's time to make the \n do-nuts. With new\nlines.");
    // from <http://sflanders.net/2014/06/04/log-insight-syslog-agents-multiline-messages/>
    messages.add("<165> 2014-05-16T21:33:39.531Z smith01.matrix Hostd: [FFC38B70 info 'Hostsvc' opID=hostd-facb] VsanSystemVmkProvider : GetRuntimeInfo: Complete, runtime info: (vim.vsan.host.VsanRuntimeInfo) {\n"
        + "     --> dynamicType = <unset>,\n"
        + "     --> accessGenNo = 0,\n"
        + "     --> }");

    return messages;
  }

  protected Map<String, SyslogMessage> getTestMessageRawToExpected() {
    Map<String, SyslogMessage> messages = new LinkedHashMap<>();

    final SyslogMessage msg1 = constructSyslogMessage(
        35,
        4,
        3,
        "Oct 24 09:12:17",
        "mymachine",
        "su: 'su root' failed for user on /dev/pts/5"
    );
    messages.put(msg1.getRawMessage(), msg1);

    final SyslogMessage msg2 = constructSyslogMessage(
        25,
        3,
        1,
        "1 ",
        "1989-04-05T11:12:13.432Z",
        "sega",
        "Someone set us up the bomb"
    );
    messages.put(msg2.getRawMessage(), msg2);

    final SyslogMessage msg3 = constructSyslogMessage(
        47,
        5,
        7,
        "-",
        "node1.cluster",
        "Something happened\nJust now\nMultiple lines"
    );
    messages.put(msg3.getRawMessage(), msg3);

    return messages;
  }

  private SyslogMessage constructSyslogMessage(
      int priority,
      int facility,
      int severity,
      String timestamp,
      String host,
      String remaining
  ) {
    return constructSyslogMessage(priority, facility, severity, "", timestamp, host, remaining);
  }

  private SyslogMessage constructSyslogMessage(
      int priority,
      int facility,
      int severity,
      String afterPriorityStuff,
      String timestamp,
      String host,
      String remaining
  ) {
    String raw = "<" + priority + ">" + afterPriorityStuff + timestamp + " " + host + " " + remaining;

    final SyslogMessage msg = new SyslogMessage();
    msg.setPriority(priority);
    msg.setFacility(facility);
    msg.setSeverity(severity);
    msg.setHost(host);
    msg.setRemainingMessage(remaining);

    final InetSocketAddress sender = getDummySender();
    msg.setSenderHost(SyslogDecoder.resolveHostAddressString(sender));
    msg.setSenderPort(sender.getPort());
    msg.setSenderAddress(msg.getSenderHost() + ":" + msg.getSenderPort());

    final InetSocketAddress receiver = getDummyReceiver();
    msg.setReceiverHost(SyslogDecoder.resolveHostAddressString(receiver));
    msg.setReceiverPort(receiver.getPort());
    msg.setReceiverAddress(msg.getReceiverHost() + ":" + msg.getReceiverPort());

    try {
      if (timestamp.startsWith("-")) {
        // use system timestamp
        msg.setTimestamp(getSystemClock().millis());
      } else if (timestamp.matches("[A-Z].*")) {
        // RFC 3164
        msg.setTimestamp(SyslogDecoder.parseRfc3164Time(timestamp));
      } else {
        // RFC 5424
        msg.setTimestamp(SyslogDecoder.parseRfc5424Date(rfc5424TsCache, timestamp));
      }
    } catch (OnRecordErrorException e) {
      throw new RuntimeException("Error parsing timestamp", e);
    }
    msg.setRawMessage(raw);
    return msg;
  }

  protected InetSocketAddress getDummySender() {
    return InetSocketAddress.createUnresolved("sender", 6006);
  }

  protected InetSocketAddress getDummyReceiver() {
    return InetSocketAddress.createUnresolved("receiver", 7007);
  }


}
