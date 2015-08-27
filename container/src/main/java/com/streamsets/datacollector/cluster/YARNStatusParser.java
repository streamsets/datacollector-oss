/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.cluster;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class YARNStatusParser {
  private static final Logger LOG = LoggerFactory.getLogger(YARNStatusParser.class);

  private static final String NEW = "NEW";
  private static final String SUBMITTED = "SUBMITTED";
  private static final String ACCEPTED = "ACCEPTED";
  private static final String RUNNING = "RUNNING";
  private static final String SUCCEEDED = "SUCCEEDED";
  private static final String FAILED = "FAILED";
  private static final String KILLED = "KILLED";

  private static Pattern patternForState(String state) {
    return Pattern.compile("^\\s+State : (" + state + ").*$");
  }
  private static Pattern patternForFinalState(String state) {
    return Pattern.compile("^\\s+Final-State : (" + state + ").*$");
  }

  private static final ImmutableMap<String, String> STATE_MAP = ImmutableMap.<String, String>builder()
    .put(NEW, RUNNING)
    .put(ACCEPTED, RUNNING)
    .put(RUNNING, RUNNING)
    .put(SUCCEEDED, SUCCEEDED)
    .put(FAILED, FAILED)
    .put(KILLED, KILLED).build();

  private static final List<Pattern> PATTERNS = Arrays.asList(
    patternForState(NEW),
    patternForState(SUBMITTED),
    patternForState(ACCEPTED),
    patternForState(RUNNING),
    patternForFinalState(SUCCEEDED),
    patternForFinalState(FAILED),
    patternForFinalState(KILLED)
  );

  public String parseStatus(Collection<String> lines) {
    for (String line : lines) {
      for (Pattern pattern : PATTERNS) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
          String input = matcher.group(1);
          String output = Strings.nullToEmpty(STATE_MAP.get(input));
          if (!output.isEmpty()) {
            return output;
          }
        }
      }
    }
    String msg = "Could not match any YARN status";
    LOG.error(msg + ":" + Joiner.on("\n").join(lines));
    throw new IllegalStateException(msg + ". See logs.");
  }
}