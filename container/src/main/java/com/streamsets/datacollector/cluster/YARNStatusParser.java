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

  private static final String ACCEPTED = "ACCEPTED";
  private static final String RUNNING = "RUNNING";
  private static final String SUCCEEDED = "SUCCEEDED";
  private static final String FAILED = "FAILED";
  private static final Pattern ACCEPTED_PATTERN = Pattern.compile("^\\s+State : (ACCEPTED).*$");
  private static final Pattern RUNNING_PATTERN = Pattern.compile("^\\s+State : (RUNNING).*$");
  private static final Pattern SUCCEEDED_PATTERN = Pattern.compile("^\\s+Final-State : (SUCCEEDED).*$");
  private static final Pattern FAILED_PATTERN = Pattern.compile("^\\s+Final-State : (FAILED).*$");

  private static final ImmutableMap<String, String> STATE_MAP = ImmutableMap.of(
    ACCEPTED, RUNNING,
    RUNNING, RUNNING,
    SUCCEEDED, SUCCEEDED,
    FAILED, FAILED
  );

  private static final List<Pattern> PATTERNS = Arrays.asList(RUNNING_PATTERN, ACCEPTED_PATTERN, SUCCEEDED_PATTERN,
    FAILED_PATTERN);

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
    throw new IllegalStateException("Could not match any status lines: " + Joiner.on("\n").join(lines));
  }
}
