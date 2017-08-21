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

public class MesosStatusParser {
  private static final Logger LOG = LoggerFactory.getLogger(MesosStatusParser.class);

  private static final String QUEUED = "QUEUED";
  private static final String RUNNING = "RUNNING";
  private static final String TASK_FINISHED = "TASK_FINISHED";
  private static final String TASK_LOST = "TASK_LOST";
  private static final String TASK_FAILED = "TASK_FAILED";
  private static final String TASK_KILLED = "TASK_KILLED";
  private static final String SUCCEEDED = "SUCCEEDED";
  private static final String FAILED = "FAILED";
  private static final String KILLED = "KILLED";

  private static Pattern patternForState(String state) {
    return Pattern.compile("^.*state\\s*: (" + state + ").*$", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
  }

  private static final ImmutableMap<String, String> STATE_MAP = ImmutableMap.<String, String>builder()
    .put(QUEUED, RUNNING)
    .put(RUNNING, RUNNING)
    .put(TASK_FINISHED, SUCCEEDED)
    .put(TASK_FAILED, FAILED)
    .put(TASK_LOST, FAILED)
    .put(TASK_KILLED, KILLED).build();

  private static final List<Pattern> PATTERNS = Arrays.asList(
    patternForState(QUEUED),
    patternForState(RUNNING),
    patternForState(TASK_FINISHED),
    patternForState(TASK_FAILED),
    patternForState(TASK_KILLED),
    patternForState(TASK_LOST)
  );

  public String parseStatus(Collection<String> lines) {
    for (final String line : lines) {
      for (final Pattern pattern : PATTERNS) {
        String noQuoteLine = line.replaceAll("\"", "");
        final Matcher matcher = pattern.matcher(noQuoteLine);
        if (matcher.matches()) {
          final String input = matcher.group(1);
          final String output = Strings.nullToEmpty(STATE_MAP.get(input));
          if (!output.isEmpty()) {
            return output;
          }
        }
      }
    }
    final String msg = "Could not match any Mesos status";
    LOG.error(msg + ":" + Joiner.on("\n").join(lines));
    throw new IllegalStateException(msg + ". See logs.");
  }
}
