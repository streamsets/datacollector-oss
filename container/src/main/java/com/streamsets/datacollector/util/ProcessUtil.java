/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Util class to start temporary, short-lived external processes and get their output.
 */
public class ProcessUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessUtil.class);


  /**
   * Functional interface to process output of the process.
   */
  public interface ProcessOutput {
    void process(Path stdout, Path stderr) throws Exception;
  }

  /**
   * Simple utility to write out output of a command to the bundle
   */
  public static boolean executeCommand(
      List<String> commandLine,
      long timeout,
      ProcessOutput processOutput
  ) {
    try {
      Path outputFile = Files.createTempFile("sdc-process", "out");
      Path errorFile = Files.createTempFile("sdc-process", "err");

      ProcessBuilder builder = new ProcessBuilder(commandLine);
      builder.redirectError(errorFile.toFile());
      builder.redirectOutput(outputFile.toFile());

      Process process = builder.start();
      boolean finished = process.waitFor(timeout, TimeUnit.SECONDS);
      if (!finished) {
        process.destroyForcibly();
      }

      processOutput.process(outputFile, errorFile);

      Files.delete(outputFile);
      Files.delete(errorFile);

      return finished ? process.exitValue() == 0 : false;
    } catch (Throwable ex) {
      LOG.error("Can't run command: {}", commandLine, ex);
    }

    return false;
  }
}
