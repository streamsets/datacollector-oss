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
package com.streamsets.datacollector.usagestats.cli;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;

import java.util.Arrays;

/**
 * Class used by StreamSets CLI for Telemetry
 */
public class TelemetryCli {

  public static void main(String[] args) {
    @SuppressWarnings("unchecked")
    Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("telemetry")
        .withDescription("StreamSets Telemetry CLI")
        .withDefaultCommand(Help.class)
        .withCommands(
            Help.class,
            UploadTelemetry.class
        );

    try {
      builder.build().parse(args).run();
    } catch (Exception ex) {
      if (Arrays.asList(args).contains("--stack")) {
        ex.printStackTrace();
      } else {
        System.out.println(ex.getMessage());
      }
    }
  }
}
