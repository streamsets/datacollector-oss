/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.cli.sch;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;

import java.util.Arrays;

public class SchCli {

  public static void main(String[] args) {
    if (new SchCli().doMain(args)) {
      System.exit(0);
    } else {
      System.exit(-1);
    }
  }

  boolean doMain(String[] args) {
    Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("streamsets sch")
        .withDescription("StreamSets Data Collector CLI interface for Control Hub")
        .withDefaultCommand(Help.class)
        .withCommands(
          Help.class,
          RegisterCommand.class,
          UnregisterCommand.class
        );

    try {
      builder.build().parse(args).run();
      return true;
    } catch (Exception ex) {
      System.err.println(ex.getMessage());
      return false;
    }
  }

}
