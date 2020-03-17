/*
 * Copyright 2017 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.credential.streamsets.cli;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;

import java.util.Arrays;

public class StreamsetsCredentialStoreCli {

  public static void main(String[] args) {
    if (new StreamsetsCredentialStoreCli().doMain(args)) {
      System.exit(0);
    } else {
      System.exit(-1);
    }
  }

  boolean doMain(String[] args) {
    Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("streamsets stagelib-cli streamsets-credentialstore")
        .withDescription("StreamSets Data Collector Streamsets Credential Store CLI")
        .withDefaultCommand(Help.class)
        .withCommands(
            Help.class,
            DefaultSshKeyInfoCommand.class
        );

    try {
      builder.build().parse(args).run();
      return true;
    } catch (Exception ex) {
      if(Arrays.asList(args).contains("--stack")) {
        ex.printStackTrace(System.err);
      } else {
        System.err.println(ex.getMessage());
      }
      return false;
    }
  }

}
