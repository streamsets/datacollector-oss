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
package com.streamsets.datacollector.client.cli;

import com.streamsets.datacollector.client.cli.command.PingCommand;
import com.streamsets.datacollector.client.cli.command.definitions.DefinitionsCommand;
import com.streamsets.datacollector.client.cli.command.manager.AlertsCommand;
import com.streamsets.datacollector.client.cli.command.manager.DeleteAlertCommand;
import com.streamsets.datacollector.client.cli.command.manager.DeletePipelineHistoryCommand;
import com.streamsets.datacollector.client.cli.command.manager.ErrorMessagesCommand;
import com.streamsets.datacollector.client.cli.command.manager.ErrorRecordsCommand;
import com.streamsets.datacollector.client.cli.command.manager.GetCommittedOffsetsCommand;
import com.streamsets.datacollector.client.cli.command.manager.PipelineHistoryCommand;
import com.streamsets.datacollector.client.cli.command.manager.PipelineMetricsCommand;
import com.streamsets.datacollector.client.cli.command.manager.PipelineStatusCommand;
import com.streamsets.datacollector.client.cli.command.manager.ResetOriginCommand;
import com.streamsets.datacollector.client.cli.command.manager.SampledRecordsCommand;
import com.streamsets.datacollector.client.cli.command.manager.SnapshotCaptureCommand;
import com.streamsets.datacollector.client.cli.command.manager.SnapshotDataCommand;
import com.streamsets.datacollector.client.cli.command.manager.SnapshotDeleteCommand;
import com.streamsets.datacollector.client.cli.command.manager.SnapshotListCommand;
import com.streamsets.datacollector.client.cli.command.manager.SnapshotStatusCommand;
import com.streamsets.datacollector.client.cli.command.manager.StartPipelineCommand;
import com.streamsets.datacollector.client.cli.command.manager.StopPipelineCommand;
import com.streamsets.datacollector.client.cli.command.manager.UpdateCommittedOffsetsCommand;
import com.streamsets.datacollector.client.cli.command.preview.PreviewDataCommand;
import com.streamsets.datacollector.client.cli.command.preview.PreviewStatusCommand;
import com.streamsets.datacollector.client.cli.command.preview.RunPreviewCommand;
import com.streamsets.datacollector.client.cli.command.preview.StopPreviewCommand;
import com.streamsets.datacollector.client.cli.command.preview.ValidatePipelineCommand;
import com.streamsets.datacollector.client.cli.command.store.CreatePipelineCommand;
import com.streamsets.datacollector.client.cli.command.store.DeletePipelineCommand;
import com.streamsets.datacollector.client.cli.command.store.DeletePipelinesByFilteringCommand;
import com.streamsets.datacollector.client.cli.command.store.ExportPipelineCommand;
import com.streamsets.datacollector.client.cli.command.store.GetPipelineConfigCommand;
import com.streamsets.datacollector.client.cli.command.store.GetPipelineRulesCommand;
import com.streamsets.datacollector.client.cli.command.store.ImportPipelineCommand;
import com.streamsets.datacollector.client.cli.command.store.ListPipelinesCommand;
import com.streamsets.datacollector.client.cli.command.store.UpdatePipelineConfigCommand;
import com.streamsets.datacollector.client.cli.command.store.UpdatePipelineRulesCommand;
import com.streamsets.datacollector.client.cli.command.system.ConfigurationCommand;
import com.streamsets.datacollector.client.cli.command.system.CurrentUserCommand;
import com.streamsets.datacollector.client.cli.command.system.DirectoriesCommand;
import com.streamsets.datacollector.client.cli.command.system.DisableDPMCommand;
import com.streamsets.datacollector.client.cli.command.system.EnableDPMCommand;
import com.streamsets.datacollector.client.cli.command.system.InfoCommand;
import com.streamsets.datacollector.client.cli.command.system.ServerTimeCommand;
import com.streamsets.datacollector.client.cli.command.system.ShutdownCommand;
import com.streamsets.datacollector.client.cli.command.system.StatsCommand;
import com.streamsets.datacollector.client.cli.command.system.ThreadsCommand;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseOptionMissingException;

import java.util.Arrays;

@SuppressWarnings("unchecked")
public class DataCollector {
  public static void main(String[] args) {
    Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("cli")
        .withDescription("StreamSets Data Collector CLI")
        .withDefaultCommand(Help.class)
        .withCommands(
            Help.class,
            PingCommand.class
        );

    builder.withGroup("definitions")
        .withDescription("Returns Pipeline & Stage Configuration definitions")
        .withDefaultCommand(DefinitionsCommand.class)
        .withCommands(DefinitionsCommand.class);

    builder.withGroup("store")
        .withDescription("Store Commands")
        .withDefaultCommand(ListPipelinesCommand.class)
        .withCommands(
            ListPipelinesCommand.class,
            ExportPipelineCommand.class,
            ImportPipelineCommand.class,
            CreatePipelineCommand.class,
            GetPipelineConfigCommand.class,
            GetPipelineRulesCommand.class,
            DeletePipelineCommand.class,
            UpdatePipelineConfigCommand.class,
            UpdatePipelineRulesCommand.class,
            DeletePipelinesByFilteringCommand.class
        );

    builder.withGroup("manager")
        .withDescription("Manager Commands")
        .withDefaultCommand(PipelineStatusCommand.class)
        .withCommands(
            PipelineStatusCommand.class,
            PipelineMetricsCommand.class,
            StartPipelineCommand.class,
            StopPipelineCommand.class,
            AlertsCommand.class,
            DeleteAlertCommand.class,
            SampledRecordsCommand.class,
            ResetOriginCommand.class,
            ErrorRecordsCommand.class,
            ErrorMessagesCommand.class,
            SnapshotListCommand.class,
            SnapshotCaptureCommand.class,
            SnapshotStatusCommand.class,
            SnapshotDataCommand.class,
            SnapshotDeleteCommand.class,
            PipelineHistoryCommand.class,
            DeletePipelineHistoryCommand.class,
            GetCommittedOffsetsCommand.class,
            UpdateCommittedOffsetsCommand.class
        );

    builder.withGroup("system")
        .withDescription("System Commands")
        .withDefaultCommand(InfoCommand.class)
        .withCommands(
            ConfigurationCommand.class,
            DirectoriesCommand.class,
            InfoCommand.class,
            CurrentUserCommand.class,
            ServerTimeCommand.class,
            ShutdownCommand.class,
            StatsCommand.class,
            ThreadsCommand.class,
            EnableDPMCommand.class,
            DisableDPMCommand.class
        );

    builder.withGroup("preview")
        .withDescription("Preview Commands")
        .withDefaultCommand(RunPreviewCommand.class)
        .withCommands(
            RunPreviewCommand.class,
            PreviewStatusCommand.class,
            PreviewDataCommand.class,
            StopPreviewCommand.class,
            ValidatePipelineCommand.class
        );

    try {
      builder.build().parse(args).run();
    } catch (ParseOptionMissingException | ParseArgumentsUnexpectedException ex) {
      if(Arrays.asList(args).contains("--stack")) {
        ex.printStackTrace();
      } else {
        System.out.println(ex.getMessage());
      }
    }

  }
}
