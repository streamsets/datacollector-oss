/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.hdfs.metadataxecutor;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.el.RecordEL;

import java.util.List;

/**
 * Various actions that can be performed on the given file.
 */
public class HdfsActionsConfig {

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    defaultValue = "${record:value('/file_path')}",
    label = "Input File",
    description = "Full path to the file on which the metadata operations should be executed.",
    displayPosition = 100,
    group = "ACTIONS",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class}
  )
  public String filePath;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Move file",
    description = "Moves the file to a new location.",
    displayPosition = 110,
    group = "ACTIONS"
  )
  public boolean shouldMoveFile;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "/new/location/file.name",
    label = "New location",
    description = "New location where the file should be moved to.",
    displayPosition = 115,
    group = "ACTIONS",
    dependsOn = "shouldMoveFile",
    triggeredByValue = "true",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class}
  )
  public String newLocation;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Change ownership",
    description = "Set to change owner and group of the file.",
    displayPosition = 120,
    group = "ACTIONS"
  )
  public boolean shouldChangeOwnership;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "new_owner",
    label = "New owner",
    displayPosition = 125,
    group = "ACTIONS",
    dependsOn = "shouldChangeOwnership",
    triggeredByValue = "true",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class}
  )
  public String newOwner;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "new_group",
    label = "New group",
    displayPosition = 130,
    group = "ACTIONS",
    dependsOn = "shouldChangeOwnership",
    triggeredByValue = "true",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class}
  )
  public String newGroup;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Set permissions",
    description = "Set to override files permissions.",
    displayPosition = 135,
    group = "ACTIONS"
  )
  public boolean shouldSetPermissions;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "700",
    label = "New permissions",
    description = "Permissions in either in octal or symbolic format.",
    displayPosition = 140,
    group = "ACTIONS",
    dependsOn = "shouldSetPermissions",
    triggeredByValue = "true",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class}
  )
  public String newPermissions;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Set ACLs",
    description = "Set to set extended access attributes.",
    displayPosition = 145,
    group = "ACTIONS"
  )
  public boolean shouldSetAcls;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "user::rwx,user:foo:rw-,group::r--,other::---",
    label = "New ACLs",
    description = "List of ACLs separated by commas.",
    displayPosition = 150,
    group = "ACTIONS",
    dependsOn = "shouldSetAcls",
    triggeredByValue = "true",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = {RecordEL.class}
  )
  public String newAcls;


  public void init(Stage.Context context, String prefix, List<Stage.ConfigIssue> issues) {
    // Setting permissions and ACLs does not make sense
    if(shouldSetPermissions && shouldSetAcls) {
      issues.add(
        context.createConfigIssue(
          Groups.ACTIONS.name(),
          null,
          HdfsMetadataErrors.HDFS_METADATA_006
        )
      );
    }
  }

  public void destroy() {
    // No-op currently
  }
}
