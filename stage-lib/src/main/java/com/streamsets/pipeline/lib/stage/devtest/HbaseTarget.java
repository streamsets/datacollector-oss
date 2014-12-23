/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Dummy Hbase Target", icon = "HbaseTarget.svg")
public class HbaseTarget extends BaseTarget {

  @ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "Hbase URI", defaultValue = "",
            description = "Hbase server URI")
  public String uri;

  @ConfigDef(required = true, type = ConfigDef.Type.BOOLEAN, label = "Security enabled",
             defaultValue = "false", description = "Kerberos enabled for Hbase")
  public boolean security;

  @ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "Table", defaultValue = "",
             description = "Hbase table name")
  public String table;

  @Override
  public void write(Batch batch) throws StageException {
  }

}
