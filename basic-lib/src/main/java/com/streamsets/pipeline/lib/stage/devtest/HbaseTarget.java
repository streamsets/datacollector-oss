/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Dummy Hbase Target", icon = "HbaseTarget.svg")
@ConfigGroups(HbaseTarget.Groups.class)
public class HbaseTarget extends BaseTarget {

  public enum Groups implements ConfigGroups.Groups {
    GENERAL("General"), ADVANCED("Advanced");

    private final String label;

    private Groups(String label) {
      this.label = label;
    }

    public String getLabel() {
      return this.label;
    }
  }

  @ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "HBase URI", defaultValue = "",
            description = "HBase server URI", group="GENERAL")
  public String uri;

  @ConfigDef(required = true, type = ConfigDef.Type.BOOLEAN, label = "Security enabled",
             defaultValue = "false", description = "Kerberos enabled for HBase", group="ADVANCED")
  public boolean security;

  @ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "Table", defaultValue = "",
             description = "HBase table name", group="GENERAL")
  public String table;

  @Override
  public void write(Batch batch) throws StageException {
  }

}
