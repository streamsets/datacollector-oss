/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hive;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;

import java.util.List;
import java.util.Map;

@StageDef(
    version = 1,
    label = "Hive Streaming",
    description = "Writes data to Hive tables using the streaming API. Requires ORC storage format.",
    icon = "hive.png",
    privateClassLoader = true
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class HiveDTarget extends DTarget {

  @ConfigDef(
      required = true,
      label = "Hive Metastore Thrift URL",
      type = ConfigDef.Type.STRING,
      description = "Hive Metastore Thrift URL in the form: thrift://<host>:<port>",
      displayPosition = 10,
      group = "HIVE"
  )
  public String hiveUrl;

  @ConfigDef(
      required = true,
      label = "Schema",
      type = ConfigDef.Type.STRING,
      defaultValue = "default",
      description = "The Hive schema the target table belongs to. Also called 'database'.",
      displayPosition = 20,
      group = "HIVE"
  )
  public String schema;

  @ConfigDef(
      required = true,
      label = "Table",
      type = ConfigDef.Type.STRING,
      displayPosition = 30,
      group = "HIVE"
  )
  public String table;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "/etc/hive/conf",
      label = "Hive Configuration Directory",
      description = "An absolute path or a directory under SDC resources directory to load core-site.xml and" +
          " hive-site.xml files to configure the Hive.",
      displayPosition = 40,
      group = "HIVE"
  )
  public String hiveConfDir;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Field to Column Mapping",
      description = "Use to specify additional field mappings when input field name and column name don't match.",
      displayPosition = 50,
      group = "HIVE"
  )
  @ComplexField(FieldMappingConfig.class)
  public List<FieldMappingConfig> columnMappings;

  @ConfigDef(
      required = true,
      label = "Auto Create Partitions",
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      displayPosition = 60,
      group = "HIVE"
  )
  public boolean autoCreatePartitions;

  @ConfigDef(
      required = true,
      label = "Transaction Batch Size",
      type = ConfigDef.Type.NUMBER,
      description = "Number of transactions to request per partition batch.",
      defaultValue = "1000",
      min = 2,
      displayPosition = 70,
      group = "ADVANCED"
  )
  public int txnBatchSize;

  @ConfigDef(
      required = true,
      label = "Buffer Limit (KB)",
      type = ConfigDef.Type.NUMBER,
      description = "Low level writer buffer limit to avoid out of memory errors.",
      defaultValue = "128",
      min = 1,
      displayPosition = 80,
      group = "ADVANCED"
  )
  public int bufferLimitKb;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Hive Configuration",
      description = "Additional Hive properties to pass to the underlying Hive Configuration. These properties " +
          "have precedence over properties loaded via the 'Hive Configuration Directory' configuration.",
      displayPosition = 90,
      group = "ADVANCED"
  )
  public Map<String, String> additionalHiveProperties;

  @Override
  protected Target createTarget() {
    return new HiveTarget(
        hiveUrl,
        schema,
        table,
        hiveConfDir,
        columnMappings,
        autoCreatePartitions,
        txnBatchSize,
        bufferLimitKb,
        additionalHiveProperties
    );
  }
}
