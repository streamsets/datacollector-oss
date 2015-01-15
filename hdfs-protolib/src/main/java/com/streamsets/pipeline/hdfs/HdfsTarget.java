/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;

@GenerateResourceBundle
@StageDef(version = "1.0.0",
    label = "HDFS",
    description = "Writes records to HDFS files")
public class HdfsTarget extends BaseHdfsTarget {

    @ConfigDef(required = true,
        type = ConfigDef.Type.MODEL,
        description = "Data Format",
        label = "Data Format",
        defaultValue = "JSON",
        group = "DATA",
        dependsOn = "fileType",
        triggeredByValue = { "TEXT", "SEQUENCE_FILE"},
        displayPosition = 200)
    @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = DataFormatChooserValues.class)
    public HdfsDataFormat dataFormat;

}
