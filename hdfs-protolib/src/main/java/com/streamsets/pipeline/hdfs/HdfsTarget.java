/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.api.base.FileRawSourcePreviewer;

@GenerateResourceBundle
@StageDef(version = "1.0.0",
    label = "HDFS",
    description = "Writes records to HDFS files")
public class HdfsTarget extends AbstractHdfsTarget {

    public enum DataFormat implements BaseEnumChooserValues.EnumWithLabel {
        CSV("Comma Separated Values"),
        TSV("Tab Separated Values"),
        JSON("JSON Object"),
        XML("XML Object");

        private String label;

        DataFormat(String label) {
            this.label = label;
        }
        @Override
        public String getLabel() {
            return label;
        }

    }

    @ConfigDef(required = true,
        type = ConfigDef.Type.MODEL,
        description = "Data Format",
        label = "Data Format",
        defaultValue = "JSON")
    @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = DataFormatChooserValues.class)
    public DataFormat dataFormat;

}
