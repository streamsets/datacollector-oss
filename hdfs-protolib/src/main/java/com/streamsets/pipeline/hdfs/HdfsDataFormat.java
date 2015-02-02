/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Label;

public enum HdfsDataFormat implements Label {
    JSON("JSON Object"),
    CSV("Comma Separated Values"),
    TSV("Tab Separated Values"),
    ;

    private String label;

    HdfsDataFormat(String label) {
        this.label = label;
    }
    @Override
    public String getLabel() {
        return label;
    }

}
