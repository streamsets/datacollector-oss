/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Label;

public enum HdfsDataFormat implements Label {
    JSON("SDC Records (JSON)"),
    CSV("Delimited"),
    TSV("Tab Separated"),
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
