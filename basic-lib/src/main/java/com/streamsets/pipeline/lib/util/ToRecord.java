/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;

public interface ToRecord {

  Record createRecord(Source.Context context, String sourceFile, long offset, String line, boolean truncated)
      throws ToRecordException;
}
