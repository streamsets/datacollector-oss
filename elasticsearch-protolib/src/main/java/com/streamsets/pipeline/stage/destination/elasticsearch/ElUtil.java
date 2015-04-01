/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.lib.el.DataUtilEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;

public class ElUtil {

  public static ELEval createIndexEval(Stage.ELContext elContext) {
    return elContext.createELEval("indexTemplate");
  }

  public static ELEval createTypeEval(Stage.ELContext elContext) {
    return elContext.createELEval("typeTemplate");
  }

  public static ELEval createDocIdEval(Stage.ELContext elContext) {
    return elContext.createELEval("docIdTemplate", RecordEL.class, DataUtilEL.class);
  }

}
