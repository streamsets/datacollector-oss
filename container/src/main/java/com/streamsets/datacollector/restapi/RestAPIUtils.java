/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi;

import com.streamsets.pipeline.lib.log.LogConstants;
import org.slf4j.MDC;

public class RestAPIUtils {

  static void injectPipelineInMDC(String pipeline) {
    MDC.put(LogConstants.ENTITY, pipeline);
  }

}
