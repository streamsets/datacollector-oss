/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

public class Constants {

  /*Pipeline related constants*/
  public static final String DEFAULT_PIPELINE_NAME = "xyz";
  public static final String DEFAULT_PIPELINE_REVISION = "1.0";

  /*pipeline configuration properties*/
  public static final String MAX_BATCH_SIZE_KEY = "production.maxBatchSize";
  public static final int MAX_BATCH_SIZE_DEFAULT = 1000;
  public static final String DELIVERY_GUARANTEE = "deliveryGuarantee";

  public static final String STOP_PIPELINE_MESSAGE = "Requested via REST API";

}
