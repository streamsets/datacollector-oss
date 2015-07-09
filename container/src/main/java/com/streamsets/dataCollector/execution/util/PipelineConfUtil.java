/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.util;

import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MemoryLimitConfiguration;
import com.streamsets.pipeline.config.MemoryLimitExceeded;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import com.streamsets.pipeline.el.JvmEL;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.util.ElUtil;
import com.streamsets.pipeline.util.ValidationUtil;
import com.streamsets.pipeline.validation.ValidationError;

import java.util.List;
import java.util.Locale;

public class PipelineConfUtil {

  public static DeliveryGuarantee getDeliveryGuarantee(PipelineConfiguration pipelineConfiguration) {
    DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    for (ConfigConfiguration config : pipelineConfiguration.getConfiguration()) {
      if (com.streamsets.pipeline.prodmanager.Constants.DELIVERY_GUARANTEE.equals(config.getName())) {
        deliveryGuarantee = DeliveryGuarantee.valueOf((String) config.getValue());
      }
    }
    return deliveryGuarantee;
  }

  public static MemoryLimitConfiguration getMemoryLimitConfiguration(PipelineConfiguration pipelineConfiguration)
    throws PipelineRuntimeException {
    //Default memory limit configuration
    MemoryLimitConfiguration memoryLimitConfiguration = new MemoryLimitConfiguration();

    List<ConfigConfiguration> configuration = pipelineConfiguration.getConfiguration();
    MemoryLimitExceeded memoryLimitExceeded = null;
    long memoryLimit = 0;

    if (configuration != null) {
      for (ConfigConfiguration config : configuration) {
        if (PipelineConfigBean.MEMORY_LIMIT_EXCEEDED_CONFIG.equals(config.getName())) {
          try {
            memoryLimitExceeded = MemoryLimitExceeded.valueOf(String.valueOf(config.getValue()).
              toUpperCase(Locale.ENGLISH));
          } catch (IllegalArgumentException e) {
            //This should never happen.
            String msg = "Invalid pipeline configuration: " + PipelineConfigBean.MEMORY_LIMIT_EXCEEDED_CONFIG +
              " value: '" + config.getValue() + "'. Should never happen, please report. : " + e;
            throw new IllegalStateException(msg, e);
          }
        } else if (PipelineConfigBean.MEMORY_LIMIT_CONFIG.equals(config.getName())) {
          String memoryLimitString = String.valueOf(config.getValue());

          if(ElUtil.isElString(memoryLimitString)) {
            //Memory limit is an EL expression. Evaluate to get the value
            try {
              memoryLimit = ValidationUtil.evaluateMemoryLimit(memoryLimitString,
                ElUtil.getConstants(pipelineConfiguration));
            } catch (ELEvalException e) {
              throw new PipelineRuntimeException(ValidationError.VALIDATION_0064, e.getMessage(), e);
            }
          } else {
            //Memory limit is not an EL expression. Parse it as long.
            try {
              memoryLimit = Long.parseLong(memoryLimitString);
            } catch (NumberFormatException e) {
              throw new PipelineRuntimeException(ValidationError.VALIDATION_0062, memoryLimitString);
            }
          }

          if (memoryLimit > JvmEL.jvmMaxMemoryMB() * 0.85) {
            throw new PipelineRuntimeException(ValidationError.VALIDATION_0063, memoryLimit,
              "above the maximum", JvmEL.jvmMaxMemoryMB() * 0.85);
          }
        }
      }
    }
    if (memoryLimitExceeded != null && memoryLimit > 0) {
      memoryLimitConfiguration = new MemoryLimitConfiguration(memoryLimitExceeded, memoryLimit);
    }
    //update the pipeline memory configuration based on the calculated value
    pipelineConfiguration.setMemoryLimitConfiguration(memoryLimitConfiguration);
    return memoryLimitConfiguration;
  }
}
