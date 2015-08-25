/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.http;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/**
 * Streamsets gauge values, for example RuntimeStats, which need to be appear in JMX report will implement this
 * interface.
 */
public interface GaugeValue {

  public void serialize(JsonGenerator jg) throws IOException;
}
