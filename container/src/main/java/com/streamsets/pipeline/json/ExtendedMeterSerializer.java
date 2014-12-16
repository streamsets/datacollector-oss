/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.streamsets.pipeline.metrics.ExtendedMeter;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class ExtendedMeterSerializer extends JsonSerializer<ExtendedMeter> {
  private final String rateUnit;
  private final double rateFactor;

  private static String calculateRateUnit(TimeUnit unit, String name) {
    String s = unit.toString().toLowerCase(Locale.US);
    return name + '/' + s.substring(0, s.length() - 1);
  }

  public ExtendedMeterSerializer(TimeUnit rateUnit) {
    this.rateFactor = (double)rateUnit.toSeconds(1L);
    this.rateUnit = calculateRateUnit(rateUnit, "events");
  }

  @Override
  public void serialize(ExtendedMeter meter, JsonGenerator jgen, SerializerProvider provider) throws IOException {
    jgen.writeStartObject();
    jgen.writeNumberField("count", meter.getCount());
    jgen.writeNumberField("m1_rate", meter.getOneMinuteRate() * this.rateFactor);
    jgen.writeNumberField("m5_rate", meter.getFiveMinuteRate() * this.rateFactor);
    jgen.writeNumberField("m15_rate", meter.getFifteenMinuteRate() * this.rateFactor);
    jgen.writeNumberField("m30_rate", meter.getThirtyMinuteRate() * this.rateFactor);
    jgen.writeNumberField("h1_rate", meter.getOneHourRate() * this.rateFactor);
    jgen.writeNumberField("h6_rate", meter.getSixHourRate() * this.rateFactor);
    jgen.writeNumberField("h12_rate", meter.getTwelveHourRate() * this.rateFactor);
    jgen.writeNumberField("h24_rate", meter.getTwentyFourHourRate() * this.rateFactor);
    jgen.writeNumberField("mean_rate", meter.getMeanRate() * this.rateFactor);
    jgen.writeStringField("units", this.rateUnit);
    jgen.writeEndObject();
  }

}
