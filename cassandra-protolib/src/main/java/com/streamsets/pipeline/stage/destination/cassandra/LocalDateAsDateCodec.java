/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.cassandra;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.MappingCodec;

import java.util.Date;

/**
 * Bridge to map between Data Collector's usage of {@link java.util.Date} to represent Dates vs Cassandra's
 * usage of {@link com.datastax.driver.core.LocalDate}. Future versions of SDC may switch to {@link java.time.LocalDate}
 * making this codec no longer required as the DataStax Java Driver already includes a codec for that mapping.
 */
public class LocalDateAsDateCodec extends MappingCodec<Date, LocalDate> {
  public LocalDateAsDateCodec() {
    super(TypeCodec.date(), Date.class);
  }

  @Override
  protected Date deserialize(LocalDate localDate) { return new Date(localDate.getMillisSinceEpoch()); }

  @Override
  protected LocalDate serialize(Date d) { return LocalDate.fromMillisSinceEpoch(d.getTime()); }
}
