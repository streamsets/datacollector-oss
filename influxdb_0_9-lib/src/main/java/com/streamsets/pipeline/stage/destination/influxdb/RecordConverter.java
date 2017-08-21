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
package com.streamsets.pipeline.stage.destination.influxdb;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.influxdb.dto.Point;

import java.util.List;

/**
 * Interface for converting SDC Records to InfluxDB Points
 *
 * Concrete implementation can be something that understands collectd for example, or a generic form that requires
 * additional configuration to map into a measurement point.
 */
public interface RecordConverter {
  List<Point> getPoints(Record record) throws OnRecordErrorException;
}
