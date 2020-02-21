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
package com.streamsets.pipeline.stage.origin.mysql.offset;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

/**
 * Offset in MySql binlog.
 */
public interface SourceOffset {
  /**
   * Format to string.
   * @return
   */
  String format();

  /**
   * Set client position to this offset.
   * @param client
   */
  void positionClient(BinaryLogClient client);
}
