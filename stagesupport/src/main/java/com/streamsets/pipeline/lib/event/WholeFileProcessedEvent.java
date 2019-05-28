/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.event;

import com.streamsets.pipeline.api.EventDef;
import com.streamsets.pipeline.api.EventFieldDef;

@EventDef(
    type = WholeFileProcessedEvent.WHOLE_FILE_WRITE_FINISH_EVENT,
    description = "The destination generates a file closure event record when it closes an output file",
    version = WholeFileProcessedEvent.VERSION
)
public class WholeFileProcessedEvent {

  public static final String WHOLE_FILE_WRITE_FINISH_EVENT = "wholeFileProcessed";
  public static final int VERSION = 1;

  @EventFieldDef(
      name = WholeFileProcessedEvent.SOURCE_FILE_INFO,
      description = "A map of attributes about the original whole file that was processed. " +
          "The attributes include: size - Size of the whole file in bytes. " +
          "Includes a size attribute that provides the size of the whole file in bytes."
  )
  public static final String SOURCE_FILE_INFO = "sourceFileInfo";

  @EventFieldDef(
      name = WholeFileProcessedEvent.TARGET_FILE_INFO,
      description = "A map of attributes about the whole file written to the destination. " +
          "Includes information about the location of the processed whole file."
  )
  public static final String TARGET_FILE_INFO = "targetFileInfo";

  @EventFieldDef(
      name = WholeFileProcessedEvent.CHECKSUM,
      description = "Checksum generated for the written file. " +
          "Included only when you configure the destination to include checksums in the event record.",
      optional = true
  )
  public static final String CHECKSUM = "checksum";

  @EventFieldDef(
      name = WholeFileProcessedEvent.CHECKSUM_ALGORITHM,
      description = "Algorithm used to generate the checksum. " +
          "Included only when you configure the destination to include checksums in the event record.",
      optional = true
  )
  public static final String CHECKSUM_ALGORITHM = "checksumAlgorithm";

  public static final EventCreator FILE_TRANSFER_COMPLETE_EVENT =
      new EventCreator.Builder(WHOLE_FILE_WRITE_FINISH_EVENT, WholeFileProcessedEvent.VERSION)
          .withRequiredField(SOURCE_FILE_INFO)
          .withRequiredField(TARGET_FILE_INFO)
          .withOptionalField(CHECKSUM)
          .withOptionalField(CHECKSUM_ALGORITHM)
          .build();

}
