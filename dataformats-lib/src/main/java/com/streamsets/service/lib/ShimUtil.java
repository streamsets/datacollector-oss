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
package com.streamsets.service.lib;

import com.streamsets.pipeline.api.service.dataformats.DataGeneratorException;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;

/**
 * When using DataFormat as a service, the interfaces, exceptions and other enums lives in different package. This class
 * provides simple way how to convert the package names.
 */
public class ShimUtil {

  /**
   * Change package name for DataParserException.
   *
   * @param original Original exception from *.lib.* package
   * @return New exception from *.api.* package
   */
  public static DataParserException convert(
      com.streamsets.pipeline.lib.parser.DataParserException original
  ) {
    return new DataParserException(
      original.getErrorCode(),
      original.getParams(),
      original.getCause()
    );
  }
  /**
   * Change package name for DataGeneratorException.
   *
   * @param original Original exception from *.lib.* package
   * @return New exception from *.api.* package
   */
  public static DataGeneratorException convert(
      com.streamsets.pipeline.lib.generator.DataGeneratorException original
  ) {
   return new DataGeneratorException(
      original.getErrorCode(),
      original.getParams(),
      original.getCause()
    );
  }


  private ShimUtil() {
    // Instantiation is prohibited
  }
}
