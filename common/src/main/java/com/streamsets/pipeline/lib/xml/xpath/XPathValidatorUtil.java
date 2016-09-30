/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.xml.xpath;

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.impl.XMLChar;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class XPathValidatorUtil {
  private static final String VALID_PREDICATE = "\\[(?:(?:[0-9]+)|(?:@([^=]+)='[^']*'))\\]";
  private static final java.util.regex.Pattern VALID_PREDICATE_PATTERN = Pattern.compile(VALID_PREDICATE);

  public static boolean isValidXPath(String expression) {
    for (String str : expression.split(Constants.PATH_SEPARATOR)) {
      if (Strings.isNullOrEmpty(str)) {
        continue;
      }
      final int predicateStartInd = str.indexOf('[');
      String elementName = str;
      if (predicateStartInd > 0) {

        elementName = str.substring(0, predicateStartInd);
        final String predicate = str.substring(predicateStartInd);
        final Matcher matcher = VALID_PREDICATE_PATTERN.matcher(predicate);
        if (!matcher.matches()) {
          return false;
        } else {
          final String attributeName = matcher.group(1);
          if (!Strings.isNullOrEmpty(attributeName) && !XMLChar.isValidName(attributeName)) {
            return false;
          }
        }
      }
      if (!Constants.WILDCARD.equals(elementName) && !XMLChar.isValidName(elementName)) {
        return false;
      }
    }
    return true;
  }

  public static Set<String> getNamespacePrefixes(String expression) {

    final Set<String> prefixes = new HashSet<>();

    for (String str : expression.split(Constants.PATH_SEPARATOR)) {
      final int prefixSeparatorInd = str.indexOf(Constants.NAMESPACE_PREFIX_SEPARATOR);
      if (prefixSeparatorInd >= 0) {
        prefixes.add(str.substring(0, prefixSeparatorInd));
      }
    }

    return prefixes;
  }
}
