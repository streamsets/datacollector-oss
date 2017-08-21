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
package com.streamsets.pipeline.lib.xml.xpath;

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.impl.XMLChar;
import com.streamsets.pipeline.lib.xml.Constants;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class XPathValidatorUtil {
  private static final String VALID_PREDICATE = "\\[(?:(?:[0-9]+)|(?:@([^=]+)='[^']*'))\\]";
  private static final java.util.regex.Pattern VALID_PREDICATE_PATTERN = Pattern.compile(VALID_PREDICATE);

  public static String getXPathValidationError(String expression) {
    if (Strings.isNullOrEmpty(expression)) {
      return Constants.ERROR_EMPTY_EXPRESSION;
    } else if (expression.charAt(0) != Constants.PATH_SEPARATOR_CHAR) {
      if (expression.indexOf(Constants.PATH_SEPARATOR_CHAR) < 0) {
        // "legacy" expression
        if (XMLChar.isValidName(expression)) {
          return null;
        } else {
          return Constants.ERROR_INVALID_ELEMENT_NAME_PREFIX + expression;
        }
      } else {
        // expression that does not start with separator but is otherwise an XPath - not allowed
        return Constants.ERROR_XPATH_MUST_START_WITH_SEP + Constants.PATH_SEPARATOR_CHAR;
      }
    }

    boolean firstEmptyStringSeen = false;
    for (String str : expression.split(Constants.PATH_SEPARATOR)) {
      if (Strings.isNullOrEmpty(str)) {
        if (firstEmptyStringSeen) {
          return Constants.ERROR_DESCENDENT_OR_SELF_NOT_SUPPORTED;
        }
        firstEmptyStringSeen = true;
        continue;
      }
      final int predicateStartInd = str.indexOf('[');
      String elementName = str;
      if (predicateStartInd > 0) {
        elementName = str.substring(0, predicateStartInd);
        final String predicate = str.substring(predicateStartInd);
        final Matcher matcher = VALID_PREDICATE_PATTERN.matcher(predicate);
        if (!matcher.matches()) {
          return Constants.ERROR_INVALID_PREDICATE_PREFIX + predicate;
        } else {
          final String attributeName = matcher.group(1);
          if (!Strings.isNullOrEmpty(attributeName) && !XMLChar.isValidName(attributeName)) {
            return Constants.ERROR_INVALID_ATTRIBUTE_PREFIX + attributeName;
          }
        }
      }
      if (!Constants.WILDCARD.equals(elementName) && !XMLChar.isValidName(elementName)) {
        return Constants.ERROR_INVALID_ELEMENT_NAME_PREFIX + elementName;
      }
    }
    return null;
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
