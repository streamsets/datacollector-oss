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
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ChooserValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CharsetChooserValues implements ChooserValues {
  private static final Logger LOG = LoggerFactory.getLogger(CharsetChooserValues.class);

  private static final List<String> ALL_CHARSETS;

  static {
    Set<String> set = new LinkedHashSet<>();
    set.add("UTF-8");
    set.add("US-ASCII");
    set.add("UTF-16");
    set.add("ISO-8859-1");
    set.add("IBM-500");
    set.add("GBK");
    Iterator<String> it = set.iterator();
    while (it.hasNext()) {
      String cs = it.next();
      if (!Charset.isSupported(cs)) {
        it.remove();
        LOG.warn("Charset '{}' is not supported", cs);
      }
    }
    for (Map.Entry<String, Charset> entry : Charset.availableCharsets().entrySet()) {
      set.add(entry.getKey());
    }
    ALL_CHARSETS = Collections.unmodifiableList(new ArrayList<>(set));
  }

  public CharsetChooserValues() {
    this(null);
  }

  protected interface Filter {
    boolean accept(Charset charset);
  }

  private List<String> charsets;

  // to enable subclasses to produce subsets of charsets base on some criteria
  protected CharsetChooserValues(Filter filter) {
    if (filter == null) {
      charsets = ALL_CHARSETS;
    } else {
      charsets = new ArrayList<>(ALL_CHARSETS.size());
      for (String name : ALL_CHARSETS) {
        if (filter.accept(Charset.forName(name))) {
          charsets.add(name);
        }
      }
    }
  }

  @Override
  public String getResourceBundle() {
    return null;
  }

  @Override
  public List<String> getValues() {
    return charsets;
  }

  @Override
  public List<String> getLabels() {
    return charsets;
  }

}
