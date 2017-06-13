/**
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
package com.streamsets.datacollector.security;

import java.lang.reflect.Field;
import java.security.AccessControlContext;
import java.security.DomainCombiner;

import javax.security.auth.SubjectDomainCombiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.api.impl.Utils;

public class SecurityUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);

  // as per SDC-2917, any code going through security manager will require this lock
  public static Object getSubjectDomainLock(AccessControlContext context) {
    Object subjectLock;
    try {
      Field subjectDomainLock = SubjectDomainCombiner.class.getDeclaredField("cachedPDs");
      subjectDomainLock.setAccessible(true);
      DomainCombiner domainCombiner = context.getDomainCombiner();
      if (domainCombiner == null) {
        LOG.warn("DomainCombiner should be null only for test cases");
        subjectLock = new Object();
      } else {
        subjectLock = subjectDomainLock.get(domainCombiner);
      }
    } catch (Exception e) {
      throw new RuntimeException(Utils.format("Cannot obtain internal subject domain lock: " + "'{}'", e), e);
    }
    return subjectLock;

  }

}
