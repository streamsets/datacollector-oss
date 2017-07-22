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
package com.streamsets.lib.security;

import com.google.common.collect.ImmutableSet;
import com.streamsets.lib.security.http.SSOPrincipalJson;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.Subject;
import java.security.PrivilegedAction;

public class TestSubjectUtils {

  @Test
  public void testCreateSubject() throws Exception {
    SSOPrincipalJson principal = new SSOPrincipalJson();
    principal.setPrincipalId("p");
    Object pubCredential = new Object();
    Object privCredential = new Object();

    Subject subject = new Subject();
    subject.getPrincipals().add(principal);
    subject.getPublicCredentials().add(pubCredential);
    subject.getPrivateCredentials().add(privCredential);

    SSOPrincipalJson principalX = new SSOPrincipalJson();
    principalX.setPrincipalId("px");

    Subject subjectX = Subject.doAs(subject, (PrivilegedAction<Subject>) () -> SubjectUtils.createSubject(principalX));

    Assert.assertEquals(ImmutableSet.of(principal, principalX), subjectX.getPrincipals());
    Assert.assertEquals(ImmutableSet.of(pubCredential), subjectX.getPublicCredentials());
    Assert.assertEquals(ImmutableSet.of(privCredential), subjectX.getPrivateCredentials());
    Assert.assertFalse(subjectX.isReadOnly());
  }
}
