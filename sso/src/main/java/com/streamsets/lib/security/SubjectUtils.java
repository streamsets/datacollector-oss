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

import javax.security.auth.Subject;
import java.security.AccessController;
import java.security.Principal;

/**
 * Subject related utility classes
 */
public class SubjectUtils {

  /**
   * Creates a new principal that contains all principals and credentials of the Subject in context plus the
   * given principal.
   *
   * @param principal the principal to add to the new subject.
   * @return the new subject including all credentials and principals for the Subject in context plus the given
   * principal. It is a read only principal.
   */
  public static Subject createSubject(Principal principal) {
    Subject currentSubject = Subject.getSubject(AccessController.getContext());
    Subject subject = new Subject();
    if (currentSubject != null) {
      subject.getPrincipals().addAll(currentSubject.getPrincipals());
      subject.getPrivateCredentials().addAll(currentSubject.getPrivateCredentials());
      subject.getPublicCredentials().addAll(currentSubject.getPublicCredentials());
    }
    if (principal != null) {
      subject.getPrincipals().add(principal);
    }
//    subject.setReadOnly();
    return subject;
  }

}
