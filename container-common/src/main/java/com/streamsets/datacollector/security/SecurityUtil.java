/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security;

import javax.security.auth.AuthPermission;
import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.DomainCombiner;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;

public class SecurityUtil {

  private static final AuthPermission DO_AS_PERMISSION = new AuthPermission("doAs");

  public static class CustomCombiner extends SubjectDomainCombiner {
    private DomainCombiner combiner;
    private Subject subject;

    public CustomCombiner(DomainCombiner domainCombiner, Subject subject) {
      super(subject);
      this.combiner = domainCombiner;
      this.subject = subject;
    }

    @Override
    public ProtectionDomain[] combine(
        ProtectionDomain[] currentDomains, ProtectionDomain[] assignedDomains
    ) {
      // SujbectDomainCombiner.combine() takes a lock on map and then a look on principal set.
      // Some other thread could take a lock in reverse order causing the JDK
      // bug (http://bugs.java.com/bugdatabase/view_bug.do?bug_id=8166124) to pop up
      // To prevent this, access is ordered through a lock on principal set and then a lock on
      // cachedPds combiner map
      synchronized (subject.getPrincipals()) {
        return combiner.combine(currentDomains, assignedDomains);
      }
    }
  }

  private static AccessControlContext createContext(
      Subject subject, AccessControlContext context
  ) {
    return AccessController.doPrivileged((PrivilegedAction<AccessControlContext>) () -> {
      AccessControlContext accessControlContext;
      if (subject == null) {
        accessControlContext = new AccessControlContext(context, null);
      } else {
        accessControlContext = new AccessControlContext(context,
            new CustomCombiner(new SubjectDomainCombiner(subject), subject)
        );
      }
      return accessControlContext;
    });
  }

  private static void checkDoAsPermission() {
    SecurityManager manager = System.getSecurityManager();
    if (manager != null) {
      manager.checkPermission(DO_AS_PERMISSION);
    }
  }


  // Influenced by javax.security.auth.Subject.#doAs
  public static <T> T doAs(
      Subject subject,
      PrivilegedExceptionAction<T> privilegedExceptionAction
  ) throws PrivilegedActionException {
    checkDoAsPermission();
    if (privilegedExceptionAction == null) {
      throw new RuntimeException("No privileged exception action provided");
    }

    // The bug this class patches only affects JDK 8 & 9. In later JDK not only the issue is fixed but
    // the code collides with this patch causing strange behavior due to concurrency issues/race conditions.
    // Apply only the patch for versions <9, use the JDK AccessController directly for 9+ versions.
    if (getJavaVersion() <= 9) {
      return AccessController.doPrivileged(privilegedExceptionAction,
          createContext(subject, AccessController.getContext()));
    } else {
      return AccessController.doPrivileged(privilegedExceptionAction, AccessController.getContext());
    }
  }

  // Influenced by javax.security.auth.Subject.#doAs
  public static <T> T doAs(
      final Subject subject, final PrivilegedAction<T> privilegedAction
  ) {
    checkDoAsPermission();
    if (privilegedAction == null) {
      throw new RuntimeException("No privileged action provided");
    }
    return AccessController.doPrivileged(privilegedAction, createContext(subject, AccessController.getContext()));
  }

  // Up to Java10 the versions were in the format 1.X; starting from version 10 only the decimal number is used
  static int getJavaVersion() {
    String version = System.getProperty("java.version");
    if(version.startsWith("1.")) {
      version = version.substring(2, 3);
    } else {
      int dot = version.indexOf(".");
      if(dot != -1) { version = version.substring(0, dot); }
    } return Integer.parseInt(version);
  }
}
