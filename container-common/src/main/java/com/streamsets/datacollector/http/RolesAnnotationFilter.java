/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.http;

import org.glassfish.jersey.server.model.AnnotatedMethod;

import javax.annotation.Priority;
import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This has the same implementation as {@link org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature}
 * but providing 403 response with a list of allowed roles in the response entity such as
 * <pre>
 *   { "allowed": [ "admin", "user" ] }
 * </pre>
 * The following JavaDoc is copied from the original class.
 * <p/>
 * A {@link DynamicFeature} supporting the {@code javax.annotation.security.RolesAllowed},
 * {@code javax.annotation.security.PermitAll} and {@code javax.annotation.security.DenyAll}
 * on resource methods and sub-resource methods.
 * <p/>
 * The {@link javax.ws.rs.core.SecurityContext} is utilized, using the
 * {@link javax.ws.rs.core.SecurityContext#isUserInRole(String) } method,
 * to ascertain if the user is in one
 * of the roles declared in by a {@code RolesAllowed}. If a user is in none of
 * the declared roles then a 403 (Forbidden) response is returned.
 * <p/>
 * If the {@code DenyAll} annotation is declared then a 403 (Forbidden) response
 * is returned.
 * <p/>
 * If the {@code PermitAll} annotation is declared and is not overridden then
 * this filter will not be applied.
 * <p/>
 * If a user is not authenticated and annotated method is restricted for certain roles then a 403
 * (Not Authenticated) response is returned.
 *
 * @see org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
 */
public class RolesAnnotationFilter implements DynamicFeature {
  public static final String RESPONSE_ALLOWED_KEY = "allowed";

  @Override
  public void configure(ResourceInfo resourceInfo, FeatureContext context) {
    final AnnotatedMethod am = new AnnotatedMethod(resourceInfo.getResourceMethod());

    // DenyAll on the method take precedence over RolesAllowed and PermitAll
    if (am.isAnnotationPresent(DenyAll.class)) {
      context.register(new RolesAllowedRequestFilter());
      return;
    }

    // RolesAllowed on the method takes precedence over PermitAll
    RolesAllowed ra = am.getAnnotation(RolesAllowed.class);
    if (ra != null) {
      context.register(new RolesAllowedRequestFilter(ra.value()));
      return;
    }

    // PermitAll takes precedence over RolesAllowed on the class
    if (am.isAnnotationPresent(PermitAll.class)) {
      // Do nothing.
      return;
    }

    // DenyAll can't be attached to classes

    // RolesAllowed on the class takes precedence over PermitAll
    ra = resourceInfo.getResourceClass().getAnnotation(RolesAllowed.class);
    if (ra != null) {
      context.register(new RolesAllowedRequestFilter(ra.value()));
    }
  }

  @Priority(Priorities.AUTHORIZATION)
  private static class RolesAllowedRequestFilter implements ContainerRequestFilter {

    private final boolean denyAll;
    private final String[] rolesAllowed;

    RolesAllowedRequestFilter() {
      this.denyAll = true;
      this.rolesAllowed = null;
    }

    RolesAllowedRequestFilter(final String[] rolesAllowed) {
      this.denyAll = false;
      this.rolesAllowed = (rolesAllowed != null) ? rolesAllowed : new String[] {};
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
      if (!denyAll) {
        if (rolesAllowed.length > 0 && !isAuthenticated(requestContext)) {
          throw createForbiddenException();
        }

        for (final String role : rolesAllowed) {
          if (requestContext.getSecurityContext().isUserInRole(role)) {
            return;
          }
        }
      }
      throw createForbiddenException();
    }

    private static boolean isAuthenticated(ContainerRequestContext requestContext) {
      return requestContext.getSecurityContext().getUserPrincipal() != null;
    }

    /** @return a new ForbiddenException with a list of allowed roles */
    private ForbiddenException createForbiddenException() {
      Map<String, Object> responseEntity = new LinkedHashMap<>();
      responseEntity.put(RESPONSE_ALLOWED_KEY, rolesAllowed);
      return new ForbiddenException(Response
          .status(Response.Status.FORBIDDEN)
          .type(MediaType.APPLICATION_JSON)
          .entity(responseEntity)
          .build());
    }
  }
}
