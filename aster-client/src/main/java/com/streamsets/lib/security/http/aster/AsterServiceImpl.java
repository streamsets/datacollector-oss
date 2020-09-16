/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.lib.security.http.aster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.eclipse.jetty.security.ServerAuthException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.common.util.RandomValueStringGenerator;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The {@link AsterServiceImpl} encapsulates the interaction with Aster to register the engine
 * and to login users to the engine.
 * <p/>
 * It also provides access, via a singleton, to an Aster REST client configured with credentials
 * to interact with Aster.
 */
public class AsterServiceImpl implements AsterService {
  private static final Logger LOG = LoggerFactory.getLogger(AsterServiceImpl.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(
      SerializationFeature.INDENT_OUTPUT,
      true
  );

  public static final String LSTATE_QS_PARAM = "lstate";


  private static final String HTTP_GET = "GET";
  private static final String HTTP_POST = "POST";

  public static final String STATE_QS_PARAM = "state";
  public static final String CODE_QS_PARAM = "code";
  public static final String APPLICATION_JSON_MIME_TYPE = "application/json";


  private static AsterServiceImpl service;

  private final AsterServiceConfig config;
  private final File tokensFile;
  private volatile AsterRestClientImpl asterRest;
  private final RandomValueStringGenerator randomValueStringGenerator;
  private final Cache<String, String> callbackUrlCache;
  private final Collection<AsterServiceHook> hooks;

  /**
   * Constructor, sets the created instance as the singleton one.
   */
  public AsterServiceImpl(AsterServiceConfig config, File tokensFile) {
    Preconditions.checkArgument(AsterServiceProvider.isEnabled(config.getEngineConfig()),
        "To use Aster, you must configure a valid " + AsterServiceProvider.ASTER_URL);
    this.config = config;
    this.tokensFile = tokensFile;
    this.hooks = new ArrayList<>();
    randomValueStringGenerator = new RandomValueStringGenerator();
    callbackUrlCache = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).build();
    if (service != null) {
      LOG.warn("AsterService already initialized");
    }
    service = this;
  }

  /**
   * Returns the Aster REST client of the service.
   */
  public synchronized AsterRestClientImpl getRestClient() {
    // we lazy initialize the AsterRest client to make sure the engine base URL is already resolved to its correct value
    if (asterRest == null) {
      asterRest = new AsterRestClientImpl(config.getAsterRestConfig(), tokensFile);
    }
    return asterRest;
  }

  @Override
  public void registerHooks(Collection<AsterServiceHook> hooks) {
    if (null == hooks) {
      LOG.debug("Ignoring registration of null hooks");
      return;
    }
    synchronized (this.hooks) {
        this.hooks.addAll(hooks);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Registering hooks " + hooks.stream()
              .map(h -> h.getClass().getSimpleName())
              .collect(Collectors.joining(", ")));
        }
    }
  }

  /**
   * Returns the service configuration.
   */
  @Override
  public AsterServiceConfig getConfig() {
    return config;
  }

  /**
   * Returns if the engine is registered with Aster and with good standing tokens.
   */
  @Override
  public boolean isEngineRegistered() {
    return getRestClient().hasTokens();
  }

  /**
   * Caches the original engine requested URL with a random key.
   */
  @VisibleForTesting
  synchronized String storeRedirUrl(String redirUrl) {
    String key = randomValueStringGenerator.generate();
    while (callbackUrlCache.getIfPresent(key) != null) {
      key = randomValueStringGenerator.generate();
    }
    callbackUrlCache.put(key, redirUrl);
    return key;
  }


  /**
   * Retrieves the original engine requested URL from the cache using its random key and invalidates the cache
   * to avoid the possibility of a replay.
   */
  public synchronized String getRedirUrl(String key) {
    String redirUrl = callbackUrlCache.getIfPresent(key);
    if (redirUrl != null) {
      callbackUrlCache.invalidate(redirUrl);
    } else {
      throw new AsterAuthException("Missing local state, it could have expired: " + key);
    }
    return redirUrl;
  }

  /**
   * On GET, it returns over HTTP a JSON blob with the information for the UI to initiate the authorization request
   * for engine access/refresh tokens. On success it returns NULL.
   * <p/>
   * On POST, it calls ASTER to complete the engine access/refresh tokens request using the the code received in the
   * POST. On success it returns the original requested engine URL.
   * <p/>
   * On failure, in both GET and POST, it throws a {@link ServerAuthException}
   */
  public String handleEngineRegistration(String  engineBasUrl, HttpServletRequest httpReq, HttpServletResponse httpRes) {
    String redirUrl = null;
    if (HTTP_GET.equals(httpReq.getMethod())) {
      LOG.info("Initiating engine registration");
      initiateEngineRegistration(engineBasUrl, httpReq, httpRes);
    } else if (HTTP_POST.equals(httpReq.getMethod())) {
      LOG.info("Completing engine registration");
      redirUrl = completeEngineRegistration(httpReq, httpRes);
      LOG.info("Engine registered");
    } else {
      throw new AsterException("Invalid HTTP request: " + httpReq.getMethod());
    }
    return redirUrl;
  }

  /**
   * Gets a required query string parameter, throws an exception if not found.
   */
  private String getRequiredParameter(HttpServletRequest httpReq, String parameter) {
    String value = httpReq.getParameter(parameter);
    if (value == null) {
      throw new AsterException(String.format("Missing '%s' parameter", parameter));
    }
    return value;
  }

  /**
   * It returns over HTTP a JSON blob with the information for the UI to initiate the authorization request
   * for engine access/refresh tokens.
   */
  private void initiateEngineRegistration(String engineBaseUrl, HttpServletRequest httpReq, HttpServletResponse httpRes) {
    // the after registration redir URL
    String keyForRedirUrl = getRequiredParameter(httpReq, LSTATE_QS_PARAM);
    String redirUrl = getRedirUrl(keyForRedirUrl);
    try {
      AsterAuthorizationRequest engineRegistrationRequest = getRestClient().createEngineAuthorizationRequest(
          engineBaseUrl,
          redirUrl
      );
      httpRes.setStatus(HttpServletResponse.SC_OK);
      httpRes.setContentType(APPLICATION_JSON_MIME_TYPE);
      OBJECT_MAPPER.writeValue(httpRes.getWriter(), engineRegistrationRequest);
    } catch (AsterException | AsterAuthException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AsterException(String.format("Could not complete engine authorization request: %s", ex), ex);
    }
  }

  /**
   * It calls ASTER to complete the engine access/refresh tokens request using the the code received in the
   * POST. It returns the original requested engine URL, it cannot return NULL.
   */
  private String completeEngineRegistration(HttpServletRequest httpReq, HttpServletResponse httpRes)  {
    String state = getRequiredParameter(httpReq, STATE_QS_PARAM);
    String code = getRequiredParameter(httpReq, CODE_QS_PARAM);
    AsterAuthorizationRequest request = getRestClient().findRequest(state);
    getRestClient().registerEngine(request, code);
    String originalRequestedEngineUrl = request.getLocalState();
    synchronized (hooks) {
      hooks.forEach(h -> h.onSuccessfulRegistration(originalRequestedEngineUrl));
    }
    return originalRequestedEngineUrl;
  }

  /**
   * On GET, it returns over HTTP a JSON blob with the information for the UI to initiate an engine user SSO request.
   * On success it returns NULL.
   * <p/>
   * On POST, it calls ASTER to fetch the user information using the the code received in the
   * POST. On success it returns the user information and the original requested engine URL.
   * <p/>
   * On failure, in both GET and POST, it throws a {@link ServerAuthException}
   */
  public AsterUser handleUserLogin(String engineBaseUrl, HttpServletRequest httpReq, HttpServletResponse httpRes) {
    AsterUser user = null;
    if (HTTP_GET.equals(httpReq.getMethod())) {
      LOG.debug("Initiating user login");
      initiateUserLogin(engineBaseUrl, httpReq, httpRes);
    } else if (HTTP_POST.equals(httpReq.getMethod())) {
      LOG.debug("Completing user login");
      user = completeUserLogin(httpReq, httpRes);
      LOG.debug("User '{}' with roles '{}' logged in", user.getName(), user.getRoles());
    } else {
      throw new AsterException("Invalid HTTP request: " + httpReq.getMethod());
    }
    return user;
  }

  /**
   * It returns over HTTP a JSON blob with the information for the UI to initiate an engine user SSO request.
   */
  private void initiateUserLogin(String engineBaseUrl, HttpServletRequest httpReq, HttpServletResponse httpRes) {
    // the after login redir URL
    String keyForRedirUrl = getRequiredParameter(httpReq, LSTATE_QS_PARAM);
    String redirUrl = getRedirUrl(keyForRedirUrl);
    try {
      AsterAuthorizationRequest engineRegistrationRequest = getRestClient().createUserAuthorizationRequest(
          engineBaseUrl,
          redirUrl
      );
      httpRes.setStatus(HttpServletResponse.SC_OK);
      httpRes.setContentType(APPLICATION_JSON_MIME_TYPE);
      OBJECT_MAPPER.writeValue(httpRes.getWriter(), engineRegistrationRequest);
    } catch (AsterException | AsterAuthException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AsterException(String.format("Could not complete user authorization request: %s", ex), ex);
    }
  }

  /**
   * It calls ASTER to fetch the user information using the the code received in the POST.
   * On success it returns the user information and the original requested engine URL, it cannot return NULL.
   */
  private AsterUser completeUserLogin(HttpServletRequest httpReq, HttpServletResponse httpRes) {
    String state = getRequiredParameter(httpReq, STATE_QS_PARAM);
    String code = getRequiredParameter(httpReq, CODE_QS_PARAM);
    AsterAuthorizationRequest request = getRestClient().findRequest(state);
    AsterUser asterUser = getRestClient().getUserInfo(request, code);
    asterUser.setPreLoginUrl(request.getLocalState());
    return asterUser;
  }

  public void handleLogout(HttpServletRequest httpReq, HttpServletResponse httpRes) {
    try {
      AsterLogoutRequest logoutRequest = new AsterLogoutRequest().setRedirect_uri(
          service.getConfig().getAsterRestConfig().getAsterUrl() + "/logout"
      );
      httpRes.setStatus(HttpServletResponse.SC_OK);
      httpRes.setContentType(APPLICATION_JSON_MIME_TYPE);
      OBJECT_MAPPER.writeValue(httpRes.getWriter(), logoutRequest);
    } catch (AsterException | AsterAuthException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AsterException(String.format("Could not complete user logout request: %s", ex), ex);
    }

  }

  /**
   * Returns if a key matches a cached local state.
   */
  boolean isValidLocalState(String localStateKey) {
    return callbackUrlCache.getIfPresent(localStateKey) != null;
  }

}
