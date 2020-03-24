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
package com.streamsets.datacollector.http;

import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.security.AbstractLoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.resource.Resource;

import java.io.IOException;
import java.util.Set;

public class SdcHashLoginService extends AbstractLoginService {
  private static final Logger LOG = Log.getLogger(SdcHashLoginService.class);
  private static final String EMAIL_PREFIX = "email:";
  private static final String GROUP_PREFIX = "group:";

  private String _config;
  private boolean hotReload = true; // default is not to reload
  private UserStore _userStore;
  private boolean _userStoreAutoCreate = false;


  /* ------------------------------------------------------------ */
  public SdcHashLoginService()
  {
  }

  /* ------------------------------------------------------------ */
  public SdcHashLoginService(String name)
  {
    setName(name);
  }

  /* ------------------------------------------------------------ */
  public SdcHashLoginService(String name, String config)
  {
    setName(name);
    setConfig(config);
  }

  /* ------------------------------------------------------------ */
  public String getConfig()
  {
    return _config;
  }


  /* ------------------------------------------------------------ */
  @Deprecated
  public Resource getConfigResource()
  {
    return null;
  }

  /* ------------------------------------------------------------ */
  /**
   * Load realm users from properties file.
   * <p>
   * The property file maps usernames to password specs followed by an optional comma separated list of role names.
   * </p>
   *
   * @param config uri or url or path to realm properties file
   */
  public void setConfig(String config)
  {
    _config=config;
  }

  /**
   * Is hot reload enabled on this user store
   *
   * @return true if hot reload was enabled before startup
   */
  public boolean isHotReload()
  {
    return hotReload;
  }

  /**
   * Enable Hot Reload of the Property File
   *
   * @param enable true to enable, false to disable
   */
  public void setHotReload(boolean enable)
  {
    if (isRunning())
    {
      throw new IllegalStateException("Cannot set hot reload while user store is running");
    }
    this.hotReload = enable;
  }

  /**
   * Configure the {@link UserStore} implementation to use.
   * If none, for backward compat if none the {@link PropertyUserStore} will be used
   * @param userStore the {@link UserStore} implementation to use
   */
  public void setUserStore(UserStore userStore)
  {
    Utils.checkArgument(userStore instanceof PropertyUserStore, "Only PropertyUserStore is supported.");
    this._userStore = userStore;
  }

  /* ------------------------------------------------------------ */
  @Override
  protected String[] loadRoleInfo(UserPrincipal user)
  {
    UserIdentity id = _userStore.getUserIdentity(user.getName());
    if (id == null)
      return null;


    Set<RolePrincipal> roles = id.getSubject().getPrincipals(RolePrincipal.class);
    if (roles == null)
      return null;

    return roles.stream()
        .map(RolePrincipal::getName)
        .filter(role -> !role.startsWith(EMAIL_PREFIX))
        .filter(role -> !role.startsWith(GROUP_PREFIX))
        .toArray(String[]::new);
  }


  /* ------------------------------------------------------------ */
  @Override
  protected UserPrincipal loadUserInfo(String userName)
  {
    UserIdentity id = _userStore.getUserIdentity(userName);
    if (id != null)
    {
      return (UserPrincipal)id.getUserPrincipal();
    }

    return null;
  }



  /* ------------------------------------------------------------ */
  /**
   * @see org.eclipse.jetty.util.component.AbstractLifeCycle#doStart()
   */
  @Override
  protected void doStart() throws Exception
  {
    super.doStart();

    // can be null so we switch to previous behaviour using PropertyUserStore
    if (_userStore == null)
    {
      if(LOG.isDebugEnabled())
        LOG.debug("doStart: Starting new PropertyUserStore. PropertiesFile: " + _config + " hotReload: " + hotReload);
      PropertyUserStore propertyUserStore = new PropertyUserStore();
      propertyUserStore.setHotReload(hotReload);
      propertyUserStore.setConfigPath(_config);
      propertyUserStore.start();
      _userStore = propertyUserStore;
      _userStoreAutoCreate = true;
    }
  }

  /* ------------------------------------------------------------ */
  /**
   * @see org.eclipse.jetty.util.component.AbstractLifeCycle#doStop()
   */
  @Override
  protected void doStop() throws Exception
  {
    super.doStop();
    if (_userStore != null && _userStoreAutoCreate)
      _userStore.stop();
    _userStore = null;
  }

  public UserIdentity getUserIdentity(String name) {
    return _userStore.getUserIdentity(name);
  }

  public Resource getResolvedConfigResource() throws IOException {
    return ((PropertyUserStore)_userStore).getConfigResource();
  }

}
