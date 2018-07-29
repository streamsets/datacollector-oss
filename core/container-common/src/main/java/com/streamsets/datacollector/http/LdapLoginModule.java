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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.jaas.callback.ObjectCallback;
import org.eclipse.jetty.jaas.spi.AbstractLoginModule;
import org.eclipse.jetty.jaas.spi.UserInfo;
import org.eclipse.jetty.util.security.Credential;
import org.ldaptive.*;
import org.ldaptive.auth.AuthenticationRequest;
import org.ldaptive.auth.AuthenticationResponse;
import org.ldaptive.auth.Authenticator;
import org.ldaptive.auth.BindAuthenticationHandler;
import org.ldaptive.auth.SearchDnResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This is copy of class org.eclipse.jetty.server.server.plus.jaas.spi.LdapLoginModule
 * Only change is bindingLogin method to use bindDn/bindPassword for fetching roles and
 * removing 'debug' argument (we'll use LOG.debug() instead).
 *
 * A LdapLoginModule for use with JAAS setups
 * <p/>
 * The jvm should be started with the following parameter:
 * <br><br>
 * <code>
 * -Djava.security.auth.login.config=etc/ldap-loginModule.conf
 * </code>
 * <br><br>
 * and an example of the ldap-loginModule.conf would be:
 * <br><br>
 * <pre>
 * ldaploginmodule {
 *    com.streamsets.datacollector.http.LdapLoginModule required
 *    useLdaps="false"
 *    useStartTLS="false"
 *    contextFactory="com.sun.jndi.ldap.LdapCtxFactory"
 *    hostname="ldap.example.com"
 *    port="389"
 *    bindDn="cn=Directory Manager"
 *    bindPassword="directory"
 *    authenticationMethod="simple"
 *    forceBindingLogin="false"
 *    userBaseDn="ou=people,dc=alcatel"
 *    userRdnAttribute="uid"
 *    userIdAttribute="uid"
 *    userPasswordAttribute="userPassword"
 *    userObjectClass="inetOrgPerson"
 *    useFilter="uid={user}"
 *    roleBaseDn="ou=groups,dc=example,dc=com"
 *    roleNameAttribute="cn"
 *    roleMemberAttribute="uniqueMember"
 *    roleObjectClass="groupOfUniqueNames";
 *    };
 *  </pre>
 *
 *
 *
 *
 */
public class LdapLoginModule extends AbstractLoginModule
{
  private static final Logger LOG = LoggerFactory.getLogger(LdapLoginModule.class);

  /**
   * hostname of the ldap server
   */
  private String _hostname;

  /**
   * port of the ldap server
   */
  private int _port;

  /**
   * root DN used to connect to
   */
  private String _bindDn;

  /**
   * password used to connect to the root ldap context
   */
  private String _bindPassword;

  /**
   * object class of a user
   */
  private String _userObjectClass = "inetOrgPerson";

  /**
   * attribute that the principal is located
   */
  private String _userRdnAttribute = "uid";

  /**
   * attribute that the principal is located
   */
  private String _userIdAttribute = "cn";

  /**
   * name of the attribute that a users password is stored under
   * <p/>
   * NOTE: not always accessible, see force binding login
   */
  private String _userPasswordAttribute = "userPassword";

  /**
   * base DN where users are to be searched from
   */
  private String _userBaseDn;

  /**
   * base DN where role membership is to be searched from
   */
  private String _roleBaseDn;

  /**
   * object class of roles
   */
  private String _roleObjectClass = "groupOfUniqueNames";

  /**
   * name of the attribute that a username would be under a role class
   */
  private String _roleMemberAttribute = "uniqueMember";

  /**
   * the name of the attribute that a role would be stored under
   */
  private String _roleNameAttribute = "roleName";

  /**
   * if the getUserInfo can pull a password off of the user then
   * password comparison is an option for authn, to force binding
   * login checks, set this to true
   */
  private boolean _forceBindingLogin = false;

  /**
   * When true changes the protocol to ldaps
   */
  private boolean _useLdaps = false;

  /**
   * When true changes the protocol to ldaps
   */
  private boolean _useStarttls = false;

  /**
   * Default filter to do the user search.
   */
  private String _userFilter = "%s={user}";

  /**
   * Default filter to do the role search.
   */
  private String _roleFilter = "%s={dn})";

  private static final String filterFormat = "(&(objectClass=%s)%s)";

  private static final String DN = "{dn}";
  private static final String USER= "{user}";

  /**
   * LDAP configuration.
   */
  private ConnectionConfig connConfig;

  /**
   * Connection to Ldap server.
   */
  private Connection conn;

  /**
   * get the available information about the user
   * <p/>
   * for this LoginModule, the credential can be null which will result in a
   * binding ldap authentication scenario
   * <p/>
   * roles are also an optional concept if required
   *
   * @param username
   * @return the userinfo for the username
   * @throws Exception
   */
  @Override
  public UserInfo getUserInfo(String username) throws Exception
  {
    LdapEntry entry = getEntryWithCredential(username);

    if (entry == null)
    {
      return null;
    }

    String pwdCredential = getUserCredential(entry);
    pwdCredential = convertCredentialLdapToJetty(pwdCredential);
    Credential credential = Credential.getCredential(pwdCredential);
    List<String> roles = getUserRoles(username, entry.getDn());

    return new UserInfo(username, credential, roles);
  }

  /**
   * attempts to get the users credentials from the users context
   * <p/>
   * NOTE: this is not an user authenticated operation
   *
   * @param username
   * @return
   * @throws LoginException
   */
  private LdapEntry getEntryWithCredential(String username) throws LdapException
  {
    if (StringUtils.isBlank(_userObjectClass)|| StringUtils.isBlank(_userIdAttribute)
        || StringUtils.isBlank(_userBaseDn) || StringUtils.isBlank(_userPasswordAttribute)){
      LOG.error("Failed to get user because at least one of the following is null : " +
          "[_userObjectClass, _userIdAttribute, _userBaseDn, _userPasswordAttribute ]");
      return null;
    }

    // Create the format of &(objectClass=_userObjectClass)(_userIdAttribute={user}))
    String userFilter = buildFilter(_userFilter, _userObjectClass, _userIdAttribute);
    if (userFilter.contains("{user}")){
      userFilter = userFilter.replace("{user}", username);
    }
    LOG.debug("Searching user using the filter {} on user baseDn {}", userFilter, _userBaseDn);

    // Get the group names from each group, which is obtained from roleNameAttribute attribute.
    SearchRequest request = new SearchRequest(_userBaseDn, userFilter, _userPasswordAttribute);
    request.setSearchScope(SearchScope.SUBTREE);
    request.setSizeLimit(1);

    try {
      SearchOperation search = new SearchOperation(conn);
      org.ldaptive.SearchResult result = search.execute(request).getResult();
      LdapEntry entry = result.getEntry();
      LOG.info("Found user?: {}", entry != null);
      return entry;
    } catch (LdapException ex) {
      LOG.error("{}", ex.toString(), ex);
      return null;
    }
  }

  public String getUserCredential(LdapEntry entry) {
    String credential = null;
    if (entry != null) {
      LdapAttribute attr = entry.getAttribute(_userPasswordAttribute);
      if (attr == null){
        LOG.error("Failed to receive user password from LDAP server. Possibly userPasswordAttribute is wrong");
        return null;
      }
      byte[] value = attr.getBinaryValue();
      credential = new String(value, StandardCharsets.UTF_8);
    }
    return credential;
  }

  /**
   * attempts to get the users roles from the root context
   * <p/>
   * NOTE: this is not an user authenticated operation
   *
   * @param dirContext
   * @param username
   * @return
   * @throws LoginException
   */
  /**
   * attempts to get the users roles
   * <p/>
   * NOTE: this is not an user authenticated operation
   *
   * @param username
   * @return
   * @throws LoginException
   */
  private List<String> getUserRoles(String username, String userDn)
  {
    List<String> roleList = new ArrayList<>();
    if (StringUtils.isBlank(_roleBaseDn)|| StringUtils.isBlank(_roleObjectClass)
        || StringUtils.isBlank(_roleNameAttribute) || StringUtils.isBlank(_roleMemberAttribute)){
      LOG.debug("Failed to get roles because at least one of the following is null : " +
          "[_roleBaseDn, _roleObjectClass, _roleNameAttribute, _roleMemberAttribute ]");
      return roleList;
    }

    String roleFilter = buildFilter(_roleFilter, _roleObjectClass, _roleMemberAttribute);
    if (_roleFilter.contains(DN)) {
      userDn = userDn.replace("\\", "\\\\\\");
      roleFilter = roleFilter.replace(DN, userDn);
    } else if (_roleFilter.contains(USER)){
      roleFilter = roleFilter.replace(USER, username);
    } else {
      LOG.error("roleFilter contains invalid filter {}. Check the roleFilter option");
      return roleList;
    }

    LOG.debug("Searching roles using the filter {} on role baseDn {}", roleFilter, _roleBaseDn);
    // Get the group names from each group, which is obtained from roleNameAttribute attribute.
    SearchRequest request = new SearchRequest(_roleBaseDn, roleFilter, _roleNameAttribute);
    request.setSearchScope(SearchScope.SUBTREE);

    try {
      SearchOperation search = new SearchOperation(conn);
      org.ldaptive.SearchResult result = search.execute(request).getResult();
      Collection<LdapEntry> entries = result.getEntries();
      LOG.info("Found roles?: {}", !(entries == null || entries.isEmpty()));
      if (entries != null) {
        for (LdapEntry entry : entries) {
          roleList.add(entry.getAttribute().getStringValue());
        }
      }
    } catch (LdapException ex) {
      LOG.error(ex.getMessage(), ex);
    }
    LOG.info("Found roles: {}", roleList);
    return roleList;
  }

  /**
   * Given a filter(user/role filter), replace attributes using given information from config.
   * This will create complete filter, which will look like &(objectClass=inetOrgPerson)(uid={user}))
   * @param attrFilter We obtain this part from config
   * @param objClass objectClass
   * @param attrName attribute name
   * @return Complete filter
   */
  @VisibleForTesting
  static String buildFilter(String attrFilter, String objClass, String attrName){
    // check if the filter has surrounding "()"
    if(!attrFilter.startsWith("(")){
      attrFilter = "(" + attrFilter;
    }
    if (!attrFilter.endsWith(")")) {
      attrFilter = attrFilter + ")";
    }
    return String.format(filterFormat, objClass, String.format(attrFilter, attrName));
  }


  /**
   * since ldap uses a context bind for valid authentication checking, we override login()
   * <p/>
   * if credentials are not available from the users context or if we are forcing the binding check
   * then we try a binding authentication check, otherwise if we have the users encoded password then
   * we can try authentication via that mechanic
   *
   * @return true if authenticated, false otherwise
   * @throws LoginException
   */
  @Override
  public boolean login() throws LoginException
  {
    try
    {
      if (getCallbackHandler() == null)
      {
        throw new LoginException("No callback handler");
      }
      if (conn == null){
        return false;
      }

      Callback[] callbacks = configureCallbacks();
      getCallbackHandler().handle(callbacks);

      String webUserName = ((NameCallback) callbacks[0]).getName();
      Object webCredential = ((ObjectCallback) callbacks[1]).getObject();

      if (webUserName == null || webCredential == null)
      {
        setAuthenticated(false);
        return isAuthenticated();
      }

      // Please see the following stackoverflow article
      // http://security.stackexchange.com/questions/6713/ldap-security-problems
      // Some LDAP implementation "MAY" accept empty password as a sign of anonymous connection and thus
      // return "true" for the authentication request.
      if((webCredential instanceof String) && ((String)webCredential).isEmpty()) {
        LOG.info("Ignoring login request for user {} as the password is empty.", webUserName);
        setAuthenticated(false);
        return isAuthenticated();
      }
      if (_forceBindingLogin)
      {
        return bindingLogin(webUserName, webCredential);
      }

      // This sets read and the credential
      UserInfo userInfo = getUserInfo(webUserName);

      if (userInfo == null)
      {
        setAuthenticated(false);
        return false;
      }

      JAASUserInfo jaasUserInfo = new JAASUserInfo(userInfo);
      jaasUserInfo.fetchRoles();
      setCurrentUser(jaasUserInfo);

      if (webCredential instanceof String)
      {
        return credentialLogin(Credential.getCredential((String) webCredential));
      }

      return credentialLogin(webCredential);
    }
    catch (UnsupportedCallbackException e)
    {
      throw new LoginException("Error obtaining callback information.");
    }
    catch (IOException e)
    {
      LOG.error("IO Error performing login", e);
    }
    catch (Exception e)
    {
      LOG.error("IO Error performing login", e);
    }
    return false;
  }

  /**
   * password supplied authentication check
   *
   * @param webCredential
   * @return true if authenticated
   * @throws LoginException
   */
  protected boolean credentialLogin(Object webCredential) throws LoginException
  {
    boolean credResult = getCurrentUser().checkCredential(webCredential);
    setAuthenticated(credResult);
    if (!credResult){
      LOG.warn("Authentication failed - Possibly the user password is wrong");
    }
    return isAuthenticated();
  }

  /**
   * binding authentication check
   * This method of authentication works only if the user branch of the DIT (ldap tree)
   * has an ACI (access control instruction) that allow the access to any user or at least
   * for the user that logs in.
   *
   * @param username
   * @param password
   * @return true always
   * @throws LoginException
   */
  public boolean bindingLogin(String username, Object password) throws Exception {
    if (StringUtils.isBlank(_userObjectClass)|| StringUtils.isBlank(_userIdAttribute)
        || StringUtils.isBlank(_userBaseDn)){
      LOG.error("Failed to get user because at least one of the following is null : " +
          "[_userObjectClass, _userIdAttribute, _userBaseDn ]");
      return false;
    }

    LdapEntry userEntry = authenticate(username, password);
    if (userEntry == null) {
      return false;
    }
    // If authenticated by LDAP server, the returned LdapEntry contains full DN of the user
    String userDn = userEntry.getDn();

    if(userDn == null){
      // This shouldn't happen if LDAP server is configured properly.
      LOG.error("userDn is found null for the user {}", username);
      return false;
    }

    List<String> roles = getUserRoles(username, userDn);
    //Authentication already succeeded. We won't store user password so passing empty credential
    UserInfo userInfo = new UserInfo(username, Credential.getCredential(""), roles);
    JAASUserInfo jaasUserInfo = new JAASUserInfo(userInfo);
    jaasUserInfo.fetchRoles();
    setCurrentUser(jaasUserInfo);
    setAuthenticated(true);

    return true;
  }

  /**
   * Perform authentication with given username and password.
   * Receive the result from Ldap server
   * @param username Username that user entered to login
   * @param password Password that user entered to login
   * @return LdapEntry which contains all user attributes
   */
  private LdapEntry authenticate(String username,Object password)
  {
    try {
      SearchDnResolver dnResolver = new SearchDnResolver(new DefaultConnectionFactory(connConfig));

      dnResolver.setBaseDn(_userBaseDn);
      dnResolver.setSubtreeSearch(true);
      String userFilter = buildFilter(_userFilter, _userObjectClass, _userIdAttribute);
      LOG.debug("Searching a user with filter {} where user is {}", userFilter, username);
      dnResolver.setUserFilter(userFilter);

      // Set Authenticator with username and password. It will return the user if username/password matches.
      BindAuthenticationHandler authHandler = new BindAuthenticationHandler(new DefaultConnectionFactory(connConfig));
      Authenticator auth = new Authenticator(dnResolver, authHandler);
      AuthenticationRequest authRequest = new AuthenticationRequest();
      authRequest.setUser(username);
      if (password instanceof char[]) {
        authRequest.setCredential(new org.ldaptive.Credential(new String((char[]) password)));
      } else if (password instanceof String){
        authRequest.setCredential(new org.ldaptive.Credential((String)password));
      } else {
        LOG.error("Unexpected type for password '{}'", (password != null) ? password.getClass() : "NULL");
        return null;
      }
      String[] userRoleAttribute = ReturnAttributes.ALL.value();
      authRequest.setReturnAttributes(userRoleAttribute);

      LOG.debug("Retrieved authenticator from factory: {}", auth);
      LOG.debug("Retrieved authentication request from factory: {}", authRequest);

      AuthenticationResponse response = auth.authenticate(authRequest);
      LOG.info("Found user?: {}", response.getResult());
      if (response.getResult()) {
        LdapEntry entry = response.getLdapEntry();
        return entry;
      } else {
        // User not found. Most likely username/password didn't match. Log the reason.
        LOG.error("Result code: {} - {}", response.getResultCode(), response.getMessage());
      }
    } catch (LdapException e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }

  /**
   * Init LoginModule.
   * Called once by JAAS after new instance is created.
   *
   * @param subject
   * @param callbackHandler
   * @param sharedState
   * @param options
   */
  @Override
  public void initialize(Subject subject,
                         CallbackHandler callbackHandler,
                         Map<String,?> sharedState,
                         Map<String,?> options)
  {
    super.initialize(subject, callbackHandler, sharedState, options);
    LOG.debug("Initializing Ldap configuration");

    _hostname = (String) options.get("hostname");
    _port = Integer.parseInt((String) options.get("port"));
    _bindDn = (String) options.get("bindDn");
    _bindPassword = (String) options.get("bindPassword");
    _userBaseDn = (String) options.get("userBaseDn");
    _roleBaseDn = (String) options.get("roleBaseDn");

    if (options.containsKey("forceBindingLogin")) {
      _forceBindingLogin = Boolean.parseBoolean((String) options.get("forceBindingLogin"));
    }

    if (options.containsKey("useLdaps")) {
      _useLdaps = Boolean.parseBoolean((String) options.get("useLdaps"));
    }

    if (options.containsKey("useStartTLS")) {
      _useStarttls = Boolean.parseBoolean((String) options.get("useStartTLS"));
    }

    _userObjectClass = getOption(options, "userObjectClass", _userObjectClass);
    _userRdnAttribute = getOption(options, "userRdnAttribute", _userRdnAttribute); //depricated
    _userIdAttribute = getOption(options, "userIdAttribute", _userIdAttribute);
    _userPasswordAttribute = getOption(options, "userPasswordAttribute", _userPasswordAttribute);
    _roleObjectClass = getOption(options, "roleObjectClass", _roleObjectClass);
    _roleMemberAttribute = getOption(options, "roleMemberAttribute", _roleMemberAttribute);
    _roleNameAttribute = getOption(options, "roleNameAttribute", _roleNameAttribute);
    _userFilter = getOption(options, "userFilter", _userFilter);
    _roleFilter = getOption(options, "roleFilter", _roleFilter);

    if (Configuration.FileRef.isValueMyRef(_bindPassword)) {
      Configuration.FileRef fileRef = new Configuration.FileRef(_bindPassword);
      _bindPassword = fileRef.getValue();
      if (_bindPassword != null) {
        _bindPassword = _bindPassword.trim();
      }
    }

    // Setup environment. If both useLdaps and useStartTLS are set to true, apply useStartTLS
    String ldapUrl;
    if (_useStarttls){
      ldapUrl = String.format("ldap://%s:%s", _hostname, _port);
    } else {
      ldapUrl = String.format("%s://%s:%s", _useLdaps ? "ldaps" : "ldap", _hostname, _port);
    }
    LOG.info("Accessing LDAP Server: {} startTLS: {}", ldapUrl, _useStarttls);
    connConfig = new ConnectionConfig(ldapUrl);
    connConfig.setUseStartTLS(_useStarttls);
    connConfig.setConnectionInitializer(
        new BindConnectionInitializer(_bindDn, new org.ldaptive.Credential(_bindPassword))
    );
    conn = DefaultConnectionFactory.getConnection(connConfig);
    try {
      conn.open();
    } catch (LdapException ex){
      LOG.error("Failed to establish connection to the LDAP server {}. {}", ldapUrl, ex);
      // We don't throw exception here because there might be multiple LDAP servers configured
    }
  }

  @Override
  public boolean commit() throws LoginException
  {
    if (conn != null && conn.isOpen()) {
      conn.close();
    }
    return super.commit();
  }

  @Override
  public boolean abort() throws LoginException
  {
    if (conn != null && conn.isOpen()) {
      conn.close();
    }
    return super.abort();
  }

  private String getOption(Map<String,?> options, String key, String defaultValue)
  {
    Object value = options.get(key);

    if (value == null)
    {
      return defaultValue;
    }

    return (String) value;
  }

  public static String convertCredentialLdapToJetty(String encryptedPassword)
  {
    if (encryptedPassword == null)
    {
      return encryptedPassword;
    }

    if ("{MD5}".startsWith(encryptedPassword.toUpperCase(Locale.ENGLISH)))
    {
      return "MD5:" + encryptedPassword.substring("{MD5}".length(), encryptedPassword.length());
    }

    if ("{CRYPT}".startsWith(encryptedPassword.toUpperCase(Locale.ENGLISH)))
    {
      return "CRYPT:" + encryptedPassword.substring("{CRYPT}".length(), encryptedPassword.length());
    }

    return encryptedPassword;
  }
}
