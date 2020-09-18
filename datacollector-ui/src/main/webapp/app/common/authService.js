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
/**
 * Authorization Service
 */

angular.module('dataCollectorApp.common')
  .constant('userRoles', {
    admin: 'admin',
    creator: 'creator',
    manager: 'manager',
    guest: 'guest',
    adminActivation: 'admin-activation'
  })
  .service('authService', function($rootScope, $q, $cookies, api, configuration) {
    var self = this;

    this.initializeDefer = undefined;

    /**
     * Initializes by fetching User Info
     *
     * @returns {*}
     */
    this.init = function() {
      if(!self.initializeDefer) {
        self.initializeDefer = $q.defer();

        $q.all([
          api.admin.getUserInfo()
        ])
          .then(function (results) {
            self.userInfo = results[0].data;
            self.initializeDefer.resolve();
          }, function(data) {
            self.initializeDefer.reject(data);
          });
      }

      return self.initializeDefer.promise;
    };

    /**
     * Checks if the logged in User Roles matches with the given list of roles.
     *
     * @param authorizedRoles
     * @returns {*}
     */
    this.isAuthorized = function (authorizedRoles) {
      if (!angular.isArray(authorizedRoles)) {
        authorizedRoles = [authorizedRoles];
      }

      var intersection = _.intersection(self.userInfo.roles, authorizedRoles);

      return intersection && intersection.length;
    };

    /**
     * Return Logged in User Name
     *
     * @returns {user|*|exports.config.params.login.user|Frisby.current.outgoing.auth.user|mapping.user|string}
     */
    this.getUserName = function() {
      return self.userInfo ? self.userInfo.user: '';
    };

    /**
     * Return User Roles
     * @returns {w.roles|*|string}
     */
    this.getUserRoles = function() {
      return self.userInfo ? self.userInfo.roles : [''];
    };

    /**
     * Return User Groups
     * @returns {w.roles|*|string}
     */
    this.getUserGroups = function() {
      return (self.userInfo && self.userInfo.groups) ? self.userInfo.groups : [];
    };

    /**
     * Fetch Remote User Roles
     */
    this.fetchRemoteUserRoles = function() {
      api.controlHub.getRemoteRoles()
        .then(function(res) {
          self.remoteUserInfo = res.data;
        });
    };

    /**
     * Return Remote Store Base URL
     */
    this.getRemoteBaseUrl = function() {
      var remoteBaseUrl = configuration.getRemoteBaseUrl();
      if (remoteBaseUrl && remoteBaseUrl[remoteBaseUrl.length] !== '/') {
        remoteBaseUrl += '/';
      }
      return remoteBaseUrl;
    };

    /**
     * Return SSO token by extracting it from Cookie
     */
    this.getSSOToken = function() {
      var cookies = $cookies.getAll();
      var ssoToken;
      angular.forEach(cookies, function(value, cookieName) {
        if (cookieName.indexOf('SS-SSO-') != -1) {
          ssoToken = value;
        }
      });
      return ssoToken;
    };

    /**
     * Return Remote Organization ID
     * @returns {*}
     */
    this.getRemoteOrgId = function() {
      if (self.remoteUserInfo) {
        return self.remoteUserInfo.organizationId;
      }
      return '';
    };

    /**
     * Returns true if remote user contains org-admin role otherwise false
     * @returns {*|string|boolean}
     */
    this.isRemoteUserOrgAdmin = function() {
      return self.remoteUserInfo && self.remoteUserInfo.roles && self.remoteUserInfo.roles.indexOf('org-admin') !== -1;
    };

    /**
     * Returns true if remote user contains admin role otherwise false
     * @returns {*|string|boolean}
     */
    this.isUserAdmin = function() {
      return self.userInfo && self.userInfo.roles && self.userInfo.roles.indexOf('admin') !== -1;
    };

    /**
     * Gets the numbers of days until expiration from the expiration date
     * @param {number} expiration milliseconds since epoch
     */
    this.daysUntilProductExpiration = function(expirationTime) {
      var currentTime = new Date().getTime();
      if (expirationTime === -1) {
        return Infinity;
      }
      return Math.floor(( expirationTime - (currentTime - $rootScope.common.serverTimeDifference) ) / 86400000);
    };
  });
