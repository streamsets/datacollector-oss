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

angular.module('dataCollectorApp')
  .config(function($routeProvider, $locationProvider, $translateProvider, $provide,
                   tmhDynamicLocaleProvider, uiSelectConfig, $httpProvider, AnalyticsProvider) {

    $locationProvider.html5Mode({enabled: true, requireBase: false});
    $routeProvider.otherwise({
      templateUrl: 'app/home/home.tpl.html',
      controller: 'HomeController',
      resolve: {
        myVar: function(authService) {
          return authService.init();
        }
      },
      data: {
        authorizedRoles: ['admin', 'creator', 'manager', 'guest']
      }
    });

    // Default Timezone (local storage)
    $provide.decorator('dateFilter', ['$delegate', '$rootScope', function ($delegate, $rootScope) {
      var clientTimezone = moment().tz(moment.tz.guess()).format('z');
      return function (date, format, timezone) {
        return $delegate.call(
          this,
          date,
          format,
          timezone || $rootScope.$storage.preferredTimezone || clientTimezone
        );
      };
    }]);

    // Initialize angular-translate
    $translateProvider.useStaticFilesLoader({
      prefix: 'i18n/',
      suffix: '.json'
    });

    $translateProvider.preferredLanguage('en');

    $translateProvider.useCookieStorage();
    $translateProvider.useSanitizeValueStrategy('sanitize');

    tmhDynamicLocaleProvider.localeLocationPattern('bower_components/angular-i18n/angular-locale_{{locale}}.js');
    tmhDynamicLocaleProvider.useCookieStorage('NG_TRANSLATE_LANG_KEY');

    uiSelectConfig.theme = 'bootstrap';

    //Reload the page when the server is down.
    $httpProvider.interceptors.push(function($q, $rootScope) {
      return {
        response: function(response) {
          return response;
        },
        responseError: function(rejection) {
          console.log(rejection);
          if (rejection.status === 401) {
            window.location.reload();
          } else if ((rejection.status === 0 || rejection.status === -1 ||
              (rejection.data && (typeof rejection.data.indexOf === 'function') &&
                rejection.data.indexOf('login.html') !== -1))
          )  {
            // check if the error is related to remote service
            if (rejection.config && rejection.config.headers && rejection.config.headers['X-SS-User-Auth-Token']) {
              rejection.data = 'Failed to connect to Remote Service';
            } else {
              // window.location.reload();
              $rootScope.common.openConnectionLostModal();
              rejection.data = 'Connection to server lost';
            }
          }
          return $q.reject(rejection);
        }
      };
    });

    AnalyticsProvider.setAccount('UA-60917135-1');
    AnalyticsProvider.trackPages(false);
    AnalyticsProvider.trackUrlParams(true);
    AnalyticsProvider.setDomainName('none');
    AnalyticsProvider.useAnalytics(true);
    AnalyticsProvider.delayScriptTag(true);

  })
  .run(function ($location, $rootScope, $modal, api, pipelineConstant, $localStorage, contextHelpService, $modalStack,
                 $timeout, $translate, authService, userRoles, configuration, Analytics, $q, editableOptions, $http) {

    var defaultTitle = 'StreamSets | Data Collector';
    var pipelineStatusTimer;
    var alertsTimer;
    var isWebSocketSupported;
    var loc = window.location;
    var httpBaseURL = ((loc.protocol === "https:") ? "https://" : "http://") + loc.hostname + (loc.port ? ":" + loc.port : "");
    var bases = document.getElementsByTagName('base');
    var baseHref = (bases.length > 0) ? (bases[0].href).replace(httpBaseURL, '') : '/';
    var webSocketBaseURL = ((loc.protocol === "https:") ?
      "wss://" : "ws://") + loc.hostname + (((loc.protocol === "http:" && loc.port === 80) || (loc.protocol === "https:" && loc.port === 443)) ? "" : ":" + loc.port) + baseHref;
    var BACKSPACE_KEY = 8;
    var DELETE_KEY = 46;
    var D_KEY = 68;
    var Z_KEY = 90;
    var Y_KEY = 89;
    var destroyed = false;
    var webSocketStatusURL = webSocketBaseURL + 'rest/v1/webSocket?type=status';
    var statusWebSocket;
    var webSocketAlertsURL = webSocketBaseURL + 'rest/v1/webSocket?type=alerts';
    var alertsWebSocket;
    var isConnectionLostModalDisplayed;

    editableOptions.theme = 'bs3';

    // Math.random() does not provide cryptographically secure random numbers,
    // so overriding to use window.crypto.getRandomValues for getting random values.
    var randomFunction = Math.random;
    Math.random = function() {
      if(window.crypto && typeof window.crypto.getRandomValues === "function") {
        var array = new Uint32Array(10);
        window.crypto.getRandomValues(array);
        return array[0]/10000000000;
      }
      return randomFunction();
    };

    $http.defaults.headers.common['X-Requested-By'] = 'Data Collector' ;

    $rootScope.pipelineConstant = pipelineConstant;
    $rootScope.$storage = $localStorage.$default({
      displayDensity: pipelineConstant.DENSITY_COMFORTABLE,
      helpLocation: pipelineConstant.HOSTED_HELP,
      readNotifications: [],
      pipelineListState: {
        gridView: false,
        searchInput: '',
        showNameColumn: true
      },
      runPreviewForFieldPaths: true,
      lineWrapping: true
    });

    $rootScope.common = $rootScope.common || {
      title : defaultTitle,
      userName: 'Account',
      authenticationType: 'none',
      apiVersion: api.apiVersion,
      baseHref: baseHref,
      webSocketBaseURL: webSocketBaseURL,
      active: {
        home: 'active'
      },
      namePattern: '^[a-zA-Z0-9 _]+$',
      saveOperationInProgress: 0,
      pipelineStatus: {},
      pipelineStatusMap: {},
      alertsMap: {},
      alertsTotalCount: 0,
      errors: [],
      infoList: [],
      successList: [],
      activeDetailTab: undefined,
      dontShowHelpAlert: false,
      logEndingOffset: -1,
      fetchingLog: false,
      counters: {},
      serverTimeDifference: 0,
      remoteServerInfo: {
        registrationStatus: false
      },

      /**
       * Open the SCH Information Dialog
       */
      onDPMButtonClick: function() {
        $modalStack.dismissAll();
        $modal.open({
          templateUrl: 'common/administration/dpmInfo/dpmInfo.tpl.html',
          controller: 'DPMInfoModalInstanceController',
          size: 'lg',
          backdrop: 'static'
        });
      },

      /**
       * Open the Enable SCH Modal Dialog
       */
      onEnableDPMClick: function() {
        if (configuration.isManagedByClouderaManager()) {
          $translate('home.enableDPM.isManagedByClouderaManager').then(function(translation) {
            $rootScope.common.errors = [translation];
          });
          return;
        }

        if ($rootScope.common.remoteServerInfo.registrationStatus) {
          $translate('home.enableDPM.alreadyEnabledMsg').then(function(translation) {
            $rootScope.common.errors = [translation];
          });
          return;
        }

        $modalStack.dismissAll();
        $modal.open({
          templateUrl: 'common/administration/enableDPM/enableDPM.tpl.html',
          controller: 'EnableDPMModalInstanceController',
          size: 'lg',
          backdrop: 'static'
        });
      },

      /**
       * Open the Disable SCH Modal Dialog
       */
      onDisableDPMClick: function() {
        if (configuration.isManagedByClouderaManager()) {
          $translate('home.enableDPM.isManagedByClouderaManager').then(function(translation) {
            $rootScope.common.errors = [translation];
          });
          return;
        }

        $modal.open({
          templateUrl: 'common/administration/disableDPM/disableDPM.tpl.html',
          controller: 'DisableDPMModalInstanceController',
          size: '',
          backdrop: 'static'
        });
      },

      onCreateDPMUsersClick: function() {
        $modal.open({
          templateUrl: 'common/administration/createDPMUsers/createDPMUsers.tpl.html',
          controller: 'CreateDPMUsersModalInstanceController',
          size: 'lg',
          backdrop: 'static',
          resolve: {
            dpmInfoModel: function () {
              return {
                baseURL: 'https://cloud.streamsets.com',
                userID: '',
                userPassword: ''
              };
            }
          }
        });
      },

      /**
       * Open the Shutdown Modal Dialog
       */
      shutdownCollector: function() {
        $modal.open({
          templateUrl: 'common/administration/shutdown/shutdownModal.tpl.html',
          controller: 'ShutdownModalInstanceController',
          size: '',
          backdrop: true
        });
      },

      /**
       * Open the Restart Modal Dialog
       */
      restartCollector: function() {
        $modal.open({
          templateUrl: 'common/administration/restart/restartModal.tpl.html',
          controller: 'RestartModalInstanceController',
          size: '',
          backdrop: true
        });
      },

      /**
       * Logout header link command handler
       */
      logout: function() {
        api.admin.logout($rootScope.common.authenticationType, $rootScope.common.isDPMEnabled)
          .then(function() {
            location.reload();
          });
      },

      /**
       * Launch Local or Online Help based on settings.
       *
       */
      launchHelpContents: function() {
        contextHelpService.launchHelpContents();
      },

      /**
       * Open the Support Bundle Modal Dialog
       */
      showSupportBundle: function() {
        $modal.open({
          templateUrl: 'app/help/supportBundle/supportBundleModal.tpl.html',
          controller: 'SupportBundleModalInstanceController',
          size: '',
          backdrop: true
        });
      },

      /**
       * Open the About Modal Dialog
       */
      showAbout: function() {
        $modal.open({
          templateUrl: 'aboutModalContent.html',
          controller: 'AboutModalInstanceController',
          size: '',
          backdrop: true
        });
      },

      /**
       * Open the Settings Modal Dialog
       */
      showSettings: function() {
        $modal.open({
          templateUrl: 'app/help/settings/settingsModal.tpl.html',
          controller: 'SettingsModalInstanceController',
          size: '',
          backdrop: true
        });
      },

      showRegistrationModal: function() {
        $modal.open({
          templateUrl: 'app/help/register/registerModal.tpl.html',
          controller: 'RegisterModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            activationInfo: function () {
              return $rootScope.common.activationInfo;
            }
          }
        });
      },

      showSDCDirectories: function() {
        $modal.open({
          templateUrl: 'common/administration/sdcDirectories/sdcDirectoriesModal.tpl.html',
          controller: 'SDCDirectoriesModalInstanceController',
          size: '',
          backdrop: true
        });
      },

      /**
       * Clear Local Storage Contents
       */
      clearLocalStorage: function() {
        $localStorage.$reset();
      },

      /**
       * Key Event on body DOM element.
       *
       * @param $event
       */
      bodyKeyEvent: function($event) {
        if($event.target === $event.currentTarget && $event.shiftKey !== true &&
          ($event.keyCode === BACKSPACE_KEY || $event.keyCode === DELETE_KEY)) {

          //Delete Operation

          $event.preventDefault();
          $event.stopPropagation();

          $rootScope.$broadcast('bodyDeleteKeyPressed');
        } else if(($event.metaKey && $event.shiftKey && ($event.keyCode === Z_KEY)) ||
          ($event.ctrlKey && $event.keyCode === Y_KEY))  {

          //REDO Operation
          $rootScope.$broadcast('bodyRedoKeyPressed');
        } else if(($event.metaKey || $event.ctrlKey) && $event.keyCode === Z_KEY) {
          //UNDO Operation
          $rootScope.$broadcast('bodyUndoKeyPressed');
        }

        if ($event.ctrlKey && $event.altKey && $event.keyCode === D_KEY) {
          $rootScope.common.onEnableDPMClick();
        }
      },

      /**
       * Google Analytics Track Event
       *
       * @param category Typically the object that was interacted with (e.g. button)
       * @param action The type of interaction (e.g. click)
       * @param label Useful for categorizing events (e.g. nav buttons)
       * @param value Values must be non-negative. Useful to pass counts (e.g. 4 times)
       */
      trackEvent: function(category, action, label, value) {
        if(configuration.isAnalyticsEnabled()) {
          Analytics.trackEvent(category, action, label, value);
        }
      },

      /**
       * Callback function when Alert is clicked.
       *
       * @param alert
       */
      onAlertClick: function(alert) {
        $rootScope.common.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION,
          'Notification Message', 1);
        $rootScope.$broadcast('onAlertClick', alert);
      },

      /**
       * Delete Triggered Alert
       */
      deleteTriggeredAlert: function(triggeredAlert, event) {

        if(event) {
          event.preventDefault();
          event.stopPropagation();
        }

        var alerts = $rootScope.common.alertsMap[triggeredAlert.pipelineName];

        if(alerts) {
          $rootScope.common.alertsTotalCount--;

          $rootScope.common.alertsMap[triggeredAlert.pipelineName] = _.filter(alerts, function(alert) {
            return alert.ruleDefinition.id !== triggeredAlert.ruleDefinition.id;
          });
        }


        api.pipelineAgent.deleteAlert(triggeredAlert.pipelineName, triggeredAlert.ruleDefinition.id)
          .then(function() {

          })
          .catch(function(response) {
            $rootScope.common.errors = [response.data];
          });
      },

      ignoreCodeMirrorEnterKey: function() {
        //console.log('onCodeMirrorEnterKey');
      },

      openConnectionLostModal: function () {
        if (!isConnectionLostModalDisplayed) {
          isConnectionLostModalDisplayed = true;
          $modalStack.dismissAll();
          $modal.open({
            templateUrl: 'common/administration/connectionLost/connectionLost.tpl.html',
            controller: 'ConnectionLostModalInstanceController',
            size: '',
            backdrop: 'static',
            keyboard: false
          });
        }
      },

      closeConnectionLostModal: function() {
        isConnectionLostModalDisplayed = false;
      },

      clearTextSelection: function() {
        if (window.getSelection) {
          if (window.getSelection().empty) {  // Chrome
            window.getSelection().empty();
          } else if (window.getSelection().removeAllRanges) {  // Firefox
            window.getSelection().removeAllRanges();
          }
        } else if (document.selection) {  // IE?
          document.selection.empty();
        }
      },

      /**
       * Update Permissions callback function
       */
      updatePermissions: function () {
        $modal.open({
          templateUrl: 'common/administration/update_permissions/updatePermissions.tpl.html',
          controller: 'UpdatePermissionsInstanceController',
          size: 'lg',
          backdrop: 'static'
        });
      }
    };


    api.admin.getServerTime().then(function(res) {
      if (res && res.data) {
        var serverTime = res.data.serverTime,
          browserTime = (new Date()).getTime();
        $rootScope.common.serverTimeDifference = serverTime - browserTime;
      }
    });

    api.admin.getBuildInfo().then(function(res) {
      if (res && res.data) {
        $timeout(
          function() {
            Analytics.set('sdcVersion', res.data.version);
          },
          1000
        );
      }
    });

    api.admin.getRemoteServerInfo().then(function(res) {
      if (res && res.data) {
        $rootScope.common.remoteServerInfo.registrationStatus = res.data.registrationStatus;
      }
    });

    api.activation.getActivation().then(function(res) {
      if (res && res.data) {
        var activationInfo = $rootScope.common.activationInfo = res.data;
        if (activationInfo.enabled) {
          var currentTime = new Date().getTime();
          var expirationTime = activationInfo.info.expiration;
          var difDays =  Math.floor(( expirationTime - currentTime ) / 86400000);
          if (difDays < 0 ) {
            $rootScope.common.infoList = [{
              message: 'Activation key expired, you need to get a new one from StreamSets'
            }];
          } else if (difDays < 30) {
            $rootScope.common.infoList = [{
              message: 'Activation key expires in ' + difDays + '  days'
            }];
          } else if (!activationInfo.info.valid) {
            $rootScope.common.infoList = [{
              message: 'Activation key is not valid'
            }];
          }
        }
      }
    });

    $q.all([configuration.init(), authService.init()])
      .then(function(results) {
        $rootScope.common.userName = authService.getUserName();
        $rootScope.common.userRoles = authService.getUserRoles().join(', ');
        $rootScope.common.userGroups = authService.getUserGroups().join(', ');
        $rootScope.userRoles = userRoles;
        $rootScope.isAuthorized = authService.isAuthorized;
        $rootScope.common.isUserAdmin = authService.isUserAdmin();



        $rootScope.common.authenticationType = configuration.getAuthenticationType();
        $rootScope.common.isDPMEnabled = configuration.isDPMEnabled();
        $rootScope.common.isACLEnabled = configuration.isACLEnabled();
        $rootScope.common.dpmBaseURL = configuration.getRemoteBaseUrl();
        $rootScope.common.isSlaveNode = configuration.isSlaveNode();
        $rootScope.common.sdcClusterManagerURL = configuration.getSDCClusterManagerURL();
        $rootScope.common.isMetricsTimeSeriesEnabled = configuration.isMetricsTimeSeriesEnabled();
        $rootScope.common.headerTitle = configuration.getUIHeaderTitle();
        if(configuration.isAnalyticsEnabled()) {
          Analytics.createAnalyticsScriptTag();
        }

        if ($rootScope.common.isDPMEnabled && $rootScope.common.userRoles.indexOf('disconnected-sso') !== -1) {
          $rootScope.common.disconnectedMode = true;
        }

        if ($rootScope.common.isDPMEnabled && !$rootScope.common.disconnectedMode) {
          authService.fetchRemoteUserRoles();
        }

        isWebSocketSupported = (typeof(WebSocket) === "function") && configuration.isWebSocketUseEnabled();
        refreshPipelineStatus();
        refreshAlerts();

        $rootScope.$storage.serverTimezone = configuration.getServerTimezone();
        if (configuration.isUIDebugEnabled()) {
          window.$rootScope = $rootScope;
        }
      });


    api.pipelineAgent.getAllAlerts()
      .then(function(res) {
        var alertsInfoList = res.data;
        $rootScope.common.alertsTotalCount = alertsInfoList.length;
        $rootScope.common.alertsMap = _.reduce(alertsInfoList,
          function (alertsMap, alertInfo) {
            if(!alertsMap[alertInfo.pipelineName]) {
              alertsMap[alertInfo.pipelineName] = [];
            }
            alertsMap[alertInfo.pipelineName].push(alertInfo);
            return alertsMap;
          },
          {}
        );
      });

    // set actions to be taken each time the user navigates
    $rootScope.$on('$routeChangeSuccess', function (event, current, previous) {
      // set page title
      if(current.$$route && current.$$route.data) {
        var authorizedRoles = current.$$route.data.authorizedRoles;
        $rootScope.notAuthorized = !authService.isAuthorized(authorizedRoles);
      }
    });

    $rootScope.go = function ( path ) {
      $location.path( path );
    };

    /**
     * Fetch the Pipeline Status every configured refresh interval.
     *
     */
    var refreshPipelineStatus = function() {
      if(destroyed) {
        return;
      }

      if(isWebSocketSupported) {
        //WebSocket to get Pipeline Status

        statusWebSocket = new WebSocket(webSocketStatusURL);

        statusWebSocket.onmessage = function (evt) {
          var received_msg = evt.data;

          $rootScope.$apply(function() {
            var parsedStatus = JSON.parse(received_msg);
            $rootScope.common.pipelineStatusMap[parsedStatus.pipelineId] = parsedStatus;

            if(parsedStatus.status !== 'RUNNING') {
              var alerts = $rootScope.common.alertsMap[parsedStatus.pipelineId];

              if(alerts) {
                delete $rootScope.common.alertsMap[parsedStatus.pipelineId];
                $rootScope.common.alertsTotalCount -= alerts.length;
              }
            }
          });
        };

        statusWebSocket.onerror = function (evt) {
          isWebSocketSupported = false;
          refreshPipelineStatus();
        };

        statusWebSocket.onclose = function(evt) {
          $timeout(
            function() {
              refreshPipelineStatus();
            },
            100
          );
        };

      } else {
        //WebSocket is not support use polling to get Pipeline Status

        pipelineStatusTimer = $timeout(
          function() {
            //console.log( "Pipeline Status Timeout executed", Date.now() );
          },
          configuration.getRefreshInterval()
        );

        pipelineStatusTimer.then(
          function() {
            api.pipelineAgent.getAllPipelineStatus()
              .then(function(response) {
                var data = response.data;
                if(!_.isObject(data) && _.isString(data) && data.indexOf('<!doctype html>') !== -1) {
                  //Session invalidated
                  window.location.reload();
                  return;
                }

                $rootScope.common.pipelineStatusMap = data;

                refreshPipelineStatus();
              })
              .catch(function(response) {
                $rootScope.common.errors = [response.data];
              });
          },
          function() {
            //console.log( "Timer rejected!" );
          }
        );
      }
    };

    /**
     * Fetch the Pipeline Status every configured refresh interval.
     *
     */
    var refreshAlerts = function() {
      if(destroyed) {
        return;
      }

      if(isWebSocketSupported && 'Notification' in window) {
        Notification.requestPermission(function(permission) {
          if(alertsWebSocket) {
            alertsWebSocket.close();
          }
          alertsWebSocket = new WebSocket(webSocketAlertsURL);
          alertsWebSocket.onmessage = function (evt) {
            var received_msg = evt.data;
            if(received_msg) {
              var alertInfo = JSON.parse(received_msg);

              $rootScope.$apply(function() {
                var alertsMap = $rootScope.common.alertsMap;

                if(!alertsMap[alertInfo.pipelineName]) {
                  alertsMap[alertInfo.pipelineName] = [];
                }
                alertsMap[alertInfo.pipelineName].push(alertInfo);

                $rootScope.common.alertsTotalCount++;
              });

              var notification = new Notification(alertInfo.pipelineName, {
                body: alertInfo.ruleDefinition.alertText,
                icon: 'assets/favicon.png'
              });

              notification.onclick = function() {
                notification.close();
                window.open('collector/pipeline/' + alertInfo.pipelineName);
              };

            }
          };

          alertsWebSocket.onclose = function(evt) {
            $timeout(
              function() {
                refreshAlerts();
              },
              100
            );
          };

        });
      } else {
        //WebSocket is not support use polling to get Pipeline Status

        alertsTimer = $timeout(
          function() {
            //console.log( "Pipeline Status Timeout executed", Date.now() );
          },
          configuration.getRefreshInterval()
        );

        alertsTimer.then(
          function() {
            api.pipelineAgent.getAllAlerts()
              .then(function(response) {
                var data = response.data;
                if(!_.isObject(data) && _.isString(data) && data.indexOf('<!doctype html>') !== -1) {
                  //Session invalidated
                  window.location.reload();
                  return;
                }

                $rootScope.common.alertsTotalCount = data.length;
                $rootScope.common.alertsMap = _.reduce(data,
                  function (alertsMap, alertInfo) {
                    if(!alertsMap[alertInfo.pipelineName]) {
                      alertsMap[alertInfo.pipelineName] = [];
                    }
                    alertsMap[alertInfo.pipelineName].push(alertInfo);
                    return alertsMap;
                  },
                  {}
                );

                refreshAlerts();
              })
              .catch(function(response) {
                $rootScope.common.errors = [response.data];
              });
          },
          function() {
            //console.log( "Timer rejected!" );
          }
        );
      }
    };

    $rootScope.$on('$destroy', function() {
      if(isWebSocketSupported) {
        if(statusWebSocket) {
          statusWebSocket.close();
        }
        if(alertsWebSocket) {
          alertsWebSocket.close();
        }
      } else {
        $timeout.cancel(pipelineStatusTimer);
        $timeout.cancel(alertsTimer);
      }

      destroyed = true;
    });

    var unloadMessage = 'If you leave this page you are going to lose all unsaved changes, are you sure you want to leave?';

    $translate('global.messages.info.unloadMessage').then(function(translation) {
      unloadMessage = translation;
    });

    window.onbeforeunload = function (event) {
      //Check if there was any change, if no changes, then simply let the user leave

      if($rootScope.common.saveOperationInProgress <= 0){
        return;
      }

      if (typeof event === 'undefined') {
        event = window.event;
      }
      if (event) {
        event.returnValue = unloadMessage;
      }
      return unloadMessage;
    };

  });
