angular.module('dataCollectorApp')
  .config(function($routeProvider, $locationProvider, $translateProvider, tmhDynamicLocaleProvider,
                   uiSelectConfig, $httpProvider, AnalyticsProvider){
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

    // Initialize angular-translate
    $translateProvider.useStaticFilesLoader({
      prefix: '/i18n/',
      suffix: '.json'
    });

    $translateProvider.preferredLanguage('en');

    $translateProvider.useCookieStorage();

    tmhDynamicLocaleProvider.localeLocationPattern('bower_components/angular-i18n/angular-locale_{{locale}}.js');
    tmhDynamicLocaleProvider.useCookieStorage('NG_TRANSLATE_LANG_KEY');

    uiSelectConfig.theme = 'bootstrap';

    //Reload the page when the server is down.
    $httpProvider.interceptors.push(function($q) {
      return {
        responseError: function(rejection) {
          console.log(rejection);
          if(rejection.status === 0) {
            window.location.reload();
            return;
          }
          return $q.reject(rejection);
        }
      };
    });

    AnalyticsProvider.setAccount('UA-60917135-1');
    AnalyticsProvider.trackPages(true);
    AnalyticsProvider.trackUrlParams(true);
    AnalyticsProvider.setDomainName('none');
    AnalyticsProvider.useAnalytics(true);
    AnalyticsProvider.delayScriptTag(true);

  })
  .run(function ($location, $rootScope, $modal, api, pipelineConstant, $localStorage, contextHelpService,
                 $timeout, $translate, authService, userRoles, configuration, Analytics, $q) {
    var defaultTitle = 'StreamSets Data Collector',
      pipelineStatusTimer,
      alertsTimer,
      isWebSocketSupported,
      loc = window.location,
      webSocketBaseURL = ((loc.protocol === "https:") ?
          "wss://" : "ws://") + loc.hostname + (((loc.port != 80) && (loc.port != 443)) ? ":" + loc.port : ""),
      BACKSPACE_KEY = 8,
      DELETE_KEY = 46,
      Z_KEY = 90,
      Y_KEY = 89,
      destroyed = false,
      webSocketStatusURL = webSocketBaseURL + '/rest/v1/webSocket?type=status',
      statusWebSocket,
      webSocketAlertsURL = webSocketBaseURL + '/rest/v1/webSocket?type=alerts',
      alertsWebSocket;

    $rootScope.pipelineConstant = pipelineConstant;
    $rootScope.$storage = $localStorage.$default({
      displayDensity: pipelineConstant.DENSITY_COMFORTABLE,
      helpLocation: pipelineConstant.LOCAL_HELP,
      readNotifications: []
    });

    $rootScope.common = $rootScope.common || {
        title : defaultTitle,
        userName: 'Account',
        authenticationType: 'none',
        apiVersion: api.apiVersion,
        sdcExecutionMode: pipelineConstant.STANDALONE,
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
         * Logout header link command handler
         */
        logout: function() {
          api.admin.logout()
            .success(function() {
              location.reload();
            })
            .error(function() {

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

        showSDCDirectories: function() {
          $modal.open({
            templateUrl: 'common/administration/sdcDirectories/sdcDirectoriesModal.tpl.html',
            controller: 'SDCDirectoriesModalInstanceController',
            size: '',
            backdrop: true
          });
        },

        /**
         * Return logs collected from Log WebSocket
         * @returns {string}
         */
        getLogMessages: function() {
          return logMessages.join('\n');
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

          //var edges = $scope.edges,
            //edge;

          //$scope.$storage.maximizeDetailPane = false;
          //$scope.$storage.minimizeDetailPane = false;

          if(alert.ruleDefinition.metricId) {
            //Select Pipeline Config
            /*
            $scope.$broadcast('selectNode');
            $scope.changeStageSelection({
              selectedObject: undefined,
              type: pipelineConstant.PIPELINE
            });*/
            $location.path('/collector/pipeline/' + alert.pipelineName);
          } else {
            //Select edge
            /*edge = _.find(edges, function(ed) {
              return ed.outputLane === alert.rule.lane;
            });

            $scope.changeStageSelection({
              selectedObject: edge,
              type: pipelineConstant.LINK
            });*/

            $location.path('/collector/pipeline/' + alert.pipelineName);
          }
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
            .success(function() {

            })
            .error(function(data, status, headers, config) {
              $rootScope.common.errors = [data];
            });
        }

      };

    var logMessages = [];

    authService.init().then(function() {
      $rootScope.common.userName = authService.getUserName();
      $rootScope.common.userRoles = authService.getUserRoles().join(', ');
      $rootScope.userRoles = userRoles;
      $rootScope.isAuthorized = authService.isAuthorized;
    });

    $q.all([api.pipelineAgent.getAllAlerts(), configuration.init()])
      .then(function(results) {
        $rootScope.common.authenticationType = configuration.getAuthenticationType();
        $rootScope.common.sdcExecutionMode = configuration.getSDCExecutionMode();
        $rootScope.common.sdcClusterManagerURL = configuration.getSDCClusterManagerURL();
        $rootScope.common.isMetricsTimeSeriesEnabled = configuration.isMetricsTimeSeriesEnabled();
        if(configuration.isAnalyticsEnabled()) {
          Analytics.createAnalyticsScriptTag();
        }

        var alertsInfoList = results[0].data;
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


        isWebSocketSupported = (typeof(WebSocket) === "function") && configuration.isWebSocketUseEnabled();
        refreshPipelineStatus();

        if($rootScope.common.sdcExecutionMode !== pipelineConstant.CLUSTER) {
          refreshAlerts();
        }
      });

    // set actions to be taken each time the user navigates
    $rootScope.$on('$routeChangeSuccess', function (event, current, previous) {
      // set page title
      if(current.$$route && current.$$route.data) {
        var authorizedRoles = current.$$route.data.authorizedRoles;
        $rootScope.notAuthorized = !authService.isAuthorized(authorizedRoles);
      }

      //To fix NVD3 JS errors - https://github.com/novus/nvd3/pull/396
      //window.nv.charts = {};
      //window.nv.graphs = [];
      //window.nv.logs = {};
      //window.onresize = null;
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
            $rootScope.common.pipelineStatusMap[parsedStatus.name] = parsedStatus;

            if(parsedStatus.status !== 'RUNNING') {
              var alerts = $rootScope.common.alertsMap[parsedStatus.name];

              if(alerts) {
                delete $rootScope.common.alertsMap[parsedStatus.name];
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
          //On Close try calling REST API so that if server is down it will reload the page.
          api.pipelineAgent.getAllPipelineStatus();
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
              .success(function(data) {
                if(!_.isObject(data) && _.isString(data) && data.indexOf('<!doctype html>') !== -1) {
                  //Session invalidated
                  window.location.reload();
                  return;
                }

                $rootScope.common.pipelineStatusMap = data;

                refreshPipelineStatus();
              })
              .error(function(data, status, headers, config) {
                $rootScope.common.errors = [data];
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
                icon: '/assets/favicon.png'
              });

              notification.onclick = function() {
                notification.close();
                window.open('/collector/pipeline/' + alertInfo.pipelineName);
              };

            }
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
              .success(function(data) {
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
              .error(function(data, status, headers, config) {
                $rootScope.common.errors = [data];
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

      if (typeof event == 'undefined') {
        event = window.event;
      }
      if (event) {
        event.returnValue = unloadMessage;
      }
      return unloadMessage;
    };

  });