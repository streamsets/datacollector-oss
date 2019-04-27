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
 * Logs module for displaying logs page content.
 */

angular
  .module('commonUI.logs')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/collector/logs/:pipelineTitle/:pipelineName', {
        templateUrl: 'common/administration/logs/logs.tpl.html',
        controller: 'LogsController',
        resolve: {
          myVar: function(authService) {
            return authService.init();
          }
        },
        data: {
          authorizedRoles: ['admin', 'creator', 'manager']
        }
      })
      .when('/collector/logs', {
        templateUrl: 'common/administration/logs/logs.tpl.html',
        controller: 'LogsController',
        resolve: {
          myVar: function(authService) {
            return authService.init();
          }
        },
        data: {
          authorizedRoles: ['admin', 'creator', 'manager']
        }
      });
  }])
  .controller('LogsController', function (
    $rootScope, $scope, $routeParams, $interval, api, configuration, Analytics, $timeout, $modal
  ) {
    var pipelineNameParam = $routeParams.pipelineName;
    var pipelineTitleParam = $routeParams.pipelineTitle;
    var webSocketLogURL = $rootScope.common.webSocketBaseURL + 'rest/v1/webSocket?type=log';
    var logWebSocket;
    var logWebSocketMessages = [];
    var lastMessageFiltered = false;

    if (pipelineTitleParam) {
      $rootScope.common.title = pipelineTitleParam + " | logs";
    } else {
      $rootScope.common.title = "Data Collector logs";
    }

    configuration.init().then(function() {
      if (configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/logs');
      }
    });

    $rootScope.common.errors = [];

    angular.extend($scope, {
      logMessages: [],
      logEndingOffset: -1,
      logFiles: [],
      loading: true,
      fetchingLog: false,
      extraMessage: undefined,
      filterSeverity: undefined,
      filterPipeline: pipelineNameParam,
      filterPipelineLabel: pipelineTitleParam + '/' + pipelineNameParam,
      pipelines: [],
      pauseLogAutoFetch: $rootScope.$storage.pauseLogAutoFetch,

      loadPreviousLog: function() {
        $scope.fetchingLog = true;
        api.log.getCurrentLog($scope.logEndingOffset, $scope.extraMessage, $scope.filterPipeline,
            $scope.filterSeverity).then(function(res) {

          if (res.data && res.data.length > 0) {
            //check if first message is extra line
            if (!res.data[0].timeStamp && res.data[0].exception) {
              $scope.extraMessage = res.data[0].exception;
              res.data.shift();
            }
            $scope.logMessages = res.data.concat($scope.logMessages);
          }

          $scope.logEndingOffset = +res.headers('X-SDC-LOG-PREVIOUS-OFFSET');
          $scope.fetchingLog = false;
        }, function() {
          $scope.fetchingLog = false;
        });
      },

      severityFilterChanged: function(severity) {
        if ($scope.filterSeverity !== severity) {
          $scope.filterSeverity = severity;
          $scope.refreshLogs();
        }
      },

      pipelineFilterChanged: function(pipeline) {
        if (pipeline && $scope.filterPipeline !== pipeline.name) {
          $scope.filterPipeline = pipeline.name;
          $scope.filterPipelineLabel = pipeline.title + '/' + pipeline.name;
          $scope.refreshLogs();
        } else if (pipeline === undefined && $scope.filterPipeline !== undefined) {
          $scope.filterPipeline = undefined;
          $scope.filterPipelineLabel = 'All';
          $scope.refreshLogs();
        }
      },

      onLogConfigClick: function() {
        $modal.open({
          templateUrl: 'common/administration/logs/logConfig/logConfig.tpl.html',
          controller: 'LogConfigModalInstanceController',
          backdrop: 'static',
          size: 'lg'
        });
      },

      toggleAutoFetch: function () {
        $rootScope.$storage.pauseLogAutoFetch = $scope.pauseLogAutoFetch = !$scope.pauseLogAutoFetch;
        if ($scope.pauseLogAutoFetch) {
          if (logWebSocket) {
            logWebSocket.close();
          }
        } else {
          $scope.refreshLogs();
        }
      },

      refreshLogs: function () {
        $scope.logEndingOffset = -1;
        $scope.extraMessage = '';
        $scope.logMessages = [];
        lastMessageFiltered = false;
        refreshLogContents();
      }
    });

    var refreshLogContents = function () {
      if (logWebSocket) {
        logWebSocket.close();
      }

      if (!$rootScope.common.isUserAdmin && !$scope.filterPipeline) {
        if ($scope.pipelines.length) {
          $scope.filterPipeline = $scope.pipelines[0].name;
          $scope.filterPipelineLabel = $scope.pipelines[0].title + '/' + $scope.pipelines[0].name;
        } else {
          // For non admins - they can access only pipeline filtered logs
          $scope.logMessages = [];
          $scope.loading = false;
          return;
        }
      }

      $scope.loading = true;

      api.log.getCurrentLog(-1, $scope.extraMessage, $scope.filterPipeline, $scope.filterSeverity).then(
        function(res) {
          //check if first message is extra line
          if (res.data && res.data.length > 0 && !res.data[0].timeStamp && res.data[0].exception) {
            $scope.extraMessage = res.data[0].exception;
            res.data.shift();
          }

          $scope.logMessages = res.data;
          $scope.logEndingOffset = +res.headers('X-SDC-LOG-PREVIOUS-OFFSET');
          $scope.loading = false;

          $timeout(function() {
            var $panelBody = $('.logs-page > .panel-body');
            $panelBody.scrollTop($panelBody[0].scrollHeight);
          }, 500);

          if (!$scope.pauseLogAutoFetch && $rootScope.common.isUserAdmin) {
            logWebSocket = new WebSocket(webSocketLogURL);
            logWebSocket.onmessage = function (evt) {
              var received_msg = JSON.parse(evt.data);
              logWebSocketMessages.push(received_msg);
            };
          }
        },
        function (res) {
          $scope.loading = false;
          $rootScope.common.errors = [res.data];
        }
      );
    };

    var intervalPromise = $interval(function() {
      if (logWebSocketMessages && logWebSocketMessages.length) {

        angular.forEach(logWebSocketMessages, function(logWebSocketMessage) {
          if (!logWebSocketMessage.exceptionMessagePart) {

            if ($scope.filterSeverity && $scope.filterSeverity != logWebSocketMessage.severity) {
              lastMessageFiltered = true;
              return;
            }

            if ($scope.filterPipeline && $scope.filterPipelineLabel != logWebSocketMessage['s-entity']) {
              lastMessageFiltered = true;
              return;
            }

            lastMessageFiltered = false;

            $scope.logMessages.push(logWebSocketMessage);


            if ($scope.logMessages.length > 10) {
              $scope.logMessages.shift();
              $scope.logEndingOffset = -1;
            }

          } else if (!lastMessageFiltered){
            var lastMessage = $scope.logMessages[$scope.logMessages.length - 1];
            if (!lastMessage.exception) {
              lastMessage.exception = logWebSocketMessage.exceptionMessagePart;
            } else {
              lastMessage.exception += '\n' +   logWebSocketMessage.exceptionMessagePart;
            }
          }
        });

        logWebSocketMessages = [];
      }

    }, 2000);

    if ($rootScope.common.isUserAdmin) {
      api.log.getFilesList().then(
        function(res) {
          $scope.logFiles = res.data;
        },
        function (res) {
          $rootScope.common.errors = [res.data];
        }
      );
    }

    api.pipelineAgent.getPipelines(null, null, 0, 50, 'NAME', 'ASC', false).then(
      function (res) {
        $scope.pipelines = res.data;
        refreshLogContents();
      },
      function (res) {
        $rootScope.common.errors = [res.data];
      }
    );

    $scope.$on('$destroy', function() {
      if (angular.isDefined(intervalPromise)) {
        $interval.cancel(intervalPromise);
      }

      if (logWebSocket) {
        logWebSocket.close();
      }
    });
  });
