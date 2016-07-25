/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
    $routeProvider.when('/collector/logs',
      {
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
      }
    );
  }])
  .controller('LogsController', function ($rootScope, $scope, $interval, api, configuration, Analytics,
                                          pipelineService, $timeout) {

    var webSocketLogURL = $rootScope.common.webSocketBaseURL + 'rest/v1/webSocket?type=log',
        logWebSocket,
        logWebSocketMessages = [],
        lastMessageFiltered = false;

    configuration.init().then(function() {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/logs');
      }
    });

    angular.extend($scope, {
      logMessages: [],
      logEndingOffset: -1,
      logFiles: [],
      fetchingLog: false,
      extraMessage: undefined,
      filterSeverity: undefined,
      filterPipeline: undefined,
      pipelines: [],

      loadPreviousLog: function() {
        $scope.fetchingLog = true;
        api.log.getCurrentLog($scope.logEndingOffset, $scope.extraMessage, $scope.filterPipeline,
            $scope.filterSeverity).then(function(res) {

          if(res.data && res.data.length > 0) {
            //check if first message is extra line
            if(!res.data[0].timeStamp && res.data[0].exception) {
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
        if($scope.filterSeverity != severity) {
          $scope.filterSeverity = severity;
          $scope.logEndingOffset = -1;
          $scope.extraMessage = '';
          $scope.logMessages = [];
          lastMessageFiltered = false;
          $scope.loadPreviousLog();
        }
      },

      pipelineFilterChanged: function(pipeline) {
        if($scope.filterPipeline != pipeline) {
          $scope.filterPipeline = pipeline;
          $scope.logEndingOffset = -1;
          $scope.extraMessage = '';
          $scope.logMessages = [];
          lastMessageFiltered = false;
          $scope.loadPreviousLog();
        }
      }
    });

    api.log.getCurrentLog($scope.logEndingOffset).then(function(res) {
      //check if first message is extra line
      if(res.data && res.data.length > 0 && !res.data[0].timeStamp && res.data[0].exception) {
        $scope.extraMessage = res.data[0].exception;
        res.data.shift();
      }

      $scope.logMessages = res.data;
      $scope.logEndingOffset = +res.headers('X-SDC-LOG-PREVIOUS-OFFSET');

      logWebSocket = new WebSocket(webSocketLogURL);

      logWebSocket.onmessage = function (evt) {
        var received_msg = JSON.parse(evt.data);
        logWebSocketMessages.push(received_msg);
      };

    });

    var intervalPromise = $interval(function() {
      if(logWebSocketMessages && logWebSocketMessages.length) {

        angular.forEach(logWebSocketMessages, function(logWebSocketMessage) {
          if(!logWebSocketMessage.exceptionMessagePart) {

            if($scope.filterSeverity && $scope.filterSeverity != logWebSocketMessage.severity) {
              lastMessageFiltered = true;
              return;
            }

            if($scope.filterPipeline && $scope.filterPipeline != logWebSocketMessage['s-entity']) {
              lastMessageFiltered = true;
              return;
            }

            lastMessageFiltered = false;
            $scope.logMessages.push(logWebSocketMessage);

          } else if(!lastMessageFiltered){
            var lastMessage = $scope.logMessages[$scope.logMessages.length - 1];
            if(!lastMessage.exception) {
              lastMessage.exception = logWebSocketMessage.exceptionMessagePart;
            } else {
              lastMessage.exception += '\n' +   logWebSocketMessage.exceptionMessagePart;
            }
          }
        });

        logWebSocketMessages = [];
      }

    }, 2000);

    api.log.getFilesList().then(function(res) {
      $scope.logFiles = res.data;
    });

    pipelineService.init().then(function() {
      $scope.pipelines = pipelineService.getPipelines();
    });


    $scope.$on('$destroy', function() {
      if(angular.isDefined(intervalPromise)) {
        $interval.cancel(intervalPromise);
      }

      logWebSocket.close();
    });

    $timeout(function() {
      var $panelBody = $('.logs-page > .panel-body');
      $panelBody.scrollTop($panelBody[0].scrollHeight);
    }, 1000);
  });
