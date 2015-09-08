/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
  .module('dataCollectorApp.logs')
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
  .controller('LogsController', function ($rootScope, $scope, $interval, api, configuration, Analytics) {

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

      loadPreviousLog: function() {
        $scope.fetchingLog = true;
        api.log.getCurrentLog($scope.logEndingOffset).then(function(res) {
          $scope.logMessages[0] = res.data;
          $scope.logEndingOffset = +res.headers('X-SDC-LOG-PREVIOUS-OFFSET');

          if ($scope.logEndingOffset !== 0) {
            $scope.logMessages.unshift('.................................................................................................................................................\n');
          }

          $scope.fetchingLog = false;

        }, function() {
          $scope.fetchingLog = false;
        });
      }
    });

    var webSocketLogURL = $rootScope.common.webSocketBaseURL + 'rest/v1/webSocket?type=log',
      logWebSocket,
      logWebSocketMessages = [];


    api.log.getCurrentLog($scope.logEndingOffset).then(function(res) {
      $scope.logMessages.push(res.data);
      $scope.logEndingOffset = +res.headers('X-SDC-LOG-PREVIOUS-OFFSET');

      if ($scope.logEndingOffset !== 0) {
        $scope.logMessages.unshift('.................................................................................................................................................\n');
      }

      logWebSocket = new WebSocket(webSocketLogURL);

      logWebSocket.onmessage = function (evt) {
        var received_msg = evt.data;
        logWebSocketMessages.push(received_msg);
      };

    });

    var intervalPromise = $interval(function() {
      if(logWebSocketMessages && logWebSocketMessages.length) {
        if($scope.logMessages.length > 1000) {
          $scope.logMessages.shift();
        }
        $scope.logMessages.push(logWebSocketMessages.join('\n') + '\n');
        logWebSocketMessages = [];
      }

    }, 2000);

    api.log.getFilesList().then(function(res) {
      $scope.logFiles = res.data;
    });

    $scope.$on('$destroy', function() {
      if(angular.isDefined(intervalPromise)) {
        $interval.cancel(intervalPromise);
      }

      logWebSocket.close();
    });

  });