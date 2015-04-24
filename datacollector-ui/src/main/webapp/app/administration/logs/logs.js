/**
 * Logs module for displaying logs page content.
 */

angular
  .module('dataCollectorApp.logs')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/logs',
      {
        templateUrl: 'app/administration/logs/logs.tpl.html',
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
  .controller('LogsController', function ($scope, $interval, api, configuration, Analytics) {

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

    var loc = window.location,
      webSocketLogURL = ((loc.protocol === "https:") ?
          "wss://" : "ws://") + loc.hostname + (((loc.port != 80) && (loc.port != 443)) ? ":" + loc.port : "") +
        '/rest/v1/webSocket?type=log',
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