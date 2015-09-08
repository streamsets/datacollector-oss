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
 * SDC RESTful API module.
 */

angular
  .module('dataCollectorApp.restapi')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/restapi',
      {
        templateUrl: 'app/help/restapi/restapi.tpl.html',
        controller: 'RESTfulAPIController',
        resolve: {
          myVar: function(authService) {
            return authService.init();
          }
        },
        data: {
          authorizedRoles: ['admin', 'creator', 'manager', 'guest']
        },
        reloadOnSearch: false
      }
    );
  }])
  .controller('RESTfulAPIController', function ($scope, $rootScope, $q, configuration, Analytics, $http, _) {
    $scope.swaggerURL = 'rest/swagger.json';

    $q.all([
      configuration.init()
    ]).then(function(results) {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/restapi');
      }
    });
  });