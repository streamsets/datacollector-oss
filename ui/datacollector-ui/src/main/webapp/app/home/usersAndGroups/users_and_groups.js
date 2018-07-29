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
 * Package Manager module for displaying Package Manager content.
 */

angular
  .module('dataCollectorApp.home')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/collector/usersAndGroups', {
        templateUrl: 'app/home/usersAndGroups/users_and_groups.tpl.html',
        controller: 'UsersAndGroupsController',
        resolve: {
          myVar: function(authService) {
            return authService.init();
          }
        },
        data: {
          authorizedRoles: ['admin']
        }
      });
  }])
  .controller('UsersAndGroupsController', function ($scope, api) {
    angular.extend($scope, {
      fetching: false,
      userList: []
    });

    var getUsers = function() {
      $scope.fetching = true;
      $scope.userList = [];
      api.admin.getUsers()
        .then(
          function (res) {
            $scope.fetching = false;
            $scope.userList = _.sortBy(res.data, 'name');
          },
          function (res) {
            $rootScope.common.errors = [res.data];
          }
        );
    };

    getUsers();

  });
