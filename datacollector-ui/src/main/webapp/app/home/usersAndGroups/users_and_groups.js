/*
 * Copyright 2020 StreamSets Inc.
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
  .controller('UsersAndGroupsController', function ($scope, $rootScope, api, $modal, $q) {
    var userRolesMap = {
      admin: 'Admin',
      manager: 'Manager',
      creator: 'Creator',
      guest: 'Guest'
    };
    angular.extend($scope, {
      fetching: false,
      userList: [],

      getUserIdx: function(userName) {
        return $scope.userList.findIndex(function(user) {
          return user.id === userName;
        });
      },

      getUserByIdx: function(idx) {return $scope.userList[idx];},

      getRoles: function(user) {
        return user.roles.map(function(role) {
          return userRolesMap[role];
        }).join(', ');
      },

      showError: function(message) {
        $rootScope.common.errors = [message];
      },

      openUserModal: function(user, isNew) {
        return $modal.open({
          templateUrl: 'app/home/usersAndGroups/userDetails/user.tpl.html',
          controller: 'UserModalController',
          backdrop: 'static',
          resolve: {
            user: function() {return user;},
            isNew: function() {return isNew;},
            groupsList: function() {return $scope.groupList;}
          }
        });
      },

      onAddUserClick: function() {
        var modalInstance = this.openUserModal({}, true);
        modalInstance.result.then(function(newUser) {
          getUsersAndGroups();
        }, function () {
        });
      },

      onEditUserClick: function(userName) {
        var idx = this.getUserIdx(userName);
        if(idx > -1) {
          var user = this.getUserByIdx(idx);
          var modalInstance = this.openUserModal(user, false);
          modalInstance.result.then(function(editedUser) {
            $scope.userList[idx] = editedUser;
            getUsersAndGroups();
          }, function () {
          });

        } else {
          this.showError('User not found "' + userName + '".');
        }
      },

      onDeleteUserClick: function(userName) {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/usersAndGroups/delete/delete.tpl.html',
          controller: 'DeleteModalController',
          backdrop: 'static',
          resolve: {
            userName: function() {return userName;},
          }
        });
        modalInstance.result.then(function() {
          var idx = this.getUserIdx(userName);
          $scope.userList.splice(idx, 1);
          //getUsersAndGroups();
        }, function () {
        });
      },

    });

    var getUsersAndGroups = function() {
      $scope.fetching = true;
      $q.all([
        api.admin.getGroups(),
        api.admin.getUsers2()
      ]).then(
        function (res) {
          $scope.fetching = false;
          // - list of groups
          $scope.groupList = res[0].data;
          // - list of users
          $scope.userList = res[1].data.data.sort(function(a, b) {return a.id.localeCompare(b.id);});
          $scope.userList.forEach(function(user) {user.groups = user.groups.sort();});
        },
        function (res) {
          $rootScope.common.errors = [res.data];
        }
      );
    };

    $rootScope.common.title = "Users & Groups";
    getUsersAndGroups();

  });
