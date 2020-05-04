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
      canManageUsers: false, // TODO: FEATURE PLANNED FOR 3.17

      getUserIdx: function(userId) {
        return $scope.userList.findIndex(function(user) {
          return user.id === userId;
        });
      },

      getUserByIdx: function(idx) {
        return $scope.userList[idx];
      },

      getUserById: function(userId) {
        var idx = $scope.getUserIdx(userId);
        return $scope.getUserByIdx(idx);
      },

      getRoles: function(user) {
        return user.roles.map(function(role) {
          return userRolesMap[role];
        }).join(', ');
      },

      showError: function(message) {
        $rootScope.common.errors = [message];
      },

      openUserModal: function(user, isNew) {
        $scope.common.errors = [];
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

      isLastAdmin: function(userId) {
        var user = $scope.getUserById(userId);
        if(user.roles.indexOf('admin') > -1) {
          var adminCount = 0;
          $scope.userList.forEach(function(user) {
            if(user.roles.indexOf('admin') > -1) {
              adminCount++;
            }
          });
          return adminCount === 1;
        }
        return false;
      },

      onAddUserClick: function() {
        var modalInstance = $scope.openUserModal({}, true);
        modalInstance.result.then(function(newUser) {
          getUsersAndGroups();
        }, function () {
        });
      },

      onEditUserClick: function(userId) {
        var idx = $scope.getUserIdx(userId);
        if(idx > -1) {
          var user = $scope.getUserByIdx(idx);
          var modalInstance = $scope.openUserModal(user, false);
          modalInstance.result.then(function(editedUser) {
            $scope.userList[idx] = editedUser;
            getUsersAndGroups();
          }, function () {
          });
        } else {
          $scope.showError('User not found "' + userId + '".');
        }
      },

      onDeleteUserClick: function(userId) {
        var isLastAdmin = $scope.isLastAdmin(userId);
        var modalInstance = $modal.open({
          templateUrl: 'app/home/usersAndGroups/delete/delete.tpl.html',
          controller: 'DeleteModalController',
          backdrop: 'static',
          resolve: {
            userId: function() {return userId;},
            isLastAdmin: function() {return isLastAdmin;}
          }
        });
        modalInstance.result.then(function() {
          var idx = $scope.getUserIdx(userId);
          $scope.userList.splice(idx, 1);
          //getUsersAndGroups();
        }, function () {
        });
      }

    });

    var getUsersAndGroups = function() {
      $scope.fetching = true;
      $q.all([
        api.admin.getGroups(),
        api.admin[$scope.canManageUsers ? 'getUsers2' : 'getUsers' ]()
      ]).then(
        function (res) {
          $scope.fetching = false;
          // - list of groups
          $scope.groupList = res[0].data;
          // - list of users
          if($scope.canManageUsers){
            $scope.userList = res[1].data.data.sort(function(a, b) {return a.id.localeCompare(b.id);});
            $scope.userList.forEach(function(user) {user.groups = user.groups.sort();});
          }else{
            $scope.userList = res[1].data.sort(function(a, b) {return a.name.localeCompare(b.name);});
          }
        },
        function (res) {
          $rootScope.common.errors = [res.data];
        }
      );
    };

    $rootScope.common.title = "Users & Groups";
    getUsersAndGroups();

  });
