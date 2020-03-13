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
    const userRolesMap = {
      admin: 'Admin',
      manager: 'Manager',
      creator: 'Creator',
      guest: 'Guest'
    };
    angular.extend($scope, {
      fetching: false,
      userList: [],

      getUserIdx: userName => $scope.userList.findIndex(user => user.id === userName),

      getUserByIdx: idx => $scope.userList[idx],

      getRoles: user => user.roles.map(role => userRolesMap[role]).join(', '),

      showError: message => {
        $rootScope.common.errors = [message];
      },

      openUserModal: (user, isNew) => $modal.open({
        templateUrl: 'app/home/usersAndGroups/userDetails/user.tpl.html',
        controller: 'UserModalController',
        backdrop: 'static',
        resolve: {
          user: () => user,
          isNew: () => isNew,
          groupsList: () => $scope.groupList
        }
      }),

      onAddUserClick: function() {
        const modalInstance = this.openUserModal({}, true);
        modalInstance.result.then(newUser => {
          getUsersAndGroups();
        }, function () {
        });
      },

      onEditUserClick: function(userName) {
        const idx = this.getUserIdx(userName);
        if(idx > -1) {
          const user = this.getUserByIdx(idx);
          const modalInstance = this.openUserModal(user, false);
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
        const modalInstance = $modal.open({
          templateUrl: 'app/home/usersAndGroups/delete/delete.tpl.html',
          controller: 'DeleteModalController',
          backdrop: 'static',
          resolve: {
            userName: () => userName,
          }
        });
        modalInstance.result.then(() => {
          const idx = this.getUserIdx(userName);
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
          $scope.userList = res[1].data.data.sort((a, b) => a.id.localeCompare(b.id));
          $scope.userList.forEach(user => user.groups = user.groups.sort());
        },
        function (res) {
          $rootScope.common.errors = [res.data];
        }
      );
    };

    $rootScope.common.title = "Users & Groups";
    getUsersAndGroups();

  });
