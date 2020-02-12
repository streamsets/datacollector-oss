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
 * Controller for Library Pane Share Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('ShareModalInstanceController', function (
    $rootScope, $scope, $modalInstance, $translate, $q, api, authService, configuration, pipelineInfo
  ) {
    var userList = [];
    var groupList = [];

    angular.extend($scope, {
      pipelineInfo: pipelineInfo,
      common: {
        errors: []
      },
      loading:true,
      newSubjectList: {
        value: []
      },
      filteredSubjects: [],
      isACLReadyOnly: true,
      isACLEnabled: configuration.isACLEnabled(),

      save : function () {
        api.acl.savePipelineAcl(pipelineInfo.pipelineId, $scope.acl)
          .then(
            function(res) {
              $modalInstance.close(res.data);
            },
            function(res) {
              $scope.common.errors = [res.data];
            }
          );
      },

      cancel : function () {
        $modalInstance.dismiss('cancel');
      },

      inviteOthers: function () {
        if ($scope.newSubjectList.value && $scope.newSubjectList.value.length) {
          angular.forEach($scope.newSubjectList.value, function(newSubject) {
            var subjectType = 'USER';
            if (groupList.indexOf(newSubject) !== -1) {
              subjectType = 'GROUP';
            }
            $scope.acl.permissions.push({
              subjectId: newSubject,
              subjectType: subjectType,
              actions: ['READ', 'WRITE', 'EXECUTE']
            });
          });
          $scope.newSubjectList.value = [];
          filterUsersAndGroups();
        }
      },

      groupSubjectsFn: function(item) {
        if (groupList.indexOf(item) !== -1) {
          return 'Groups';
        }
        return 'Users';
      },

      onActionToggle: function (permission, action, $event) {
        var checkbox = $event.target;
        if (checkbox.checked && permission.actions.indexOf(action) === -1) {
          permission.actions.push(action);
        } else {
          permission.actions = _.filter(permission.actions, function(act) {
            return act !== action;
          });
        }
      },

      removePermission: function (permission, index) {
        $scope.acl.permissions.splice(index, 1);
        filterUsersAndGroups();
      },

      changeOwner: function (permission) {
        $scope.acl.resourceOwner = permission.subjectId;
      }
    });

    var fetchAcl = function() {
      api.acl.getPipelineConfigAcl(pipelineInfo.pipelineId)
        .then(
          function(res) {
            $scope.acl = res.data;
            if ($rootScope.common.isUserAdmin || $rootScope.common.userName === $scope.acl.resourceOwner) {
              $scope.isACLReadyOnly = false;

              if (configuration.isDPMEnabled()) {
                fetchRemoteUsersAndGroups();
              } else {
                fetchUsersAndGroups();
              }
            }
          },
          function(res) {
            $scope.common.errors = [res.data];
          }
        );
    };

    var fetchUsersAndGroups = function() {
      $scope.fetching = true;

      $q.all([
        api.admin.getGroups(),
        api.admin.getUsers()
      ]).then(
        function (results) {
          $scope.fetching = false;
          groupList = results[0].data;
          userList = [];
          angular.forEach(results[1].data, function (user) {
            if (!user.roles || user.roles.indexOf('admin') === -1) {
              userList.push(user.name);
            }
          });
          userList.sort();
          filterUsersAndGroups();
        },
        function (res) {
          $rootScope.common.errors = [res.data];
        }
      );
    };

    var fetchRemoteUsersAndGroups = function () {
      $scope.fetching = true;

      $q.all([
        api.controlHub.getRemoteGroups(0, 50),
        api.controlHub.getRemoteUsers(0, 50)
      ]).then(
        function (results) {
          $scope.fetching = false;
          groupList = _.pluck(results[0].data, 'id');
          userList = [];
          angular.forEach(results[1].data, function (user) {
            if (!user.roles || user.roles.indexOf('datacollector:admin') === -1) {
              userList.push(user.id);
            }
          });
          filterUsersAndGroups();
        },
        function (res) {
          $rootScope.common.errors = [res.data];
        }
      );
    };

    var filterUsersAndGroups = function () {
      var alreadyAddedSubjects = [];
      if ($scope.acl && $scope.acl.permissions) {
        angular.forEach($scope.acl.permissions, function(permission) {
          alreadyAddedSubjects.push(permission.subjectId);
        });
      }
      var filteredSubjects = [];
      angular.forEach(groupList.concat(userList), function(subject) {
        if(alreadyAddedSubjects.indexOf(subject) === -1 && filteredSubjects.indexOf(subject) === -1) {
          filteredSubjects.push(subject);
        }
      });

      $scope.filteredSubjects = filteredSubjects;
    };

    fetchAcl();

  });
