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
  .controller('UpdatePermissionsInstanceController', function (
    $rootScope, $scope, $modalInstance, $translate, $q, $timeout, api, authService, configuration, pipelineService
  ) {
    var userList = [];
    var groupList = [];

    angular.extend($scope, {
      common: {
        errors: []
      },
      loading:true,
      currentSubjects: [],
      userList: [],
      groupList: [],
      subjectMapping: {
        value: []
      },
      bulkEdit: false,
      operationStatus: '',

      getCodeMirrorOptions: function(options) {
        return angular.extend({}, pipelineService.getDefaultELEditorOptions(), options);
      },

      addToMap: function() {
        var firstUser = $scope.userList.length ? $scope.userList[0] : '';
        $scope.subjectMapping.value.push({
          from: '',
          to: firstUser
        });
        $scope.refreshCodemirror = true;
        $timeout(function () {
          $scope.refreshCodemirror = false;
        }, 100);
      },

      removeFromMap: function (index) {
        $scope.subjectMapping.value.splice(index, 1);

        $scope.refreshCodemirror = true;
        $timeout(function () {
          $scope.refreshCodemirror = false;
        }, 100);
      },

      updatePermissions : function () {
        var mapping = {};
        var isValid = true;

        angular.forEach($scope.subjectMapping.value, function (mapObject) {
          if (mapObject.from &&  mapObject.to) {
            mapping[mapObject.from] = mapObject.to;
          } else {
            isValid = false;
          }
        });

        if (!isValid) {
          $scope.common.errors =['Invalid User Mapping'];
          return;
        }

        $scope.operationStatus = 'Updating';
        api.acl.updateSubjects(mapping)
          .then(
            function(res) {
              $scope.operationStatus = 'Completed';
            },
            function(res) {
              $scope.common.errors = [res.data];
              $scope.operationStatus = 'Error';
            }
          );
      },

      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

    var getSubjects = function() {
      api.acl.getSubjects()
        .then(
          function (res) {
            $scope.fetching = false;
            $scope.currentSubjects = res.data;

            var firstGroup = $scope.groupList.length ? $scope.groupList[0] : '';
            angular.forEach($scope.currentSubjects.groups, function (currentSubject) {
              if ($scope.groupList.indexOf(currentSubject) === -1) {
                // If Current Subject is not part current list of groups
                $scope.subjectMapping.value.push({
                  from: currentSubject,
                  to: firstGroup
                });
              }
            });

            var firstUser = $scope.userList.length ? $scope.userList[0] : '';
            angular.forEach($scope.currentSubjects.users, function (currentSubject) {
              if ($scope.userList.indexOf(currentSubject) === -1) {
                // If Current Subject is not part current list of users
                $scope.subjectMapping.value.push({
                  from: currentSubject,
                  to: firstUser
                });
              }
            });

          },
          function (res) {
            $rootScope.common.errors = [res.data];
          }
        );
    };

    var fetchLocalUsersAndGroups = function() {
      $scope.fetching = true;

      $q.all([
        api.admin.getGroups(),
        api.admin.getUsers()
      ]).then(
        function (results) {
          groupList = results[0].data;
          userList = [];
          angular.forEach(results[1].data, function (user) {
            userList.push(user.name);
          });
          userList.sort();

          $scope.groupList = groupList;
          $scope.userList = userList;
          getSubjects();
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
          groupList = _.pluck(results[0].data, 'id');
          userList = [];
          angular.forEach(results[1].data, function (user) {
            userList.push(user.id);
          });

          $scope.groupList = groupList;
          $scope.userList = userList;
          getSubjects();
        },
        function (res) {
          $rootScope.common.errors = [res.data];
        }
      );
    };

    if (configuration.isDPMEnabled()) {
      fetchRemoteUsersAndGroups();
    } else {
      fetchLocalUsersAndGroups();
    }

  });
