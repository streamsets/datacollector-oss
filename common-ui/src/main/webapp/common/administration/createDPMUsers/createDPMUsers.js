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
 * Controller for Creating Control Hub Users Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('CreateDPMUsersModalInstanceController', function (
    $rootScope, $scope, $modalInstance, $modalStack, $modal, api, authService, configuration, pipelineService,
    dpmInfoModel
  ) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      isRemoteUserOrgAdmin: authService.isRemoteUserOrgAdmin(),
      dpmInfoModel: dpmInfoModel,
      isCreateInProgress: false,
      createResponse: undefined,
      isRestartInProgress: false,

      sdcUserList: [],
      sdcGroupList: [],

      onCreateSubjectsSubmit: function() {
        $scope.common.errors = [];
        // $scope.isCreateInProgress = true;

        var orgId = $scope.dpmInfoModel.userID.split('@')[1];
        angular.forEach($scope.dpmGroupList, function (group) {
          group.organization = orgId;
        });
        angular.forEach($scope.dpmUserList, function (user) {
          user.organization = orgId;
        });

        $scope.dpmInfoModel.organization = orgId;
        $scope.dpmInfoModel.dpmUserList = $scope.dpmUserList;
        $scope.dpmInfoModel.dpmGroupList = $scope.dpmGroupList;

        api.admin.createDPMGroupsAndUsers($scope.dpmInfoModel)
          .then(function(res) {
            $scope.isCreateInProgress = false;
            $scope.createResponse = res.data;
          })
          .catch(function(res) {
            $scope.isCreateInProgress = false;
            $scope.common.errors = [res.data];
          });
      },

      restart: function() {
        $scope.isRestartInProgress = true;
        api.admin.restartDataCollector();
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      },

      removeFromList: function(list, $index) {
        list.splice($index, 1);
      }
    });

    var sdcToDPMRolesMapping = {
      admin: 'datacollector:admin',
      creator: 'datacollector:creator',
      manager: 'datacollector:manager',
      guest: 'datacollector:guest'
    };


    var getUsers = function() {
      $scope.fetching = true;
      $scope.userList = [];
      api.admin.getUsers()
        .then(
          function (res) {
            $scope.fetching = false;
            var sdcUserList = _.sortBy(res.data, 'name');
            var sdcGroupList = [];
            var dpmUserList = [];
            var dpmGroupList = [];
            var orgId = '';
            if ($scope.dpmInfoModel && $scope.dpmInfoModel.userID) {
              orgId = $scope.dpmInfoModel.userID.split('@')[1];
            }
            angular.forEach(sdcUserList, function (sdcUser) {
              var groups = [];
              var dpmRoles = [
                'user',
                'org-user',
                'pipelinestore:pipelineEditor',
                'jobrunner:operator',
                'timeseries:reader',
                'topology:editor',
                'notification:user',
                'sla:editor'
              ];

              if (sdcUser.roles) {
                angular.forEach(sdcUser.roles, function(role) {
                  if (sdcToDPMRolesMapping[role]) {
                    dpmRoles.push(sdcToDPMRolesMapping[role]);
                  }
                });
              }

              if (sdcUser.groups && sdcUser.groups.length) {
                angular.forEach(sdcUser.groups, function(group) {
                  if (sdcGroupList.indexOf(group) === -1 && group !== 'all') {
                    sdcGroupList.push(group);
                    dpmGroupList.push({
                      id: group + '@' + orgId,
                      organization: orgId,
                      name: group,
                      roles: [
                        'user'
                      ]
                    });
                  }

                  if (group != 'all') {
                    groups.push(group + '@' + orgId);
                  }
                });
              }


              dpmUserList.push({
                id: sdcUser.name + '@' + orgId,
                organization: orgId,
                name: sdcUser.name,
                email: sdcUser.name + '@' + orgId,
                roles: dpmRoles,
                groups: groups,
                active: true
              });

            });

            $scope.dpmUserList = dpmUserList;
            $scope.sdcUserList = sdcUserList;
            $scope.dpmGroupList = dpmGroupList;
            $scope.sdcGroupList = sdcGroupList;
          },
          function (res) {
            $rootScope.common.errors = [res.data];
          }
        );
    };

    getUsers();


  });
