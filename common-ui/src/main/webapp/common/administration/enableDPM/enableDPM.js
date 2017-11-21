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
 * Controller for Enable SCH Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('EnableDPMModalInstanceController', function (
    $rootScope, $scope, $modalInstance, $modalStack, $modal, api, authService, configuration, pipelineService
  ) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      isRemoteUserOrgAdmin: authService.isRemoteUserOrgAdmin(),
      dpmInfoModel: {
        baseURL: 'https://cloud.streamsets.com',
        userID: '',
        userPassword: '',
        labels: ['label1', 'label2']
      },
      isEnableInProgress: false,
      dpmEnabled: false,
      isRestartInProgress: false,
      isStatsLibraryInstalled: true,

      onEnableDPMSubmit: function() {
        $scope.common.errors = [];
        $scope.isEnableInProgress = true;
        $scope.dpmInfoModel.organization = $scope.dpmInfoModel.userID.split('@')[1];
        api.admin.enableDPM($scope.dpmInfoModel)
          .then(function() {
            $scope.isEnableInProgress = false;
            $scope.dpmEnabled = true;
          })
          .catch(function(res) {
            $scope.isEnableInProgress = false;
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

      /**
       * Callback function on clicking install Statistics library link
       */
      onInstallStatisticsLibraryClick: function() {
        $modalStack.dismissAll();
        $modal.open({
          templateUrl: 'app/home/packageManager/install/install.tpl.html',
          controller: 'InstallModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            customRepoUrl: function () {
              return $rootScope.$storage.customPackageManagerRepoUrl;
            },
            libraryList: function () {
              return [{
                id: 'streamsets-datacollector-stats-lib',
                label: 'Statistics'
              }];
            }
          }
        });
      },

      onCreateDPMUsersClick: function () {
        $modalStack.dismissAll();
        $modal.open({
          templateUrl: 'common/administration/createDPMUsers/createDPMUsers.tpl.html',
          controller: 'CreateDPMUsersModalInstanceController',
          size: 'lg',
          backdrop: 'static',
          resolve: {
            dpmInfoModel: function () {
              return $scope.dpmInfoModel;
            }
          }
        });
      }
    });

    var currentDPMLabels = configuration.getDPMLabels();
    if (currentDPMLabels && currentDPMLabels.length) {
      $scope.dpmInfoModel.labels = currentDPMLabels;
    }

    $scope.isStatsLibraryInstalled = pipelineService.isDPMStatisticsLibraryInstalled();

  });
