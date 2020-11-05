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
/**
 * Controller for Missing Stage Library Install Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('InstallMissingLibraryModalInstanceController',
      function ($scope, $rootScope, $modalInstance, missingStageInfo, libraryList, withStageLibVersion, api,
                pipelineConstant, $modal, authService) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      missingStageInfo: _.clone(missingStageInfo),
      libraryList: _.clone(libraryList),
      maprStageLib: false,
      operationStatus: 'incomplete',
      failedLibraries: [],
      errorRes: undefined,
      registrationNeeded: false,
      selectedLibraryModel: {
        libraryId: missingStageInfo.libraryList[0].libraryId
      },

      install: function() {
        var libraryInfo = _.find(missingStageInfo.libraryList, function (l) {
          return l.libraryId === $scope.selectedLibraryModel.libraryId;
        });

        $scope.operationStatus = 'installing';
        var stageLibIdList = [$scope.selectedLibraryModel.libraryId];
        api.pipelineAgent.installLibraries(stageLibIdList, withStageLibVersion)
            .then(function () {
              $rootScope.common.trackEvent(
                  pipelineConstant.STAGE_LIBRARY_CATEGORY,
                  pipelineConstant.INSTALL_ACTION,
                  libraryInfo.libraryLabel,
                  1
              );
              $scope.operationStatus = 'complete';
            })
            .catch(function (res) {
              if (res.data) {
                $scope.common.errors = [res.data];
                $scope.operationStatus = 'failed';
              }
            }
          );
      },

      retry: function() {
        var libraries = $scope.failedLibraries;

        $scope.errorMap = {};
        $scope.common.errors = [];
        $scope.failedLibraries = [];

        $scope.install(libraries);
      },

      restart: function() {
        $scope.operationStatus = 'restarting';
        api.admin.restartDataCollector();
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      },

      inStatus: function(library, status) {
        return $scope.operationStatusMap[library.stageLibraryManifest.stageLibId] === status;
      },

      showError: function(library) {
        var err = $scope.errorMap[library.stageLibraryManifest.stageLibId];

        // If we know how to properly display the error
        if(err.RemoteException) {
          $modal.open({
            templateUrl: 'errorModalContent.html',
            controller: 'ErrorModalInstanceController',
            size: 'lg',
            backdrop: true,
            resolve: {
              errorObj: function () {
                return err;
              }
            }
          });
        } else {
          $scope.common.errors = [err];
        }
      },

      hasError: function(library) {
        return !!$scope.errorMap[library.stageLibraryManifest.stageLibId];
      },

      hasErrors: function() {
        return _.any($scope.errorMap);
      },

      register: function() {
        $modalInstance.dismiss('cancel');
        $rootScope.common.showRegistrationModal();
      }
    });

    if (libraryList && libraryList.length) {
      angular.forEach(libraryList, function(library) {
        if (library.stageLibraryManifest.stageLibId.indexOf('streamsets-datacollector-mapr_') !== -1) {
          $scope.maprStageLib = true;
        }
      });
    }

    if ($rootScope.common.activationInfo) {
      var activationInfo = $rootScope.common.activationInfo;
      if (activationInfo.info && activationInfo.enabled) {
        var difDays = authService.daysUntilProductExpiration(activationInfo.info.expiration);
        if (activationInfo.info.valid && difDays < 0) {
          // registration will be needed after any new stages are installed
          $scope.registrationNeeded = true;
        }
      }
    }
  });
