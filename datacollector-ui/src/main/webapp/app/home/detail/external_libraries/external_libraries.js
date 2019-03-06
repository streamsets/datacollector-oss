/*
 * Copyright 2018 StreamSets Inc.
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
  .controller('ExternalLibrariesController', function (
    $scope, $rootScope, $routeParams, $q, $modal, $location, pipelineService, api, configuration, pipelineConstant
  ) {
    $rootScope.common.errors = [];

    var pipelinesLimit = 60;

    angular.extend($scope, {
      uploadFile: {},
      stageLibrariesExtras: [],
      allSelected: false,
      isManagedByClouderaManager: false,
      fetching: true,
      header: {
        customRepoUrl: $rootScope.$storage.customPackageManagerRepoUrl,
        sortColumn: 'label',
        sortReverse: false,
        searchInput: ''
      },
      selectedStageLibraryList: [],
      selectedStageLibraryMap: {},

      /**
       * On Select All check box select
       */
      selectAll: function () {
        $scope.selectedStageLibraryList = [];
        $scope.selectedStageLibraryMap = {};
        var list = $scope.stageLibrariesExtras;
        angular.forEach(list, function(stageLibrary) {
          $scope.selectedStageLibraryList.push(stageLibrary.id);
          $scope.selectedStageLibraryMap[stageLibrary.id] = true;
        });
        $scope.allSelected = true;
      },

      /**
       * On UnSelect All check box select
       */
      unSelectAll: function () {
        $scope.selectedStageLibraryList = [];
        $scope.selectedStageLibraryMap = {};
        $scope.allSelected = false;
        $scope.limit = pipelinesLimit;
      },

      /**
       * On Selecting Individual stageLibrary checkbox
       * @param stageLibrary
       */
      selectStageLibrary: function(stageLibrary) {
        $scope.selectedStageLibraryMap[stageLibrary.id] = true;
        $scope.selectedStageLibraryList.push(stageLibrary.id);
      },

      /**
       * On UnSelecting Individual stageLibrary checkbox
       * @param stageLibrary
       */
      unSelectStageLibrary: function(stageLibrary) {
        $scope.selectedStageLibraryMap[stageLibrary.id] = false;
        var index = $scope.selectedStageLibraryList.indexOf(stageLibrary.id);
        if (index !== -1) {
          $scope.selectedStageLibraryList.splice(index, 1);
        }
        $scope.allSelected = false;
      },

      /**
       * On Clicking on Column Header
       * @param columnName
       */
      onSortColumnHeaderClick: function(columnName) {
        $scope.header.sortColumn = columnName;
        $scope.header.sortReverse = !$scope.header.sortReverse;
      },

      /**
       * Custom sort function for filtering Stage Libraries
       * @param stageLibrary
       * @returns {*}
       */
      customStageLibrarySortFunction: function (stageLibrary) {
        return stageLibrary[$scope.header.sortColumn];
      },

      /**
       * Callback function when Show more link clicked.
       *
       * @param $event
       */
      onShowMoreClick: function($event) {
        $event.preventDefault();
        $scope.limit += pipelinesLimit;
      },

      onUploadExtrasClick: function () {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Install Additional Drivers',
          1
        );

        var installedLibraries = [{
          stageLibraryManifest: {
            stageLibId: $scope.detailPaneConfigDefn.library,
            stageLibLabel: $scope.detailPaneConfigDefn.libraryLabel
          }
        }];

        var modalInstance = $modal.open({
          templateUrl: 'app/home/packageManager/upload_extras/uploadExtras.tpl.html',
          controller: 'UploadExtrasModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            installedLibraries: function () {
              return installedLibraries;
            }
          }
        });

        modalInstance.result.then(function() {
          getStageLibrariesExtras();
        }, function() {
          getStageLibrariesExtras();
        });
      },

      onDeleteExtrasClick: function () {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Delete Additional Drivers',
          1
        );

        var selectedList = _.filter($scope.stageLibrariesExtras, function(lib) {
          return  $scope.selectedStageLibraryList.indexOf(lib.id) !== -1;
        });

        var modalInstance = $modal.open({
          templateUrl: 'app/home/packageManager/delete_extras/deleteExtras.tpl.html',
          controller: 'DeleteExtrasModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            stageLibrariesExtras: function () {
              return selectedList;
            }
          }
        });

        modalInstance.result.then(function() {
          getStageLibrariesExtras();
        }, function() {
          getStageLibrariesExtras();
        });
      },

      uploadFileBtn: function(uploadFile) {
        api.pipelineAgent.installExtras('libraryId', uploadFile);
      }

    });

    var getStageLibrariesExtras = function() {
      $scope.stageLibrariesExtras = [];
      $scope.fetching = true;
      api.pipelineAgent.getStageLibrariesExtras($scope.detailPaneConfigDefn.library)
        .then(
          function (res) {
            $scope.stageLibrariesExtras = res.data;
            $scope.fetching = false;
          },
          function (res) {
            $rootScope.common.errors = [res.data];
            $scope.fetching = false;
          }
        );
    };

    getStageLibrariesExtras();

    $scope.$on('onSelectionChange', function() {
      getStageLibrariesExtras();
    });
  });
