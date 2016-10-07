/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
      .when('/collector/packageManager', {
        templateUrl: 'app/home/packageManager/package_manager.tpl.html',
        controller: 'PackageManagerController',
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
  .controller('PackageManagerController', function ($scope, $rootScope, $routeParams, $q, $modal, $location,
                                                    pipelineService, api, configuration, pipelineConstant, Analytics,
                                                    $route, $translate) {
    $location.search('auth_token', null);
    $location.search('auth_user', null);

    if($routeParams.errors) {
      $rootScope.common.errors = [$routeParams.errors];
      //$location.search('errors', null);
    } else {
      $rootScope.common.errors = [];
    }

    var pipelinesLimit = 60;

    angular.extend($scope, {
      loaded: false,
      navigationItems: [
        'All Stage Libraries',
        'Installed Stage Libraries',
        'Apache Kafka',
        'Apache Kudu',
        'Apache Solr',
        'CDH',
        'Elasticsearch',
        'HDP',
        'MapR'
      ],
      selectedNavigationItem: 'All Stage Libraries',
      stageLibraries: [],
      filteredStageLibraries: [],
      header: {
        pipelineGridView: $rootScope.$storage.pipelineGridView,
        sortColumn: 'label',
        sortReverse: false,
        searchInput: ''
      },
      selectedStageLibraryMap: {},
      selectedStageLibraryList: [],
      allSelected: false,
      showDetails: false,
      hideLibraryPanel: false,
      limit: pipelinesLimit,
      manifestURL: '',
      isManagedByClouderaManager: false,

      toggleLibraryPanel: function () {
        $scope.hideLibraryPanel = !$scope.hideLibraryPanel;
      },

      onNavigationItemClick: function(navigationItem) {
        $scope.selectedNavigationItem = navigationItem;
        $scope.header.searchInput = '';
        $scope.updateStageLibraryList();
        $scope.unSelectAll();
      },

      updateStageLibraryList: function() {
        var regex = new RegExp($scope.header.searchInput, 'i');
        switch ($scope.selectedNavigationItem) {
          case 'All Stage Libraries':
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return regex.test(stageLibrary.label);
            });
            break;
          case 'Installed Stage Libraries':
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return regex.test(stageLibrary.label) && stageLibrary.installed;
            });
            break;
          default:
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return regex.test(stageLibrary.label) && stageLibrary.label.indexOf($scope.selectedNavigationItem) !== -1;
            });
        }
      },

      /**
       * On Select All check box select
       */
      selectAll: function () {
        $scope.selectedStageLibraryList = [];
        $scope.selectedStageLibraryMap = {};
        angular.forEach($scope.filteredStageLibraries, function(stageLibrary) {
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
        if (index != -1) {
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

      /**
       * Callback function when Install button clicked.
       */
      onInstallSelectedLibrariesClick: function() {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Install Stage Libraries',
          1
        );

        var selectedStageLibraryList = $scope.selectedStageLibraryList;
        var libraryList = [];
        var validationIssues = [];
        angular.forEach($scope.stageLibraries, function(stageLibrary) {
          if (selectedStageLibraryList.indexOf(stageLibrary.id) !== -1) {
            if (stageLibrary.installed) {
              validationIssues.push('Stage library - ' + stageLibrary.label + ' is already installed');
            }
            libraryList.push(stageLibrary);
          }
        });

        if (validationIssues.length > 0) {
          $rootScope.common.errors = validationIssues;
          return;
        }

        installStageLibraries(libraryList);
      },

      /**
       * Callback function when Uninstall button clicked.
       */
      onUninstallSelectedLibrariesClick: function() {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Uninstall Stage Libraries',
          1
        );

        var selectedStageLibraryList = $scope.selectedStageLibraryList;
        var libraryList = [];
        var validationIssues = [];
        angular.forEach($scope.stageLibraries, function(stageLibrary) {
          if (selectedStageLibraryList.indexOf(stageLibrary.id) !== -1) {
            if (!stageLibrary.installed) {
              validationIssues.push('Stage library - ' + stageLibrary.label + ' is already uninstalled');
            }
            libraryList.push(stageLibrary);
          }
        });

        if (validationIssues.length > 0) {
          $rootScope.common.errors = validationIssues;
          return;
        }

        uninstallStageLibraries(libraryList);
      },

      /**
       * Callback function when Install button clicked.
       */
      onInstallLibraryClick: function(stageLibrary) {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Install Stage Library',
          1
        );
        installStageLibraries([stageLibrary]);
      },

      /**
       * Callback function when Uninstall button clicked.
       */
      onUninstallLibraryClick: function(stageLibrary) {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Uninstall Stage Library',
          1
        );
        uninstallStageLibraries([stageLibrary]);
      }

    });

    $q.all([
      api.pipelineAgent.getLibraries(),
      configuration.init()
    ]).then(
      function (results) {
        $scope.loaded = true;
        $scope.stageLibraries = results[0].data;
        $scope.manifestURL = results[0].headers('REPO_URL');

        $scope.updateStageLibraryList();

        if(configuration.isAnalyticsEnabled()) {
          Analytics.trackPage('/collector/packageManager');
        }

        $scope.isManagedByClouderaManager = configuration.isManagedByClouderaManager();
      },
      function (results) {
        $scope.loaded = true;
      }
    );


    var pipelineGridViewWatchListener = $scope.$watch('header.pipelineGridView', function() {
      $rootScope.$storage.pipelineGridView = $scope.header.pipelineGridView;
    });

    $scope.$on('$destroy', function() {
      if (pipelineGridViewWatchListener) {
        pipelineGridViewWatchListener();
      }
    });

    var installStageLibraries = function(libraryList) {
      var modalInstance = $modal.open({
        templateUrl: 'app/home/packageManager/install/install.tpl.html',
        controller: 'InstallModalInstanceController',
        size: '',
        backdrop: 'static',
        resolve: {
          libraryList: function () {
            return libraryList;
          }
        }
      });
      modalInstance.result.then(function() {
        angular.forEach(libraryList, function(library) {
          library.installed = true;
          $scope.trackEvent(
            pipelineConstant.STAGE_LIBRARY_CATEGORY,
            pipelineConstant.INSTALL_ACTION,
            library.label,
            1
          );
        });
      }, function () {
      });
    };

    var uninstallStageLibraries = function(libraryList) {
      var modalInstance = $modal.open({
        templateUrl: 'app/home/packageManager/uninstall/uninstall.tpl.html',
        controller: 'UninstallModalInstanceController',
        size: '',
        backdrop: 'static',
        resolve: {
          libraryList: function () {
            return libraryList;
          }
        }
      });
      modalInstance.result.then(function() {
        angular.forEach(libraryList, function(library) {
          library.installed = false;
          $scope.trackEvent(
            pipelineConstant.STAGE_LIBRARY_CATEGORY,
            pipelineConstant.UNINSTALL_ACTION,
            library.label,
            1
          );
        });
      }, function () {
      });
    };
  });
