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
  .controller('PackageManagerController', function (
    $scope, $rootScope, $routeParams, $q, $modal, $location, pipelineService, api, configuration, pipelineConstant,
    Analytics
  ) {
    $location.search('auth_token', null);
    $location.search('auth_user', null);
    $rootScope.common.errors = [];
    $rootScope.common.title = "Package Manager";

    var mlRegex = new RegExp('(TensorFlow)|(Databricks ML)|(PMML)|(MLeap)', 'i');
    var awsRegex = new RegExp('(Amazon)|(AWS)', 'i');
    var credentialStoresRegex = new RegExp('Credential Store', 'i');
    var pipelinesLimit = 60;

    angular.extend($scope, {
      uploadFile: {},
      navigationItems: [
        'All Stage Libraries',
        'Installed Stage Libraries',
        'Enterprise Stage Libraries',
        'Legacy Stage Libraries',

        'Amazon Web Services (AWS)',
        'Azure',
        'Google Cloud',

        'Apache Kafka',
        'Apache Kudu',
        'Apache Solr',
        'CDH',
        'CDP',
        'Credential Stores',
        'Elasticsearch',
        'Groovy',
        'HDP',
        'InfluxDB',
        'JDBC',
        'JMS',
        'Jython',
        'Machine Learning',
        'MapR',
        'MongoDB',
        'MySql BinLog',
        'Omniture',
        'Orchestrator',
        'RabbitMQ'
      ],
      extrasNavigationItem: 'EXTRAS',
      selectedNavigationItem: 'All Stage Libraries',
      stageLibraries: [],
      filteredStageLibraries: [],
      stageLibrariesExtras: [],
      header: {
        customRepoUrl: $rootScope.$storage.customPackageManagerRepoUrl,
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
      fetching: true,
      showLibraryId: false,

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
              return stageLibrary.stageLibraryManifest && regex.test(stageLibrary.stageLibraryManifest.stageLibLabel);
            });
            break;
          case 'Installed Stage Libraries':
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return stageLibrary.stageLibraryManifest && regex.test(stageLibrary.stageLibraryManifest.stageLibLabel) &&
                stageLibrary.stageLibraryManifest.installed;
            });
            break;
          case 'Enterprise Stage Libraries':
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return stageLibrary.stageLibraryManifest && regex.test(stageLibrary.stageLibraryManifest.stageLibLabel) &&
                stageLibrary.stageLibraryManifest.stageLibLicense === 'StreamSetsEnterprise1.0';
            });
            break;
          case 'Legacy Stage Libraries':
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return stageLibrary.legacy;
            });
            break;
          case 'Machine Learning':
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return stageLibrary.stageLibraryManifest && regex.test(stageLibrary.stageLibraryManifest.stageLibLabel) &&
                mlRegex.test(stageLibrary.stageLibraryManifest.stageLibLabel);
            });
            break;
          case 'Amazon Web Services (AWS)':
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return stageLibrary.stageLibraryManifest && regex.test(stageLibrary.stageLibraryManifest.stageLibLabel) &&
                awsRegex.test(stageLibrary.stageLibraryManifest.stageLibLabel);
            });
            break;
          case 'Credential Stores':
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return stageLibrary.stageLibraryManifest && regex.test(stageLibrary.stageLibraryManifest.stageLibLabel) &&
                credentialStoresRegex.test(stageLibrary.stageLibraryManifest.stageLibLabel);
            });
            break;
          default:
            $scope.filteredStageLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
              return stageLibrary.stageLibraryManifest && regex.test(stageLibrary.stageLibraryManifest.stageLibLabel) &&
                stageLibrary.stageLibraryManifest.stageLibLabel.indexOf($scope.selectedNavigationItem) !== -1;
            });
        }
      },

      getStageLibraryKey: function(stageLibrary) {
        return getStageLibraryKey(stageLibrary);
      },

      /**
       * On Select All check box select
       */
      selectAll: function () {
        $scope.selectedStageLibraryList = [];
        $scope.selectedStageLibraryMap = {};
        var list = $scope.filteredStageLibraries;
        if ($scope.selectedNavigationItem === $scope.extrasNavigationItem) {
          list = $scope.stageLibrariesExtras;
        }
        angular.forEach(list, function(stageLibrary) {
          var id = getStageLibraryKey(stageLibrary);
          $scope.selectedStageLibraryList.push(id);
          $scope.selectedStageLibraryMap[id] = true;
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
        var id = getStageLibraryKey(stageLibrary);
        $scope.selectedStageLibraryMap[id] = true;
        $scope.selectedStageLibraryList.push(id);
      },

      /**
       * On UnSelecting Individual stageLibrary checkbox
       * @param stageLibrary
       */
      unSelectStageLibrary: function(stageLibrary) {
        var id = getStageLibraryKey(stageLibrary);
        $scope.selectedStageLibraryMap[id] = false;
        var index = $scope.selectedStageLibraryList.indexOf(id);
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
        if (stageLibrary.stageLibraryManifest) {
          if ($scope.header.sortColumn === 'label') {
            return stageLibrary.stageLibraryManifest.stageLibLabel;
          } else if ($scope.header.sortColumn === 'installed') {
            return stageLibrary.stageLibraryManifest.installed;
          }
        }
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
        if($scope.isManagedByClouderaManager) {
          return;
        }

        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Install Stage Libraries',
          1
        );

        installStageLibraries(_.filter($scope.filteredStageLibraries, function(lib) {
          return !lib.stageLibraryManifest.installed && $scope.selectedStageLibraryList.indexOf(getStageLibraryKey(lib)) !== -1;
        }));
      },

      /**
       * Callback function when Uninstall button clicked.
       */
      onUninstallSelectedLibrariesClick: function() {
        if($scope.isManagedByClouderaManager) {
          return;
        }

        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Uninstall Stage Libraries',
          1
        );

        uninstallStageLibraries(_.filter($scope.filteredStageLibraries, function(lib) {
          return lib.stageLibraryManifest.installed && $scope.selectedStageLibraryList.indexOf(getStageLibraryKey(lib)) !== -1;
        }));
      },

      /**
       * From the package selection, determine whether there is
       *
       * @toInstall[true] at least one selected package to install that is not yet installed, or
       * @toInstall[false] at least one selected package to uninstall that is already installed
       *
       * @param toInstall boolean - whether to install or uninstall
       * @returns {boolean}
       */
      hasSelectedLibrary: function(toInstall) {
        return !_.any($scope.filteredStageLibraries, function(lib) {
          var condition = toInstall ? !lib.stageLibraryManifest.installed : lib.stageLibraryManifest.installed;
          return condition && $scope.selectedStageLibraryList.indexOf(getStageLibraryKey(lib)) !== -1;
        });
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
      },

      /**
       * Callback function when on Custom Repo URL menu item click
       */
      onCustomRepoURLClick: function() {
        updateCustomRepoUrl();
      },

      onUploadExtrasClick: function () {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Install Additional Drivers',
          1
        );
        var installedLibraries = _.filter($scope.stageLibraries, function(stageLibrary) {
          return stageLibrary.stageLibraryManifest && stageLibrary.stageLibraryManifest.installed;
        });

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
          return $scope.selectedStageLibraryList.indexOf(getStageLibraryKey(lib)) !== -1;
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
      },

      getStageInfoList: function(stageDefList) {
        var stageInfoList = [];
        var originList = [];
        var processorList = [];
        var destinationList = [];
        var others = [];
        angular.forEach(stageDefList, function (stageInfo) {
          if (!stageInfo.errorStage && !stageInfo.statsAggregatorStage && !stageInfo.connectionVerifierStage) {
            if (stageInfo.type === 'SOURCE') {
              originList.push(stageInfo);
            } else if (stageInfo.type === 'PROCESSOR') {
              processorList.push(stageInfo);
            } else if (stageInfo.type === 'TARGET') {
              destinationList.push(stageInfo);
            } else {
              others.push(stageInfo);
            }
          }
        });
        stageInfoList.push.apply(stageInfoList, originList.sort(sortStageInfo));
        stageInfoList.push.apply(stageInfoList, processorList.sort(sortStageInfo));
        stageInfoList.push.apply(stageInfoList, destinationList.sort(sortStageInfo));
        stageInfoList.push.apply(stageInfoList, others.sort(sortStageInfo));
        return stageInfoList;
      }

    });

    var sortStageInfo = function(a, b) {
      if (a.label.toLowerCase() > b.label.toLowerCase()) {
        return 1;
      } else {
        return -1;
      }
    };

    $q.all([
      configuration.init()
    ]).then(
      function () {
        if(configuration.isAnalyticsEnabled()) {
          Analytics.trackPage('/collector/packageManager');
        }
        $scope.isManagedByClouderaManager = configuration.isManagedByClouderaManager();
      },
      function (res) {
        $rootScope.common.errors = [res.data];
      }
    );

    var getLibraries = function(repoUrl, installedOnly) {
      $scope.fetching = true;
      $scope.stageLibraries = [];
      api.pipelineAgent.getLibraries(repoUrl, installedOnly)
        .then(
          function (res) {
            $scope.fetching = false;
            $scope.repositoryManifestList = res.data;
            angular.forEach($scope.repositoryManifestList, function (value) {
              $scope.stageLibraries.push.apply($scope.stageLibraries, value.stageLibraries);
            });

            $scope.manifestURL = res.headers('REPO_URL');
            $scope.updateStageLibraryList();
          },
          function (res) {
            $rootScope.common.errors = [res.data];

            // Fetch only locally installed libraries
            api.pipelineAgent.getLibraries(repoUrl, true)
              .then(
                function (res) {
                  $scope.fetching = false;
                  $scope.repositoryManifestList = res.data;
                  angular.forEach($scope.repositoryManifestList, function (value) {
                    $scope.stageLibraries.push.apply($scope.stageLibraries, value.stageLibraries);
                  });
                  $scope.updateStageLibraryList();
                },
                function (res) {
                  $rootScope.common.errors = [res.data];
                }
              );
          }
        );
    };

    var getStageLibrariesExtras = function() {
      $scope.stageLibrariesExtras = [];
      api.pipelineAgent.getStageLibrariesExtras()
        .then(
          function (res) {
            $scope.stageLibrariesExtras = res.data;
          },
          function (res) {
            $rootScope.common.errors = [res.data];
          }
        );
    };

    if (window.navigator.onLine) {
      getLibraries($scope.header.customRepoUrl, false);
    } else {
      $rootScope.common.errors = ['Unable to connect to the Internet'];
      getLibraries(null, true);
    }
    getStageLibrariesExtras();

    var pipelineGridViewWatchListener = $scope.$watch('header.pipelineGridView', function() {
      $rootScope.$storage.pipelineListState.gridView = $scope.header.pipelineGridView;
    });

    $scope.$on('$destroy', function() {
      if (pipelineGridViewWatchListener) {
        pipelineGridViewWatchListener();
      }
    });

    var installStageLibraries = function(libraryList) {
      $modal.open({
        templateUrl: 'app/home/packageManager/install/install.tpl.html',
        controller: 'InstallModalInstanceController',
        size: '',
        backdrop: 'static',
        resolve: {
          libraryList: function () {
            return libraryList;
          },
          withStageLibVersion:function () {
            return true;
          }
        }
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
          library.stageLibraryManifest.installed = false;
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

    var updateCustomRepoUrl= function() {
      var modalInstance = $modal.open({
        templateUrl: 'app/home/packageManager/customRepoUrl/customRepoUrl.tpl.html',
        controller: 'CustomRepoUrlInstanceController',
        size: '',
        backdrop: 'static',
        resolve: {
          customRepoUrl: function () {
            return $scope.header.customRepoUrl;
          }
        }
      });
      modalInstance.result.then(function(repoUrl) {
        $scope.header.customRepoUrl = $rootScope.$storage.customPackageManagerRepoUrl = repoUrl;
        getLibraries($scope.header.customRepoUrl, false);
      }, function () {
      });
    };

    var getStageLibraryKey = function(stageLibrary) {
      if (stageLibrary.stageLibraryManifest) {
        return stageLibrary.stageLibraryManifest.stageLibId + ':' + stageLibrary.stagelibVersion;
      } else {
        return stageLibrary.id;
      }
    };
  });
