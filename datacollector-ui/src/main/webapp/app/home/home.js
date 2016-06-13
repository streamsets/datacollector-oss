/**
 * Copyright 2015 StreamSets Inc.
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
 * Home module for displaying home page content.
 */

angular
  .module('dataCollectorApp.home')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'app/home/home.tpl.html',
        controller: 'HomeController',
        resolve: {
          myVar: function(authService) {
            return authService.init();
          }
        },
        data: {
          authorizedRoles: ['admin', 'creator', 'manager', 'guest']
        }
      });
  }])
  .controller('HomeController', function ($scope, $rootScope, $routeParams, $q, $modal, $location, pipelineService, api,
                                          configuration, pipelineConstant, Analytics, $route, $translate) {
    $location.search('auth_token', null);
    $location.search('auth_user', null);

    if($routeParams.errors) {
      $rootScope.common.errors = [$routeParams.errors];
      //$location.search('errors', null);
    } else {
      $rootScope.common.errors = [];
    }

    var pipelinesLimit = 30;

    angular.extend($scope, {
      loaded: false,
      pipelines: [],
      selectedPipelineLabel: 'All Pipelines',
      filteredPipelines: [],
      header: {
        pipelineGridView: $rootScope.$storage.pipelineGridView,
        sortColumn: 'lastModified',
        sortReverse: true,
        searchInput: ''
      },
      selectedPipelineMap: {},
      selectedPipelineList: [],
      allSelected: false,
      showDetails: false,
      hideLibraryPanel: false,
      limit: pipelinesLimit,

      toggleLibraryPanel: function () {
        $scope.hideLibraryPanel = !$scope.hideLibraryPanel;
      },

      selectPipelineLabel: function(pipelineLabel) {
        $scope.selectedPipelineLabel = pipelineLabel;
        $scope.header.searchInput = '';
        $scope.updateFilteredPipelines();
        $scope.unSelectAll();
      },

      updateFilteredPipelines: function() {
        var regex = new RegExp($scope.header.searchInput, 'i');
        switch ($scope.selectedPipelineLabel) {
          case 'All Pipelines':
            $scope.filteredPipelines = _.filter($scope.pipelines, function(pipelineInfo) {
              return regex.test(pipelineInfo.name);
            });
            break;
          case 'Running Pipelines':
            $scope.filteredPipelines = _.filter($scope.pipelines, function(pipelineInfo) {
              var pipelineStatus = $rootScope.common.pipelineStatusMap[pipelineInfo.name];
              return (pipelineStatus && pipelineStatus.name === pipelineInfo.name && regex.test(pipelineInfo.name) &&
              _.contains(['RUNNING', 'STARTING', 'CONNECT_ERROR', 'RETRY', 'STOPPING'], pipelineStatus.status));
            });
            break;
          case 'Non Running Pipelines':
            $scope.filteredPipelines = _.filter($scope.pipelines, function(pipelineInfo) {
              var pipelineStatus = $rootScope.common.pipelineStatusMap[pipelineInfo.name];
              return (pipelineStatus && pipelineStatus.name === pipelineInfo.name && regex.test(pipelineInfo.name) &&
              !_.contains(['RUNNING', 'STARTING', 'CONNECT_ERROR', 'RETRY', 'STOPPING'], pipelineStatus.status));
            });
            break;
          case 'Invalid Pipelines':
            $scope.filteredPipelines = _.filter($scope.pipelines, function(pipelineInfo) {
              return !pipelineInfo.valid && regex.test(pipelineInfo.name);
            });
            break;
          case 'Error Pipelines':
            $scope.filteredPipelines = _.filter($scope.pipelines, function(pipelineInfo) {
              var pipelineStatus = $rootScope.common.pipelineStatusMap[pipelineInfo.name];
              return (pipelineStatus && pipelineStatus.name === pipelineInfo.name && regex.test(pipelineInfo.name) &&
              _.contains(['START_ERROR', 'RUNNING_ERROR', 'RUN_ERROR', 'CONNECT_ERROR'], pipelineStatus.status));
            });
            break;
          default:
            // User labels
            $scope.filteredPipelines = _.filter($scope.pipelines, function(pipelineInfo) {
              return regex.test(pipelineInfo.name) && pipelineInfo.metadata && pipelineInfo.metadata.labels &&
                _.contains(pipelineInfo.metadata.labels, $scope.selectedPipelineLabel);
            });
            break;
        }
      },

      /**
       * Refresh pipelines
       */
      refreshPipelines: function() {
        $scope.unSelectAll();
        $q.all([
          api.pipelineAgent.getAllPipelineStatus(),
          pipelineService.refreshPipelines()
        ]).then(
          function (results) {
            $rootScope.common.pipelineStatusMap = results[0].data;
            $scope.pipelines = pipelineService.getPipelines();
            $scope.updateFilteredPipelines();
          }
        );
      },

      /**
       * Add New Pipeline Configuration
       */
      addPipelineConfig: function() {
        pipelineService.addPipelineConfigCommand();
      },

      /**
       * Import Pipeline Configuration
       */
      importPipelineConfig: function(pipelineInfo, $event) {
        pipelineService.importPipelineConfigCommand(pipelineInfo, $event);
      },

      /**
       * Delete Pipeline Configuration
       */
      deletePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.deletePipelineConfigCommand(pipelineInfo, $event)
          .then(function(pipelines) {
            $scope.pipelines = pipelines;
            $scope.updateFilteredPipelines();
          });
      },

      /**
       * Delete all selected Pipelines
       */
      deleteSelectedPipeline: function() {
        var selectedPipelineList = $scope.selectedPipelineList;
        var selectedPipelineInfoList = [];
        var validationIssues = [];
        angular.forEach($scope.pipelines, function(pipelineInfo) {
          if (selectedPipelineList.indexOf(pipelineInfo.name) !== -1) {
            var pipelineStatus = $rootScope.common.pipelineStatusMap[pipelineInfo.name];
            if (pipelineStatus && pipelineStatus.name === pipelineInfo.name &&
              _.contains(['RUNNING', 'STARTING', 'CONNECT_ERROR', 'RETRY', 'STOPPING'], pipelineStatus.status)) {
              validationIssues.push('Delete operation is not supported for Pipeline "' +
                pipelineInfo.name + '" with state ' +  pipelineStatus.status );
            }
            selectedPipelineInfoList.push(pipelineInfo);
          }
        });

        if (validationIssues.length > 0) {
          $rootScope.common.errors = validationIssues;
          return;
        }

        $rootScope.common.errors = [];

        pipelineService.deletePipelineConfigCommand(selectedPipelineInfoList)
          .then(function(pipelines) {
            $scope.pipelines = pipelines;
            $scope.updateFilteredPipelines();
            $scope.unSelectAll();
          });

      },

      /**
       * Duplicate Pipeline Configuration
       */
      duplicatePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.duplicatePipelineConfigCommand(pipelineInfo, $event)
          .then(function(pipelines) {
            $scope.pipelines = pipelineService.getPipelines();
            $scope.updateFilteredPipelines();
          });
      },

      /**
       * Duplicate Selected Pipeline
       */
      duplicatePipelines: function() {
        if ($scope.selectedPipelineList && $scope.selectedPipelineList.length > 0) {
          var selectedPipeline = _.find($scope.pipelines, function(pipeline) {
            return $scope.selectedPipelineList[0] === pipeline.name
          });
          pipelineService.duplicatePipelineConfigCommand(selectedPipeline)
            .then(function(pipelines) {
              $scope.refreshPipelines();
            });
        }
      },

      /**
       * Export link command handler
       */
      exportPipelineConfig: function(pipelineInfo, includeDefinitions, $event) {
        $event.stopPropagation();
        api.pipelineAgent.exportPipelineConfig(pipelineInfo.name, includeDefinitions);
      },

      /**
       * Export link command handler
       */
      exportSelectedPipelines: function(includeDefinitions) {
        var selectedPipelineList = $scope.selectedPipelineList;
        api.pipelineAgent.exportSelectedPipelines(selectedPipelineList, includeDefinitions);
        $scope.unSelectAll();
      },


      /**
       * Download Remote Pipeline Configuration
       */
      downloadRemotePipelineConfig: function($event) {
        pipelineService.downloadRemotePipelineConfigCommand($event)
          .then(function() {
            $route.reload();
          });
      },

      /**
       * Open pipeline
       * @param pipeline
       */
      openPipeline: function(pipeline) {
        $location.path('/collector/pipeline/' + pipeline.name);
      },

      /**
       * On Start Pipeline button click.
       *
       */
      startPipeline: function(pipelineInfo, $event) {
        if($event) {
          $event.stopPropagation();
        }

        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Start Pipeline',
          1
        );

        if($rootScope.common.pipelineStatusMap[pipelineInfo.name].state !== 'RUNNING') {
          api.pipelineAgent.startPipeline(pipelineInfo.name, 0).
          then(
            function (res) {
              $rootScope.common.pipelineStatusMap[pipelineInfo.name] = res.data;
            },
            function (res) {
              $rootScope.common.errors = [res.data];
            }
          );
        } else {
          $translate('home.graphPane.startErrorMessage', {
            name: pipelineInfo.name
          }).then(function(translation) {
            $rootScope.common.errors = [translation];
          });
        }
      },

      /**
       * On Start Pipelines button click.
       *
       */
      startSelectedPipelines: function() {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Start Pipelines',
          1
        );

        var selectedPipelineList = $scope.selectedPipelineList;
        var validationIssues = [];
        angular.forEach($scope.pipelines, function(pipelineInfo) {
          if (selectedPipelineList.indexOf(pipelineInfo.name) !== -1) {
            var pipelineStatus = $rootScope.common.pipelineStatusMap[pipelineInfo.name];
            if (pipelineStatus && pipelineStatus.name === pipelineInfo.name &&
              _.contains(['RUNNING', 'STARTING', 'CONNECT_ERROR', 'RETRY', 'STOPPING'], pipelineStatus.status)) {
              validationIssues.push('Start operation is not supported for Pipeline "' +
                pipelineInfo.name + '" with state ' +  pipelineStatus.status );
            }

            if (!pipelineInfo.valid) {
              validationIssues.push('Pipeline "' + pipelineInfo.name + '" is not valid');
            }
          }
        });

        if (validationIssues.length > 0) {
          $rootScope.common.errors = validationIssues;
          return;
        }
        $rootScope.common.errors = [];

        angular.forEach(selectedPipelineList, function(pipelineName) {
          api.pipelineAgent.startPipeline(pipelineName, 0)
            .then(
              function (res) {
                $rootScope.common.pipelineStatusMap[pipelineName] = res.data;
              },
              function (res) {
                $rootScope.common.errors.push(res.data);
              }
            );
        });

        $scope.unSelectAll();
      },

      /**
       * On Stop Pipeline button click.
       *
       */
      stopPipeline: function(pipelineInfo, $event) {
        if($event) {
          $event.stopPropagation();
        }

        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Stop Pipeline',
          1
        );
        var modalInstance = $modal.open({
          templateUrl: 'app/home/header/stop/stopConfirmation.tpl.html',
          controller: 'StopConfirmationModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return pipelineInfo;
            }
          }
        });

        modalInstance.result.then(function(status) {
          $rootScope.common.pipelineStatusMap[pipelineInfo.name] = status;

          var alerts = $rootScope.common.alertsMap[pipelineInfo.name];

          if(alerts) {
            delete $rootScope.common.alertsMap[pipelineInfo.name];
            $rootScope.common.alertsTotalCount -= alerts.length;
          }
        }, function () {

        });
      },

      /**
       * On Stop Pipelines button click.
       */
      stopSelectedPipelines: function() {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Stop Pipelines',
          1
        );

        var selectedPipelineList = $scope.selectedPipelineList;
        var selectedPipelineInfoList = [];
        var validationIssues = [];
        angular.forEach($scope.pipelines, function(pipelineInfo) {
          if (selectedPipelineList.indexOf(pipelineInfo.name) !== -1) {
            var pipelineStatus = $rootScope.common.pipelineStatusMap[pipelineInfo.name];
            if (pipelineStatus && pipelineStatus.name === pipelineInfo.name &&
              !_.contains(['RUNNING', 'STARTING', 'CONNECT_ERROR', 'RETRY', 'STOPPING'], pipelineStatus.status)) {
              validationIssues.push('Stop operation is not supported for Pipeline "' +
                pipelineInfo.name + '" with state ' +  pipelineStatus.status );
            }
            selectedPipelineInfoList.push(pipelineInfo);
          }
        });

        if (validationIssues.length > 0) {
          $rootScope.common.errors = validationIssues;
          return;
        }
        $rootScope.common.errors = [];

        var modalInstance = $modal.open({
          templateUrl: 'app/home/header/stop/stopConfirmation.tpl.html',
          controller: 'StopConfirmationModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return selectedPipelineInfoList;
            }
          }
        });

        modalInstance.result.then(function(statusList) {
          angular.forEach(selectedPipelineList, function(pipelineName) {
            $rootScope.common.pipelineStatusMap[pipelineName] = statusList[pipelineName];
            var alerts = $rootScope.common.alertsMap[pipelineName];
            if(alerts) {
              delete $rootScope.common.alertsMap[pipelineName];
              $rootScope.common.alertsTotalCount -= alerts.length;
            }
          });
        }, function () {

        });

        $scope.unSelectAll();
      },

      /**
       * On Publish Pipelines Click
       */
      publishSelectedPipelines: function() {
        $rootScope.common.trackEvent(
          pipelineConstant.BUTTON_CATEGORY,
          pipelineConstant.CLICK_ACTION,
          'Publish Pipelines',
          1
        );

        var selectedPipelineList = $scope.selectedPipelineList;
        var selectedPipelineInfoList = [];
        var validationIssues = [];
        angular.forEach($scope.pipelines, function(pipelineInfo) {
          if (selectedPipelineList.indexOf(pipelineInfo.name) !== -1) {
            var pipelineStatus = $rootScope.common.pipelineStatusMap[pipelineInfo.name];
            if (pipelineStatus && pipelineStatus.name === pipelineInfo.name) {
              if (!pipelineInfo.valid) {
                validationIssues.push('Publish operation is not supported for Invalid Pipeline - ' + pipelineInfo.name);
              } else if (pipelineStatus.attributes && pipelineStatus.attributes.IS_REMOTE_PIPELINE) {
                validationIssues.push('Publish operation is not supported for Remote Pipeline "' + pipelineInfo.name);
              }
              selectedPipelineInfoList.push(pipelineInfo);
            }
          }
        });

        if (validationIssues.length > 0) {
          $rootScope.common.errors = validationIssues;
          return;
        }

        $rootScope.common.errors = [];

        pipelineService.publishPipelineCommand(selectedPipelineInfoList)
          .then(function() {
            $scope.unSelectAll();
          });
      },

      /**
       * Return pipeline alerts for tooltip
       *
       * @param pipelineAlerts
       */
      getPipelineAlerts: function(pipelineAlerts) {
        var alertMsg ='<span class="stage-errors-tooltip">';
        if(pipelineAlerts) {
          angular.forEach(pipelineAlerts, function(alert) {
            if(alert.ruleDefinition.family === 'drift' && alert.gauge.value.alertTexts &&
              alert.gauge.value.alertTexts.length) {
              //Data Drift Alert
              alertMsg += alert.gauge.value.alertTexts.join('<br>') + '<br>';
            } else {
              //General Data Alert
              alertMsg += alert.ruleDefinition.alertText + '<br>';
            }
          });
        }
        alertMsg += '</span>';
        return alertMsg;
      },

      /**
       * On Select All check box select
       */
      selectAll: function () {
        $scope.selectedPipelineList = [];
        $scope.selectedPipelineMap = {};
        angular.forEach($scope.filteredPipelines, function(pipelineInfo) {
          $scope.selectedPipelineList.push(pipelineInfo.name);
          $scope.selectedPipelineMap[pipelineInfo.name] = true;
        });
        $scope.allSelected = true;
      },

      /**
       * On UnSelect All check box select
       */
      unSelectAll: function () {
        $scope.selectedPipelineList = [];
        $scope.selectedPipelineMap = {};
        $scope.allSelected = false;
        $scope.limit = pipelinesLimit;
      },

      /**
       * On Selecting Individual Pipeline checkbox
       * @param pipeline
       */
      selectPipeline: function(pipeline) {
        $scope.selectedPipelineMap[pipeline.name] = true;
        $scope.selectedPipelineList.push(pipeline.name);
      },

      /**
       * On UnSelecting Individual Pipeline checkbox
       * @param pipeline
       */
      unSelectPipeline: function(pipeline) {
        $scope.selectedPipelineMap[pipeline.name] = false;
        var index = $scope.selectedPipelineList.indexOf(pipeline.name);
        if (index != -1) {
          $scope.selectedPipelineList.splice(index, 1);
        }
        $scope.allSelected = false;
      },

      /**
       * On Show Details link click
       */
      showPipelineDetails: function () {
        $scope.showDetails = true;
      },

      /**
       * On Hide Details link click
       */
      hidePipelineDetails: function () {
        $scope.showDetails = false;
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
       * Custom sort function for filtering pipelines
       * @param pipelineInfo
       * @returns {*}
       */
      customPipelineSortFunction: function (pipelineInfo) {
        if ($scope.header.sortColumn === 'status') {
          var pipelineStatus = $rootScope.common.pipelineStatusMap[pipelineInfo.name];
          return pipelineStatus.status;
        }
        return pipelineInfo[$scope.header.sortColumn];
      },

      /**
       * Callback function when Show more link clicked.
       *
       * @param $event
       */
      onShowMoreClick: function($event) {
        $event.preventDefault();
        $scope.limit += pipelinesLimit;
      }
    });

    $q.all([
      api.pipelineAgent.getAllPipelineStatus(),
      pipelineService.init(true),
      configuration.init()
    ]).then(
      function (results) {
        $scope.loaded = true;
        $rootScope.common.pipelineStatusMap = results[0].data;
        $scope.pipelines = pipelineService.getPipelines();
        $scope.updateFilteredPipelines();

        if($scope.pipelines && $scope.pipelines.length) {
          $rootScope.common.sdcClusterManagerURL = configuration.getSDCClusterManagerURL() +
            '/collector/pipeline/' + $scope.pipelines[0].name;
        }

        if(configuration.isAnalyticsEnabled()) {
          Analytics.trackPage('/');
        }
      },
      function (results) {
        $scope.loaded = true;
      }
    );

    $scope.$on('onAlertClick', function(event, alert) {
      if(alert && alert.pipelineName) {
        $rootScope.common.clickedAlert = alert;
        $location.path('/collector/pipeline/' + alert.pipelineName);
      }
    });

    var pipelineGridViewWatchListener = $scope.$watch('header.pipelineGridView', function() {
      $rootScope.$storage.pipelineGridView = $scope.header.pipelineGridView;
    });

    $scope.$on('$destroy', function() {
      if (pipelineGridViewWatchListener) {
        pipelineGridViewWatchListener();
      }
    });

  });
