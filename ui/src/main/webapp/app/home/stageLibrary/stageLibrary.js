/**
 * Controller for Stage Library Pane.
 */

angular
  .module('dataCollectorApp.home')
  .controller('StageLibraryController', function ($scope, pipelineService, pipelineConstant) {
    angular.extend($scope, {
      filteredStageLibraries: [],

      allStage: {
        group: 'All'
      },

      /**
       * Return Stage Icon URL
       *
       * @param stage
       * @returns {*}
       */
      getStageIconURL: function(stage) {
        return pipelineService.getStageIconURL(stage);
      },


      /**
       * Callback function when stageFilterGroup is updated.
       *
       */
      onStageFilterGroupChange: function() {
        var stageNameList = [];
        $scope.filteredStageLibraries = [];
        angular.forEach($scope.stageLibraries, function(stageLibrary) {
          if(libraryFilter(stageLibrary) && !_.contains(stageNameList, stageLibrary.name)) {
            stageNameList.push(stageLibrary.name);
            $scope.filteredStageLibraries.push(stageLibrary);
          }
        });
      }
    });

    var typeGroups = [{
      group: 'Type',
      name: pipelineConstant.SOURCE_STAGE_TYPE,
      label: 'Origins'
    },{
      group: 'Type',
      name: pipelineConstant.PROCESSOR_STAGE_TYPE,
      label: 'Processors'
    },{
      group: 'Type',
      name: pipelineConstant.TARGET_STAGE_TYPE,
      label: 'Destinations'
    }];

    /**
     * Filter callback function
     *
     * @param stage
     * @returns {boolean}
     */
    var libraryFilter = function(stage) {
      var filterGroup = $scope.$storage.stageFilterGroup;

      if(filterGroup === undefined) {
        $scope.$storage.stageFilterGroup = '';
      }

      if(filterGroup) {
        if(filterGroup.group === 'Type') {
          return stage.type === filterGroup.name;
        } else if(filterGroup.group === 'Library') {
          return stage.library === filterGroup.name;
        }
      }

      return true;
    };

    var updateStageGroups = function() {
      var stageGroups = [],
        libraryList = _.chain($scope.stageLibraries)
          .pluck("library")
          .unique()
          .value(),
        libraryLabelList = _.chain($scope.stageLibraries)
          .pluck("libraryLabel")
          .unique()
          .value();

      stageGroups = stageGroups.concat(typeGroups);

      angular.forEach(libraryList, function(library, index) {
        stageGroups.push({
          group: 'Library',
          name: library,
          label: libraryLabelList[index]
        });
      });

      $scope.stageGroups = stageGroups;

      $scope.onStageFilterGroupChange();
    };

    updateStageGroups();

    $scope.$watch('stageLibraries', function() {
      updateStageGroups();
    });

    $scope.$on('updateGraph', function(event, options) {
      if(options && options.nodes && options.nodes.length === 0) {
        $scope.$storage.stageFilterGroup = _.find($scope.stageGroups, function(group) {
          return group.name === pipelineConstant.SOURCE_STAGE_TYPE;
        });
        $scope.onStageFilterGroupChange();
      }
    });

  });