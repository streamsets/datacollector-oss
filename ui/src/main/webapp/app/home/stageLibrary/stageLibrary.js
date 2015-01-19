/**
 * Controller for Stage Library Pane.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('StageLibraryController', function ($scope, pipelineService, pipelineConstant) {
    angular.extend($scope, {

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
       * Filter callback function
       *
       * @param stage
       * @returns {boolean}
       */
      libraryFilter: function(stage) {
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
    };

    updateStageGroups();


    $scope.$watch('stageLibraries', function() {
      updateStageGroups();
    });

  });