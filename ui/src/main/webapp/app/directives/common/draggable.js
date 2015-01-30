/**
 * Directive for draggable.
 */

angular.module('dataCollectorApp.commonDirectives')
  .directive("draggable", function () {
    return {
      scope: {
        dragData: '=dragData'
      },
      link: function (scope, element) {
        var el = element[0];

        el.draggable = true;

        el.addEventListener(
          'dragstart',
          function(e) {
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('dragData', JSON.stringify(scope.dragData));
            this.classList.add('drag');
            return false;
          },
          false
        );

        el.addEventListener(
          'dragend',
          function(e) {
            this.classList.remove('drag');
            return false;
          },
          false
        );
      }
    };
  });