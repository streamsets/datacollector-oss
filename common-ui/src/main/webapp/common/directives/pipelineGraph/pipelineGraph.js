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
 * Module definition for Pipeline Graph Directive.
 */

angular.module('pipelineGraphDirectives', [])
  .directive('pipelineGraph', function() {
    return {
      restrict: 'E',
      //replace: true,
      controller: 'PipelineGraphController',
      templateUrl: 'common/directives/pipelineGraph/pipelineGraph.tpl.html'
    };
  })
  .controller('PipelineGraphController', function($scope, $rootScope, $element, _, $filter, $location, $modal,
                                                  pipelineConstant, $translate, pipelineService){

    var showTransition = false;
    var graphErrorBadgeLabel = '';

    $translate('global.messages.info.graphErrorBadgeLabel').then(function(translation) {
      graphErrorBadgeLabel = [translation];
    });

    // define graphcreator object
    var GraphCreator = function(svg, nodes, edges, issues) {
      var thisGraph = this;
      thisGraph.idct = 0;

      thisGraph.nodes = nodes || [];
      thisGraph.edges = edges || [];
      thisGraph.issues = issues || {};

      // define arrow markers for graph links

      var markerWidth = 3.5;
      var markerHeight = 3.5;
      var cRadius = -7;
      // play with the cRadius value
      var refX = cRadius + (markerWidth * 2);
      var defs = svg.append('svg:defs');

      defs.append('svg:marker')
        .attr('id', 'end-arrow')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', refX)
        .attr('markerWidth', markerWidth)
        .attr('markerHeight', markerHeight)
        .attr('orient', 'auto')
        .append('svg:path')
        .attr('d', 'M0,-5L10,0L0,5');

      thisGraph.svg = svg;

      //Background lines
      var margin = {top: -5, right: -5, bottom: -5, left: -5};

      var svgWidth = 2500;
      var svgHeight = 2500;

      if (svg.length && svg[0] && svg[0].length) {
        var clientWidth = svg[0][0].clientWidth;
        var clientHeight = svg[0][0].clientHeight;

        if (clientWidth > svgWidth) {
          svgWidth = clientWidth;
        }

        if (clientHeight > svgHeight) {
          svgHeight = clientHeight;
        }
      }

      var width = svgWidth - margin.left - margin.right;
      var height = svgHeight - margin.top - margin.bottom;

      var container = svg.append('g');
      container.append('g')
        .attr('class', 'x axis')
        .selectAll('line')
        .data(d3.range(0, width, 10))
        .enter().append('line')
        .attr('x1', function(d) { return d; })
        .attr('y1', 0)
        .attr('x2', function(d) { return d; })
        .attr('y2', height);

      container.append('g')
        .attr('class', 'y axis')
        .selectAll('line')
        .data(d3.range(0, height, 10))
        .enter().append('line')
        .attr('x1', 0)
        .attr('y1', function(d) { return d; })
        .attr('x2', width)
        .attr('y2', function(d) { return d; });

      thisGraph.svgG = svg.append('g')
        .classed(thisGraph.consts.graphClass, true);
      var svgG = thisGraph.svgG;

      // displayed when dragging between nodes
      thisGraph.dragLine = svgG.append('svg:path')
        .attr('class', 'link dragline hidden')
        .attr('d', 'M0,0L0,0')
        .style('marker-end', 'url(' + $location.absUrl() + '#mark-end-arrow)');

      // svg nodes and edges
      thisGraph.paths = svgG.append('g').selectAll('g');
      thisGraph.rects = svgG.append('g').selectAll('g');


      thisGraph.drag = d3.behavior.drag()
        .origin(function(d){
          return {x: d.uiInfo.xPos, y: d.uiInfo.yPos};
        })
        .on('drag', function(args){
          $scope.state.justDragged = true;
          thisGraph.dragmove.call(thisGraph, args);
        })
        .on('dragend', function() {
          // todo check if edge-mode is selected
        });

      // listen for key events
      svg.on('keydown', function() {
        thisGraph.svgKeyDown.call(thisGraph);
      })
      .on('keyup', function() {
        thisGraph.svgKeyUp.call(thisGraph);
      })
      .on('mousedown', function(d) {
        thisGraph.svgMouseDown.call(thisGraph, d);
      })
      .on('mouseup', function(d) {
        thisGraph.svgMouseUp.call(thisGraph, d);
      });

      // listen for dragging

      thisGraph.zoom = d3.behavior.zoom()
        .scaleExtent([0.1, 2])
        .on('zoom', function(){
          if (d3.event && d3.event.sourceEvent && d3.event.sourceEvent.shiftKey){
            // TODO  the internal d3 state is still changing
            return false;
          } else{
            thisGraph.zoomed.call(thisGraph);
          }
          return true;
        })
        .on('zoomstart', function() {
          if (d3.event && d3.event.sourceEvent && !d3.event.sourceEvent.shiftKey) {
            d3.select('body').style('cursor', 'move');
          }
        })
        .on('zoomend', function(){
          d3.select('body').style('cursor', 'auto');
        });

      svg.call(thisGraph.zoom)
        .on('dblclick.zoom', null);

      //svg.on('mousedown.zoom', null);
      //svg.on('mousemove.zoom', null);

      //To disable zoom on mouse scroll
      svg.on('dblclick.zoom', null);
      svg.on('touchstart.zoom', null);
      svg.on('wheel.zoom', null);
      svg.on('mousewheel.zoom', null);
      svg.on('MozMousePixelScroll.zoom', null);
    };

    GraphCreator.prototype.setIdCt = function(idct){
      this.idct = idct;
    };

    GraphCreator.prototype.consts =  {
      selectedClass: 'selected',
      connectClass: 'connect-node',
      rectGClass: 'rectangleG',
      pathGClass: 'pathG',
      graphClass: 'graph',
      startNodeClass: 'startNode',
      endNodeClass: 'endNode',
      BACKSPACE_KEY: 8,
      DELETE_KEY: 46,
      ENTER_KEY: 13,
      COMMAND_KEY: 91,
      CTRL_KEY: 17,
      COPY_KEY: 67,
      PASTE_KEY: 86,
      nodeRadius: 70,
      rectWidth: 140,
      rectHeight: 100,
      rectRound: 14
    };

    /* PROTOTYPE FUNCTIONS */

    GraphCreator.prototype.dragmove = function(d) {
      var thisGraph = this;
      if ($scope.state.shiftNodeDrag){
        var sourceX = (d.uiInfo.xPos + $scope.state.shiftNodeDragXPos);
        var sourceY = (d.uiInfo.yPos + $scope.state.shiftNodeDragYPos);
        var targetX = d3.mouse(thisGraph.svgG.node())[0];
        var targetY = d3.mouse(this.svgG.node())[1];
        var sourceTangentX;

        if ($scope.state.mouseDownNodeLane) {
          sourceTangentX = sourceX + (targetX - sourceX) / 2;
        } else if ($scope.state.mouseDownNodeEventLane) {
          sourceTangentX = sourceX;
        }

        var sourceTangentY = sourceY;
        var targetTangentX = targetX - (targetX - sourceX) / 2;
        var targetTangentY = targetY;

        thisGraph.dragLine.attr('d', 'M ' + sourceX + ',' + sourceY +
        'C' + sourceTangentX + ',' + sourceTangentY + ' ' +
        targetTangentX + ',' + targetTangentY + ' ' +
        targetX + ',' + targetY);
      } else{
        $scope.$apply(function() {
          d.uiInfo.xPos += d3.event.dx;
          d.uiInfo.yPos +=  d3.event.dy;
          thisGraph.updateGraph();
        });
      }
    };

    GraphCreator.prototype.deleteGraph = function(){
      var thisGraph = this;
      thisGraph.nodes = [];
      thisGraph.edges = [];
      $scope.state.selectedNode = null;
      $scope.state.selectedEdge = null;

      $('.graph-bootstrap-tooltip').each(function() {
        var $this = $(this);
        $this.tooltip('destroy');
      });

      thisGraph.updateGraph();
    };

    /* select all text in element: taken from http://stackoverflow.com/questions/6139107/programatically-select-text-in-a-contenteditable-html-element */
    GraphCreator.prototype.selectElementContents = function(el) {
      var range = document.createRange();
      range.selectNodeContents(el);
      var sel = window.getSelection();
      sel.removeAllRanges();
      sel.addRange(range);
    };

    /**
     * http://bl.ocks.org/mbostock/7555321
     *
     * @param gEl
     * @param title
     */
    GraphCreator.prototype.insertTitleLinebreaks = function (gEl, title) {
      var el = gEl.append('text')
        .attr('text-anchor','middle')
        .attr('x', 50)
        .attr('y', 75),
        text = el,
        words = title.split(/\s+/).reverse(),
        word,
        line = [],
        lineNumber = 0,
        lineHeight = 1.1, // ems
        y = text.attr('y'),
        dy = 0,
        tspan = text.text(null).append('tspan').attr('x', 70).attr('y', y).attr('dy', dy + 'em'),
        totalLines = 1;

      if (words.length === 1) {
        tspan.text(title.substring(0, 23));
      } else {
        while (word = words.pop()) {
          line.push(word);
          tspan.text(line.join(' '));
          if (tspan.node().getComputedTextLength() > this.consts.rectWidth - 10) {
            line.pop();
            tspan.text(line.join(' ').substring(0, 23));

            if (totalLines === 2) {
              break;
            }

            line = [word];
            tspan = text.append('tspan').attr('x', 70).attr('y', y).attr('dy', ++lineNumber * lineHeight + dy + 'em').text(word);
            totalLines++;
          }
        }
      }

    };

    // remove edges associated with a node
    GraphCreator.prototype.spliceLinksForNode = function(node) {
      var thisGraph = this,
        toSplice = thisGraph.edges.filter(function(l) {
          return (l.source.instanceName === node.instanceName || l.target.instanceName === node.instanceName);
        });
      toSplice.map(function(l) {
        thisGraph.edges.splice(thisGraph.edges.indexOf(l), 1);
      });
    };

    GraphCreator.prototype.replaceSelectEdge = function(d3Path, edgeData){
      var thisGraph = this;
      if ($scope.state.selectedEdge){
        thisGraph.removeSelectFromEdge();
      }
      d3Path.classed(thisGraph.consts.selectedClass, true);
      $scope.state.selectedEdge = edgeData;
    };

    GraphCreator.prototype.replaceSelectNode = function(d3Node, nodeData){
      var thisGraph = this;
      if ($scope.state.selectedNode){
        thisGraph.removeSelectFromNode();
      }
      d3Node.classed(this.consts.selectedClass, true);
      $scope.state.selectedNode = nodeData;
    };

    GraphCreator.prototype.removeSelectFromNode = function(){
      var thisGraph = this;
      thisGraph.rects.filter(function(cd){
        return cd.instanceName === $scope.state.selectedNode.instanceName;
      }).classed(thisGraph.consts.selectedClass, false);
      $scope.state.selectedNode = null;
    };

    GraphCreator.prototype.removeSelectFromEdge = function(){
      var thisGraph = this;
      thisGraph.paths.filter(function(cd){
        return cd === $scope.state.selectedEdge;
      }).classed(thisGraph.consts.selectedClass, false);
      $scope.state.selectedEdge = null;
    };

    GraphCreator.prototype.pathMouseDown = function(d3path, d){
      var thisGraph = this,
        state = $scope.state;
      d3.event.stopPropagation();
      state.mouseDownLink = d;

      if (state.selectedNode){
        thisGraph.removeSelectFromNode();
      }

      var prevEdge = state.selectedEdge;
      if (!prevEdge || prevEdge !== d){
        thisGraph.replaceSelectEdge(d3path, d);
      }
    };

    // mousedown on node
    GraphCreator.prototype.stageMouseDown = function(d3node, d){
      var thisGraph = this,
        state = $scope.state;
      d3.event.stopPropagation();
      state.mouseDownNode = d;
      if (state.shiftNodeDrag){
        // reposition dragged directed edge
        thisGraph.dragLine.classed('hidden', false)
          .attr('d', 'M' + d.uiInfo.xPos + ',' + d.uiInfo.yPos + 'L' + d.uiInfo.xPos + ',' + d.uiInfo.yPos);
      }
    };

    // mouseup on nodes
    GraphCreator.prototype.stageMouseUp = function(d3node, d){
      var thisGraph = this,
        state = $scope.state,
        consts = thisGraph.consts;
      // reset the states
      state.shiftNodeDrag = false;
      d3node.classed(consts.connectClass, false);

      var mouseDownNode = state.mouseDownNode;
      var mouseDownNodeLane = state.mouseDownNodeLane;
      var mouseDownNodeEventLane = state.mouseDownNodeEventLane;

      if (!mouseDownNode) {
        return;
      }

      thisGraph.dragLine.classed('hidden', true);

      if (mouseDownNode.instanceName && mouseDownNode.instanceName !== d.instanceName &&
        d.uiInfo.stageType !== pipelineConstant.SOURCE_STAGE_TYPE && !thisGraph.isReadOnly){
        // we're in a different node: create new edge for mousedown edge and add to graph
        var newEdge = {
          source: mouseDownNode,
          target: d
        };
        if (mouseDownNodeLane) {
          newEdge.outputLane = mouseDownNodeLane;
        } else if (mouseDownNodeEventLane) {
          newEdge.eventLane = mouseDownNodeEventLane;
        }

        var filtRes = thisGraph.paths.filter(function(d){
          return d.source.instanceName === newEdge.source.instanceName &&
            d.target.instanceName === newEdge.target.instanceName;
        });
        if (!filtRes[0].length) {
          thisGraph.edges.push(newEdge);
          thisGraph.updateGraph();

          $scope.$apply(function() {
            //Double Check
            if (newEdge.source.instanceName !== newEdge.target.instanceName) {
              if (!newEdge.target.inputLanes) {
                newEdge.target.inputLanes = [];
              }

              if (newEdge.source.outputLanes && newEdge.source.outputLanes.length && mouseDownNodeLane) {
                newEdge.target.inputLanes.push(mouseDownNodeLane);
              } else if (newEdge.source.eventLanes && newEdge.source.eventLanes.length && mouseDownNodeEventLane) {
                newEdge.target.inputLanes.push(mouseDownNodeEventLane);
              }
            }
          });
        }
      } else {
        state.justDragged = false;
        if (state.selectedEdge){
          thisGraph.removeSelectFromEdge();
        }
        var prevNode = state.selectedNode;

        if (!prevNode || prevNode.instanceName !== d.instanceName){
          thisGraph.replaceSelectNode(d3node, d);
        }
      }
      state.mouseDownNode = null;
      state.mouseDownNodeLane = null;
      state.mouseDownNodeEventLane = null;
    }; // end of rects mouseup

    // mousedown on main svg
    GraphCreator.prototype.svgMouseDown = function(){
      $scope.state.graphMouseDown = true;
    };

    // mouseup on main svg
    GraphCreator.prototype.svgMouseUp = function(){
      var thisGraph = this,
        state = $scope.state;
      if (state.justScaleTransGraph) {
        // dragged not clicked
        state.justScaleTransGraph = false;
      } else if (state.shiftNodeDrag){
        // dragged from node
        state.shiftNodeDrag = false;
        thisGraph.dragLine.classed('hidden', true);
      } else if ($scope.state.graphMouseDown && !this.isPreviewMode) {
        if ($scope.state.selectedNode) {
          this.removeSelectFromNode();
        } else if ($scope.state.selectedEdge) {
          this.removeSelectFromEdge();
        }

        $scope.$apply(function(){
          $scope.$emit('onRemoveNodeSelection', {
            selectedObject: undefined,
            type: pipelineConstant.PIPELINE
          });
        });
      }
      state.graphMouseDown = false;
    };

    // keydown on main svg
    GraphCreator.prototype.svgKeyDown = function() {
      var thisGraph = this,
        state = $scope.state,
        consts = thisGraph.consts;

      // make sure repeated key presses don't register for each keydown
      if (state.lastKeyDown !== -1 && state.lastKeyDown !== consts.COMMAND_KEY &&
        state.lastKeyDown !== consts.CTRL_KEY) {
        return;
      }

      state.lastKeyDown = d3.event.keyCode;
      var selectedNode = state.selectedNode,
        selectedEdge = state.selectedEdge;

      switch(d3.event.keyCode) {
        case consts.BACKSPACE_KEY:
        case consts.DELETE_KEY:
          d3.event.preventDefault();
          $scope.$apply(function() {
            $scope.deleteSelected();
          });
          break;

        case consts.COPY_KEY:
          if((d3.event.metaKey || d3.event.ctrlKey) && selectedNode) {
            $rootScope.common.copiedStage = selectedNode;
          }
          break;


        case consts.PASTE_KEY:
          if (thisGraph.isReadOnly) {
            return;
          }
          if((d3.event.metaKey || d3.event.ctrlKey) && $rootScope.common.copiedStage) {
            $scope.$apply(function() {
              $scope.$emit('onPasteNode', $rootScope.common.copiedStage);
            });
          }
          break;
      }
    };

    GraphCreator.prototype.svgKeyUp = function() {
      $scope.state.lastKeyDown = -1;
    };

    GraphCreator.prototype.addNode = function(node, edges, relativeX, relativeY) {
      var thisGraph = this;

      if (relativeX && relativeY) {
        var offsets = $element[0].getBoundingClientRect(),
          top = offsets.top,
          left = offsets.left,
          currentTranslatePos = thisGraph.zoom.translate(),
          startX = (currentTranslatePos[0] + left),
          startY = (currentTranslatePos[1] + top);

        node.uiInfo.xPos = (relativeX - startX)/ $scope.state.currentScale;
        node.uiInfo.yPos = (relativeY - startY)/ $scope.state.currentScale;
      }

      thisGraph.nodes.push(node);

      if (edges) {
        thisGraph.edges = edges;
      }

      thisGraph.updateGraph();
      thisGraph.selectNode(node);

      if (!relativeX) {
        thisGraph.moveNodeToVisibleArea(node);
      }

    };


    GraphCreator.prototype.selectNode = function(node) {
      var thisGraph = this,
        nodeExists,
        addedNode = thisGraph.rects.filter(function(cd){
          if (cd.instanceName === node.instanceName) {
            nodeExists = true;
          }
          return cd.instanceName === node.instanceName;
        });

      if (nodeExists) {
        thisGraph.replaceSelectNode(addedNode, node);
      }

    };

    GraphCreator.prototype.selectEdge = function(edge) {
      var thisGraph = this,
        edgeExists,
        addedEdge = thisGraph.paths.filter(function(d){
          if (d.source.instanceName === edge.source.instanceName &&
            d.target.instanceName === edge.target.instanceName) {
            edgeExists = true;
            return true;
          }
          return false;
        });

      if (edgeExists) {
        thisGraph.replaceSelectEdge(addedEdge, edge);
      }
    };

    // call to propagate changes to graph
    GraphCreator.prototype.updateGraph = function(){

      var thisGraph = this,
        consts = thisGraph.consts,
        state = $scope.state,
        stageErrorCounts = thisGraph.stageErrorCounts,
        firstConfigIssue;

      if (graph.isPreviewMode) {
        stageErrorCounts = graph.previewStageErrorCounts;
      }

      thisGraph.paths = thisGraph.paths.data(thisGraph.edges, function(d){
        return String(d.source.instanceName) + '+' + String(d.target.instanceName);
      });

      // update existing nodes
      thisGraph.rects = thisGraph.rects.data(thisGraph.nodes, function(d) {
        return d.instanceName;
      });
      thisGraph.rects.attr('transform', function(d) {
        return 'translate(' + (d.uiInfo.xPos) + ',' + (d.uiInfo.yPos) + ')';
      });

      // add new nodes
      var newGs= thisGraph.rects.enter()
        .append('g');

      newGs.classed(consts.rectGClass, true)
        .attr('transform', function(d){return 'translate(' + d.uiInfo.xPos + ',' + d.uiInfo.yPos + ')';})
        .on('mouseover', function(d){
          if (state.shiftNodeDrag){
            d3.select(this).classed(consts.connectClass, true);
          }
        })
        .on('mouseout', function(d){
          d3.select(this).classed(consts.connectClass, false);
        })
        .on('mousedown', function(d){
          thisGraph.stageMouseDown.call(thisGraph, d3.select(this), d);

          var options = {
            selectedObject: d,
            type: pipelineConstant.STAGE_INSTANCE
          };

          if (firstConfigIssue) {
            options.detailTabName = 'configuration';
            options.configGroup = firstConfigIssue.configGroup;
            options.configName =  firstConfigIssue.configName;
          }

          if ($scope.state.showBadRecords) {
            options.detailTabName = 'errors';
          } else if ($scope.state.showConfiguration) {
            options.detailTabName = 'configuration';
          }

          $scope.$apply(function(){
            $scope.$emit('onNodeSelection', options);
          });
        })
        .on('mouseup', function(d){
          thisGraph.stageMouseUp.call(thisGraph, d3.select(this), d);
        })
        .call(thisGraph.drag);

      newGs.append('rect')
        .attr({
          'height': this.consts.rectHeight,
          'width': this.consts.rectWidth,
          'rx': this.consts.rectRound,
          'ry': this.consts.rectRound
        });

      //Event Connectors
      newGs.append('circle')
        .filter(function(d) {
          return d.eventLanes.length;
        })
        .attr({
          'cx': consts.rectWidth/2,
          'cy': consts.rectHeight,
          'r': 10,
          'class': function(d) {
            return 'graph-bootstrap-tooltip ' + d.eventLanes[0]
          },
          'title': 'Events'
        })
        .on('mousedown', function(d){
          $scope.state.shiftNodeDrag = true;
          $scope.state.shiftNodeDragXPos = consts.rectWidth/2;
          $scope.state.shiftNodeDragYPos = consts.rectHeight;
          $scope.state.mouseDownNodeEventLane = d.eventLanes[0];
        });

      newGs.append('text')
        .filter(function(d) {
          return d.eventLanes.length;
        })
        .attr({
          'x': consts.rectWidth/2 - 5,
          'y': consts.rectHeight + 5,
          'class': 'lane-number graph-bootstrap-tooltip',
          'title': 'Events'
        })
        .text('E')
        .on('mousedown', function(d){
          $scope.state.shiftNodeDrag = true;
          $scope.state.shiftNodeDragXPos = consts.rectWidth/2;
          $scope.state.shiftNodeDragYPos = consts.rectHeight;
          $scope.state.mouseDownNodeEventLane = d.eventLanes[0];
        });

      //Input Connectors
      newGs.append('circle')
        .filter(function(d) {
          return d.uiInfo.stageType !== pipelineConstant.SOURCE_STAGE_TYPE;
        })
        .attr({
          'cx': 0,
          'cy': consts.rectHeight/2,
          'r': 10
        });

      //Output Connectors
      newGs.each(function(d) {
        var stageNode = d3.select(this);

        //Output Connectors

        if (d.uiInfo.stageType !== pipelineConstant.TARGET_STAGE_TYPE && d.uiInfo.stageType !== pipelineConstant.EXECUTOR_STAGE_TYPE) {

          var totalLanes = d.outputLanes.length,
            lanePredicatesConfiguration = _.find(d.configuration, function(configuration) {
              return configuration.name === 'lanePredicates';
            }),
            outputStreamLabels = d.uiInfo.outputStreamLabels;
          angular.forEach(d.outputLanes, function(lane, index) {
            var y = Math.round(((consts.rectHeight) / (2 * totalLanes) ) +
              ((consts.rectHeight * (index))/totalLanes)),
              lanePredicate = lanePredicatesConfiguration ? lanePredicatesConfiguration.value[index] : undefined,
              outputStreamLabel = outputStreamLabels ? outputStreamLabels[index] : undefined;
            stageNode
              .append('circle')
              .attr({
                'cx': consts.rectWidth,
                'cy': y,
                'r': 10,
                'class': 'graph-bootstrap-tooltip ' + lane,
                'title': lanePredicate ? lanePredicate.predicate : ''
              }).on('mousedown', function(d){
                $scope.state.shiftNodeDrag = true;
                $scope.state.shiftNodeDragXPos = thisGraph.consts.rectWidth;
                $scope.state.shiftNodeDragYPos = y;
                $scope.state.mouseDownNodeLane = lane;
              });

            if (totalLanes > 1) {
              stageNode
                .append('text')
                .attr({
                  'x': consts.rectWidth - 3,
                  'y': y + 5,
                  'class': 'lane-number graph-bootstrap-tooltip',
                  'title': lanePredicate ? lanePredicate.predicate : outputStreamLabel
                })
                .text(index+1)
                .on('mousedown', function(d){
                  $scope.state.shiftNodeDrag = true;
                  $scope.state.shiftNodeDragXPos = thisGraph.consts.rectWidth;
                  $scope.state.shiftNodeDragYPos = y;
                  $scope.state.mouseDownNodeLane = lane;
                });
            }

          });
        }

        thisGraph.insertTitleLinebreaks(stageNode, d.uiInfo.label);
      });

      //Add Stage icons
      newGs.append('svg:image')
        .attr('class', 'node-icon')
        .attr('x',(consts.rectWidth - 48)/2)
        .attr('y',10)
        .attr('width', 48)
        .attr('height', 48)
        .attr('xlink:href', function(d) {
          return 'rest/v1/definitions/stages/' + d.library + '/' + d.stageName + '/icon';
        });

      //Add Error icons
      newGs.append('svg:foreignObject')
        .filter(function(d) {
          return thisGraph.issues && thisGraph.issues.stageIssues &&
            thisGraph.issues.stageIssues[d.instanceName];
        })
        .attr('width', 30)
        .attr('height', 30)
        .attr('x', consts.rectWidth - 20)
        .attr('y', consts.rectHeight - 12)
        .append('xhtml:span')
        .attr('class', 'node-warning fa fa-exclamation-triangle graph-bootstrap-tooltip')
        .attr('title', function(d) {
          var issues = thisGraph.issues.stageIssues[d.instanceName],
            title = '<span class="stage-errors-tooltip">';

          angular.forEach(issues, function(issue) {
            title += pipelineService.getIssuesMessage(d, issue) + '<br>';
          });

          title += '</span>';

          return title;
        })
        .attr('data-html', true)
        .attr('data-placement', 'bottom')
        .on('mousedown', function(d) {
          var issues = thisGraph.issues.stageIssues[d.instanceName];

          angular.forEach(issues, function(issue) {
            if (issue.configName && !firstConfigIssue) {
              firstConfigIssue = issue;
            }
          });
        })
        .on('mouseup', function(d){
          firstConfigIssue = undefined;
        });

      //Add Configuration Icon
      /*
      newGs.append('svg:foreignObject')
        .filter(function(d) {
          var configurationExists = false;

          angular.forEach(d.configuration, function(c) {
            if (c.value !== undefined && c.value !== null) {
              configurationExists = true;
            }
          });

          return configurationExists;
        })
        .attr('width', 20)
        .attr('height', 20)
        .attr('x', consts.rectWidth - 25)
        .attr('y', 10)
        .append('xhtml:span')
        .attr('class', 'node-config fa fa-gear graph-bootstrap-tooltip')
        .attr('title', function(d) {
          return pipelineService.getStageConfigurationHTML(d);
        })
        .attr('data-html', true)
        .attr('data-placement', 'bottom')
        .on('mousedown', function() {
          $scope.state.showConfiguration = true;
        })
        .on('mouseup', function() {
          $scope.state.showConfiguration = false;
        });
      */

      //Add bad records count
      newGs.append('svg:foreignObject')
        .attr('width', 100)
        .attr('height', 30)
        .attr('x', 10)
        .attr('y', 10)
        .append('xhtml:span')
        .attr('title', graphErrorBadgeLabel)
        .attr('class', 'badge alert-danger pointer graph-bootstrap-tooltip')
        .style('visibility', function(d) {
          if (stageErrorCounts && stageErrorCounts[d.instanceName] &&
            parseInt(stageErrorCounts[d.instanceName]) > 0) {
            return 'visible';
          } else {
            return 'hidden';
          }
        })
        .html(function(d) {
          if (stageErrorCounts) {
            return $filter('abbreviateNumber')(stageErrorCounts[d.instanceName]);
          }
          return '';
        })
        .on('mousedown', function() {
          $scope.state.showBadRecords = true;
        })
        .on('mouseup', function() {
          $scope.state.showBadRecords = false;
        });


      // remove old nodes
      thisGraph.rects.exit().remove();

      var paths = thisGraph.paths;

      // update existing paths
      paths.selectAll('path')
        .style('marker-end', 'url(' + $location.absUrl() + '#end-arrow)')
        .classed(consts.selectedClass, function(d) {
          return d === state.selectedEdge;
        })
        .attr('d', function(d) {
          return thisGraph.getPathDValue(d);
        });

      paths.selectAll('.edge-preview-container')
        .classed(consts.selectedClass, function(d) {
          return d === state.selectedEdge;
        })
        .attr('x', function(d) {
          if (d.outputLane) {
            return (d.source.uiInfo.xPos + (consts.rectWidth) + (d.target.uiInfo.xPos -30))/2;
          } else if (d.eventLane) {
            return (d.source.uiInfo.xPos + (consts.rectWidth/2) + (d.target.uiInfo.xPos -30))/2;
          }
        })
        .attr('y', function(d) {
          if (d.outputLane) {
            var totalLanes = d.source.outputLanes.length,
              outputLaneIndex = _.indexOf(d.source.outputLanes, d.outputLane),
              y = Math.round(((consts.rectHeight) / (2 * totalLanes) ) + ((consts.rectHeight * (outputLaneIndex))/totalLanes));

            return ((d.source.uiInfo.yPos + y + d.target.uiInfo.yPos + consts.rectHeight/2))/2 - 20;
          } else if (d.eventLane) {
            return ((d.source.uiInfo.yPos + d.target.uiInfo.yPos + consts.rectHeight))/2 + 30;
          }
        });

      var pathNewGs= paths.enter()
        .append('g');


      pathNewGs
        .classed(consts.pathGClass, true)
        .on('mousedown', function(d) {
          if (!thisGraph.isPreviewMode) {
            thisGraph.pathMouseDown.call(thisGraph, d3.select(this), d);
            $scope.$apply(function(){
              $scope.$emit('onEdgeSelection', d);
            });
          }
        })
        .on('mouseup', function(d) {
          state.mouseDownLink = null;
        });

      // add new paths
      pathNewGs
        .append('path')
        .style('marker-end', 'url(' + $location.absUrl() + '#end-arrow)')
        .classed('link', true)
        .attr('d', function(d) {
          return thisGraph.getPathDValue(d);
        });


      if (thisGraph.showEdgePreviewIcon) {
        pathNewGs
          .append('svg:foreignObject')
          .attr('class', 'edge-preview-container graph-bootstrap-tooltip')
          .attr('width', 25)
          .attr('height', 25)
          .attr('x', function(d) {
            if (d.outputLane) {
              return (d.source.uiInfo.xPos + (consts.rectWidth) + (d.target.uiInfo.xPos -30))/2;
            } else if (d.eventLane) {
              return (d.source.uiInfo.xPos + (consts.rectWidth/2) + (d.target.uiInfo.xPos -30))/2;
            }
          })
          .attr('y', function(d) {
            if (d.outputLane) {
              var totalLanes = d.source.outputLanes.length,
                outputLaneIndex = _.indexOf(d.source.outputLanes, d.outputLane),
                y = Math.round(((consts.rectHeight) / (2 * totalLanes) ) + ((consts.rectHeight * (outputLaneIndex))/totalLanes));

              return ((d.source.uiInfo.yPos + y + d.target.uiInfo.yPos + consts.rectHeight/2))/2 - 20;
            } else if (d.eventLane) {
              return ((d.source.uiInfo.yPos + d.target.uiInfo.yPos + consts.rectHeight))/2 + 30;
            }
          })
          .append('xhtml:span')
          .attr('class', function(d) {
            return getEdgePreviewIcon(graph.pipelineRules, graph.triggeredAlerts, d);
          });
      }

      // remove old links
      paths.exit().remove();

      //Pipeline Warning
      if (graph.issues && graph.issues.pipelineIssues) {
        var pipelineIssuesStr = '';

        angular.forEach(graph.issues.pipelineIssues, function(issue) {
          pipelineIssuesStr += issue.message + '<br>';
        });

        if (graph.errorStage && graph.errorStage.instanceName &&
          graph.issues.stageIssues[graph.errorStage.instanceName]) {
          angular.forEach(graph.issues.stageIssues[graph.errorStage.instanceName], function(issue) {
            pipelineIssuesStr += issue.message + '<br>';
          });
        }

        if (pipelineIssuesStr) {
          graphWarning
            .attr({
              'title': pipelineIssuesStr,
              'data-original-title': pipelineIssuesStr
            })
            .style('visibility', 'visible');
        } else {
          graphWarning.style('visibility', 'hidden');
        }

      } else {
        graphWarning.style('visibility', 'hidden');
      }


      $('.graph-bootstrap-tooltip').each(function() {
        var $this = $(this),
          title = $this.attr('title');
        if (title) {
          $this.attr('title', '');
          $this.tooltip({
            title: title,
            container:'body'
          });
        }
      });
    };

    GraphCreator.prototype.getPathDValue = function(d) {
      var thisGraph = this;
      var consts = thisGraph.consts;
      var sourceX;
      var sourceY;
      var targetX;
      var targetY;
      var sourceTangentX;
      var sourceTangentY;
      var targetTangentX;
      var targetTangentY;

      if (d.outputLane) {
        var totalLanes = d.source.outputLanes.length;
        var outputLaneIndex = _.indexOf(d.source.outputLanes, d.outputLane);
        var y = Math.round(
          ((consts.rectHeight) / (2 * totalLanes) ) + ((consts.rectHeight * (outputLaneIndex)) / totalLanes)
        );
        sourceX = (d.source.uiInfo.xPos + consts.rectWidth);
        sourceY = (d.source.uiInfo.yPos + y);

        if (d.target.uiInfo.xPos > (sourceX + 30)) {
          targetX = (d.target.uiInfo.xPos - 30);
        } else if (d.target.uiInfo.xPos > sourceX) {
          targetX = (d.target.uiInfo.xPos + 10);
        } else {
          targetX = (d.target.uiInfo.xPos + 30);
        }
        targetY = (d.target.uiInfo.yPos + consts.rectWidth/2 - 20);

        sourceTangentX = sourceX + (targetX - sourceX)/2;
        sourceTangentY = sourceY;
        targetTangentX = targetX - (targetX - sourceX)/2;
        targetTangentY = targetY;

      } else if (d.eventLane) {
        sourceX = (d.source.uiInfo.xPos + (consts.rectWidth / 2));
        sourceY = (d.source.uiInfo.yPos + consts.rectHeight);

        if (d.target.uiInfo.xPos > (sourceX + 30)) {
          targetX = (d.target.uiInfo.xPos - 30);
        } else if (d.target.uiInfo.xPos > sourceX) {
          targetX = (d.target.uiInfo.xPos + 10);
        } else {
          targetX = (d.target.uiInfo.xPos + 30);
        }
        targetY = (d.target.uiInfo.yPos + consts.rectWidth/2 - 20);

        sourceTangentX = sourceX;
        sourceTangentY = sourceY;
        targetTangentX = targetX - (targetX - sourceX)/2;
        targetTangentY = targetY;
      }

      return 'M ' + sourceX + ',' + sourceY +
        'C' + sourceTangentX + ',' + sourceTangentY + ' ' +
        targetTangentX + ',' + targetTangentY + ' ' +
        targetX + ',' + targetY;
    };

    GraphCreator.prototype.zoomed = function() {
      $scope.state.justScaleTransGraph = true;

      if (showTransition) {
        showTransition = false;
        this.svgG
          .transition()
          .duration(750)
          .attr('transform', 'translate(' + d3.event.translate + ') scale(' + d3.event.scale + ')');
      } else {
        this.svgG
          .attr('transform', 'translate(' + d3.event.translate + ') scale(' + d3.event.scale + ')');
      }

    };

    GraphCreator.prototype.zoomIn = function() {
      if ($scope.state.currentScale < this.zoom.scaleExtent()[1]) {
        $scope.state.currentScale = Math.round(($scope.state.currentScale + 0.1) * 10)/10 ;
        this.zoom.scale($scope.state.currentScale).event(this.svg);
      }
    };

    GraphCreator.prototype.zoomOut = function() {
      if ($scope.state.currentScale > this.zoom.scaleExtent()[0]) {
        $scope.state.currentScale = Math.round(($scope.state.currentScale - 0.1) * 10)/10 ;
        this.zoom.scale($scope.state.currentScale).event(this.svg);
      }
    };

    GraphCreator.prototype.panUp = function() {
      var translatePos = this.zoom.translate();
      translatePos[1] += 150;
      showTransition = true;
      this.zoom.translate(translatePos).event(this.svg);
    };

    GraphCreator.prototype.panRight = function() {
      var translatePos = this.zoom.translate();
      translatePos[0] -= 250;
      showTransition = true;
      this.zoom.translate(translatePos).event(this.svg);
    };

    GraphCreator.prototype.panHome = function(onlyZoomIn) {
      var thisGraph = this,
        nodes = thisGraph.nodes,
        consts = thisGraph.consts,
        svgWidth = thisGraph.svg.style('width').replace('px', ''),
        svgHeight = thisGraph.svg.style('height').replace('px', ''),
        xScale,
        yScale,
        minX,
        minY,
        maxX,
        maxY,
        currentScale;

      if (!nodes || nodes.length < 1) {
        return;
      }

      angular.forEach(nodes, function(node) {
        var xPos = node.uiInfo.xPos,
          yPos = node.uiInfo.yPos;
        if (minX === undefined) {
          minX = xPos;
          maxX = xPos;
          minY = yPos;
          maxY = yPos;
        } else {
          if (xPos < minX) {
            minX = xPos;
          }

          if (xPos > maxX) {
            maxX = xPos;
          }

          if (yPos < minY) {
            minY = yPos;
          }

          if (yPos > maxY) {
            maxY = yPos;
          }
        }
      });

      xScale =  svgWidth / (maxX + consts.rectWidth + 30);
      yScale =  svgHeight / (maxY + consts.rectHeight + 30);

      showTransition = true;
      currentScale = xScale < yScale ? xScale : yScale;

      if (currentScale < 1 || !onlyZoomIn) {
        $scope.state.currentScale = currentScale;
        this.zoom.translate([0, 0]).scale(currentScale).event(this.svg);
      }

    };


    GraphCreator.prototype.panLeft = function() {
      var translatePos = this.zoom.translate();
      translatePos[0] += 250;
      showTransition = true;
      this.zoom.translate(translatePos).event(this.svg);
    };

    GraphCreator.prototype.panDown = function() {
      var translatePos = this.zoom.translate();
      translatePos[1] -= 150;
      showTransition = true;
      this.zoom.translate(translatePos).event(this.svg);
    };

    GraphCreator.prototype.moveNodeToCenter = function(stageInstance) {
      var thisGraph = this,
        consts = thisGraph.consts,
        svgWidth = thisGraph.svg.style('width').replace('px', ''),
        svgHeight = thisGraph.svg.style('height').replace('px', ''),
        currentScale = $scope.state.currentScale,
        x = svgWidth / 2 - (stageInstance.uiInfo.xPos + consts.rectWidth/2) * currentScale,
        y = svgHeight / 2 - (stageInstance.uiInfo.yPos + consts.rectHeight/2) * currentScale;

      showTransition = true;
      this.zoom.translate([x, y]).event(this.svg);
    };

    GraphCreator.prototype.moveNodeToVisibleArea = function(stageInstance) {
      var thisGraph = this,
        currentScale = $scope.state.currentScale,
        svgWidth = thisGraph.svg.style('width').replace('px', ''),
        svgHeight = thisGraph.svg.style('height').replace('px', ''),
        currentTranslatePos = this.zoom.translate(),
        startX = -(currentTranslatePos[0]),
        startY = -(currentTranslatePos[1]),
        endX = parseInt(startX) + parseInt(svgWidth),
        endY = parseInt(startY) + parseInt(svgHeight),
        nodeStartXPos = ((stageInstance.uiInfo.xPos) * currentScale),
        nodeStartYPos = ((stageInstance.uiInfo.yPos) * currentScale),
        nodeEndXPos = ((stageInstance.uiInfo.xPos + thisGraph.consts.rectWidth) * currentScale),
        nodeEndYPos = ((stageInstance.uiInfo.yPos + thisGraph.consts.rectHeight) * currentScale);

      if (parseInt(svgWidth) > 0 && parseInt(svgHeight) > 0 &&
        (nodeStartXPos < startX || nodeEndXPos > endX || nodeStartYPos < startY || nodeEndYPos > endY)) {
        thisGraph.moveNodeToCenter(stageInstance);
      }
    };

    GraphCreator.prototype.moveGraphToCenter = function() {
      showTransition = true;
      this.zoom.translate([0,0]).event(this.svg);
    };


    GraphCreator.prototype.clearStartAndEndNode = function() {
      var thisGraph = this;
      thisGraph.rects.classed(thisGraph.consts.startNodeClass, false);
      thisGraph.rects.classed(thisGraph.consts.endNodeClass, false);
    };

    GraphCreator.prototype.addDirtyNodeClass = function() {
      var thisGraph = this;
      thisGraph.rects.selectAll('circle')
        .attr('class', function(d) {
          var currentClass = d3.select(this).attr('class');
          if (currentClass) {
            var currentClassArr = currentClass.split(' '),
              intersection = _.intersection(thisGraph.dirtyLanes, currentClassArr);

            if (intersection && intersection.length) {
              return currentClass + ' dirty';
            }
          }
          return currentClass;
        });
    };

    GraphCreator.prototype.clearDirtyNodeClass = function() {
      var thisGraph = this;
      thisGraph.rects.selectAll('circle')
        .attr('class', function(d) {
          var currentClass = d3.select(this).attr('class');

          if (currentClass && currentClass.indexOf('dirty') !== -1) {
            currentClass = currentClass.replace(/dirty/g, '');
          }

          return currentClass;
        });
    };

    GraphCreator.prototype.updateStartAndEndNode = function(startNode, endNode) {
      var thisGraph = this;

      thisGraph.clearStartAndEndNode();

      if (startNode) {
        thisGraph.rects.filter(function(cd){
          return cd.instanceName === startNode.instanceName;
        }).classed(thisGraph.consts.startNodeClass, true);
      }

      if (endNode) {
        thisGraph.rects.filter(function(cd){
          return cd.instanceName === endNode.instanceName;
        }).classed(thisGraph.consts.endNodeClass, true);
      }

    };

    /** MAIN SVG **/
    var graphContainer, svg, graph, toolbar, graphWarning;

    $scope.$on('updateGraph', function(event, options) {
      var nodes = options.nodes,
        edges = options.edges,
        issues = options.issues,
        selectNode = options.selectNode,
        selectEdge = options.selectEdge,
        stageErrorCounts = options.stageErrorCounts,
        showEdgePreviewIcon = options.showEdgePreviewIcon;

      if (graph !== undefined) {
        graph.deleteGraph();
      } else {
        graphContainer = d3.select($element[0]);
        svg = graphContainer.select('svg');
        graphWarning = graphContainer.select('.warning-toolbar');

        graph = new GraphCreator(svg, nodes, edges || [], issues);
        graph.setIdCt(2);
      }

      graph.nodes = nodes;
      graph.edges = edges;
      graph.issues = issues;
      graph.stageErrorCounts = stageErrorCounts;
      graph.showEdgePreviewIcon = showEdgePreviewIcon;
      graph.isReadOnly = options.isReadOnly;
      graph.pipelineRules = options.pipelineRules;
      graph.triggeredAlerts = options.triggeredAlerts;
      graph.errorStage = options.errorStage;
      graph.updateGraph();

      if (graph.dirtyLanes) {
        graph.addDirtyNodeClass();
      }

      if (selectNode) {
        graph.selectNode(selectNode);
      } else if (selectEdge) {
        graph.selectEdge(selectEdge);
      }

      if (options.fitToBounds) {
        graph.panHome(true);
      }
    });


    angular.extend($scope, {
      state: {
        selectedNode: null,
        selectedEdge: null,
        mouseDownNode: null,
        mouseDownNodeLane: null,
        mouseDownNodeEventLane: null,
        mouseDownLink: null,
        justDragged: false,
        justScaleTransGraph: false,
        lastKeyDown: -1,
        shiftNodeDrag: false,
        selectedText: null,
        currentScale: 1,
        copiedStage: undefined,
        showBadRecords: false,
        showConfiguration: false
      },

      panUp: function($event) {
        graph.panUp();
        $event.preventDefault();
      },

      panDown: function($event) {
        graph.panDown();
        $event.preventDefault();
      },

      panLeft: function($event) {
        graph.panLeft();
        $event.preventDefault();
      },

      panRight: function($event) {
        graph.panRight();
        $event.preventDefault();
      },

      panHome: function($event) {
        graph.panHome();
        $event.preventDefault();
      },

      zoomIn: function($event) {
        graph.zoomIn();
        $event.preventDefault();
      },

      zoomOut: function($event) {
        graph.zoomOut();
        $event.preventDefault();
      },

      /**
       * Callback function on warning icon sign
       * @param $event
       */
      onWarningClick: function($event) {
        if ($scope.state.selectedNode) {
          graph.removeSelectFromNode();
        } else if ($scope.state.selectedEdge) {
          graph.removeSelectFromEdge();
        }

        var options = {
          selectedObject: undefined,
          type: pipelineConstant.PIPELINE
        }, firstConfigIssue;

        if (graph.issues && graph.issues.pipelineIssues) {
          angular.forEach(graph.issues.pipelineIssues, function(issue) {
            if (issue.configName && !firstConfigIssue) {
              firstConfigIssue = issue;
            }
          });

          if (!firstConfigIssue && graph.errorStage && graph.errorStage.instanceName &&
            graph.issues.stageIssues[graph.errorStage.instanceName]) {
            angular.forEach(graph.issues.stageIssues[graph.errorStage.instanceName], function(issue) {
              if (issue.configName && !firstConfigIssue) {
                firstConfigIssue = issue;
                options.errorStage = true;
              }
            });
          }

          if (firstConfigIssue) {
            options.detailTabName = 'configuration';
            options.configGroup = firstConfigIssue.configGroup;
            options.configName =  firstConfigIssue.configName;
          }
        }

        $scope.$emit('onRemoveNodeSelection', options);
      },

      /**
       * Callback function to delete stage/stream
       */
      deleteSelected: function() {
        var state = $scope.state,
          selectedNode = state.selectedNode,
          selectedEdge = state.selectedEdge;

        if (graph.isReadOnly) {
          //Graph is read only
          return;
        }
        if (selectedNode) {

          if (selectedNode.uiInfo.stageType == pipelineConstant.SOURCE_STAGE_TYPE ) {
            var modalInstance = $modal.open({
                templateUrl: 'common/directives/pipelineGraph/deleteOrigin.tpl.html',
                controller: 'DeleteOriginModalInstanceController',
                size: '',
                backdrop: 'static'
              });

            modalInstance.result.then(function (configInfo) {
              deleteSelectedNode(selectedNode);
              $scope.$emit('onOriginStageDelete', selectedNode);
            }, function () {

            });
          } else {
            deleteSelectedNode(selectedNode);
          }
        } else if (selectedEdge) {
          var edgeIndex = graph.edges.indexOf(selectedEdge);
          if (edgeIndex !== -1) {
            //Update pipeline target input lanes.

            if (selectedEdge.eventLane) {
              selectedEdge.target.inputLanes = _.filter(selectedEdge.target.inputLanes, function(inputLane) {
                return !_.contains(selectedEdge.source.eventLanes, inputLane);
              });
            } else {
              selectedEdge.target.inputLanes = _.filter(selectedEdge.target.inputLanes, function(inputLane) {
                return !_.contains(selectedEdge.source.outputLanes, inputLane);
              });
            }

            graph.edges.splice(edgeIndex, 1);
            state.selectedEdge = null;
            $scope.$emit('onRemoveNodeSelection', {
              selectedObject: undefined,
              type: pipelineConstant.PIPELINE
            });
            graph.updateGraph();
          }
        }
      },

      /**
       * Callback function for dupl
       */
      duplicateStage: function() {
        $scope.$emit('onPasteNode', $scope.state.selectedNode);
      }

    });

    $scope.$on('addNode', function(event, stageInstance, edges, relativeX, relativeY) {
      graph.addNode(stageInstance, edges, relativeX, relativeY);
      $(graph.svg[0]).focus();
    });

    $scope.$on('deleteSelectionInGraph', function() {
      $scope.deleteSelected();
    });

    $scope.$on('selectNode', function(event, stageInstance, moveToCenter) {
      if (stageInstance) {

        if (moveToCenter) {
          graph.moveNodeToCenter(stageInstance);
        } else {
          graph.moveNodeToVisibleArea(stageInstance);
        }

        if ($scope.state.selectedEdge) {
          graph.removeSelectFromEdge();
        }

        graph.selectNode(stageInstance);
      } else {
        if ($scope.state.selectedNode){
          graph.removeSelectFromNode();
        }

        if ($scope.state.selectedEdge) {
          graph.removeSelectFromEdge();
        }

        graph.moveGraphToCenter();
      }
    });

    $scope.$on('selectEdge', function(event, edge) {
      graph.moveNodeToVisibleArea(edge.source);
      if ($scope.state.selectedNode) {
        graph.removeSelectFromNode();
      }
      graph.selectEdge(edge);
    });

    $scope.$on('updateErrorCount', function(event, stageInstanceErrorCounts) {
      if (graph) {
        graph.rects.selectAll('span.badge')
          .style('visibility', function(d) {
            if (graph.isPreviewMode) {
              graph.previewStageErrorCounts = stageInstanceErrorCounts;
            }
            if (stageInstanceErrorCounts[d.instanceName] &&
              parseInt(stageInstanceErrorCounts[d.instanceName]) > 0) {
              return 'visible';
            } else {
              return 'hidden';
            }
          })
          .html(function(d) {
            return $filter('abbreviateNumber')(stageInstanceErrorCounts[d.instanceName]);
          });
      }
    });

    $scope.$on('updateDirtyLaneConnector', function(event, dirtyLanes) {
      if (graph) {
        graph.dirtyLanes = dirtyLanes;
        graph.addDirtyNodeClass(dirtyLanes);
      }
    });

    $scope.$on('clearDirtyLaneConnector', function() {
      if (graph) {
        graph.dirtyLanes = undefined;
        graph.clearDirtyNodeClass();
      }
    });

    $scope.$on('moveGraphToCenter', function() {
      if (graph) {
        if ($scope.state.selectedNode) {
          graph.removeSelectFromNode();
        }

        if ($scope.state.selectedEdge) {
          graph.removeSelectFromEdge();
        }

        graph.clearStartAndEndNode();
        graph.clearDirtyNodeClass();
        graph.moveGraphToCenter();
      }
    });

    $scope.$on('updateStartAndEndNode', function(event, startNode, endNode) {
      if (graph) {
        graph.updateStartAndEndNode(startNode, endNode);
      }
    });

    $scope.$on('clearStartAndEndNode', function() {
      if (graph) {
        graph.clearStartAndEndNode();
      }
    });

    $scope.$on('setGraphReadOnly', function(event, flag) {
      if (graph) {
        graph.isReadOnly = flag;
      }
    });

    $scope.$on('setGraphPreviewMode', function(event, flag) {
      if (graph) {
        graph.isPreviewMode = flag;
        if (!flag) {
          graph.clearStartAndEndNode();
          graph.clearDirtyNodeClass();
          graph.rects.selectAll('span.badge').style('visibility', 'hidden');
        }
      }
    });

    $scope.$on('updateEdgePreviewIconColor', function(event, pipelineRules, triggeredAlerts) {
      if (graph) {
        graph.paths.selectAll('span.edge-preview')
          .attr('class', function(d) {
            return getEdgePreviewIcon(pipelineRules, triggeredAlerts, d);
          });
      }
    });

    var getEdgePreviewIcon = function(pipelineRules, triggeredAlerts, d) {
      var atLeastOneRuleDefined = false;
      var atLeastOneRuleActive = false;
      var triggeredAlert = _.filter(triggeredAlerts, function (triggered) {
        return triggered.ruleDefinition.lane && (triggered.ruleDefinition.lane === d.outputLane ||
          triggered.ruleDefinition.lane === d.eventLane);
      });

      _.each(pipelineRules.dataRuleDefinitions, function(ruleDefn) {
        if (ruleDefn.lane === d.outputLane || ruleDefn.lane === d.eventLane) {
          if (ruleDefn.enabled) {
            atLeastOneRuleActive = true;
          }
          atLeastOneRuleDefined = true;
        }
      });

      _.each(pipelineRules.driftRuleDefinitions, function(ruleDefn) {
        if (ruleDefn.lane === d.outputLane || ruleDefn.lane === d.eventLane) {
          if (ruleDefn.enabled) {
            atLeastOneRuleActive = true;
          }
          atLeastOneRuleDefined = true;
        }
      });

      if (triggeredAlert && triggeredAlert.length) {
        return 'fa fa-tachometer fa-16x pointer edge-preview alert-triggered';
      } else if (atLeastOneRuleActive){
        return 'fa fa-tachometer fa-16x pointer edge-preview active-alerts-defined';
      } else if (atLeastOneRuleDefined){
        return 'fa fa-tachometer fa-16x pointer edge-preview alerts-defined';
      } else {
        return 'fa fa-tachometer fa-16x pointer edge-preview';
      }
    };

    var deleteSelectedNode = function(selectedNode) {
      var nodeIndex = graph.nodes.indexOf(selectedNode);
      var state = $scope.state;

      if (nodeIndex !== -1) {
        graph.nodes.splice(nodeIndex, 1);

        //Remove the input lanes in all stages having output/event lanes of delete node.
        _.each(graph.edges, function(edge) {
          if (edge.source.instanceName === selectedNode.instanceName) {
            edge.target.inputLanes = _.filter(edge.target.inputLanes, function(inputLane) {
              return !_.contains(edge.source.outputLanes, inputLane) && !_.contains(edge.source.eventLanes, inputLane);
            });
          }
        });

        graph.spliceLinksForNode(selectedNode);
        state.selectedNode = null;
        $scope.$emit('onRemoveNodeSelection', {
          selectedObject: undefined,
          type: pipelineConstant.PIPELINE
        });
        graph.updateGraph();
      }
    }

  })
  .controller('DeleteOriginModalInstanceController', function ($scope, $modalInstance) {
    angular.extend($scope, {
      yes: function() {
        $modalInstance.close();
      },
      no: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });
