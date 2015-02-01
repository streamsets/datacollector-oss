/**
 * Module definition for Pipeline Graph Directive.
 */

angular.module('pipelineGraphDirectives', ['underscore'])
  .directive('pipelineGraph', function() {
    return {
      restrict: 'E',
      replace: false,
      controller: 'PipelineGraphController'
    };
  })
  .controller('PipelineGraphController', function($scope, $rootScope, $element, _, $filter, pipelineConstant, $translate){

    var showTransition = false,
      graphErrorBadgeLabel = '';

    $translate('global.messages.info.graphErrorBadgeLabel').then(function(translation) {
      graphErrorBadgeLabel = [translation];
    });

    // define graphcreator object
    var GraphCreator = function(svg, nodes, edges, issues){
      var thisGraph = this;
      thisGraph.idct = 0;

      thisGraph.nodes = nodes || [];
      thisGraph.edges = edges || [];
      thisGraph.issues = issues || {};

      thisGraph.state = {
        selectedNode: null,
        selectedEdge: null,
        mouseDownNode: null,
        mouseDownNodeLane: null,
        mouseDownLink: null,
        justDragged: false,
        justScaleTransGraph: false,
        lastKeyDown: -1,
        shiftNodeDrag: false,
        selectedText: null,
        currentScale: 1,
        copiedStage: undefined
      };

      // define arrow markers for graph links

      var markerWidth = 3.5,
        markerHeight = 3.5,
        cRadius = -7, // play with the cRadius value
        refX = cRadius + (markerWidth * 2),
        defs = svg.append('svg:defs');

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
      var margin = {top: -5, right: -5, bottom: -5, left: -5},
        width = 2500 - margin.left - margin.right,
        height = 2500 - margin.top - margin.bottom;

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
        .style('marker-end', 'url(#mark-end-arrow)');

      // svg nodes and edges
      thisGraph.paths = svgG.append('g').selectAll('g');
      thisGraph.rects = svgG.append('g').selectAll('g');


      thisGraph.drag = d3.behavior.drag()
        .origin(function(d){
          return {x: d.uiInfo.xPos, y: d.uiInfo.yPos};
        })
        .on('drag', function(args){
          thisGraph.state.justDragged = true;
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
      if (thisGraph.state.shiftNodeDrag){
        var sourceX = (d.uiInfo.xPos + thisGraph.consts.rectWidth),
          sourceY = (d.uiInfo.yPos + thisGraph.state.shiftNodeDragYPos),
          targetX = d3.mouse(thisGraph.svgG.node())[0],
          targetY = d3.mouse(this.svgG.node())[1],
          sourceTangentX = sourceX + (targetX - sourceX)/2,
          sourceTangentY = sourceY,
          targetTangentX = targetX - (targetX - sourceX)/2,
          targetTangentY = targetY;

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
      thisGraph.state.selectedNode = null;
      thisGraph.state.selectedEdge = null;

      $('.graph-bootstrap-tooltip').each(function() {
        var $this = $(this),
          title = $this.attr('title');
        if(title) {
          $this.tooltip('destroy');
        }
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

      if(words.length === 1) {
        tspan.text(title.substring(0, 23));
      } else {
        while (word = words.pop()) {
          line.push(word);
          tspan.text(line.join(' '));
          if (tspan.node().getComputedTextLength() > this.consts.rectWidth - 10) {
            line.pop();
            tspan.text(line.join(' ').substring(0, 23));

            if(totalLines === 2) {
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
          return (l.source === node || l.target === node);
        });
      toSplice.map(function(l) {
        thisGraph.edges.splice(thisGraph.edges.indexOf(l), 1);
      });
    };

    GraphCreator.prototype.replaceSelectEdge = function(d3Path, edgeData){
      var thisGraph = this;
      if (thisGraph.state.selectedEdge){
        thisGraph.removeSelectFromEdge();
      }
      d3Path.classed(thisGraph.consts.selectedClass, true);
      thisGraph.state.selectedEdge = edgeData;
    };

    GraphCreator.prototype.replaceSelectNode = function(d3Node, nodeData){
      var thisGraph = this;
      if (thisGraph.state.selectedNode){
        thisGraph.removeSelectFromNode();
      }
      d3Node.classed(this.consts.selectedClass, true);
      thisGraph.state.selectedNode = nodeData;
    };

    GraphCreator.prototype.removeSelectFromNode = function(){
      var thisGraph = this;
      thisGraph.rects.filter(function(cd){
        return cd.instanceName === thisGraph.state.selectedNode.instanceName;
      }).classed(thisGraph.consts.selectedClass, false);
      thisGraph.state.selectedNode = null;
    };

    GraphCreator.prototype.removeSelectFromEdge = function(){
      var thisGraph = this;
      thisGraph.paths.filter(function(cd){
        return cd === thisGraph.state.selectedEdge;
      }).classed(thisGraph.consts.selectedClass, false);
      thisGraph.state.selectedEdge = null;
    };

    GraphCreator.prototype.pathMouseDown = function(d3path, d){
      var thisGraph = this,
        state = thisGraph.state;
      d3.event.stopPropagation();
      state.mouseDownLink = d;

      if (state.selectedNode){
        thisGraph.removeSelectFromNode();
      }

      var prevEdge = state.selectedEdge;
      if (!prevEdge || prevEdge !== d){
        thisGraph.replaceSelectEdge(d3path, d);
      } else{
        thisGraph.removeSelectFromEdge();
      }
    };

    // mousedown on node
    GraphCreator.prototype.stageMouseDown = function(d3node, d){
      var thisGraph = this,
        state = thisGraph.state;
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
        state = thisGraph.state,
        consts = thisGraph.consts;
      // reset the states
      state.shiftNodeDrag = false;
      d3node.classed(consts.connectClass, false);

      var mouseDownNode = state.mouseDownNode,
        mouseDownNodeLane = state.mouseDownNodeLane;

      if (!mouseDownNode) {
        return;
      }

      thisGraph.dragLine.classed('hidden', true);

      if (mouseDownNode.instanceName && mouseDownNode.instanceName !== d.instanceName &&
        d.uiInfo.stageType !== pipelineConstant.SOURCE_STAGE_TYPE && !thisGraph.isReadOnly){
        // we're in a different node: create new edge for mousedown edge and add to graph
        var newEdge = {
          source: mouseDownNode,
          target: d,
          outputLane: mouseDownNodeLane
        };
        var filtRes = thisGraph.paths.filter(function(d){
          return d.source.instanceName === newEdge.source.instanceName &&
            d.target.instanceName === newEdge.target.instanceName;
        });
        if (!filtRes[0].length){
          thisGraph.edges.push(newEdge);
          thisGraph.updateGraph();

          $scope.$apply(function() {
            //Double Check
            if(newEdge.source.instanceName !== newEdge.target.instanceName) {
              if(!newEdge.target.inputLanes) {
                newEdge.target.inputLanes = [];
              }

              if(newEdge.source.outputLanes && newEdge.source.outputLanes.length &&
                mouseDownNodeLane) {
                newEdge.target.inputLanes.push(mouseDownNodeLane);
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
    }; // end of rects mouseup

    // mousedown on main svg
    GraphCreator.prototype.svgMouseDown = function(){
      this.state.graphMouseDown = true;
    };

    // mouseup on main svg
    GraphCreator.prototype.svgMouseUp = function(){
      var thisGraph = this,
        state = thisGraph.state;
      if (state.justScaleTransGraph) {
        // dragged not clicked
        state.justScaleTransGraph = false;
      } else if (state.shiftNodeDrag){
        // dragged from node
        state.shiftNodeDrag = false;
        thisGraph.dragLine.classed('hidden', true);
      } else if(this.state.graphMouseDown && !this.isPreviewMode) {
        if(this.state.selectedNode) {
          this.removeSelectFromNode();
        } else if(this.state.selectedEdge) {
          this.removeSelectFromEdge();
        }

        $scope.$apply(function(){
          $scope.$emit('onRemoveNodeSelection');
        });
      }
      state.graphMouseDown = false;
    };

    // keydown on main svg
    GraphCreator.prototype.svgKeyDown = function() {
      var thisGraph = this,
        state = thisGraph.state,
        consts = thisGraph.consts;

      // make sure repeated key presses don't register for each keydown
      if(state.lastKeyDown !== -1 && state.lastKeyDown !== consts.COMMAND_KEY &&
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
          if(thisGraph.isReadOnly) {
            //Graph is read only
            return;
          }
          if (selectedNode) {
            $scope.$apply(function() {
              var nodeIndex = thisGraph.nodes.indexOf(selectedNode);

              if(nodeIndex !== -1) {
                thisGraph.nodes.splice(nodeIndex, 1);

                //Remove the input lanes in all stages having output lanes of delete node.
                _.each(thisGraph.edges, function(edge) {
                  if(edge.source === selectedNode) {
                    edge.target.inputLanes = _.filter(edge.target.inputLanes, function(inputLane) {
                      return !_.contains(edge.source.outputLanes, inputLane);
                    });
                  }
                });

                thisGraph.spliceLinksForNode(selectedNode);
                state.selectedNode = null;
                $scope.$emit('onRemoveNodeSelection');
                thisGraph.updateGraph();
              }
            });
          } else if (selectedEdge) {
            $scope.$apply(function() {
              var edgeIndex = thisGraph.edges.indexOf(selectedEdge);
              if(edgeIndex !== -1) {
                //Update pipeline target input lanes.
                selectedEdge.target.inputLanes = _.filter(selectedEdge.target.inputLanes, function(inputLane) {
                  return !_.contains(selectedEdge.source.outputLanes, inputLane);
                });

                thisGraph.edges.splice(edgeIndex, 1);
                state.selectedEdge = null;
                $scope.$emit('onRemoveNodeSelection');
                thisGraph.updateGraph();
              }
            });
          }
          break;

        case consts.COPY_KEY:
          if((d3.event.metaKey || d3.event.ctrlKey) && selectedNode) {
            state.copiedStage = selectedNode;
          }
          break;


        case consts.PASTE_KEY:
          if(thisGraph.isReadOnly) {
            return;
          }
          if((d3.event.metaKey || d3.event.ctrlKey) && state.copiedStage) {
            $scope.$apply(function() {
              $scope.$emit('onPasteNode', state.copiedStage);
            });
          }
          break;
      }
    };

    GraphCreator.prototype.svgKeyUp = function() {
      this.state.lastKeyDown = -1;
    };

    GraphCreator.prototype.addNode = function(node, edge, relativeX, relativeY) {
      var thisGraph = this;

      if(relativeX && relativeY) {
        var offsets = $element[0].getBoundingClientRect(),
          top = offsets.top,
          left = offsets.left,
          currentTranslatePos = thisGraph.zoom.translate(),
          startX = (currentTranslatePos[0] + left),
          startY = (currentTranslatePos[1] + top);

        node.uiInfo.xPos = (relativeX - startX)/ thisGraph.state.currentScale;
        node.uiInfo.yPos = (relativeY - startY)/ thisGraph.state.currentScale;
      }

      thisGraph.nodes.push(node);

      if(edge) {
        thisGraph.edges.push(edge);
      }

      thisGraph.updateGraph();
      thisGraph.selectNode(node);

      if(!relativeX) {
        thisGraph.moveNodeToVisibleArea(node);
      }

    };


    GraphCreator.prototype.selectNode = function(node) {
      var thisGraph = this,
        nodeExists,
        addedNode = thisGraph.rects.filter(function(cd){
          if(cd.instanceName === node.instanceName) {
            nodeExists = true;
          }
          return cd.instanceName === node.instanceName;
        });

      if(nodeExists) {
        thisGraph.replaceSelectNode(addedNode, node);
      }

    };

    GraphCreator.prototype.selectEdge = function(edge) {
      var thisGraph = this,
        edgeExists,
        addedEdge = thisGraph.paths.filter(function(d){
          if(d.source.instanceName === edge.source.instanceName &&
            d.target.instanceName === edge.target.instanceName) {
            edgeExists = true;
            return true;
          }
          return false;
        });

      if(edgeExists) {
        thisGraph.replaceSelectEdge(addedEdge, edge);
      }
    };

    // call to propagate changes to graph
    GraphCreator.prototype.updateGraph = function(){

      var thisGraph = this,
        consts = thisGraph.consts,
        state = thisGraph.state,
        stageErrorCounts = thisGraph.stageErrorCounts;

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
          $scope.$apply(function(){
            $scope.$emit('onNodeSelection', d);
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

        if(d.uiInfo.stageType !== pipelineConstant.TARGET_STAGE_TYPE) {

          var totalLanes = d.outputLanes.length,
            lanePredicatesConfiguration = _.find(d.configuration, function(configuration) {
              return configuration.name === 'lanePredicates';
            });
          angular.forEach(d.outputLanes, function(lane, index) {
            var y = Math.round(((consts.rectHeight) / (2 * totalLanes) ) +
              ((consts.rectHeight * (index))/totalLanes)),
              lanePredicate = lanePredicatesConfiguration ? lanePredicatesConfiguration.value[index] : undefined;
            stageNode
              .append('circle')
              .attr({
                'cx': consts.rectWidth,
                'cy': y,
                'r': 10,
                'class': 'graph-bootstrap-tooltip',
                'title': lanePredicate ? lanePredicate.predicate : ''
              }).on('mousedown', function(d){
                thisGraph.state.shiftNodeDrag = true;
                thisGraph.state.shiftNodeDragYPos = y;
                thisGraph.state.mouseDownNodeLane = lane;
              });

            if(totalLanes > 1) {
              stageNode
                .append('text')
                .attr({
                  'x': consts.rectWidth - 3,
                  'y': y + 5,
                  'class': 'lane-number graph-bootstrap-tooltip',
                  'title': lanePredicate ? lanePredicate.predicate : ''
                })
                .text(index+1)
                .on('mousedown', function(d){
                  thisGraph.state.shiftNodeDrag = true;
                  thisGraph.state.shiftNodeDragYPos = y;
                  thisGraph.state.mouseDownNodeLane = lane;
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
          return d.uiInfo.icon;
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
        .attr('class', 'node-warning fa fa-exclamation-triangle');



      //Add bad records count
      newGs.append('svg:foreignObject')
        .filter(function(d) {
          return true;
        })
        .attr('width', 100)
        .attr('height', 30)
        .attr('x', consts.rectWidth - 40)
        .attr('y', 10)
        .append('xhtml:span')
        .attr('title', graphErrorBadgeLabel)
        .attr('class', 'badge alert-danger pointer graph-bootstrap-tooltip')
        .style('visibility', function(d) {
          if(stageErrorCounts && stageErrorCounts[d.instanceName] &&
            parseInt(stageErrorCounts[d.instanceName]) > 0) {
            return 'visible';
          } else {
            return 'hidden';
          }
        })
        .html(function(d) {
          if(stageErrorCounts) {
            return $filter('abbreviateNumber')(stageErrorCounts[d.instanceName]);
          }
          return '';
        })
        .on('mousedown', function() {
          $rootScope.$apply(function() {
            $rootScope.$broadcast('showBadRecordsSelected');
          });
        });


      // remove old nodes
      thisGraph.rects.exit().remove();

      var paths = thisGraph.paths;

      // update existing paths
      paths.selectAll('path')
        .style('marker-end', 'url(#end-arrow)')
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
          return (d.source.uiInfo.xPos + consts.rectWidth + (d.target.uiInfo.xPos -30))/2;
        })
        .attr('y', function(d) {
          var totalLanes = d.source.outputLanes.length,
            outputLaneIndex = _.indexOf(d.source.outputLanes, d.outputLane),
            y = Math.round(((consts.rectHeight) / (2 * totalLanes) ) + ((consts.rectHeight * (outputLaneIndex))/totalLanes));

          return ((d.source.uiInfo.yPos + y + d.target.uiInfo.yPos + consts.rectHeight/2))/2 - 26;
        });

      var pathNewGs= paths.enter()
        .append('g');


      pathNewGs
        .classed(consts.pathGClass, true)
        .on('mousedown', function(d) {
          if(!thisGraph.isPreviewMode) {
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
        .style('marker-end', 'url(#end-arrow)')
        .classed('link', true)
        .attr('d', function(d) {
          return thisGraph.getPathDValue(d);
        });


      if(thisGraph.showEdgePreviewIcon) {
        pathNewGs
          .append('svg:foreignObject')
          .attr('class', 'edge-preview-container graph-bootstrap-tooltip')
          .attr('title', 'Inspect Data')
          .attr('width', 30)
          .attr('height', 30)
          .attr('x', function(d) {
            return (d.source.uiInfo.xPos + consts.rectWidth + (d.target.uiInfo.xPos -30))/2;
          })
          .attr('y', function(d) {
            var totalLanes = d.source.outputLanes.length,
              outputLaneIndex = _.indexOf(d.source.outputLanes, d.outputLane),
              y = Math.round(((consts.rectHeight) / (2 * totalLanes) ) + ((consts.rectHeight * (outputLaneIndex))/totalLanes));

            return ((d.source.uiInfo.yPos + y + d.target.uiInfo.yPos + consts.rectHeight/2))/2 - 26;
          })
          .append('xhtml:span')
          .attr('class', 'fa fa-tachometer fa-2x pointer edge-preview');
      }

      // remove old links
      paths.exit().remove();


      $('.graph-bootstrap-tooltip').each(function() {
        var $this = $(this),
          title = $this.attr('title');
        if(title) {
          $this.tooltip({
            title: title,
            container:'body'
          });
        }
      });
    };

    GraphCreator.prototype.getPathDValue = function(d) {
      var thisGraph = this,
        consts = thisGraph.consts,
        totalLanes = d.source.outputLanes.length,
        outputLaneIndex = _.indexOf(d.source.outputLanes, d.outputLane),
        y = Math.round(((consts.rectHeight) / (2 * totalLanes) ) + ((consts.rectHeight * (outputLaneIndex))/totalLanes)),
        sourceX,sourceY,targetX,targetY, sourceTangentX, sourceTangentY, targetTangentX, targetTangentY;



      sourceX = (d.source.uiInfo.xPos + consts.rectWidth);
      sourceY = (d.source.uiInfo.yPos + y);

      if(d.target.uiInfo.xPos > (sourceX + 30)) {
        targetX = (d.target.uiInfo.xPos - 30);
      } else if(d.target.uiInfo.xPos > sourceX) {
        targetX = (d.target.uiInfo.xPos + 10);
      } else {
        targetX = (d.target.uiInfo.xPos + 30);
      }


      targetY = (d.target.uiInfo.yPos + consts.rectWidth/2 - 20);
      sourceTangentX = sourceX + (targetX - sourceX)/2;
      sourceTangentY = sourceY;
      targetTangentX = targetX - (targetX - sourceX)/2;
      targetTangentY = targetY;

      return 'M ' + sourceX + ',' + sourceY +
        'C' + sourceTangentX + ',' + sourceTangentY + ' ' +
        targetTangentX + ',' + targetTangentY + ' ' +
        targetX + ',' + targetY;


      /*return 'M ' + sourceX + ',' + sourceY +
        'L' + (d.target.uiInfo.xPos - 30) + ',' + targetY;*/
    };

    GraphCreator.prototype.zoomed = function() {
      this.state.justScaleTransGraph = true;

      if(showTransition) {
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
      if(this.state.currentScale < this.zoom.scaleExtent()[1]) {
        this.state.currentScale = Math.round((this.state.currentScale + 0.1) * 10)/10 ;
        this.zoom.scale(this.state.currentScale).event(this.svg);
      }
    };

    GraphCreator.prototype.zoomOut = function() {
      if(this.state.currentScale > this.zoom.scaleExtent()[0]) {
        this.state.currentScale = Math.round((this.state.currentScale - 0.1) * 10)/10 ;
        this.zoom.scale(this.state.currentScale).event(this.svg);
      }
    };

    GraphCreator.prototype.moveNodeToCenter = function(stageInstance) {
      var thisGraph = this,
        consts = thisGraph.consts,
        svgWidth = thisGraph.svg.style('width').replace('px', ''),
        svgHeight = thisGraph.svg.style('height').replace('px', ''),
        currentScale = thisGraph.state.currentScale,
        x = svgWidth / 2 - (stageInstance.uiInfo.xPos + consts.rectWidth/2) * currentScale,
        y = svgHeight / 2 - (stageInstance.uiInfo.yPos + consts.rectHeight/2) * currentScale;

      showTransition = true;
      this.zoom.translate([x, y]).event(this.svg);
    };

    GraphCreator.prototype.moveNodeToVisibleArea = function(stageInstance) {
      var thisGraph = this,
        currentScale = thisGraph.state.currentScale,
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
      
      if(nodeStartXPos < startX || nodeEndXPos > endX || nodeStartYPos < startY || nodeEndYPos > endY) {
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

    GraphCreator.prototype.updateStartAndEndNode = function(startNode, endNode) {
      var thisGraph = this;

      thisGraph.clearStartAndEndNode();

      thisGraph.rects.filter(function(cd){
        return cd.instanceName === startNode.instanceName;
      }).classed(thisGraph.consts.startNodeClass, true);

      thisGraph.rects.filter(function(cd){
        return cd.instanceName === endNode.instanceName;
      }).classed(thisGraph.consts.endNodeClass, true);
    };

    /** MAIN SVG **/
    var graphContainer, svg, graph, toolbar;

    $scope.$on('updateGraph', function(event, options) {
      var nodes = options.nodes,
        edges = options.edges,
        issues = options.issues,
        selectNode = options.selectNode,
        stageErrorCounts = options.stageErrorCounts,
        showEdgePreviewIcon = options.showEdgePreviewIcon;

      if(graph !== undefined) {
        graph.deleteGraph();
      } else {
        graphContainer = d3.select($element[0]).append('div')
          .attr('class', 'graph-container');
        svg = graphContainer.append('svg')
          .attr('width', '100%')
          .attr('height', '100%')
          .attr('tabindex', 0);
        graph = new GraphCreator(svg, nodes, edges || [], issues);
        graph.setIdCt(2);


        //Toolbar

        toolbar = graphContainer.append('div')
          .attr('class', 'graph-toolbar');


        toolbar.append('div')
          .append('span')
          .attr('class', 'pointer fa fa-plus')
          .on('mousedown', function() {
            graph.zoomIn();
            d3.event.preventDefault();
          });

        toolbar.append('div')
          .append('span')
          .attr('class', 'pointer fa fa-minus')
          .on('mousedown', function() {
            graph.zoomOut();
            d3.event.preventDefault();
          });

      }

      graph.nodes = nodes;
      graph.edges = edges;
      graph.issues = issues;
      graph.stageErrorCounts = stageErrorCounts;
      graph.showEdgePreviewIcon = showEdgePreviewIcon;
      graph.isReadOnly = options.isReadOnly;
      graph.updateGraph();

      if(selectNode) {
        graph.selectNode(selectNode);
      }
    });

    $scope.$on('addNode', function(event, stageInstance, edge, relativeX, relativeY) {
      graph.addNode(stageInstance, edge, relativeX, relativeY);
      $(graph.svg[0]).focus();
    });

    $scope.$on('selectNode', function(event, stageInstance, moveToCenter) {
      if(stageInstance) {

        if(moveToCenter) {
          graph.moveNodeToCenter(stageInstance);
        } else {
          graph.moveNodeToVisibleArea(stageInstance);
        }

        if(graph.state.selectedEdge) {
          graph.removeSelectFromEdge();
        }

        graph.selectNode(stageInstance);
      } else {
        if (graph.state.selectedNode){
          graph.removeSelectFromNode();
        }

        if(graph.state.selectedEdge) {
          graph.removeSelectFromEdge();
        }

        graph.moveGraphToCenter();
      }
    });

    $scope.$on('selectEdge', function(event, edge) {
      graph.moveNodeToVisibleArea(edge.source);
      if(graph.state.selectedNode) {
        graph.removeSelectFromNode();
      }
      graph.selectEdge(edge);
    });

    $scope.$on('updateErrorCount', function(event, stageInstanceErrorCounts) {
      if(graph) {
        graph.rects.selectAll('span.badge')
          .style('visibility', function(d) {
            if(stageInstanceErrorCounts[d.instanceName] &&
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

    $scope.$on('moveGraphToCenter', function() {
      if(graph) {
        if (graph.state.selectedNode) {
          graph.removeSelectFromNode();
        }

        if(graph.state.selectedEdge) {
          graph.removeSelectFromEdge();
        }

        graph.clearStartAndEndNode();
        graph.moveGraphToCenter();
      }
    });

    $scope.$on('updateStartAndEndNode', function(event, startNode, endNode) {
      if(graph) {
        graph.updateStartAndEndNode(startNode, endNode);
      }
    });

    $scope.$on('clearStartAndEndNode', function() {
      if(graph) {
        graph.clearStartAndEndNode();
      }
    });

    $scope.$on('setGraphReadOnly', function(event, flag) {
      if(graph) {
        graph.isReadOnly = flag;
      }
    });

    $scope.$on('setGraphPreviewMode', function(event, flag) {
      if(graph) {
        graph.isPreviewMode = flag;
      }
    });

  });