/**
 * Module definition for Pipeline Graph.
 */

angular.module('pipelineGraphDirectives', [])
  .directive('pipelineGraph', function() {
    return {
      restrict: 'E',
      replace: false,
      controller: 'PipelineGraphController'
    };
  })
  .controller('PipelineGraphController', function($scope, $element){

    var consts = {
      defaultTitle: "random variable"
    };

    // define graphcreator object
    var GraphCreator = function(svg, nodes, edges){
      var thisGraph = this;
      thisGraph.idct = 0;

      thisGraph.nodes = nodes || [];
      thisGraph.edges = edges || [];

      thisGraph.state = {
        selectedNode: null,
        selectedEdge: null,
        mouseDownNode: null,
        mouseDownLink: null,
        justDragged: false,
        justScaleTransGraph: false,
        lastKeyDown: -1,
        shiftNodeDrag: false,
        selectedText: null
      };

      // define arrow markers for graph links
      var defs = svg.append('svg:defs');
      defs.append('svg:marker')
        .attr('id', 'end-arrow')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', "13")
        .attr('markerWidth', 3.5)
        .attr('markerHeight', 3.5)
        .attr('orient', 'auto')
        .append('svg:path')
        .attr('d', 'M0,-5L10,0L0,5');

      // define arrow markers for leading arrow
      /*defs.append('svg:marker')
        .attr('id', 'mark-end-arrow')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 7)
        .attr('markerWidth', 3.5)
        .attr('markerHeight', 3.5)
        .attr('orient', 'auto')
        .append('svg:path')
        .attr('d', 'M0,-5L10,0L0,5');*/

      thisGraph.svg = svg;
      thisGraph.svgG = svg.append("g")
        .classed(thisGraph.consts.graphClass, true);
      var svgG = thisGraph.svgG;

      // displayed when dragging between nodes
      thisGraph.dragLine = svgG.append('svg:path')
        .attr('class', 'link dragline hidden')
        .attr('d', 'M0,0L0,0')
        .style('marker-end', 'url(#mark-end-arrow)');

      // svg nodes and edges
      thisGraph.paths = svgG.append("g").selectAll("g");
      thisGraph.rects = svgG.append("g").selectAll("g");

      thisGraph.drag = d3.behavior.drag()
        .origin(function(d){
          return {x: d.uiInfo.xPos, y: d.uiInfo.yPos};
        })
        .on("drag", function(args){
          thisGraph.state.justDragged = true;
          thisGraph.dragmove.call(thisGraph, args);
        })
        .on("dragend", function() {
          // todo check if edge-mode is selected
        });

      // listen for key events
      svg.on("keydown", function() {
        thisGraph.svgKeyDown.call(thisGraph);
      })
      .on("keyup", function() {
        thisGraph.svgKeyUp.call(thisGraph);
      })
      .on("mousedown", function(d) {
        thisGraph.svgMouseDown.call(thisGraph, d);
      })
      .on("mouseup", function(d) {
        thisGraph.svgMouseUp.call(thisGraph, d);
      });

      // listen for dragging
      var dragSvg = d3.behavior.zoom()
        .scaleExtent([0.1, 10])
        .on("zoom", function(){
          if (d3.event.sourceEvent.shiftKey){
            // TODO  the internal d3 state is still changing
            return false;
          } else{
            thisGraph.zoomed.call(thisGraph);
          }
          return true;
        })
        .on("zoomstart", function(){
          var ael = d3.select("#" + thisGraph.consts.activeEditId).node();
          if (ael){
            ael.blur();
          }
          if (!d3.event.sourceEvent.shiftKey) {
            d3.select('body').style("cursor", "move");
          }
        })
        .on("zoomend", function(){
          d3.select('body').style("cursor", "auto");
        });

      svg.call(dragSvg).on("dblclick.zoom", null);

      // listen for resize
      window.onresize = function(){thisGraph.updateWindow(svg);};

    };

    GraphCreator.prototype.setIdCt = function(idct){
      this.idct = idct;
    };

    GraphCreator.prototype.consts =  {
      selectedClass: "selected",
      connectClass: "connect-node",
      circleGClass: "conceptG",
      graphClass: "graph",
      activeEditId: "active-editing",
      BACKSPACE_KEY: 8,
      DELETE_KEY: 46,
      ENTER_KEY: 13,
      nodeRadius: 70,
      rectWidth: 160,
      rectHeight: 120,
      rectRound: 20
    };

    /* PROTOTYPE FUNCTIONS */

    GraphCreator.prototype.dragmove = function(d) {
      var thisGraph = this;
      if (thisGraph.state.shiftNodeDrag){
        thisGraph.dragLine.attr('d', 'M' + (d.uiInfo.xPos + thisGraph.consts.rectWidth) + ',' + (d.uiInfo.yPos + thisGraph.consts.rectHeight/2) +
          'L' + d3.mouse(thisGraph.svgG.node())[0] + ',' + d3.mouse(this.svgG.node())[1]);
      } else{
        $scope.$apply(function() {
          d.uiInfo.xPos += d3.event.dx;
          d.uiInfo.yPos +=  d3.event.dy;
          thisGraph.updateGraph();
        });
      }
    };

    GraphCreator.prototype.deleteGraph = function(skipPrompt){
      var thisGraph = this;
      thisGraph.nodes = [];
      thisGraph.edges = [];
      thisGraph.state.selectedNode = null;
      thisGraph.state.selectedEdge = null;
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


    /* insert svg line breaks: taken from http://stackoverflow.com/questions/13241475/how-do-i-include-newlines-in-labels-in-d3-charts */
    GraphCreator.prototype.insertTitleLinebreaks = function (gEl, title) {
      var words = title.split(/\s+/g),
        nwords = words.length;

      var el = gEl.append("text")
        .attr("text-anchor","middle")
        .attr("x", 80)
        .attr("y", 60)
        .attr("dy", "-" + (nwords-1)*7.5);

      for (var i = 0; i < words.length; i++) {
        var tspan = el.append('tspan').text(words[i]);
        if (i > 0) {
          tspan.attr('x', 80)
            .attr('dy', '15');
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
      d3Path.classed(thisGraph.consts.selectedClass, true);
      if (thisGraph.state.selectedEdge){
        thisGraph.removeSelectFromEdge();
      }
      thisGraph.state.selectedEdge = edgeData;
    };

    GraphCreator.prototype.replaceSelectNode = function(d3Node, nodeData){
      var thisGraph = this;
      d3Node.classed(this.consts.selectedClass, true);
      if (thisGraph.state.selectedNode){
        thisGraph.removeSelectFromNode();
      }
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
    GraphCreator.prototype.circleMouseDown = function(d3node, d){
      var thisGraph = this,
        state = thisGraph.state;
      d3.event.stopPropagation();
      state.mouseDownNode = d;
      if (state.shiftNodeDrag){
        // reposition dragged directed edge
        thisGraph.dragLine.classed('hidden', false)
          .attr('d', 'M' + d.uiInfo.xPos + ',' + d.uiInfo.yPos + 'L' + d.uiInfo.xPos + ',' + d.uiInfo.yPos);
        return;
      }
    };

    /* place editable text on node in place of svg text */
    GraphCreator.prototype.changeTextOfNode = function(d3node, d){
      var thisGraph= this,
        consts = thisGraph.consts,
        htmlEl = d3node.node();
      d3node.selectAll("text").remove();
      var nodeBCR = htmlEl.getBoundingClientRect(),
        curScale = nodeBCR.width/consts.nodeRadius,
        placePad  =  5*curScale,
        useHW = curScale > 1 ? nodeBCR.width*0.71 : consts.nodeRadius*1.42;
      // replace with editableconent text
      var d3txt = thisGraph.svg.selectAll("foreignObject")
        .data([d])
        .enter()
        .append("foreignObject")
        .attr("x", nodeBCR.left + placePad )
        .attr("y", nodeBCR.top + placePad)
        .attr("height", 2*useHW)
        .attr("width", useHW)
        .append("xhtml:p")
        .attr("id", consts.activeEditId)
        .attr("contentEditable", "true")
        .text(d.title)
        .on("mousedown", function(d){
          d3.event.stopPropagation();
        })
        .on("keydown", function(d){
          d3.event.stopPropagation();
          if (d3.event.keyCode == consts.ENTER_KEY && !d3.event.shiftKey){
            this.blur();
          }
        })
        .on("blur", function(d){
          d.title = this.textContent;
          thisGraph.insertTitleLinebreaks(d3node, d.title);
          d3.select(this.parentElement).remove();
        });
      return d3txt;
    };

    // mouseup on nodes
    GraphCreator.prototype.circleMouseUp = function(d3node, d){
      var thisGraph = this,
        state = thisGraph.state,
        consts = thisGraph.consts;
      // reset the states
      state.shiftNodeDrag = false;
      d3node.classed(consts.connectClass, false);

      var mouseDownNode = state.mouseDownNode;

      if (!mouseDownNode) {
        return;
      }

      thisGraph.dragLine.classed("hidden", true);

      if (mouseDownNode !== d){
        // we're in a different node: create new edge for mousedown edge and add to graph
        var newEdge = {source: mouseDownNode, target: d};
        var filtRes = thisGraph.paths.filter(function(d){
          if (d.source === newEdge.target && d.target === newEdge.source){
            thisGraph.edges.splice(thisGraph.edges.indexOf(d), 1);
          }
          return d.source === newEdge.source && d.target === newEdge.target;
        });
        if (!filtRes[0].length){
          thisGraph.edges.push(newEdge);
          thisGraph.updateGraph();

          $scope.$apply(function() {
            newEdge.source.outputLanes = [newEdge.source.instanceName + 'outputLane'];
            newEdge.target.inputLanes = [newEdge.source.instanceName + 'outputLane'];
          });
        }
      } else{
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

      if(this.state.selectedNode) {
        this.removeSelectFromNode();
      } else if(this.state.selectedEdge) {
        this.removeSelectFromEdge();
      }

      $scope.$apply(function(){
        $scope.$emit('onRemoveNodeSelection');
      });

    };

    // mouseup on main svg
    GraphCreator.prototype.svgMouseUp = function(){
      var thisGraph = this,
        state = thisGraph.state;
      if (state.justScaleTransGraph) {
        // dragged not clicked
        state.justScaleTransGraph = false;
      } else if (state.graphMouseDown && d3.event.shiftKey){
        // clicked not dragged from svg
        var xycoords = d3.mouse(thisGraph.svgG.node()),
          d = {id: thisGraph.idct++, title: consts.defaultTitle, x: xycoords[0], y: xycoords[1]};
        thisGraph.nodes.push(d);
        thisGraph.updateGraph();
        // make title of text immediently editable
        var d3txt = thisGraph.changeTextOfNode(thisGraph.rects.filter(function(dval){
            return dval.instanceName === d.instanceName;
          }), d),
          txtNode = d3txt.node();
        thisGraph.selectElementContents(txtNode);
        txtNode.focus();
      } else if (state.shiftNodeDrag){
        // dragged from node
        state.shiftNodeDrag = false;
        thisGraph.dragLine.classed("hidden", true);
      }
      state.graphMouseDown = false;
    };

    // keydown on main svg
    GraphCreator.prototype.svgKeyDown = function() {
      var thisGraph = this,
        state = thisGraph.state,
        consts = thisGraph.consts;
      // make sure repeated key presses don't register for each keydown
      if(state.lastKeyDown !== -1) {
        return;
      }

      state.lastKeyDown = d3.event.keyCode;
      var selectedNode = state.selectedNode,
        selectedEdge = state.selectedEdge;

      switch(d3.event.keyCode) {
        case consts.BACKSPACE_KEY:
        case consts.DELETE_KEY:
          d3.event.preventDefault();
          if (selectedNode) {
            $scope.$apply(function() {
              thisGraph.nodes.splice(thisGraph.nodes.indexOf(selectedNode), 1);
              thisGraph.spliceLinksForNode(selectedNode);
              state.selectedNode = null;
              thisGraph.updateGraph();
            });
          } else if (selectedEdge) {
            $scope.$apply(function() {
              thisGraph.edges.splice(thisGraph.edges.indexOf(selectedEdge), 1);
              state.selectedEdge = null;
              thisGraph.updateGraph();
            });
          }
          break;
      }
    };

    GraphCreator.prototype.svgKeyUp = function() {
      this.state.lastKeyDown = -1;
    };

    GraphCreator.prototype.addNode = function(node) {
      var thisGraph = this;
      thisGraph.nodes.push(node);
      thisGraph.updateGraph();

      var addedNode = thisGraph.rects.filter(function(cd){
        return cd.instanceName === node.instanceName;
      });

      thisGraph.replaceSelectNode(addedNode, node);
    };

    // call to propagate changes to graph
    GraphCreator.prototype.updateGraph = function(){

      var thisGraph = this,
        consts = thisGraph.consts,
        state = thisGraph.state;



      thisGraph.paths = thisGraph.paths.data(thisGraph.edges, function(d){
        return String(d.source.instanceName) + "+" + String(d.target.instanceName);
      });

      // update existing nodes
      thisGraph.rects = thisGraph.rects.data(thisGraph.nodes, function(d){ return d.instanceName;});
      thisGraph.rects.attr("transform", function(d) {
        return "translate(" + (d.uiInfo.xPos) + "," + (d.uiInfo.yPos) + ")";
      });

      // add new nodes
      var newGs= thisGraph.rects.enter()
        .append("g");

      newGs.classed(consts.circleGClass, true)
        .attr("transform", function(d){return "translate(" + d.uiInfo.xPos + "," + d.uiInfo.yPos + ")";})
        .on("mouseover", function(d){
          if (state.shiftNodeDrag){
            d3.select(this).classed(consts.connectClass, true);
          }
        })
        .on("mouseout", function(d){
          d3.select(this).classed(consts.connectClass, false);
        })
        .on("mousedown", function(d){
          thisGraph.circleMouseDown.call(thisGraph, d3.select(this), d);
          $scope.$apply(function(){
            $scope.$emit('onNodeSelection', d);
          });
        })
        .on("mouseup", function(d){
          thisGraph.circleMouseUp.call(thisGraph, d3.select(this), d);
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
          return d.uiInfo.inputConnectors && d.uiInfo.inputConnectors.length;
        })
        .attr({
          'cx': 0,
          'cy': consts.rectWidth/2 - 20,
          'r': 10
        });

      //Output Connectors
      newGs.append('circle')
        .filter(function(d) {
          return d.uiInfo.outputConnectors && d.uiInfo.outputConnectors.length;
        })
        .attr({
          'cx': consts.rectWidth,
          'cy': consts.rectWidth/2 - 20,
          'r': 10
        }).on("mousedown", function(d){
          thisGraph.state.shiftNodeDrag = true;
        });

      newGs.each(function(d){
        thisGraph.insertTitleLinebreaks(d3.select(this), d.uiInfo.label);
      });

      // remove old nodes
      thisGraph.rects.exit().remove();


      var paths = thisGraph.paths;
      // update existing paths
      paths.style('marker-end', 'url(#end-arrow)')
        .classed(consts.selectedClass, function(d){
        return d === state.selectedEdge;
      })
        .attr("d", function(d){
          return "M" + (d.source.uiInfo.xPos + consts.rectWidth) + "," + (d.source.uiInfo.yPos + consts.rectWidth/2 - 20) +
            "L" + d.target.uiInfo.xPos + "," + (d.target.uiInfo.yPos + consts.rectWidth/2 - 20);
        });

      // add new paths
      paths.enter()
        .append("path")
        .style('marker-end','url(#end-arrow)')
        .classed("link", true)
        .attr("d", function(d){
          return "M" + (d.source.uiInfo.xPos + consts.rectWidth) + "," + (d.source.uiInfo.yPos + consts.rectWidth/2 - 20) +
            "L" + d.target.uiInfo.xPos + "," + (d.target.uiInfo.yPos + consts.rectWidth/2 - 20);
        })
        .on("mousedown", function(d){
          thisGraph.pathMouseDown.call(thisGraph, d3.select(this), d);
        })
        .on("mouseup", function(d){
          state.mouseDownLink = null;
        });

      // remove old links
      paths.exit().remove();

    };

    GraphCreator.prototype.zoomed = function(){
      this.state.justScaleTransGraph = true;
      d3.select("." + this.consts.graphClass)
        .attr("transform", "translate(" + d3.event.translate + ") scale(" + d3.event.scale + ")");
    };

    GraphCreator.prototype.updateWindow = function(svg){
      var docEl = document.documentElement,
        bodyEl = document.getElementsByTagName('body')[0];
      var x = window.innerWidth || docEl.clientWidth || bodyEl.clientWidth;
      var y = window.innerHeight|| docEl.clientHeight|| bodyEl.clientHeight;
      svg.attr("width", x).attr("height", y);
    };

    /** MAIN SVG **/
    var svg, graph;

    $scope.$on('updateGraph', function(event, nodes, edges, selectNode) {

      if(graph !== undefined) {
        graph.deleteGraph();
      } else {
        svg = d3.select($element[0]).append("svg")
          .attr("width", "100%")
          .attr("height", "98%")
          .attr("tabindex", 0);
        graph = new GraphCreator(svg, nodes, edges || []);
        graph.setIdCt(2);
      }

      graph.nodes = nodes;
      graph.edges = edges;
      graph.updateGraph();

      if(selectNode) {
        var selectNodeDom = graph.rects.filter(function(cd){
          return cd.instanceName === selectNode.instanceName;
        });

        if(selectNodeDom) {
          graph.replaceSelectNode(selectNodeDom, selectNode);
        }
      }
    });

    $scope.$on('addNode', function(event, stageInstance){
      graph.addNode(stageInstance);
    });

  });