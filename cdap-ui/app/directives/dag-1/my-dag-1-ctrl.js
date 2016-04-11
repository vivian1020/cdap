/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

function Ctrl (Redux, MyDagStore, jsPlumb, MyDAG1Factory, $timeout, $scope, Undoable, myHelpers) {
  this.$scope = $scope;
  this.MyDagStore = MyDagStore;
  this.scale = 1.0;
  this.panning = {
    style: {
      'top': 0,
      'left': 0
    },
    top: 0,
    left: 0
  };
  this.myHelpers = myHelpers;

  let UndoableActionCreators = Undoable.ActionCreators;
  MyDagStore.subscribe(() => {
    this.nodes = MyDagStore.getState().nodes.present;
    this.nodes = this.nodes.map( node => {
      if (!node._uiPosition.left.length || !node._uiPosition.top.length){
        let position = MyDAG1Factory.getNodePosition(this.panning, node.nodeType);
        node._uiPosition.top = position.top;
        node._uiPosition.left = position.left;
      }
      return node;
    });
    let isDAGInitialized = MyDagStore.getState().isDagInitialized;
    this.isDisabled = MyDagStore.getState().isDisabled;
    if (isDAGInitialized) {
      $timeout(() => {
        render();
        this.cleanupGraph();
        this.fitToScreen();
        MyDagStore.dispatch({type: 'RESET-INIT-DAG'});
      });
    } else {
      $timeout(render);
    }
  });
  var rightEndpointSettings = angular.copy(MyDAG1Factory.getSettings(false).leftEndpoint);
  var leftEndpointSettings = angular.copy(MyDAG1Factory.getSettings(false).rightEndpoint);
  var transformSourceSettings = angular.copy(MyDAG1Factory.getSettings(false).leftLFEndpoint);
  var transformSinkSettings = angular.copy(MyDAG1Factory.getSettings(false).rightLFEndpoint);

  jsPlumb.ready(() => {
    var dagSettings = MyDAG1Factory.getSettings().default;
    jsPlumb.setContainer('dag-container');
    this.instance = jsPlumb.getInstance(dagSettings);
    this.instance.bind('connection', (info, originalEvent) => {
      if (!originalEvent) { return; }
      let conn = this.instance.getConnections().map( conn=> {
        return {from: conn.sourceId, to: conn.targetId};
      });
      MyDagStore.dispatch({
        type: 'SET-CONNECTIONS',
        connections: conn
      });
    });
    this.instance.bind('connectionDetached', (info, originalEvent) => {
      if (!originalEvent) { return; }
      let conn = this.instance.getConnections().map( conn=> {
        return {from: conn.sourceId, to: conn.targetId};
      });
      MyDagStore.dispatch({
        type: 'SET-CONNECTIONS',
        connections: conn
      });
    });

    // Making canvas draggable
    this.secondInstance = jsPlumb.getInstance();
    this.secondInstance.draggable('diagram-container', {
      start: () => {},
      stop: (e) => {
        e.el.style.top = '0px';
        e.el.style.left = '0px';
        this.panning.top += e.pos[1];
        this.panning.left += e.pos[0];
        e.el.children[0].style.top = this.panning.top + 'px';
        e.el.children[0].style.left = this.panning.left + 'px';
      }
    });
  });

  let renderConnections = (connectionsFromStore) => {
    connectionsFromStore.forEach( connection => {
      var sourceNode = this.nodes.filter( node => [node.name, node.id].indexOf(connection.from) !== -1);
      var targetNode = this.nodes.filter( node => [node.name, node.id].indexOf(connection.to) !== -1);
      if (!sourceNode.length || !targetNode.length) {
        return;
      }
      var sourceId = sourceNode[0].nodeType === 'transform' ? 'Left' + connection.from : connection.from;
      var targetId = targetNode[0].nodeType === 'transform' ? 'Right' + connection.to : connection.to;
      this.instance.connect({
        uuids: [sourceId, targetId]
      });
    });
  };
  let render = () => {
    this.instance.deleteEveryEndpoint();
    this.instance.detachEveryConnection();

    angular.forEach(this.nodes,  (node) => {
      switch(node.endpoint) {
        case 'R':
          this.instance.addEndpoint(node.id, rightEndpointSettings, {uuid: node.id});
          break;
        case 'L':
          this.instance.addEndpoint(node.id, leftEndpointSettings, {uuid: node.id});
          break;
        case 'LR':
          // Need to id each end point so that it can be used later to make connections.
          this.instance.addEndpoint(node.id, transformSourceSettings, {uuid: 'Left' + node.id});
          this.instance.addEndpoint(node.id, transformSinkSettings, {uuid: 'Right' + node.id});
          break;
      }
    });
    var nodes = document.querySelectorAll('.box');

    if (!this.isDisabled) {
      this.instance.draggable(nodes, {
        start:  () => {this.canvasIsDragged = true; },
        stop: (dragEndEvent) => {
          var config = {
            _uiPosition: {
              top: dragEndEvent.el.style.top,
              left: dragEndEvent.el.style.left
            }
          };
          this.MyDagStore.dispatch({
            type: 'UPDATE-NODE',
            id: dragEndEvent.el.id,
            config: config
          });
          $timeout(this.instance.repaintEverything);
        }
      });
    } else {
      try {
        jsPlumb.setDraggable(nodes, false);
        jsPlumb.setDraggable('diagram-container', false);
        // console.log('Successfully disabled drag');
      } catch(e) {
        // console.log('Unable to disable draggle on nodes. angular render timing issue');
      }
    }

    let connectionsFromStore = [...this.MyDagStore.getState().connections.present];
    if (connectionsFromStore.length) {
      renderConnections(connectionsFromStore);
    }
  };

  this.undo = () => {
    MyDagStore.dispatch(UndoableActionCreators.undo());
  };
  this.redo = () => {
    MyDagStore.dispatch(UndoableActionCreators.redo());
  };
  this.zoomIn = () => {
    this.scale += 0.1;
    if (this.nodes.length === 0) { return; }
    MyDAG1Factory.setZoom(this.scale, this.instance);
  };
  this.zoomOut = () => {
    if (this.scale <= 0.2) { return; }
    this.scale -=0.1;
    MyDAG1Factory.setZoom(this.scale, this.instance);
  };
  this.cleanupGraph = () => {
    let state = MyDagStore.getState();
    let nodes = state.nodes.present;
    let connections = state.connections.present;

    var graph = MyDAG1Factory.getGraphLayout(nodes, connections);
    angular.forEach(nodes, (node) => {
      node._uiPosition = node._uiPosition || {};
      node._uiPosition = {
        'top': graph._nodes[node.id].y + 'px' ,
        'left': graph._nodes[node.id].x + 'px'
      };
    });
    if (this.scale > 1) {
      this.scale = 1;
    }
    MyDAG1Factory.setZoom(this.scale, this.instance);
    this.$scope.dagContainer.style.left = '0px';
    this.$scope.dagContainer.style.top = '0px';
    var config = MyDAG1Factory.getGraphMargins(this.$scope.element, nodes);
    this.nodes = config.nodes;
    $timeout(this.instance.repaintEverything);
  };
  this.fitToScreen = () => {
    let state = MyDagStore.getState();
    let nodes = state.nodes.present;
    if (nodes.length === 0) { return; }

    /**
     * Need to find the furthest nodes:
     * 1. Left most nodes
     * 2. Right most nodes
     * 3. Top most nodes
     * 4. Bottom most nodes
     **/
    var minLeft = _.min(nodes, function (node) {
      if (node._uiPosition.left.includes('vw')) {
        var left = parseInt(node._uiPosition.left, 10)/100 * document.documentElement.clientWidth;
        node._uiPosition.left = left + 'px';
      }
      return parseInt(node._uiPosition.left, 10);
    });
    var maxLeft = _.max(nodes, function (node) {
      if (node._uiPosition.left.includes('vw')) {
        var left = parseInt(node._uiPosition.left, 10)/100 * document.documentElement.clientWidth;
        node._uiPosition.left = left + 'px';
      }
      return parseInt(node._uiPosition.left, 10);
    });

    var minTop = _.min(nodes, function (node) {
      return parseInt(node._uiPosition.top, 10);
    });

    var maxTop = _.max(nodes, function (node) {
      return parseInt(node._uiPosition.top, 10);
    });

    /**
     * Calculate the max width and height of the actual diagram by calculating the difference
     * between the furthest nodes + margins ( 50 on each side ).
     **/
    var width = parseInt(maxLeft._uiPosition.left, 10) - parseInt(minLeft._uiPosition.left, 10) + 100;
    var height = parseInt(maxTop._uiPosition.top, 10) - parseInt(minTop._uiPosition.top, 10) + 100;

    var parent = this.$scope.element[0].parentElement.getBoundingClientRect();

    // calculating the scales and finding the minimum scale
    var widthScale = (parent.width - 100) / width;
    var heightScale = (parent.height - 100) / height;

    this.scale = Math.min(widthScale, heightScale);

    if (this.scale > 1) {
      this.scale = 1;
    }
    MyDAG1Factory.setZoom(this.scale, this.instance);


    // This will move all nodes by the minimum left and minimum top by the container
    // with margin of 50px
    var offsetLeft = parseInt(minLeft._uiPosition.left, 10);
    angular.forEach(nodes, function (node) {
      node._uiPosition.left = (parseInt(node._uiPosition.left, 10) - offsetLeft + 50) + 'px';
    });

    var offsetTop = parseInt(minTop._uiPosition.top, 10);
    angular.forEach(nodes, function (node) {
      node._uiPosition.top = (parseInt(node._uiPosition.top, 10) - offsetTop + 50) + 'px';
    });
    var config = MyDAG1Factory.getGraphMargins(this.$scope.element, nodes);
    this.nodes = config.nodes;
    this.$scope.diagramContainer.style.top = '0px';
    this.$scope.diagramContainer.style.left= '0px';
    this.$scope.dagContainer.style.left = '0px';
    this.$scope.dagContainer.style.top = '0px';
    $timeout(this.instance.repaintEverything);
  };
  this.removeNode = (nodeId) => {
    this.instance.remove(nodeId);
    MyDagStore.dispatch({
      type: 'REMOVE-NODE',
      id: nodeId
    });
  };
  this.onNodeClick = (nodeId) => {
    if (this.canvasIsDragged) {
      this.canvasIsDragged = false;
      return;
    }
    this.MyDagStore.dispatch({ type: 'RESET-SELECTED'});
    this.MyDagStore.dispatch({
      type: 'UPDATE-NODE',
      id: nodeId,
      config: {
        selected: true
      }
    });
  };
}

Ctrl.$inject = ['Redux', 'MyDagStore', 'jsPlumb', 'MyDAG1Factory', '$timeout', '$scope', 'Undoable', 'myHelpers'];
angular.module(PKG.name + '.commons')
  .controller('MyDag1Ctrl', Ctrl);
