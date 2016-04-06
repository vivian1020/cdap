
function Ctrl (Redux, MyDagStore, jsPlumb, MyDAG1Factory, $timeout) {
  MyDagStore.subscribe(() => {
    this.nodes = MyDagStore.getState().nodes;
    $timeout(render.bind(this));
  });
  var rightEndpointSettings = angular.copy(MyDAG1Factory.getSettings(false).leftEndpoint);
  var leftEndpointSettings = angular.copy(MyDAG1Factory.getSettings(false).rightEndpoint);
  var transformSourceSettings = angular.copy(MyDAG1Factory.getSettings(false).leftLFEndpoint);
  var transformSinkSettings = angular.copy(MyDAG1Factory.getSettings(false).rightLFEndpoint);
  this.scale = 1.0;
  jsPlumb.ready(() => {
    var dagSettings = MyDAG1Factory.getSettings().default;

    jsPlumb.setContainer('dag-container');
    this.instance = jsPlumb.getInstance(dagSettings);
    this.instance.bind('connection', () => {
      let conn = this.instance.getConnections().map( conn=> {
        return {from: conn.sourceId, to: conn.targetId};
      });
      MyDagStore.dispatch({
        type: 'SET-CONNECTION',
        connections: conn
      });
    });
    this.instance.bind('connectionDetached', () => {
      let conn = this.instance.getConnections().map( conn=> {
        return {from: conn.sourceId, to: conn.targetId};
      });
      MyDagStore.dispatch({
        type: 'SET-CONNECTION',
        connections: conn
      });
    });

  });

  let endPoints = [];
  let render = () => {
    angular.forEach(this.nodes,  (node) => {
      if (endPoints.indexOf(node.id) !== -1) {
        return;
      }
      endPoints.push(node.id);
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
    this.instance.draggable(nodes, {
      start:  () => {},
      stop: () => {}
    });
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
  }
  this.removeNode = (nodeId) => {
    this.instance.remove(nodeId);
    endPoints = endPoints.filter(id => nodeId !== id);
    MyDagStore.dispatch({
      type: 'REMOVE-NODE',
      id: nodeId
    });
  };
}

Ctrl.$inject = ['Redux', 'MyDagStore', 'jsPlumb', 'MyDAG1Factory', '$timeout'];
angular.module(PKG.name + '.commons')
  .controller('MyDag1Ctrl', Ctrl);
