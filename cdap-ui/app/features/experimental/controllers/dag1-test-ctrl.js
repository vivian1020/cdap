function Dag1TestCtrl(MyDagStore) {
  this.addNode = (type) => {
    switch(type) {
      case 'source':
        this.addSource();
        break;
      case 'sink':
        this.addSink();
        break;
      case 'transform':
        this.addTransform();
        break;
    }
  };
  this.addSource = () => {
    MyDagStore.dispatch({
      name: 'Source1',
      cssClass: 'batchsource',
      endpoint: 'R',
      badgeInfo: 2,
      badgeToolTip: 'Some tooltip',
      badgeCssClass: 'text-warning',
      nodeType: 'source',
      tooltipCssClass: 'badge-warning',
      type: 'ADD-NODE'
    });
  };
  this.addSink = () => {
    MyDagStore.dispatch({
      name: 'Sink1',
      cssClass: 'batchsink',
      endpoint: 'L',
      badgeInfo: 2,
      badgeToolTip: 'Some tooltip',
      badgeCssClass: 'text-warning',
      nodeType: 'sink',
      tooltipCssClass: 'badge-warning',
      type: 'ADD-NODE'
    });
  };
  this.addTransform = () => {
    MyDagStore.dispatch({
      name: 'Transform1',
      cssClass: 'transform',
      endpoint: 'LR',
      badgeInfo: 2,
      badgeToolTip: 'Some tooltip',
      badgeCssClass: 'text-warning',
      tooltipCssClass: 'badge-warning',
      nodeType: 'transform',
      type: 'ADD-NODE'
    });
  };

  this.nodename = 'Script Filter';
  this.cssClass='transform';
  this.icon ='';
  this.endpointType = 'LR';
  this.badgeInfo = '2';
  this.badgeCssClass = 'badge-warning';
  this.badgeTooltip = 'Please check node config';
  this.tooltipCssClass = 'tooltip-warning';

  MyDagStore.subscribe(() => {
    this.state = MyDagStore.getState();
    this.selectedNode = this.state.nodes.filter(node => node.selected)[0] && this.state.nodes.filter(node => node.selected)[0].name;
  });
}
Dag1TestCtrl.$inject = ['MyDagStore'];

angular.module(`${PKG.name}.feature.experimental`)
  .controller('Dag1TestCtrl', Dag1TestCtrl);
