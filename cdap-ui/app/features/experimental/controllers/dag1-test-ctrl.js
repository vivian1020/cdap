function Dag1TestCtrl(MyDagStore) {
  this.addNode = () => {
    MyDagStore.dispatch({
      name: this.nodename,
      cssClass: this.cssClass,
      icon: this.icon,
      endpoint: this.endpointType,
      badgeInfo: this.badgeInfo,
      badgeTooltip: this.badgeTooltip,
      badgeCssClass: this.badgeCssClass,
      tooltipCssClass: this.tooltipCssClass,
      type: 'ADD-NODE'
    });
  };
  this.nodename = 'Twitter';
  this.cssClass='batchsource';
  this.icon ='fa-twitter';
  this.endpointType = 'R';
  this.badgeInfo = '6';
  this.badgeCssClass = 'badge-warning';
  this.badgeTooltip = 'Please check node config';
  this.tooltipCssClass = 'tooltip-warning';

  MyDagStore.subscribe(() => {
    this.state = MyDagStore.getState();
  });
}
Dag1TestCtrl.$inject = ['MyDagStore'];

angular.module(`${PKG.name}.feature.experimental`)
  .controller('Dag1TestCtrl', Dag1TestCtrl);
