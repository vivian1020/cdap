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
  });
}
Dag1TestCtrl.$inject = ['MyDagStore'];

angular.module(`${PKG.name}.feature.experimental`)
  .controller('Dag1TestCtrl', Dag1TestCtrl);
