let _uuid;
let nodes = (state = [], action = {}) => {
  switch(action.type) {
    case 'ADD-NODE':
      return [
        ...state,
        {
          id: _uuid.v4(),
          name: action.name || 'no-name',
          endpoint: action.endpoint || 'LR',
          icon: action.icon || 'fa-plug',
          cssClass: action.cssClass || '',
          badgeInfo: action.badgeInfo || null,
          badgeCssClass: action.badgeCssClass || 'badge-info',
          badgeTooltip: action.badgeTooltip || null,
          tooltipCssClass: action.tooltipCssClass || '',
          disabled: action.disabled || false,
          selected: action.selected || false
        }
      ];
    case 'REMOVE-NODE':
      return state.filter(node => node.id !== action.id);
    default:
      return state;
  }
};
let connections = (state = [], action={}) => {
  switch(action.type) {
    case 'SET-CONNECTION':
      return action.connections;
    default:
      return state;
  }
};

let Store = (Redux, uuid) => {
  _uuid = uuid;
  let combinedReducer = Redux.combineReducers({
    nodes,
    connections
  });
  return Redux.createStore(combinedReducer);
};
Store.$inject = ['Redux', 'uuid'];

angular.module(PKG.name + '.commons')
  .factory('MyDagStore', Store);
