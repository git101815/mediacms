import React from 'react';

import { ProfilePageStore } from '../../utils/stores/';
import CreatorSubscribeButton from '../subscriptions/CreatorSubscribeButton';


export default class ProfilePagesContent extends React.PureComponent {
  render() {
    if (!this.props.children) {
      return null;
    }

    const author = ProfilePageStore.get('author-data');

    return (
      <div className={'profile-page-content' + (this.props.enabledContactForm ? ' with-cform' : '')}>
        {author && author.username ? (
          <CreatorSubscribeButton
            username={author.username}
            creatorName={author.name || author.username}
            placement="profile"
            portalTargetSelector=".profile-page-header .profile-info-inner"
          />
        ) : null}
        {this.props.children}
      </div>
    );
  }
}
