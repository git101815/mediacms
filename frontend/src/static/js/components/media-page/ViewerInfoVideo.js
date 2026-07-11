import React from 'react';
import { MediaPageStore } from '../../utils/stores/';
import CreatorSubscribeButton from '../subscriptions/CreatorSubscribeButton';
import ViewerInfoContent from './ViewerInfoContent';
import ViewerInfoVideoTitleBanner from './ViewerInfoVideoTitleBanner';
import ViewerInfo from './ViewerInfo';

export default class ViewerInfoVideo extends ViewerInfo {
  render() {
    let views, categories, title, author, published, description;
    let allowDownload = false;

    if (this.state.videoLoaded) {
      allowDownload = MediaPageStore.get('media-data').allow_download;

      if (void 0 === allowDownload) {
        allowDownload = true;
      } else {
        allowDownload = !!allowDownload;
      }

      views = MediaPageStore.get('media-data').views;
      categories = MediaPageStore.get('media-data').categories_info;
      title = MediaPageStore.get('media-data').title;

      author = {
        username: MediaPageStore.get('media-data').user,
        name: MediaPageStore.get('media-data').author_name,
        url: MediaPageStore.get('media-data').author_profile,
        thumb: MediaPageStore.get('media-author-thumbnail-url'),
      };

      published = MediaPageStore.get('media-data').add_date;
      description = MediaPageStore.get('media-data').description;
    }

    return !this.state.videoLoaded ? null : (
      <div className="viewer-info">
        <div className="viewer-info-inner">
          <ViewerInfoVideoTitleBanner
            title={title}
            views={views}
            categories={categories}
            allowDownload={allowDownload}
          />
          <ViewerInfoContent author={author} published={published} description={description} />
          {author && author.username ? (
            <CreatorSubscribeButton
              username={author.username}
              creatorName={author.name || author.username}
              placement="media"
              portalTargetSelector=".viewer-info-inner .media-author-banner > div:nth-child(2) > span:first-child"
            />
          ) : null}
        </div>
      </div>
    );
  }
}
