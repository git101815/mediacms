import React from 'react';
import { useItem } from '../../utils/hooks/';
import { UserItemMemberSince, UserItemThumbnailLink } from './includes/items/';
import { Item } from './Item';

export function UserItem(props) {
  const type = 'user';

  const { titleComponent, descriptionComponent, thumbnailUrl, UnderThumbWrapper } = useItem({ ...props, type });

  function metaComponents() {
   if (props.hideAllMeta) return null;
     const count = Number(props.media_count);
     const hasCount = Number.isFinite(count) && count >= 0;
     return (
      <span className="item-meta">
        <UserItemMemberSince date={props.publish_date} />
        {hasCount && <span className="sep"> · </span>}
        {hasCount && (
           <span className="item-meta-count">
           {count} {count === 1 ? 'video' : 'videos'}
           </span>
         )}
      </span>
    );
  }

  function thumbnailComponent() {
    return <UserItemThumbnailLink src={thumbnailUrl} title={props.title} link={props.link} />;
  }

  return (
    <div className="item member-item">
      <div className="item-content">
        {thumbnailComponent()}

        <UnderThumbWrapper title={props.title} link={props.link}>
          {titleComponent()}
          {metaComponents()}
          {descriptionComponent()}
        </UnderThumbWrapper>
      </div>
    </div>
  );
}

UserItem.propTypes = {
  ...Item.propTypes,
};

UserItem.defaultProps = {
  ...Item.defaultProps,
};
