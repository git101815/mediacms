import React from 'react';
import { ApiUrlConsumer } from '../utils/contexts/';
import { MediaListWrapper } from '../components/MediaListWrapper';
import { LazyLoadItemListAsync } from '../components/item-list/LazyLoadItemListAsync.jsx';
import { Page } from './Page';

interface MembersPageProps {
  id?: string;
  title?: string;
}

export const MembersPage: React.FC<MembersPageProps> = ({ id = 'members', title = 'Creators' }) => (
  <Page id={id}>
    <ApiUrlConsumer>
      {(apiUrl) => (
        <MediaListWrapper title={title} className="items-list-ver">
          <LazyLoadItemListAsync requestUrl={`${apiUrl.users}?role=advancedUser`} sortBy="media_count_desc"/>
        </MediaListWrapper>
      )}
    </ApiUrlConsumer>
  </Page>
);
