// CelebritiesPage.tsx

import React from 'react';

import { ApiUrlConsumer } from '../utils/contexts/';

import { MediaListWrapper } from '../components/MediaListWrapper';

import { LazyLoadItemListAsync } from '../components/item-list/LazyLoadItemListAsync.jsx';

import { Page } from './Page';

import { translateString } from '../utils/helpers/';


interface CelebritiesPageProps {

  id?: string;

  title?: string;

}


export const CelebritiesPage: React.FC<CelebritiesPageProps> = ({

  id = 'celebrities',

  title = translateString('Celebrities'),

}) => (

  <Page id={id}>

    <ApiUrlConsumer>

      {(apiUrl) => (

        <MediaListWrapper title={title} className="items-list-ver">

          <LazyLoadItemListAsync

            singleLinkContent={true}

            inCelebritiesList={true}

            requestUrl={'/api/v1/celebrities'}

          />

        </MediaListWrapper>

      )}

    </ApiUrlConsumer>

  </Page>

);
