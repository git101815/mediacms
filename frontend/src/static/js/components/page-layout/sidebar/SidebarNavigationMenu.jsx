import React, { useContext } from 'react';
import urlParse from 'url-parse';
import { useUser } from '../../../utils/hooks/';
import { PageStore } from '../../../utils/stores/';
import { LinksContext, SidebarContext } from '../../../utils/contexts/';
import { NavigationMenuList } from '../../_shared';
import { translateString } from '../../../utils/helpers/';

export function SidebarNavigationMenu() {
  const { userCan, isAnonymous, pages: userPages } = useUser();

  const links = useContext(LinksContext);
  const sidebar = useContext(SidebarContext);

  const currentUrl = urlParse(window.location.href);
  const currentHostPath = (currentUrl.host + currentUrl.pathname).replace(/\/+$/, '');

  function formatItems(items) {
    return items.map((item) => {
      const url = urlParse(item.link);
      const active = currentHostPath === url.host + url.pathname;

      return {
        active,
        itemType: 'link',
        link: item.link || '#',
        icon: item.icon || null,
        iconPos: 'left',
        text: item.text || item.link || '#',
        itemAttr: {
          className: item.className || '',
        },
       linkAttr: item.linkAttr || {},
      };
    });
  }

  function MainMenuFirstSection() {
    const items = [];

    if (!sidebar.hideHomeLink) {
      items.push({
        link: links.home,
        icon: 'home',
        text: translateString('Home'),
        className: 'nav-item-home',
      });
    }

    if (PageStore.get('config-enabled').pages.featured && PageStore.get('config-enabled').pages.featured.enabled) {
      items.push({
        link: links.featured,
        icon: 'star',
        text: translateString('Featured'),
        className: 'nav-item-featured',
      });
    }

    if (
      PageStore.get('config-enabled').pages.recommended &&
      PageStore.get('config-enabled').pages.recommended.enabled
    ) {
      items.push({
        link: links.recommended,
        icon: 'bolt',
        text: translateString("Recommended"),
        className: 'nav-item-recommended',
      });
    }

    if (PageStore.get('config-enabled').pages.latest && PageStore.get('config-enabled').pages.latest.enabled) {
      items.push({
        link: links.latest,
        icon: 'local_fire_department',
        text: translateString("Latest"),
        className: 'nav-item-latest',
      });
    }

    if (
      PageStore.get('config-enabled').taxonomies.celebrities &&
      PageStore.get('config-enabled').taxonomies.celebrities.enabled
    ){
      items.push({
        link: links.archive.celebrities,
        icon: 'face_3',
        text: translateString("Celebrities"),
        className: 'nav-item-celebrities',
      });
    }
  
    if (
      !sidebar.hideCategoriesLink &&
      PageStore.get('config-enabled').taxonomies.categories &&
      PageStore.get('config-enabled').taxonomies.categories.enabled
    ) {
      items.push({
        link: links.archive.categories,
        icon: 'format_list_bulleted',
        text: translateString("Categories"),
        className: 'nav-item-categories',
      });
    }

    if (
      !sidebar.hideTagsLink &&
      PageStore.get('config-enabled').taxonomies.tags &&
      PageStore.get('config-enabled').taxonomies.tags.enabled
    ) {
      items.push({
        link: links.archive.tags,
        icon: 'tag',
        text: translateString("Tags"),
        className: 'nav-item-tags',
      });
    }

    if (PageStore.get('config-enabled').pages.members && PageStore.get('config-enabled').pages.members.enabled) {
      items.push({
        link: links.members,
        icon: 'groups',
        text: translateString("Members"),
        className: 'nav-item-members',
      });
    }

    const extraItems = PageStore.get('config-contents').sidebar.mainMenuExtra.items;

    extraItems.forEach((navitem) => {
      items.push({
        link: navitem.link,
        icon: navitem.icon,
        text: navitem.text,
        className: navitem.className,
      });
    });

    return items.length ? <NavigationMenuList key="main-first" items={formatItems(items)} /> : null;
  }

  function MainMenuSecondSection() {
    const items = [];

    if (!isAnonymous) {
      if (userCan.addMedia) {
        items.push({
          link: links.user.addMedia,
          icon: 'video_call',
          text: translateString("Upload"),
          className: 'nav-item-upload-media',
        });

        if (userPages.media) {
          items.push({
            link: userPages.media,
            icon: 'video_library',
            text: translateString("My media"),
            className: 'nav-item-my-media',
          });
        }
      }

      if (userCan.saveMedia) {
        items.push({
          link: userPages.playlists,
          icon: 'playlist_play',
          text: translateString("My playlists"),
          className: 'nav-item-my-playlists',
        });
      }
    }

    return items.length ? <NavigationMenuList key="main-second" items={formatItems(items)} /> : null;
  }

  function UserMenuSection() {
    const items = [];

    if (PageStore.get('config-enabled').pages.history && PageStore.get('config-enabled').pages.history.enabled) {
      items.push({
        link: links.user.history,
        icon: 'history',
        text: translateString("History"),
        className: 'nav-item-history',
      });
    }

    if (
      userCan.likeMedia &&
      PageStore.get('config-enabled').pages.liked &&
      PageStore.get('config-enabled').pages.liked.enabled
    ) {
      items.push({
        link: links.user.liked,
        icon: 'thumb_up',
        text: translateString("Liked media"),
        className: 'nav-item-liked',
      });
    }

    return items.length ? <NavigationMenuList key="user" items={formatItems(items)} /> : null;
  }

  function CustomMenuSection() {
    const items = [];

    items.push({
      link: '/about',
      icon: 'info',
      text: translateString("About"),
      className: 'nav-item-about',
    });
/*
    items.push({
      link: 'https://theporndude.com/',
      icon: 'verified',
      text: translateString("Reviewed on PornDude"),
      className: 'nav-item-reviewed',
      linkAttr: { rel: 'nofollow noopener noreferrer', target: '_blank' },
    });
*/
    items.push({
      link: '/partnership',
      icon: 'verified',
      text: translateString("Our partners"),
      className: 'nav-item-reviewed',
      linkAttr: { rel: 'nofollow noopener', target: '_blank' },
    });

    items.push({
      link: '/tos',
      icon: 'description',
      text: translateString("Terms"),
      className: 'nav-item-terms',
    });

    items.push({
      link: '/contact',
      icon: 'alternate_email',
      text: translateString("Contact"),
      className: 'nav-item-contact',
    });

    items.push({
      link: '/setlanguage',
      icon: 'translate',
      text: translateString("Language"),
      className: 'nav-item-language',
    });

    return items.length ? <NavigationMenuList key="custom" items={formatItems(items)} /> : null;
  }

  function AdminMenuSection() {
    const items = [];

    if (userCan.manageMedia) {
      items.push({
        link: links.manage.media,
        icon: 'miscellaneous_services',
        text: translateString("Manage media"),
        className: 'nav-item-manage-media',
      });
    }

    if (userCan.manageUsers) {
      items.push({
        link: links.manage.users,
        icon: 'miscellaneous_services',
        text: translateString("Manage users"),
        className: 'nav-item-manage-users',
      });
    }

    if (userCan.manageComments) {
      items.push({
        link: links.manage.comments,
        icon: 'miscellaneous_services',
        text: translateString("Manage comments"),
        className: 'nav-item-manage-comments',
      });
    }

    return items.length ? <NavigationMenuList key="admin" items={formatItems(items)} /> : null;
  }

  return [MainMenuFirstSection(), MainMenuSecondSection(), UserMenuSection(), CustomMenuSection(), AdminMenuSection()];
}
