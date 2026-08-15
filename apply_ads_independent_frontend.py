#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path.cwd()
FILES = {'ads/static/ads/ads.css': "/*\n * Ads advertiser UI.\n * Geometry, typography, spacing and colors intentionally mirror the\n * supplied Clickaine advertiser reference instead of the previous custom UI.\n */\n*, *::after, *::before {\n  margin: 0;\n  padding: 0;\n  box-sizing: inherit;\n  -moz-box-sizing: inherit;\n  -webkit-box-sizing: inherit;\n  -webkit-font-smoothing: antialiased !important;\n}\n\nhtml {\n  -moz-osx-font-smoothing: grayscale;\n  -webkit-font-smoothing: antialiased;\n}\n\nbody {\n  min-width: 1200px;\n  box-sizing: border-box;\n  font-family: Roboto, sans-serif;\n  background: #fff;\n  color: #4A4A4A;\n}\n\na { color: inherit; }\nbutton, input, select, textarea { font-family: inherit; }\nh1, h2, h3, h4, h5 { color: #4A4A4A; font-weight: 400; }\n#app { height: 100%; }\n\n/* Top navbar: copied from the supplied Clickaine layout dimensions. */\n.navbar {\n  width: 100%;\n  height: 50px;\n  background-color: #37474f;\n  padding: 8px 0 8px 19px;\n  position: fixed;\n  top: 0;\n  left: 0;\n  z-index: 7;\n}\n\n.navbar__logo-link {\n  display: block;\n  width: 260px;\n  height: 34px;\n}\n\n.navbar .clickaine-logo {\n  position: absolute;\n  top: 50%;\n  transform: translate(0, -50%);\n  display: flex;\n  align-items: center;\n  width: 260px;\n  height: 22px;\n  color: #fff;\n  text-decoration: none;\n  white-space: nowrap;\n  overflow: visible;\n}\n\n.clickaine-logo__mark {\n  display: inline-block;\n  width: 23px;\n  height: 23px;\n  margin-right: 8px;\n  position: relative;\n  flex: 0 0 23px;\n}\n.clickaine-logo__mark::before,\n.clickaine-logo__mark::after {\n  content: '';\n  position: absolute;\n  background: #00acc1;\n}\n.clickaine-logo__mark::before {\n  width: 17px;\n  height: 17px;\n  left: 3px;\n  top: 3px;\n  transform: rotate(45deg);\n}\n.clickaine-logo__mark::after {\n  width: 7px;\n  height: 7px;\n  left: 8px;\n  top: 8px;\n  background: #37474f;\n}\n.clickaine-logo__text {\n  font-size: 18px;\n  line-height: 20px;\n  font-weight: 500;\n  letter-spacing: .4px;\n  max-width: none;\n  overflow: visible;\n  text-overflow: clip;\n}\n\n.navbar--switch-menu {\n  position: absolute;\n  top: 50%;\n  transform: translate(0, -50%);\n  margin-left: 215px;\n}\n.navbar--switch-menu ul { list-style: none; }\n.navbar--switch-menu ul li { display: inline-block; }\n.navbar--switch-menu ul li a {\n  padding: 18px 0;\n  margin: 0 12px;\n  display: inline-block;\n  text-decoration: none;\n  text-transform: uppercase;\n  font-weight: 400;\n  font-size: 14px;\n  color: #8e8e8e;\n  letter-spacing: 1px;\n}\n.navbar--switch-menu ul li a:hover { color: #00e5ff; }\n.navbar--switch-menu ul li a.navbar-active {\n  color: #fff;\n  position: relative;\n}\n.navbar--switch-menu ul li a.navbar-active::after {\n  display: block;\n  position: absolute;\n  width: 100%;\n  height: 3px;\n  background: #019fb6;\n  top: 48px;\n  content: '';\n}\n.navbar--switch-menu ul li a.navbar-active:hover { color: #fff; }\n\n.navbar-right { display: block; }\n.navbar #navbar-info {\n  position: absolute;\n  top: 50%;\n  right: 82px;\n  transform: translate(0, -50%);\n}\n.navbar #navbar-amount {\n  font-size: 22px;\n  text-decoration: none;\n  color: #05a0b3;\n}\n.navbar #navbar-amount:hover { color: #00e5ff; }\n.navbar #logout {\n  cursor: pointer;\n  text-decoration: none;\n  display: inline-block;\n  top: 50%;\n  transform: translate(0, -50%);\n  right: 36px;\n  position: absolute;\n  height: 17px;\n  width: 20px;\n}\n.navbar #logout .icon--logout {\n  width: 100%;\n  height: 100%;\n  fill: #959da1;\n  transition: fill .2s ease-in-out;\n}\n.navbar #logout:hover .icon--logout { fill: #f3f3f3; }\n\n.navbar__new-version {\n  position: absolute;\n  top: 50%;\n  right: 151px;\n  transform: translate(0, -50%);\n  display: flex;\n  align-items: center;\n  padding: 6px 12px;\n  background: #00bcd4;\n  border: none;\n  border-radius: 4px;\n  color: #fff;\n  text-decoration: none;\n  font-size: 16px;\n  font-weight: 500;\n  transition: all .2s ease;\n}\n.navbar__new-version:hover { filter: brightness(1.15); }\n.navbar__new-version__icon { width: 18px; height: 18px; margin-right: 8px; }\n.navbar__new-version__icon svg { fill: #fff; }\n.navbar__new-version__badge {\n  margin-left: 6px;\n  padding: 2px 4px;\n  background: #ff5252;\n  color: #fff;\n  font-size: 9px;\n  font-weight: 700;\n}\n\n/* Sidebar: same 230px/60px behavior as supplied Clickaine page. */\n.sidebar {\n  position: fixed;\n  top: 0;\n  left: 0;\n  width: 230px;\n  height: 100%;\n  box-shadow: 2px 0 12px 0 rgba(0, 0, 0, .22);\n  overflow-y: auto;\n  overflow-x: hidden;\n  background: #fff;\n  z-index: 3;\n  transition: width .3s ease-in-out;\n}\n.sidebar.--closed { width: 60px; }\n.sidebar a div span.item--heading,\n.sidebar span div span.item--heading { opacity: 1; transition: opacity .3s ease-in-out; }\n.sidebar.--closed a div span.item--heading,\n.sidebar.--closed span div span.item--heading { opacity: 0; }\n\n.toggle-sidebar-button {\n  cursor: pointer;\n  position: fixed;\n  top: 55px;\n  transition: left .3s ease-in-out;\n  left: calc(230px - 20px);\n  z-index: 8;\n  width: 12px;\n  height: 12px;\n  border: 0;\n  background: transparent;\n  padding: 0;\n}\n.toggle-sidebar-button.--closed { left: 40px; }\n.toggle-sidebar-button svg { width: 12px; height: 12px; fill: #9b9b9b75; transition: transform .3s ease; }\n.toggle-sidebar-button.--closed svg { transform: rotate(180deg); }\n\n.sidebar-item {\n  display: block;\n  fill: #9b9b9b;\n  color: #9b9b9b;\n  padding: 0 19px;\n  position: relative;\n  font-weight: 500;\n  margin: 16px 0;\n  text-decoration: none;\n  height: 34px;\n}\n.sidebar-item:first-child { margin-top: 80px; margin-bottom: 36px; }\n.sidebar-item:last-child { margin-bottom: 50px; }\n.sidebar-item.active { color: #37474f; font-weight: 700; }\n.sidebar-item.active .sidebar-item--icon svg { fill: #00acc1; }\n.sidebar-item.active:hover { color: #37474f; }\n.sidebar-item:hover { color: #00acc1; fill: #00acc1; }\n.sidebar-item .item--heading {\n  line-height: 34px;\n  font-size: 17px;\n  position: absolute;\n  top: 50%;\n  transform: translate(0, -50%);\n  user-select: none;\n  white-space: nowrap;\n}\n.sidebar-item .sidebar-item--icon {\n  display: inline-block;\n  width: 22px;\n  height: 22px;\n  margin-right: 11px;\n  margin-top: 6px;\n}\n.sidebar-item .sidebar-item--icon .icon { width: 100%; height: 100%; }\n.sidebar-separator { height: 1px; background: #f1f1f1; margin: 26px 18px; }\n\n/* Main Clickaine content shell. */\n.active-page {\n  margin-left: 230px;\n  transition: margin-left .3s ease-in-out;\n  padding-bottom: 80px;\n}\n.active-page.--sidebar-closed { margin-left: 60px; }\n.active-page__top-space { padding-top: 70px; }\n.active-page--container { position: relative; }\n.active-page--content { margin-top: 18px; margin-bottom: 80px; }\n\n.page-header {\n  position: relative;\n  display: flex;\n  justify-content: space-between;\n  margin-right: 34px;\n  flex-wrap: wrap;\n  align-items: center;\n}\n.page-header__right {\n  display: flex;\n  align-items: center;\n  margin-left: 34px;\n  flex-wrap: wrap;\n}\n.page-header__right__date-range { margin-right: 25px; }\n\n.active-page--title {\n  font-size: 36px;\n  font-weight: 400;\n  color: #4A4A4A;\n  display: inline;\n  padding-left: 34px;\n  white-space: nowrap;\n}\n\n.active-page--header--filter {\n  cursor: pointer;\n  display: flex;\n  color: #00ACC1;\n  text-decoration: none;\n  font-size: 16px;\n  font-weight: 300;\n  line-height: 42px;\n  position: relative;\n}\n.active-page--header--filter:hover { color: #00c8ff; }\n\n/* Clickaine buttons. */\n.btn {\n  display: inline-block;\n  text-align: center;\n  text-decoration: none;\n  text-transform: uppercase;\n  border: none;\n  border-radius: 2px;\n  color: #fff;\n  cursor: pointer;\n  outline: none;\n  font-size: 14px;\n  letter-spacing: .5px;\n  margin-right: 14px;\n  transition: box-shadow .3s ease-in-out, color .3s ease-in-out, background .3s ease-in-out;\n  user-select: none;\n}\n.btn:last-child { margin-right: 0; }\n.btn.add { background: #3f51b5; padding: 10px 12.17px; }\n.btn.add:hover { box-shadow: 0 2px 3px rgba(0,0,0,.26); }\n.btn.add .btn-icon {\n  float: left;\n  margin-right: 4px;\n  margin-left: 0;\n  width: 15px;\n  height: 15px;\n  position: relative;\n}\n.btn.add .btn-icon::before,\n.btn.add .btn-icon::after {\n  content: '';\n  position: absolute;\n  background: #fff;\n  top: 50%;\n  left: 50%;\n  transform: translate(-50%, -50%);\n}\n.btn.add .btn-icon::before { width: 12px; height: 2px; }\n.btn.add .btn-icon::after { width: 2px; height: 12px; }\n.btn.success { padding: 10px 16px; background-color: #4caf50; }\n.btn.success:hover { box-shadow: 0 2px 3px rgba(0,0,0,.26); }\n.btn.cancel { background: none; padding: 10px 12px; color: #9b9b9b; }\n.btn.cancel:hover { background: rgba(155,155,155,.2); }\n.btn.danger { background: none; padding: 10px 12px; color: #ff9797; }\n.btn.danger:hover { background: rgba(255,151,151,.2); }\n.table-btn {\n  display: inline-block;\n  text-align: center;\n  text-decoration: none;\n  border: none;\n  cursor: pointer;\n  outline: none;\n  font-size: 13px;\n  user-select: none;\n  color: #00acc1;\n  background-color: transparent;\n  transition: color .2s ease;\n}\n.table-btn:hover { color: #00c8ff; }\n\n/* Statistics tabs: exact geometry from the saved Clickaine page. */\n.active-page--add-zone--choice {\n  overflow: hidden;\n  width: 100%;\n  list-style: none;\n  display: flex;\n  border-bottom: 1px solid #e0e0e0;\n}\n.active-page--add-zone--choice li { margin-left: 43px; }\n.active-page--add-zone--choice li:first-child { margin-left: 34px; }\n.active-page--add-zone--choice li button,\n.active-page--add-zone--choice li a {\n  display: block;\n  padding: 16px 0;\n  text-decoration: none;\n  text-transform: uppercase;\n  border: none;\n  background: none;\n  font-size: 14px;\n  font-weight: 400;\n  letter-spacing: .5px;\n  color: #a8a8a8;\n  position: relative;\n  white-space: nowrap;\n}\n.active-page--add-zone--choice li a:not(.disable):hover { color: #606060; cursor: pointer; }\n.active-page--add-zone--choice li a:not(.disable):hover::after,\n.active-page--add-zone--choice li a.active::after {\n  content: '';\n  display: block;\n  position: absolute;\n  width: 100%;\n  height: 3px;\n  background: #00ACC1;\n  top: 46px;\n}\n.active-page--add-zone--choice li a.active { color: #606060; }\n.active-page--add-zone--choice li a.disable { cursor: default; }\n\n/* Statistics/campaign table: same compact Clickaine table system. */\n.active-page--content--table { width: 100%; border-collapse: collapse; }\n.active-page--content--table thead tr { border-bottom: 1px solid #e8e7e8; }\n.active-page--content--table thead tr th {\n  font-size: 15px;\n  user-select: none;\n  vertical-align: bottom;\n  color: #00acc1;\n  font-weight: 400;\n  padding: 9px 10px 9px 0;\n  white-space: nowrap;\n  text-align: left;\n}\n.active-page--content--table thead tr th:first-child { text-align: left; padding-left: 34px; }\n.active-page--content--table thead tr th:last-child { padding-right: 34px; }\n.active-page--content--table tbody tr td {\n  font-size: 15px;\n  padding: 8px 8px 8px 0;\n  color: #828282;\n  background: #fbfcfe;\n  border-bottom: 1px solid #eae9e9;\n  background-clip: padding-box;\n  font-weight: 300;\n  vertical-align: middle;\n}\n.active-page--content--table tbody tr td:not(:first-child) { color: #3e3e3e; }\n.active-page--content--table tbody tr td:first-child {\n  text-align: left;\n  padding-right: 0;\n  color: #00acc1;\n  padding-left: 34px;\n}\n.active-page--content--table tbody tr:hover td { background: #f4feff; }\n.active-page--content--table tbody tr td .link {\n  color: #00acc1;\n  font-weight: 400;\n  cursor: pointer;\n  text-decoration: none;\n}\n.active-page--content--table tbody tr td .link:hover { color: #00c8ff; }\n.table-text__small > * { font-size: 12px; }\n.table-text__left { text-align: left; }\n.table-text__right { text-align: right; }\n.table-text__center { text-align: center; }\n.table-text__light { font-weight: 300; }\n.table-text__regular { font-weight: 400; }\n.table-text__bold { font-weight: 500; }\n.table-text__black { color: #3e3e3e; }\n.table-text__green { color: #4caf50; }\n\n.status-dot {\n  display: inline-block;\n  width: 10px;\n  height: 10px;\n  border-radius: 50%;\n  margin-right: 7px;\n  vertical-align: 1px;\n  background: #9b9b9b;\n}\n.status-dot.active { background: #4caf50; }\n.status-dot.pending { background: #ff9800; }\n.status-dot.rejected { background: #f44336; }\n.status-dot.paused, .status-dot.funds { background: #9b9b9b; }\n.row-action-form { display: inline; }\n.row-action-form button { padding: 0; }\n\n.totals-row td,\n.active-page--content--table tbody tr.totals-row:hover td {\n  background: #fff;\n  color: #4A4A4A;\n  padding-top: 16px;\n  padding-bottom: 16px;\n  border-bottom: none;\n  font-size: 12px;\n}\n.totals-row b { font-weight: 500; }\n\n.empty-row td {\n  height: 110px;\n  text-align: center !important;\n  color: #9b9b9b !important;\n  padding-left: 34px !important;\n  background: #fbfcfe !important;\n}\n\n/* Campaign form: Clickaine general-form styling. */\n.campaign-actions-wrapper {\n  position: fixed;\n  top: 118px;\n  right: 34px;\n  width: auto;\n  z-index: 6;\n}\n.campaign-container { padding-bottom: 180px; }\n.adv-form {\n  width: 550px;\n  margin: 34px 0 0 34px;\n}\n.active-page .general-form {\n  margin-left: 34px;\n  margin-top: 36px;\n  width: 461px;\n}\n.general-form-section { margin-bottom: 42px; }\n.general-form-section__title {\n  font-size: 20px;\n  font-weight: 400;\n  color: #4A4A4A;\n  margin-bottom: 30px;\n}\n.active-page .general-form .group {\n  position: relative;\n  margin-bottom: 30px;\n}\n.active-page .general-form .group .title {\n  font-weight: 400;\n  position: absolute;\n  pointer-events: none;\n  left: 0;\n  top: 10px;\n  transition: top .2s ease, font-size .2s ease;\n  font-size: 16px;\n  color: #b6b6b6;\n}\n.active-page .general-form .group .form-input,\n.active-page .general-form .group input:not([type='file']),\n.active-page .general-form .group select {\n  width: 100%;\n  border: none;\n  border-bottom: 1px solid #f0f0f0;\n  padding: 6px 0;\n  font-size: 16px;\n  color: #5c5c5c;\n  background: transparent;\n  min-height: 34px;\n  border-radius: 0;\n}\n.active-page .general-form .group select { cursor: pointer; }\n.active-page .general-form .group .form-input:focus,\n.active-page .general-form .group input:not([type='file']):focus,\n.active-page .general-form .group select:focus {\n  outline: none;\n  border-bottom: 1px solid #00ACC1;\n}\n.active-page .general-form .group.has-value .title,\n.active-page .general-form .group:focus-within .title {\n  top: -12px;\n  font-size: 13px;\n}\n.active-page .general-form .group:focus-within .title { color: #00ACC1; }\n.group-help {\n  font-size: 12px;\n  line-height: 18px;\n  color: #a0a0a0;\n  margin-top: 6px;\n}\n.form-errors,\n.group .errorlist {\n  list-style: none;\n  color: #f44336;\n  font-size: 12px;\n  margin-top: 5px;\n}\n.form-errors {\n  width: 550px;\n  margin: 22px 0 0 34px;\n  padding: 12px 16px;\n  background: #ffebee;\n}\n\n.radio-list { list-style: none; margin-top: 8px; }\n.radio-list label {\n  display: flex;\n  align-items: center;\n  height: 36px;\n  cursor: pointer;\n  color: #4A4A4A;\n  font-size: 15px;\n}\n.radio-list input[type='radio'] {\n  appearance: none;\n  -webkit-appearance: none;\n  width: 18px;\n  height: 18px;\n  border: 2px solid #c9c9c9;\n  border-radius: 50%;\n  margin-right: 10px;\n  position: relative;\n}\n.radio-list input[type='radio']:checked { border-color: #00ACC1; }\n.radio-list input[type='radio']:checked::after {\n  content: '';\n  width: 10px;\n  height: 10px;\n  position: absolute;\n  left: 2px;\n  top: 2px;\n  border-radius: 50%;\n  background: #00ACC1;\n}\n\n.file-control {\n  position: relative;\n  border-bottom: 1px solid #f0f0f0;\n  padding: 8px 0 10px;\n  min-height: 40px;\n}\n.file-control input[type='file'] {\n  width: 100%;\n  color: #5c5c5c;\n  font-size: 14px;\n}\n.creative-preview {\n  margin-top: 14px;\n  max-width: 461px;\n  background: #fafafa;\n  border: 1px solid #f0f0f0;\n  min-height: 76px;\n  display: flex;\n  align-items: center;\n  justify-content: center;\n  overflow: hidden;\n  color: #b6b6b6;\n  font-size: 13px;\n}\n.creative-preview img { max-width: 100%; max-height: 250px; display: block; }\n\n.form-back-link {\n  display: inline-block;\n  margin: 0 0 14px 34px;\n  color: #00acc1;\n  font-size: 14px;\n  text-decoration: none;\n}\n.form-back-link:hover { color: #00c8ff; }\n\n@media (max-width: 1199px) {\n  body { min-width: 1000px; }\n}\n\n\n/* Ads-only product surface. No apex/main-site navigation is exposed. */\n.navbar--switch-menu,\n.navbar__new-version { display: none !important; }\n.navbar #navbar-info { right: 82px; }\n.navbar #navbar-amount::before { content: ''; }\n\n.ads-page-note {\n  color: #a8a8a8;\n  font-size: 14px;\n  font-weight: 300;\n  margin: 8px 34px 0;\n}\n\n/* Creatives */\n.creative-thumb {\n  width: 96px;\n  height: 46px;\n  object-fit: contain;\n  background: #f7f7f7;\n  border: 1px solid #eee;\n}\n.creative-name {\n  max-width: 260px;\n  overflow: hidden;\n  text-overflow: ellipsis;\n  white-space: nowrap;\n}\n\n/* Finance — same flat/material language as the supplied Clickaine finance UI. */\n.finance-content {\n  margin: 26px 34px 80px;\n  max-width: 980px;\n}\n.finance-balance {\n  display: flex;\n  align-items: baseline;\n  gap: 18px;\n  padding: 8px 0 24px;\n  border-bottom: 1px solid #ededed;\n}\n.finance-balance__amount {\n  color: #00acc1;\n  font-size: 34px;\n  font-weight: 300;\n}\n.finance-balance__tokens {\n  color: #9b9b9b;\n  font-size: 14px;\n  font-weight: 300;\n}\n.finance-section {\n  padding: 30px 0 34px;\n  border-bottom: 1px solid #ededed;\n}\n.finance-section:last-child { border-bottom: 0; }\n.finance-section h2 {\n  margin: 0 0 22px;\n  font-size: 20px;\n  font-weight: 400;\n}\n.finance-section__hint {\n  color: #9b9b9b;\n  font-size: 13px;\n  font-weight: 300;\n  margin: -14px 0 22px;\n}\n.finance-form {\n  width: 620px;\n  max-width: 100%;\n}\n.finance-field {\n  position: relative;\n  margin-bottom: 28px;\n}\n.finance-field label {\n  display: block;\n  color: #9b9b9b;\n  font-size: 13px;\n  margin-bottom: 7px;\n}\n.finance-field select {\n  width: 100%;\n  height: 38px;\n  padding: 0 2px;\n  border: 0;\n  border-bottom: 1px solid #e6e6e6;\n  border-radius: 0;\n  background: #fff;\n  color: #4a4a4a;\n  font-size: 16px;\n  outline: none;\n}\n.finance-field select:focus { border-bottom-color: #00acc1; }\n.token-pack-list {\n  width: 100%;\n  border-collapse: collapse;\n  margin: 4px 0 24px;\n}\n.token-pack-list tr { border-bottom: 1px solid #eee; }\n.token-pack-list tr:first-child { border-top: 1px solid #eee; }\n.token-pack-list td { padding: 13px 8px; color: #616161; font-weight: 300; }\n.token-pack-list td:first-child { width: 42px; }\n.token-pack-list td:last-child { text-align: right; color: #00acc1; }\n.token-pack-list input[type='radio'] {\n  appearance: none;\n  -webkit-appearance: none;\n  width: 18px;\n  height: 18px;\n  border: 2px solid #c9c9c9;\n  border-radius: 50%;\n  position: relative;\n  vertical-align: middle;\n}\n.token-pack-list input[type='radio']:checked { border-color: #00acc1; }\n.token-pack-list input[type='radio']:checked::after {\n  content: '';\n  position: absolute;\n  width: 10px;\n  height: 10px;\n  left: 2px;\n  top: 2px;\n  border-radius: 50%;\n  background: #00acc1;\n}\n.finance-actions { display: flex; align-items: center; gap: 12px; }\n.finance-actions .btn { margin-right: 0; }\n.finance-message {\n  margin-bottom: 18px;\n  padding: 10px 12px;\n  border-left: 3px solid #00acc1;\n  background: #f7fbfc;\n  color: #606060;\n  font-size: 14px;\n}\n\n/* Deposit session */\n.deposit-sheet {\n  margin: 30px 34px 80px;\n  width: 720px;\n  max-width: calc(100% - 68px);\n}\n.deposit-status-line {\n  display: flex;\n  align-items: center;\n  gap: 10px;\n  padding: 0 0 22px;\n  border-bottom: 1px solid #ededed;\n}\n.deposit-status-dot {\n  width: 9px;\n  height: 9px;\n  border-radius: 50%;\n  background: #00acc1;\n}\n.deposit-status-text { color: #4a4a4a; font-size: 18px; }\n.deposit-grid {\n  display: grid;\n  grid-template-columns: 210px 1fr;\n  border-bottom: 1px solid #ededed;\n}\n.deposit-grid dt,\n.deposit-grid dd {\n  margin: 0;\n  padding: 13px 0;\n  border-top: 1px solid #f3f3f3;\n  font-size: 14px;\n}\n.deposit-grid dt { color: #9b9b9b; font-weight: 300; }\n.deposit-grid dd { color: #4a4a4a; word-break: break-word; }\n.deposit-grid dt:first-of-type,\n.deposit-grid dt:first-of-type + dd { border-top: 0; }\n.deposit-address-row { display: flex; gap: 10px; align-items: center; }\n.deposit-address-row code {\n  font-family: monospace;\n  font-size: 13px;\n  color: #4a4a4a;\n  word-break: break-all;\n}\n.deposit-actions { display: flex; align-items: center; gap: 12px; margin-top: 24px; }\n.deposit-actions form { display: inline; }\n\n/* AuthWall — local Ads sign-in, no main-site visual dependency. */\n.auth-page {\n  min-height: 100vh;\n  background: #f7f8f9;\n  padding-top: 50px;\n}\n.auth-panel {\n  width: 410px;\n  margin: 92px auto 0;\n  background: #fff;\n  box-shadow: 0 2px 12px rgba(0,0,0,.16);\n  padding: 38px 42px 42px;\n}\n.auth-panel h1 {\n  font-size: 30px;\n  font-weight: 300;\n  margin-bottom: 8px;\n  text-align: center;\n}\n.auth-panel__subtitle {\n  text-align: center;\n  color: #9b9b9b;\n  font-size: 14px;\n  font-weight: 300;\n  margin-bottom: 34px;\n}\n.auth-field { position: relative; margin-bottom: 30px; }\n.auth-field label {\n  display: block;\n  color: #9b9b9b;\n  font-size: 13px;\n  margin-bottom: 4px;\n}\n.auth-field input {\n  width: 100%;\n  border: 0;\n  border-bottom: 1px solid #dedede;\n  border-radius: 0;\n  padding: 8px 0;\n  font-size: 16px;\n  color: #4a4a4a;\n  outline: 0;\n}\n.auth-field input:focus { border-bottom-color: #00acc1; }\n.auth-error {\n  color: #f44336;\n  font-size: 13px;\n  line-height: 19px;\n  margin: -8px 0 22px;\n}\n.auth-submit {\n  width: 100%;\n  height: 42px;\n  border: 0;\n  border-radius: 2px;\n  background: #00acc1;\n  color: #fff;\n  text-transform: uppercase;\n  font-size: 14px;\n  letter-spacing: .5px;\n  cursor: pointer;\n}\n.auth-submit:hover { background: #00bfd7; box-shadow: 0 2px 3px rgba(0,0,0,.22); }\n.auth-wordmark {\n  display: flex;\n  align-items: center;\n  color: #fff;\n  font-size: 18px;\n  font-weight: 500;\n  white-space: nowrap;\n  text-decoration: none;\n}\n.auth-navbar { padding-left: 19px; }\n\n@media (max-width: 1199px) {\n  .finance-content { max-width: calc(100vw - 130px); }\n}\n", 'ads/static/ads/ads.js': '(() => {\n  const sidebar = document.querySelector(\'.sidebar\');\n  const page = document.querySelector(\'.active-page\');\n  const toggle = document.querySelector(\'.toggle-sidebar-button\');\n\n  if (sidebar && page && toggle) {\n    toggle.addEventListener(\'click\', () => {\n      const closed = sidebar.classList.toggle(\'--closed\');\n      page.classList.toggle(\'--sidebar-closed\', closed);\n      toggle.classList.toggle(\'--closed\', closed);\n    });\n  }\n\n  document.querySelectorAll(\'.general-form .group\').forEach((group) => {\n    const control = group.querySelector(\'input:not([type="file"]), select, textarea\');\n    if (!control) return;\n    const sync = () => {\n      const value = control.value == null ? \'\' : String(control.value).trim();\n      group.classList.toggle(\'has-value\', value.length > 0);\n    };\n    control.addEventListener(\'input\', sync);\n    control.addEventListener(\'change\', sync);\n    sync();\n  });\n\n  const file = document.querySelector(\'input[type="file"][name="creative"]\');\n  const preview = document.querySelector(\'[data-creative-preview]\');\n  if (file && preview) {\n    file.addEventListener(\'change\', () => {\n      const selected = file.files && file.files[0];\n      if (!selected) return;\n      const url = URL.createObjectURL(selected);\n      preview.innerHTML = \'\';\n      const img = document.createElement(\'img\');\n      img.src = url;\n      img.alt = \'Creative preview\';\n      img.onload = () => URL.revokeObjectURL(url);\n      preview.appendChild(img);\n    });\n  }\n\n  const copyButton = document.querySelector(\'[data-copy-address]\');\n  if (copyButton) {\n    copyButton.addEventListener(\'click\', async () => {\n      const value = copyButton.getAttribute(\'data-copy-address\') || \'\';\n      if (!value) return;\n      try {\n        await navigator.clipboard.writeText(value);\n        const previous = copyButton.textContent;\n        copyButton.textContent = \'copied\';\n        window.setTimeout(() => { copyButton.textContent = previous; }, 1200);\n      } catch (_) {}\n    });\n  }\n\n  const statusNode = document.querySelector(\'[data-deposit-status-url]\');\n  if (statusNode) {\n    const statusUrl = statusNode.getAttribute(\'data-deposit-status-url\');\n    const label = document.querySelector(\'[data-deposit-status-label]\');\n    const terminal = new Set([\'transaction_complete\', \'canceled\', \'failed\', \'expired\']);\n    const poll = async () => {\n      try {\n        const response = await fetch(statusUrl, {credentials: \'same-origin\', cache: \'no-store\'});\n        if (!response.ok) return;\n        const payload = await response.json();\n        if (label && payload.status_label) label.textContent = payload.status_label;\n        if (terminal.has(payload.status)) return;\n      } catch (_) {}\n      window.setTimeout(poll, 3000);\n    };\n    window.setTimeout(poll, 1500);\n  }\n})();\n', 'ads/templates/ads/dashboard.html': '{% load static %}\n<!doctype html>\n<html lang="en">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">\n  <title>Statistics · {{ portal_name }} Ads</title>\n  <link href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700" rel="stylesheet">\n  <link rel="stylesheet" href="{% static \'ads/ads.css\' %}">\n</head>\n<body>\n<div id="app">\n\n<div class="navbar">\n  <a class="navbar__logo-link" href="/"><span class="clickaine-logo"><span class="clickaine-logo__mark"></span><span class="clickaine-logo__text">{{ portal_name }} Ads</span></span></a>\n  <span class="navbar-right">\n    <div id="navbar-info"><a id="navbar-amount" href="/finance/">{{ balance_usd }}</a></div>\n    <a id="logout" href="/logout/" aria-label="Logout"><svg class="icon icon--logout" viewBox="0 0 20 17"><path d="M13.5 11.3a.53.53 0 0 0 .75 0l3.5-3.45a.5.5 0 0 0 0-.72l-3.5-3.45a.53.53 0 0 0-.75.74L16.1 7H7.8a.52.52 0 1 0 0 1.05h8.3l-2.6 2.56a.5.5 0 0 0 0 .69zM9.4 14.9a.52.52 0 0 0 .52-.52v-2.56a.52.52 0 1 0-1.04 0v2.04H1.14V1.12h7.69v2.04a.52.52 0 1 0 1.04 0V.6a.52.52 0 0 0-.52-.52H.6A.52.52 0 0 0 .08.6v13.79c0 .29.23.52.52.52h8.8z"/></svg></a>\n  </span>\n</div>\n<div class="sidebar">\n  <a class="sidebar-item active" title="Campaigns" href="/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 3h18v5H3V3zm0 7h18v5H3v-5zm0 7h18v4H3v-4z"/></svg></span><span class="item--heading">Campaigns</span></div>\n  </a>\n  <a class="sidebar-item" title="Creatives" href="/creatives/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M4 4h16v16H4V4zm2 2v9l3-3 2 2 4-5 3 4V6H6z"/></svg></span><span class="item--heading">Creatives</span></div>\n  </a>\n  <a class="sidebar-item" title="Finance" href="/finance/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 6h18v13H3V6zm2 3v8h14V9H5zm2 2h5v2H7v-2zM5 3h14v2H5V3z"/></svg></span><span class="item--heading">Finance</span></div>\n  </a>\n</div>\n<button class="toggle-sidebar-button" type="button" aria-label="Toggle sidebar"><svg viewBox="0 0 12 12"><path d="M8.5 1.5 4 6l4.5 4.5-1.4 1.4L1.2 6l5.9-5.9z"/></svg></button>\n\n<div class="active-page">\n  <div class="active-page__top-space">\n    <div class="active-page--container">\n      <header class="page-header">\n        <h1 class="active-page--title">Statistics</h1>\n        <div class="page-header__right">\n          <span class="page-header__right__date-range active-page--header--filter">All time</span>\n          <a href="/campaigns/new/"><button type="button" class="btn add"><span class="btn-icon"></span>add campaign</button></a>\n        </div>\n      </header>\n\n      <section class="active-page--add-zone">\n        <ul class="active-page--add-zone--choice">\n          <li><a class="active" href="/">Campaigns</a></li>\n        </ul>\n      </section>\n\n      <section class="active-page--content">\n        <table class="active-page--content--table">\n          <thead><tr>\n            <th style="width:5%">ID</th><th style="width:8%">Status</th><th style="width:19%">Campaign</th>\n            <th style="width:14%">Format</th><th style="width:8%">Pay type</th><th style="width:8%">Bid</th>\n            <th style="width:8%">Views</th><th style="width:8%">Clicks</th><th style="width:7%">CTR</th>\n            <th style="width:10%">Spendings</th><th style="width:5%"></th>\n          </tr></thead>\n          <tbody>\n          {% if rows %}\n            {% for row in rows %}\n            <tr>\n              <td><a class="link" href="/campaigns/{{ row.campaign.id }}/edit/">{{ row.campaign.id }}</a></td>\n              <td><span class="status-dot {{ row.status_class }}"></span><span class="table-text__regular">{{ row.status_label }}</span></td>\n              <td><a class="link" href="/campaigns/{{ row.campaign.id }}/edit/">{{ row.campaign.name }}</a></td>\n              <td>{{ row.campaign.get_placement_display }}</td>\n              <td class="table-text__regular">{{ row.campaign.get_pricing_model_display }}</td>\n              <td>{{ row.bid }}</td><td>{{ row.impressions }}</td><td>{{ row.clicks }}</td><td>{{ row.ctr }}%</td>\n              <td><span class="table-text__green table-text__bold">{{ row.spend }}</span></td>\n              <td>\n                <a class="table-btn" href="/campaigns/{{ row.campaign.id }}/edit/">edit</a>\n                {% if row.campaign.delivery_status == \'active\' or row.campaign.delivery_status == \'paused_user\' %}\n                <form class="row-action-form" method="post" action="/campaigns/{{ row.campaign.id }}/toggle/">{% csrf_token %}<button class="table-btn" type="submit">{% if row.campaign.delivery_status == \'active\' %}pause{% else %}resume{% endif %}</button></form>\n                {% endif %}\n              </td>\n            </tr>\n            {% endfor %}\n            <tr class="totals-row"><td></td><td></td><td></td><td></td><td></td><td></td>\n              <td><b>Total views:</b><br>{{ total_impressions }}</td><td><b>Clicks:</b><br>{{ total_clicks }}</td>\n              <td><b>CTR:</b><br>{{ total_ctr }}%</td><td><b>Spendings:</b><br>{{ total_spend }}</td><td></td>\n            </tr>\n          {% else %}\n            <tr class="empty-row"><td colspan="11">No campaigns. Use “add campaign” to create the first one.</td></tr>\n          {% endif %}\n          </tbody>\n        </table>\n      </section>\n    </div>\n  </div>\n</div>\n</div>\n<script src="{% static \'ads/ads.js\' %}"></script>\n</body>\n</html>\n', 'ads/templates/ads/campaign_form.html': '{% load static %}\n<!doctype html>\n<html lang="en">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">\n  <title>{{ title }} · {{ portal_name }} Ads</title>\n  <link href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700" rel="stylesheet">\n  <link rel="stylesheet" href="{% static \'ads/ads.css\' %}">\n</head>\n<body>\n<div id="app">\n\n<div class="navbar">\n  <a class="navbar__logo-link" href="/"><span class="clickaine-logo"><span class="clickaine-logo__mark"></span><span class="clickaine-logo__text">{{ portal_name }} Ads</span></span></a>\n  <span class="navbar-right">\n    <div id="navbar-info"><a id="navbar-amount" href="/finance/">{{ balance_usd }}</a></div>\n    <a id="logout" href="/logout/" aria-label="Logout"><svg class="icon icon--logout" viewBox="0 0 20 17"><path d="M13.5 11.3a.53.53 0 0 0 .75 0l3.5-3.45a.5.5 0 0 0 0-.72l-3.5-3.45a.53.53 0 0 0-.75.74L16.1 7H7.8a.52.52 0 1 0 0 1.05h8.3l-2.6 2.56a.5.5 0 0 0 0 .69zM9.4 14.9a.52.52 0 0 0 .52-.52v-2.56a.52.52 0 1 0-1.04 0v2.04H1.14V1.12h7.69v2.04a.52.52 0 1 0 1.04 0V.6a.52.52 0 0 0-.52-.52H.6A.52.52 0 0 0 .08.6v13.79c0 .29.23.52.52.52h8.8z"/></svg></a>\n  </span>\n</div>\n<div class="sidebar">\n  <a class="sidebar-item active" title="Campaigns" href="/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 3h18v5H3V3zm0 7h18v5H3v-5zm0 7h18v4H3v-4z"/></svg></span><span class="item--heading">Campaigns</span></div>\n  </a>\n  <a class="sidebar-item" title="Creatives" href="/creatives/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M4 4h16v16H4V4zm2 2v9l3-3 2 2 4-5 3 4V6H6z"/></svg></span><span class="item--heading">Creatives</span></div>\n  </a>\n  <a class="sidebar-item" title="Finance" href="/finance/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 6h18v13H3V6zm2 3v8h14V9H5zm2 2h5v2H7v-2zM5 3h14v2H5V3z"/></svg></span><span class="item--heading">Finance</span></div>\n  </a>\n</div>\n<button class="toggle-sidebar-button" type="button" aria-label="Toggle sidebar"><svg viewBox="0 0 12 12"><path d="M8.5 1.5 4 6l4.5 4.5-1.4 1.4L1.2 6l5.9-5.9z"/></svg></button>\n\n<div class="active-page campaign-container">\n  <div class="active-page__top-space">\n    <div class="active-page--container">\n      <header class="page-header"><h1 class="active-page--title">{% if campaign %}Edit campaign{% else %}Add campaign{% endif %}</h1></header>\n      <div class="campaign-actions-wrapper">\n        <a href="/" class="btn cancel">cancel</a>\n        <button class="btn success" type="submit" form="campaign-form">{% if campaign %}save{% else %}create campaign{% endif %}</button>\n      </div>\n      <section class="active-page--add-zone"><ul class="active-page--add-zone--choice"><li><a class="active" href="#">General</a></li></ul></section>\n      {% if form.non_field_errors %}<div class="form-errors">{{ form.non_field_errors }}</div>{% endif %}\n      <form id="campaign-form" class="general-form" method="post" enctype="multipart/form-data">\n        {% csrf_token %}\n        <div class="general-form-section">\n          <h2 class="general-form-section__title">General</h2>\n          <div class="group">{{ form.name }}<span class="title">Campaign name</span>{{ form.name.errors }}</div>\n          <div class="group">{{ form.target_url }}<span class="title">URL</span>{{ form.target_url.errors }}</div>\n        </div>\n        <div class="general-form-section">\n          <h2 class="general-form-section__title">Ad format</h2>\n          <div class="group has-value">{{ form.placement }}<span class="title">Format</span>{{ form.placement.errors }}<div class="group-help">Homepage 728×90 or Video Sidebar 300×250.</div></div>\n        </div>\n        <div class="general-form-section">\n          <h2 class="general-form-section__title">Pricing</h2>\n          <div class="group has-value">{{ form.pricing_model }}<span class="title">Pay type</span>{{ form.pricing_model.errors }}</div>\n          <div class="group">{{ form.bid_tokens }}<span class="title">Bid, tokens</span>{{ form.bid_tokens.errors }}<div class="group-help">CPM: tokens per 1,000 impressions. CPC: tokens per valid click.</div></div>\n        </div>\n        <div class="general-form-section">\n          <h2 class="general-form-section__title">Creative</h2>\n          <div class="file-control">{{ form.creative }}{{ form.creative.errors }}</div>\n          <div class="group-help">The image must exactly match the selected placement dimensions.</div>\n          <div class="creative-preview" data-creative-preview>{% if campaign and campaign.creative %}<img src="{{ campaign.creative.url }}" alt="Current creative">{% else %}<span>Creative preview</span>{% endif %}</div>\n        </div>\n      </form>\n    </div>\n  </div>\n</div>\n</div>\n<script src="{% static \'ads/ads.js\' %}"></script>\n</body>\n</html>\n', 'ads/templates/ads/creatives.html': '{% load static %}\n<!doctype html>\n<html lang="en">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">\n  <title>Creatives · {{ portal_name }} Ads</title>\n  <link href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700" rel="stylesheet">\n  <link rel="stylesheet" href="{% static \'ads/ads.css\' %}">\n</head>\n<body>\n<div id="app">\n\n<div class="navbar">\n  <a class="navbar__logo-link" href="/"><span class="clickaine-logo"><span class="clickaine-logo__mark"></span><span class="clickaine-logo__text">{{ portal_name }} Ads</span></span></a>\n  <span class="navbar-right">\n    <div id="navbar-info"><a id="navbar-amount" href="/finance/">{{ balance_usd }}</a></div>\n    <a id="logout" href="/logout/" aria-label="Logout"><svg class="icon icon--logout" viewBox="0 0 20 17"><path d="M13.5 11.3a.53.53 0 0 0 .75 0l3.5-3.45a.5.5 0 0 0 0-.72l-3.5-3.45a.53.53 0 0 0-.75.74L16.1 7H7.8a.52.52 0 1 0 0 1.05h8.3l-2.6 2.56a.5.5 0 0 0 0 .69zM9.4 14.9a.52.52 0 0 0 .52-.52v-2.56a.52.52 0 1 0-1.04 0v2.04H1.14V1.12h7.69v2.04a.52.52 0 1 0 1.04 0V.6a.52.52 0 0 0-.52-.52H.6A.52.52 0 0 0 .08.6v13.79c0 .29.23.52.52.52h8.8z"/></svg></a>\n  </span>\n</div>\n<div class="sidebar">\n  <a class="sidebar-item" title="Campaigns" href="/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 3h18v5H3V3zm0 7h18v5H3v-5zm0 7h18v4H3v-4z"/></svg></span><span class="item--heading">Campaigns</span></div>\n  </a>\n  <a class="sidebar-item active" title="Creatives" href="/creatives/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M4 4h16v16H4V4zm2 2v9l3-3 2 2 4-5 3 4V6H6z"/></svg></span><span class="item--heading">Creatives</span></div>\n  </a>\n  <a class="sidebar-item" title="Finance" href="/finance/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 6h18v13H3V6zm2 3v8h14V9H5zm2 2h5v2H7v-2zM5 3h14v2H5V3z"/></svg></span><span class="item--heading">Finance</span></div>\n  </a>\n</div>\n<button class="toggle-sidebar-button" type="button" aria-label="Toggle sidebar"><svg viewBox="0 0 12 12"><path d="M8.5 1.5 4 6l4.5 4.5-1.4 1.4L1.2 6l5.9-5.9z"/></svg></button>\n\n<div class="active-page">\n  <div class="active-page__top-space">\n    <div class="active-page--container">\n      <header class="page-header">\n        <h1 class="active-page--title">Creatives</h1>\n        <div class="page-header__right"><a href="/campaigns/new/"><button type="button" class="btn add"><span class="btn-icon"></span>add campaign</button></a></div>\n      </header>\n      <p class="ads-page-note">Creatives currently belong directly to campaigns. Edit the campaign to replace a banner.</p>\n      <section class="active-page--content">\n        <table class="active-page--content--table">\n          <thead><tr><th>ID</th><th>Creative</th><th>Campaign</th><th>Format</th><th>Status</th><th></th></tr></thead>\n          <tbody>\n          {% if rows %}\n            {% for row in rows %}\n            <tr>\n              <td>{{ row.campaign.id }}</td>\n              <td>{% if row.campaign.creative %}<img class="creative-thumb" src="{{ row.campaign.creative.url }}" alt="{{ row.campaign.name }} creative">{% endif %}</td>\n              <td><a class="link" href="/campaigns/{{ row.campaign.id }}/edit/">{{ row.campaign.name }}</a></td>\n              <td>{{ row.campaign.get_placement_display }}</td>\n              <td><span class="status-dot {{ row.status_class }}"></span>{{ row.status_label }}</td>\n              <td><a class="table-btn" href="/campaigns/{{ row.campaign.id }}/edit/">edit</a></td>\n            </tr>\n            {% endfor %}\n          {% else %}\n            <tr class="empty-row"><td colspan="6">No creatives yet.</td></tr>\n          {% endif %}\n          </tbody>\n        </table>\n      </section>\n    </div>\n  </div>\n</div>\n</div>\n<script src="{% static \'ads/ads.js\' %}"></script>\n</body>\n</html>\n', 'ads/templates/ads/finance.html': '{% load static %}\n<!doctype html>\n<html lang="en">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">\n  <title>Finance · {{ portal_name }} Ads</title>\n  <link href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700" rel="stylesheet">\n  <link rel="stylesheet" href="{% static \'ads/ads.css\' %}">\n</head>\n<body>\n<div id="app">\n\n<div class="navbar">\n  <a class="navbar__logo-link" href="/"><span class="clickaine-logo"><span class="clickaine-logo__mark"></span><span class="clickaine-logo__text">{{ portal_name }} Ads</span></span></a>\n  <span class="navbar-right">\n    <div id="navbar-info"><a id="navbar-amount" href="/finance/">{{ balance_usd }}</a></div>\n    <a id="logout" href="/logout/" aria-label="Logout"><svg class="icon icon--logout" viewBox="0 0 20 17"><path d="M13.5 11.3a.53.53 0 0 0 .75 0l3.5-3.45a.5.5 0 0 0 0-.72l-3.5-3.45a.53.53 0 0 0-.75.74L16.1 7H7.8a.52.52 0 1 0 0 1.05h8.3l-2.6 2.56a.5.5 0 0 0 0 .69zM9.4 14.9a.52.52 0 0 0 .52-.52v-2.56a.52.52 0 1 0-1.04 0v2.04H1.14V1.12h7.69v2.04a.52.52 0 1 0 1.04 0V.6a.52.52 0 0 0-.52-.52H.6A.52.52 0 0 0 .08.6v13.79c0 .29.23.52.52.52h8.8z"/></svg></a>\n  </span>\n</div>\n<div class="sidebar">\n  <a class="sidebar-item" title="Campaigns" href="/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 3h18v5H3V3zm0 7h18v5H3v-5zm0 7h18v4H3v-4z"/></svg></span><span class="item--heading">Campaigns</span></div>\n  </a>\n  <a class="sidebar-item" title="Creatives" href="/creatives/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M4 4h16v16H4V4zm2 2v9l3-3 2 2 4-5 3 4V6H6z"/></svg></span><span class="item--heading">Creatives</span></div>\n  </a>\n  <a class="sidebar-item active" title="Finance" href="/finance/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 6h18v13H3V6zm2 3v8h14V9H5zm2 2h5v2H7v-2zM5 3h14v2H5V3z"/></svg></span><span class="item--heading">Finance</span></div>\n  </a>\n</div>\n<button class="toggle-sidebar-button" type="button" aria-label="Toggle sidebar"><svg viewBox="0 0 12 12"><path d="M8.5 1.5 4 6l4.5 4.5-1.4 1.4L1.2 6l5.9-5.9z"/></svg></button>\n\n<div class="active-page">\n  <div class="active-page__top-space">\n    <div class="active-page--container">\n      <header class="page-header"><h1 class="active-page--title">Finance</h1></header>\n      <div class="finance-content">\n        {% if messages %}{% for message in messages %}<div class="finance-message">{{ message }}</div>{% endfor %}{% endif %}\n        <section class="finance-section">\n          <h2>Balance</h2>\n          <div class="finance-balance"><span class="finance-balance__amount">{{ balance_usd }}</span><span class="finance-balance__tokens">{{ balance }} tokens available</span></div>\n        </section>\n\n        <section class="finance-section">\n          <h2>Add funds</h2>\n          <p class="finance-section__hint">Choose a payment method and a token pack. Payment checkout opens in a new tab so the Ads dashboard stays open.</p>\n          {% if deposit_options and token_pack_rows %}\n          <form class="finance-form" method="post" action="/finance/deposit-request/" target="_blank">\n            {% csrf_token %}\n            <div class="finance-field">\n              <label for="deposit-option">Payment method</label>\n              <select id="deposit-option" name="deposit_option_key" required>\n                <option value="">Select payment method</option>\n                {% for option in deposit_options %}\n                <option value="{{ option.key }}">{{ option.payment_group_label }}{% if option.payment_method_label and option.payment_method_label != option.payment_group_label %} · {{ option.payment_method_label }}{% endif %}{% if option.network_display and option.payment_requires_route_selection %} · {{ option.network_display }}{% endif %}</option>\n                {% endfor %}\n              </select>\n            </div>\n            <table class="token-pack-list">\n              <tbody>\n                {% for pack in token_pack_rows %}\n                <tr>\n                  <td><input type="radio" name="token_pack_key" value="{{ pack.code }}" {% if forloop.first %}checked{% endif %} required></td>\n                  <td><strong>{{ pack.token_amount_display }} tokens</strong>{% if pack.description %}<br><small>{{ pack.description }}</small>{% endif %}</td>\n                  <td>{{ pack.price_display }}$</td>\n                </tr>\n                {% endfor %}\n              </tbody>\n            </table>\n            <input type="hidden" name="return_tab" value="all">\n            <input type="hidden" name="return_status" value="all">\n            <div class="finance-actions"><button class="btn success --big" type="submit">add funds</button></div>\n          </form>\n          {% else %}\n          <div class="finance-message">Top-ups are currently unavailable.</div>\n          {% endif %}\n        </section>\n\n        <section class="finance-section">\n          <h2>Recent deposits</h2>\n          {% if recent_deposit_sessions %}\n          <table class="active-page--content--table">\n            <thead><tr><th>Date</th><th>Method</th><th>Status</th><th>Amount</th><th></th></tr></thead>\n            <tbody>\n              {% for row in recent_deposit_sessions %}\n              <tr>\n                <td>{{ row.created_at|date:"Y-m-d H:i" }}</td>\n                <td>{{ row.display_label }}</td>\n                <td>{{ row.status_label }}</td>\n                <td>{% if row.credited_amount_display %}{{ row.credited_amount_display }} tokens{% else %}—{% endif %}</td>\n                <td>{% if row.show_view %}<a class="table-btn" href="/finance/deposits/{{ row.public_id }}/">view</a>{% endif %}</td>\n              </tr>\n              {% endfor %}\n            </tbody>\n          </table>\n          {% else %}<p class="finance-section__hint" style="margin-top:0">No deposits yet.</p>{% endif %}\n        </section>\n      </div>\n    </div>\n  </div>\n</div>\n</div>\n<script src="{% static \'ads/ads.js\' %}"></script>\n</body>\n</html>\n', 'ads/templates/ads/deposit_session.html': '{% load static %}\n<!doctype html>\n<html lang="en">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">\n  <title>Deposit · {{ portal_name }} Ads</title>\n  <link href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700" rel="stylesheet">\n  <link rel="stylesheet" href="{% static \'ads/ads.css\' %}">\n</head>\n<body>\n<div id="app">\n\n<div class="navbar">\n  <a class="navbar__logo-link" href="/"><span class="clickaine-logo"><span class="clickaine-logo__mark"></span><span class="clickaine-logo__text">{{ portal_name }} Ads</span></span></a>\n  <span class="navbar-right">\n    <div id="navbar-info"><a id="navbar-amount" href="/finance/">{{ balance_usd }}</a></div>\n    <a id="logout" href="/logout/" aria-label="Logout"><svg class="icon icon--logout" viewBox="0 0 20 17"><path d="M13.5 11.3a.53.53 0 0 0 .75 0l3.5-3.45a.5.5 0 0 0 0-.72l-3.5-3.45a.53.53 0 0 0-.75.74L16.1 7H7.8a.52.52 0 1 0 0 1.05h8.3l-2.6 2.56a.5.5 0 0 0 0 .69zM9.4 14.9a.52.52 0 0 0 .52-.52v-2.56a.52.52 0 1 0-1.04 0v2.04H1.14V1.12h7.69v2.04a.52.52 0 1 0 1.04 0V.6a.52.52 0 0 0-.52-.52H.6A.52.52 0 0 0 .08.6v13.79c0 .29.23.52.52.52h8.8z"/></svg></a>\n  </span>\n</div>\n<div class="sidebar">\n  <a class="sidebar-item" title="Campaigns" href="/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 3h18v5H3V3zm0 7h18v5H3v-5zm0 7h18v4H3v-4z"/></svg></span><span class="item--heading">Campaigns</span></div>\n  </a>\n  <a class="sidebar-item" title="Creatives" href="/creatives/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M4 4h16v16H4V4zm2 2v9l3-3 2 2 4-5 3 4V6H6z"/></svg></span><span class="item--heading">Creatives</span></div>\n  </a>\n  <a class="sidebar-item active" title="Finance" href="/finance/">\n    <div><span class="sidebar-item--icon"><svg class="icon" viewBox="0 0 24 24"><path d="M3 6h18v13H3V6zm2 3v8h14V9H5zm2 2h5v2H7v-2zM5 3h14v2H5V3z"/></svg></span><span class="item--heading">Finance</span></div>\n  </a>\n</div>\n<button class="toggle-sidebar-button" type="button" aria-label="Toggle sidebar"><svg viewBox="0 0 12 12"><path d="M8.5 1.5 4 6l4.5 4.5-1.4 1.4L1.2 6l5.9-5.9z"/></svg></button>\n\n<div class="active-page">\n  <div class="active-page__top-space">\n    <div class="active-page--container">\n      <header class="page-header"><h1 class="active-page--title">Deposit</h1><div class="page-header__right"><a class="active-page--header--filter" href="/finance/">Back to finance</a></div></header>\n      <div class="deposit-sheet" data-deposit-status-url="{{ wallet_deposit_session_status_url }}">\n        <div class="deposit-status-line"><span class="deposit-status-dot"></span><span class="deposit-status-text" data-deposit-status-label>{{ deposit_session.status_label }}</span></div>\n        <dl class="deposit-grid">\n          <dt>Token pack</dt><dd>{{ deposit_session.token_pack_label|default:"—" }}</dd>\n          <dt>Payment method</dt><dd>{{ deposit_session.payment_method_label|default:deposit_session.display_label }}</dd>\n          <dt>Expected payment</dt><dd>{{ deposit_session.expected_payment_amount_display }} {{ deposit_session.expected_payment_currency }}</dd>\n          {% if deposit_session.network_display %}<dt>Network</dt><dd>{{ deposit_session.network_display }}</dd>{% endif %}\n          {% if deposit_session.deposit_address and not deposit_session.is_provider_checkout %}\n          <dt>Deposit address</dt><dd><div class="deposit-address-row"><code>{{ deposit_session.deposit_address }}</code><button type="button" class="table-btn" data-copy-address="{{ deposit_session.deposit_address }}">copy</button></div></dd>\n          {% endif %}\n          {% if deposit_session.observed_txid %}<dt>Transaction</dt><dd>{{ deposit_session.observed_txid }}</dd>{% endif %}\n        </dl>\n        <div class="deposit-actions">\n          {% if deposit_session.checkout_url and not deposit_session.is_terminal %}<a class="btn success" href="{{ deposit_session.checkout_url }}" target="_blank" rel="noopener">continue payment</a>{% endif %}\n          {% if not deposit_session.is_terminal %}<form method="post" action="{{ cancel_url }}">{% csrf_token %}<button class="btn cancel" type="submit">cancel</button></form>{% endif %}\n          <a class="btn cancel" href="/finance/">finance</a>\n        </div>\n      </div>\n    </div>\n  </div>\n</div>\n</div>\n<script src="{% static \'ads/ads.js\' %}"></script>\n</body>\n</html>\n', 'ads/templates/ads/login.html': '{% load static %}\n<!doctype html>\n<html lang="en">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">\n  <title>Sign in · {{ portal_name }} Ads</title>\n  <link href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700" rel="stylesheet">\n  <link rel="stylesheet" href="{% static \'ads/ads.css\' %}">\n</head>\n<body>\n<div id="app" class="auth-page">\n  <div class="navbar auth-navbar">\n    <a class="navbar__logo-link" href="/login/"><span class="clickaine-logo"><span class="clickaine-logo__mark"></span><span class="clickaine-logo__text">{{ portal_name }} Ads</span></span></a>\n  </div>\n  <main class="auth-panel">\n    <h1>Advertiser sign in</h1>\n    <p class="auth-panel__subtitle">Sign in to manage campaigns and advertising funds.</p>\n    {% if denied %}<div class="auth-error">This account does not have advertiser access.</div>{% endif %}\n    {% if form.non_field_errors %}<div class="auth-error">{{ form.non_field_errors }}</div>{% endif %}\n    <form method="post" action="/login/">\n      {% csrf_token %}\n      <input type="hidden" name="next" value="{{ next_path }}">\n      <div class="auth-field"><label for="{{ form.username.id_for_label }}">Username or email</label>{{ form.username }}{% if form.username.errors %}<div class="auth-error">{{ form.username.errors }}</div>{% endif %}</div>\n      <div class="auth-field"><label for="{{ form.password.id_for_label }}">Password</label>{{ form.password }}{% if form.password.errors %}<div class="auth-error">{{ form.password.errors }}</div>{% endif %}</div>\n      <button class="auth-submit" type="submit">sign in</button>\n    </form>\n  </main>\n</div>\n</body>\n</html>\n'}
HOST_URLS = 'from django.urls import path\n\nfrom . import views\n\nurlpatterns = [\n    path("login/", views.ads_login, name="ads_login"),\n    path("auth/callback/", views.sso_callback, name="ads_sso_callback"),\n    path("logout/", views.ads_logout, name="ads_logout"),\n    path("", views.dashboard, name="ads_dashboard"),\n    path("campaigns/new/", views.campaign_create, name="ads_campaign_create"),\n    path("campaigns/<int:campaign_id>/edit/", views.campaign_edit, name="ads_campaign_edit"),\n    path("campaigns/<int:campaign_id>/toggle/", views.campaign_toggle, name="ads_campaign_toggle"),\n    path("creatives/", views.creatives, name="ads_creatives"),\n    path("finance/", views.finance, name="wallet"),\n    path("finance/deposit-request/", views.finance_deposit_request, name="wallet_deposit_request"),\n    path("finance/deposits/<uuid:public_id>/", views.finance_deposit_session, name="wallet_deposit_session"),\n    path("finance/deposits/<uuid:public_id>/status/", views.finance_deposit_session_status, name="wallet_deposit_session_status"),\n    path("finance/deposits/<uuid:public_id>/cancel/", views.finance_deposit_session_cancel, name="wallet_deposit_session_cancel"),\n    path("finance/deposits/<uuid:public_id>/dfx-launch/", views.finance_dfx_launch, name="wallet_dfx_launch"),\n    path("finance/deposits/<uuid:public_id>/dfx-return/", views.finance_dfx_return, name="wallet_dfx_return"),\n    path("finance/deposits/<uuid:public_id>/dfx-return/buy", views.finance_dfx_return, name="wallet_dfx_return_buy"),\n    path("finance/deposits/<uuid:public_id>/mtpelerin-launch/", views.finance_mtpelerin_launch, name="wallet_mtpelerin_launch"),\n    path("finance/deposits/<uuid:public_id>/banxa-launch/", views.finance_banxa_launch, name="wallet_banxa_launch"),\n]\n'
MIDDLEWARE = 'from urllib.parse import urlencode\n\nfrom django.conf import settings\nfrom django.shortcuts import redirect\n\n\nclass AdsHostMiddleware:\n    """Route ADS_HOST to an independent advertiser UI and local AuthWall."""\n\n    PUBLIC_PATHS = (\n        "/login/",\n        "/auth/callback/",\n    )\n\n    def __init__(self, get_response):\n        self.get_response = get_response\n\n    def __call__(self, request):\n        host = request.get_host().split(":", 1)[0].lower()\n        ads_host = str(getattr(settings, "ADS_HOST", "") or "").strip().lower()\n        if not ads_host or host != ads_host:\n            return self.get_response(request)\n\n        request.urlconf = "ads.host_urls"\n\n        if request.path.startswith(self.PUBLIC_PATHS):\n            return self.get_response(request)\n\n        if not getattr(request.user, "is_authenticated", False):\n            next_path = request.get_full_path()\n            if not next_path.startswith("/") or next_path.startswith("//"):\n                next_path = "/"\n            return redirect(f"/login/?{urlencode({\'next\': next_path})}")\n\n        if not (\n            getattr(request.user, "advertiserUser", False)\n            or getattr(request.user, "is_superuser", False)\n        ):\n            return redirect("/login/?denied=1")\n\n        return self.get_response(request)\n'


def fail(message):
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def replace_once(text, old, new, label):
    if new in text:
        return text, False
    count = text.count(old)
    if count != 1:
        fail(f"{label}: expected one anchor, found {count}")
    return text.replace(old, new, 1), True


def main():
    required = [
        "ads/views.py", "ads/middleware.py", "ads/host_urls.py",
        "ads/templates/ads", "ads/static/ads", "files/views.py",
        "ledger/dfx_deposits.py", "ledger/banxa_deposits.py",
    ]
    missing = [item for item in required if not (ROOT / item).exists()]
    if missing:
        fail("run from the MediaCMS repository root; missing: " + ", ".join(missing))

    for rel, content in FILES.items():
        path = ROOT / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        print("wrote", rel)

    (ROOT / "ads/host_urls.py").write_text(HOST_URLS, encoding="utf-8")
    (ROOT / "ads/middleware.py").write_text(MIDDLEWARE, encoding="utf-8")
    print("wrote ads/host_urls.py")
    print("wrote ads/middleware.py")

    # ads/views.py imports.
    path = ROOT / "ads/views.py"
    text = path.read_text(encoding="utf-8")
    text, _ = replace_once(
        text,
        "from django.contrib.auth import get_user_model, login, logout\n",
        "from django.contrib.auth import get_user_model, login, logout\nfrom django.contrib.auth.forms import AuthenticationForm\n",
        "ads/views.py AuthenticationForm import",
    )
    text, _ = replace_once(
        text,
        "from django.views.decorators.http import require_GET, require_http_methods, require_POST\n",
        "from django.views.decorators.cache import never_cache\nfrom django.views.decorators.http import require_GET, require_http_methods, require_POST\n",
        "ads/views.py never_cache import",
    )
    text, _ = replace_once(
        text,
        "from ledger.services import get_wallet_available_balance\n",
        "from ledger.models import DepositSession, TokenWallet\nfrom ledger.services import (\n    PLATFORM_TOKEN_DECIMALS,\n    PLATFORM_TOKENS_PER_STABLECOIN,\n    get_wallet_available_balance,\n)\n",
        "ads/views.py ledger imports",
    )

    # Replace nav helper; this removes all user-facing FRONTEND_HOST links.
    start = text.find("def _ads_nav_context(user):")
    end = text.find("@require_GET\ndef sso_start", start)
    if start == -1 or end == -1:
        fail("ads/views.py nav helper anchors not found")
    helper = '''def _format_ads_balance_usd(available_micro):
    tokens = (
        Decimal(int(available_micro))
        / (Decimal(10) ** PLATFORM_TOKEN_DECIMALS)
    )
    usd_value = tokens / Decimal(PLATFORM_TOKENS_PER_STABLECOIN)
    text = f"{usd_value:,.2f}".rstrip("0").rstrip(".")
    return f"{text or '0'}$"


def _get_user_wallet(user):
    wallet, _created = TokenWallet.objects.get_or_create(
        user=user,
        defaults={
            "wallet_type": TokenWallet.TYPE_USER,
            "allow_negative": False,
        },
    )
    return wallet


def _ads_nav_context(user):
    try:
        wallet = _get_user_wallet(user)
        available_micro = get_wallet_available_balance(wallet)
        balance = _format_tokens_from_micro(available_micro)
        balance_usd = _format_ads_balance_usd(available_micro)
    except Exception:
        balance = "Unavailable"
        balance_usd = "—"
    return {
        "balance": balance,
        "balance_usd": balance_usd,
        "finance_url": "/finance/",
        "portal_name": getattr(settings, "PORTAL_NAME", "MediaCMS"),
    }


@never_cache
@require_http_methods(["GET", "POST"])
def ads_login(request):
    next_path = _safe_next(request.POST.get("next") or request.GET.get("next"))
    if _is_advertiser(request.user):
        return redirect(next_path)

    form = AuthenticationForm(
        request=request,
        data=request.POST if request.method == "POST" else None,
    )
    if request.method == "POST" and form.is_valid():
        user = form.get_user()
        if not _is_advertiser(user):
            form.add_error(None, "This account does not have advertiser access.")
        else:
            login(request, user)
            return redirect(next_path)

    return render(
        request,
        "ads/login.html",
        {
            "form": form,
            "next_path": next_path,
            "denied": request.GET.get("denied") == "1",
            "portal_name": getattr(settings, "PORTAL_NAME", "MediaCMS"),
        },
    )


'''
    text = text[:start] + helper + text[end:]

    # Logout stays on Ads.
    old_logout = '''@require_GET
def ads_logout(request):
    logout(request)
    return redirect(str(settings.FRONTEND_HOST).rstrip("/") + "/")
'''
    new_logout = '''@require_GET
def ads_logout(request):
    logout(request)
    return redirect("/login/")
'''
    if old_logout in text:
        text = text.replace(old_logout, new_logout, 1)
    elif new_logout not in text:
        fail("ads/views.py logout anchor not found")

    # Dashboard uses the same local nav/balance context as every other Ads page.
    old_wallet_block = '''    wallet = request.user.token_wallet
    try:
        available_micro = get_wallet_available_balance(wallet)
        balance = _format_tokens_from_micro(available_micro)
    except Exception:
        balance = "Unavailable"

'''
    if old_wallet_block in text:
        text = text.replace(old_wallet_block, "    nav_context = _ads_nav_context(request.user)\n\n", 1)
    elif "    nav_context = _ads_nav_context(request.user)\n" not in text:
        fail("ads/views.py dashboard wallet block not found")

    old_context_tail = '''        "balance": balance,
        "total_impressions": totals["impressions"],
        "total_clicks": totals["clicks"],
        "total_ctr": f"{total_ctr:.2f}",
        "total_spend": _format_tokens_from_nanos(totals["spend_nanos"]),
        "active_campaigns": totals["active"],
        "add_funds_url": (
            str(settings.FRONTEND_HOST).rstrip("/")
            + reverse("wallet", urlconf="cms.urls")
        ),
        "profile_url": (
            str(settings.FRONTEND_HOST).rstrip("/")
            + reverse(
                "get_user",
                kwargs={"username": request.user.username},
                urlconf="cms.urls",
            )
        ),
        "portal_name": getattr(settings, "PORTAL_NAME", "MediaCMS"),
'''
    new_context_tail = '''        **nav_context,
        "total_impressions": totals["impressions"],
        "total_clicks": totals["clicks"],
        "total_ctr": f"{total_ctr:.2f}",
        "total_spend": _format_tokens_from_nanos(totals["spend_nanos"]),
        "active_campaigns": totals["active"],
'''
    if old_context_tail in text:
        text = text.replace(old_context_tail, new_context_tail, 1)
    elif new_context_tail not in text:
        fail("ads/views.py dashboard context anchor not found")

    # Add real Creatives + Ads-local Finance views once.
    marker = "# ads-independent-subdomain-v1"
    if marker not in text:
        anchor = "def _no_store(response):"
        pos = text.find(anchor)
        if pos == -1:
            fail("ads/views.py insertion anchor not found")
        extra = '''# ads-independent-subdomain-v1
def _wallet_views():
    from files import views as wallet_views
    return wallet_views


@require_GET
def creatives(request):
    rows = []
    campaigns = _campaigns_for_user(request.user).order_by("-updated_at", "-id")
    for campaign in campaigns:
        status_label, status_class = _status_view(campaign)
        rows.append({
            "campaign": campaign,
            "status_label": status_label,
            "status_class": status_class,
        })
    return render(
        request,
        "ads/creatives.html",
        {"rows": rows, **_ads_nav_context(request.user)},
    )


@never_cache
@require_GET
def finance(request):
    wallet = _get_user_wallet(request.user)
    wallet_views = _wallet_views()
    return render(
        request,
        "ads/finance.html",
        {
            **_ads_nav_context(request.user),
            "deposit_options": wallet_views._build_wallet_deposit_options(),
            "token_pack_rows": wallet_views._build_wallet_token_pack_rows(),
            "recent_deposit_sessions": wallet_views._build_recent_deposit_session_rows(wallet),
        },
    )


@require_POST
def finance_deposit_request(request):
    return _wallet_views().wallet_deposit_request(request)


@never_cache
@require_GET
def finance_deposit_session(request, public_id):
    wallet_views = _wallet_views()
    session = get_object_or_404(
        DepositSession.objects.select_related("wallet"),
        public_id=public_id,
        user=request.user,
    )
    return render(
        request,
        "ads/deposit_session.html",
        {
            **_ads_nav_context(request.user),
            "deposit_session": wallet_views._build_deposit_session_payload(session),
            "wallet_deposit_session_status_url": reverse(
                "wallet_deposit_session_status",
                kwargs={"public_id": session.public_id},
            ),
            "cancel_url": reverse(
                "wallet_deposit_session_cancel",
                kwargs={"public_id": session.public_id},
            ),
        },
    )


@never_cache
@require_GET
def finance_deposit_session_status(request, public_id):
    session = get_object_or_404(
        DepositSession.objects.only(
            "public_id", "user_id", "status", "chain", "asset_code",
            "deposit_address", "required_confirmations", "confirmations",
            "min_amount", "observed_txid", "observed_amount", "expires_at",
            "metadata",
        ),
        public_id=public_id,
        user=request.user,
    )
    return JsonResponse(_wallet_views()._build_deposit_session_payload(session))


@require_POST
def finance_deposit_session_cancel(request, public_id):
    return _wallet_views().wallet_deposit_session_cancel(request, public_id)


@never_cache
@require_GET
def finance_dfx_launch(request, public_id):
    return _wallet_views().wallet_dfx_launch(request, public_id)


@require_GET
def finance_dfx_return(request, public_id):
    return _wallet_views().wallet_dfx_return(request, public_id)


@never_cache
@require_GET
def finance_mtpelerin_launch(request, public_id):
    return _wallet_views().wallet_mtpelerin_launch(request, public_id)


@never_cache
@require_GET
def finance_banxa_launch(request, public_id):
    return _wallet_views().wallet_banxa_launch(request, public_id)


'''
        text = text[:pos] + extra + text[pos:]

    path.write_text(text, encoding="utf-8")
    print("patched ads/views.py")

    # DFX: optional first-party return URI. Default behavior remains unchanged for the main site.
    path = ROOT / "ledger/dfx_deposits.py"
    text = path.read_text(encoding="utf-8")
    old = '''def prepare_dfx_browser_launch(
    *,
    session: DepositSession,
    actor,
) -> dict:
'''
    new = '''def prepare_dfx_browser_launch(
    *,
    session: DepositSession,
    actor,
    redirect_uri: str | None = None,
) -> dict:
'''
    if old in text:
        text = text.replace(old, new, 1)
    elif new not in text:
        fail("ledger/dfx_deposits.py signature anchor not found")
    old = '''            redirect_uri=_absolute_dfx_return_url(
                session.public_id
            ),
'''
    new = '''            redirect_uri=(
                str(redirect_uri or "").strip()
                or _absolute_dfx_return_url(session.public_id)
            ),
'''
    if old in text:
        text = text.replace(old, new, 1)
    elif new not in text:
        fail("ledger/dfx_deposits.py redirect anchor not found")
    path.write_text(text, encoding="utf-8")
    print("patched ledger/dfx_deposits.py")

    # Banxa: same idea for its browser return URL.
    path = ROOT / "ledger/banxa_deposits.py"
    text = path.read_text(encoding="utf-8")
    old = '''def prepare_banxa_browser_launch(
    *,
    session: DepositSession,
    actor,
) -> dict:
'''
    new = '''def prepare_banxa_browser_launch(
    *,
    session: DepositSession,
    actor,
    return_url: str | None = None,
) -> dict:
'''
    if old in text:
        text = text.replace(old, new, 1)
    elif new not in text:
        fail("ledger/banxa_deposits.py signature anchor not found")
    old = '''        return_url=_absolute_banxa_return_url(
            session.public_id
        ),
'''
    new = '''        return_url=(
            str(return_url or "").strip()
            or _absolute_banxa_return_url(session.public_id)
        ),
'''
    if old in text:
        text = text.replace(old, new, 1)
    elif new not in text:
        fail("ledger/banxa_deposits.py return anchor not found")
    path.write_text(text, encoding="utf-8")
    print("patched ledger/banxa_deposits.py")

    # The shared launch views detect Ads requests and feed Ads-local return URLs to providers.
    path = ROOT / "files/views.py"
    text = path.read_text(encoding="utf-8")
    dfx_old = '''        launch = prepare_dfx_browser_launch(
            session=session,
            actor=request.user,
        )
'''
    dfx_new = '''        dfx_kwargs = {
            "session": session,
            "actor": request.user,
        }
        ads_host = str(getattr(settings, "ADS_HOST", "") or "").strip().lower()
        request_host = request.get_host().split(":", 1)[0].lower()
        if ads_host and request_host == ads_host:
            ads_scheme = str(getattr(settings, "ADS_SCHEME", "https") or "https")
            dfx_kwargs["redirect_uri"] = (
                f"{ads_scheme}://{request.get_host()}"
                + reverse(
                    "wallet_dfx_return",
                    kwargs={"public_id": session.public_id},
                )
            )
        launch = prepare_dfx_browser_launch(**dfx_kwargs)
'''
    if dfx_old in text:
        text = text.replace(dfx_old, dfx_new, 1)
    elif dfx_new not in text:
        fail("files/views.py DFX launch anchor not found")

    banxa_old = '''        launch = prepare_banxa_browser_launch(
            session=session,
            actor=request.user,
        )
'''
    banxa_new = '''        banxa_kwargs = {
            "session": session,
            "actor": request.user,
        }
        ads_host = str(getattr(settings, "ADS_HOST", "") or "").strip().lower()
        request_host = request.get_host().split(":", 1)[0].lower()
        if ads_host and request_host == ads_host:
            ads_scheme = str(getattr(settings, "ADS_SCHEME", "https") or "https")
            banxa_kwargs["return_url"] = (
                f"{ads_scheme}://{request.get_host()}"
                + reverse(
                    "wallet_deposit_session",
                    kwargs={"public_id": session.public_id},
                )
            )
        launch = prepare_banxa_browser_launch(**banxa_kwargs)
'''
    if banxa_old in text:
        text = text.replace(banxa_old, banxa_new, 1)
    elif banxa_new not in text:
        fail("files/views.py Banxa launch anchor not found")

    path.write_text(text, encoding="utf-8")
    print("patched files/views.py")

    print("\nAds subdomain frontend is now first-party independent:")
    print("  /login/      local advertiser AuthWall")
    print("  /            campaigns/statistics")
    print("  /creatives/  real creative listing")
    print("  /finance/    local Ads balance + top-ups")
    print("\nNo database migration is required.")
    print("Verify with: python3 manage.py check")


if __name__ == "__main__":
    main()
