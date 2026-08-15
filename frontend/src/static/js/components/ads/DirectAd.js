import React from 'react';

export class DirectAd extends React.PureComponent {
  constructor(props) {
    super(props);
    this.state = { ad: null };
    this._mounted = false;
  }

  componentDidMount() {
    this._mounted = true;
    const flags = window.MediaCMS && window.MediaCMS.user && window.MediaCMS.user.is;
    if (flags && (flags.adFree || flags.advanced)) {
      return;
    }

    fetch(`/api/v1/direct-ads/serve/${encodeURIComponent(this.props.slot)}/`, {
      method: 'GET',
      credentials: 'same-origin',
      cache: 'no-store',
      headers: { Accept: 'application/json' },
    })
      .then((response) => {
        if (response.status === 204) return null;
        if (!response.ok) return null;
        return response.json();
      })
      .then((ad) => {
        if (this._mounted && ad && ad.creative_url && ad.click_url) {
          this.setState({ ad });
        }
      })
      .catch(() => {});
  }

  componentWillUnmount() {
    this._mounted = false;
  }

  render() {
    if (!this.state.ad) return null;
    const slot = this.props.slot;
    return (
      <div className={`direct-ad direct-ad--${slot}`}>
        <a
          href={this.state.ad.click_url}
          target="_blank"
          rel="sponsored noopener noreferrer"
        >
          <img src={this.state.ad.creative_url} alt="" />
        </a>
      </div>
    );
  }
}
