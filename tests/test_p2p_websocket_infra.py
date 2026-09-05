from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def test_p2p_websocket_dependencies_are_pinned():
    requirements = _read("requirements.txt")
    assert "channels==4.3.2" in requirements
    assert "channels-redis==4.3.0" in requirements
    assert "daphne==4.2.3" in requirements


def test_p2p_websocket_is_proxied_to_supervised_asgi():
    nginx = _read("deploy/docker/nginx_http_only.conf")
    prestart = _read("deploy/docker/prestart.sh")
    asgi = _read("deploy/docker/supervisord/supervisord-asgi.conf")

    assert "location /ws/" in nginx
    assert "proxy_pass http://127.0.0.1:9001" in nginx
    assert 'proxy_set_header Upgrade $http_upgrade' in nginx
    assert "supervisord-asgi.conf" in prestart
    assert "127.0.0.1 -p 9001" in asgi
    assert "--ping-interval 20" in asgi


def test_production_web_healthchecks_include_asgi():
    for compose_path in ("docker-compose.yaml", "docker-compose-cloudflare.yaml"):
        compose = _read(compose_path)
        web = compose.split("  web:\n", 1)[1]
        assert "socket.create_connection(('127.0.0.1', 9001), 2)" in web
        assert "CHANNEL_REDIS_LOCATION" in compose


def test_browser_chat_uses_websocket_without_poll_loop():
    js = _read("frontend/src/static/js/pages/p2p-exchange.js")
    template = _read("templates/cms/p2p_exchange.html")
    assert "new WebSocket" in js
    assert "data-websocket-path" in template
    assert "schedulePoll" not in js
    assert "after_id=" not in js
