class WatchlistClientMixin:
    def get_watchlist(self, options: list[dict]) -> list[dict]:
        result = self.post_signed(
            "/api/internal/ledger/deposit-watchlist",
            {"options": options},
        )
        return result["results"]