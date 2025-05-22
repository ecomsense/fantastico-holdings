from constants import logging


class Wsocket:
    tokens = []
    _ltp = {}

    def __init__(self, api, tokens):
        self.api = api
        self.tokens = tokens
        self.kws = api.kite.kws()
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_reconnect = self.on_reconnect
        self.kws.on_noreconnect = self.on_noreconnect

        # Infinite loop on the main thread. Nothing after this will run.
        # You have to use the pre-defined callbacks to manage subscriptions.
        self.kws.connect(threaded=True)

    def ltp(self, tokens=None):
        if tokens is not None:
            self.tokens = tokens
        return self._ltp

    def on_ticks(self, ws, ticks):
        if self.tokens is not None:
            ws.subscribe(self.tokens)
            # self.tokens = False
        self._ltp = {tick["instrument_token"]: tick["last_price"] for tick in ticks}

    def on_connect(self, ws, response):
        if response:
            print(f"on connect: {response}")
        ws.subscribe(self.tokens)
        # Set RELIANCE to tick in `full` mode.
        ws.set_mode(ws.MODE_LTP, self.tokens)

    def on_close(self, ws, code, reason):
        # On connection close stop the main loop
        # Reconnection will not happen after executing `ws.stop()`
        # ws.stop()
        """
        logging.error(
            "Wsocket close: {code} - {reason}".format(code=code, reason=reason)
        )
        """
        print("wsocket closed")

    def on_error(self, ws, code, reason):
        # Callback when connection closed with error.
        """
        logging.error(
            "Connection error: {code} - {reason}".format(code=code, reason=reason)
        )
        """
        print("error in websocket")

    def on_reconnect(self, ws, attempts_count):
        # Callback when reconnect is on progress
        logging.warning("Reconnecting: {}".format(attempts_count))

    # Callback when all reconnect failed (exhausted max retries)

    def on_noreconnect(self, ws):
        logging.error("Reconnect failed.")
