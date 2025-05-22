import pandas as pd
from constants import S_DATA, O_FUTL


class Equity:
    def __init__(self, exchange):
        self._exchange = exchange
        self._json_file = S_DATA + self._exchange + ".json"
        if O_FUTL.is_file_not_2day(self._json_file):
            self._download()

    def _download(self):
        url = f"https://api.kite.trade/instruments/{self._exchange}"
        df = pd.read_csv(url)
        df = df[["tradingsymbol", "instrument_token", "exchange"]]
        sym_from_json = df.to_dict(orient="records")
        O_FUTL.write_file(self._json_file, sym_from_json)

    def find_token_from_tradingsymbol(self, tradingsymbols):
        if isinstance(tradingsymbols, str):
            tradingsymbols = [tradingsymbols]

        sym_from_json: list = O_FUTL.read_file(self._json_file)
        if isinstance(sym_from_json, list):
            return [
                dct
                for dct in sym_from_json
                if dct.get("tradingsymbol") in tradingsymbols
            ]
        return [{}]


if __name__ == "__main__":
    obj = Equity("NSE")
    lst = obj.find_token_from_tradingsymbol(["SBIN", "PARADEEP"])
