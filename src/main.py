from constants import logging, O_SETG
from helper import Helper
from strategies.fantastico import Fantastico
from toolkit.kokoo import is_time_past, kill_tmux, timer, blink
from traceback import print_exc
from symbols import Equity
from typing import Any, Dict, List
from wsocket import Wsocket
import pandas as pd


def get_tokens_from_symbols(obj: Fantastico) -> List[Dict[Any, Any]]:
    """
    returns instruments details from strategy
    """
    tokens_and_tradingsymbols = []
    df1 = obj.df_stocks_in_play
    df2 = obj.df_delivered
    if df1.empty:
        df = df2.reset_index()
    elif df2.empty:
        df = df1.reset_index()
    else:
        df = pd.concat([df1, df2])

    print("SUBSCRIBING LIST", df)
    if len(df.index) > 0:
        # df = df.reset_index(names="Symbol")
        exch_sym = df.groupby("Exch")["Symbol"].apply(list).to_dict()
        for exchange, tsym in exch_sym.items():
            lst = Equity(exchange).find_token_from_tradingsymbol(tsym)
            if any(lst):
                tokens_and_tradingsymbols = (
                    tokens_and_tradingsymbols + lst
                    if any(tokens_and_tradingsymbols)
                    else lst
                )
        return tokens_and_tradingsymbols
    else:
        print("nothing to do")
        __import__("sys").exit(1)


def change_key(ltps):
    info = Helper.symbol_info
    changed = {info[k]: v for k, v in ltps.items()}
    return changed


def subscribe(lst_of_symbols):
    Helper.symbol_info = {
        dct["instrument_token"]: dct["tradingsymbol"] for dct in lst_of_symbols
    }
    tokens = list(Helper.symbol_info.keys())

    ws = Wsocket(Helper.api(), tokens)
    prices = {}
    while not any(prices):
        prices = ws.ltp(tokens)
        timer(1)
        print("waiting for websocket")
    return ws


def main():
    try:
        start = O_SETG["program"].pop("start")
        while not is_time_past(start):
            print(f"waiting for {start}")
            blink()
        else:
            logging.info("Happy Trading")

        Helper.api()
        obj = Fantastico()
        lst_of_symbols = get_tokens_from_symbols(obj)
        ws = subscribe(lst_of_symbols)
        stop = O_SETG["program"].pop("stop")
        while not is_time_past(stop):
            new_ltps = change_key(ws._ltp)
            print("new_ltps", new_ltps)
            obj.run(new_ltps)
        else:
            obj.save_dfs()
            timer(5)
            kill_tmux()
    except KeyboardInterrupt:
        print("saving ....")
        obj.save_dfs()
        timer(5)
    except Exception as e:
        print_exc()
        logging.error(f"{e} while running strategy")


if __name__ == "__main__":
    main()
