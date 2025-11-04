from constants import logging, O_SETG
from helper import Helper
from strategies.fantastico import Fantastico
from toolkit.kokoo import is_time_past, kill_tmux, timer, blink
from traceback import print_exc
from symbols import Equity
from typing import Any, Dict, List
from wsocket import Wsocket
import pandas as pd
from kiteconnect.exceptions import KiteException, OrderException
import os


def get_tokens_from_symbols(obj) -> List[Dict[Any, Any]]:
    """
    Returns instrument details from strategy by combining stock dataframes
    (only Symbol and Exch columns) and grouping by exchange.
    """
    tokens_and_tradingsymbols = []
    df1 = obj.df_stocks_in_play
    df1 = df1.reset_index()
    df2 = obj.df_delivered

    # Extract only common relevant columns
    common_cols = ["Symbol", "Exch"]
    df1_filtered = (
        df1[common_cols] if not df1.empty else pd.DataFrame(columns=common_cols)
    )
    df2_filtered = (
        df2[common_cols] if not df2.empty else pd.DataFrame(columns=common_cols)
    )

    # Combine both
    df_combined = pd.concat([df1_filtered, df2_filtered], ignore_index=True)

    print("\nSUBSCRIBING LIST\n", df_combined, "\n")

    if not df_combined.empty:
        exch_sym = df_combined.groupby("Exch")["Symbol"].apply(list).to_dict()
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
    try:
        Helper.symbol_info = {
            dct["instrument_token"]: dct["tradingsymbol"] for dct in lst_of_symbols
        }
        tokens = list(Helper.symbol_info.keys())

        ws = Wsocket(Helper.api(), tokens)
        prices = {}
        while not any(prices):
            prices = ws.ltp(tokens)
            timer(1)
            logging.debug("waiting for websocket")
        return ws
    except Exception as e:
        logging.error(f"{e} in subscribe")
        print_exc()


def main():
    try:
        start = O_SETG["program"].pop("start")
        while not is_time_past(start):
            logging.info(f"waiting for {start}")
            blink()
        else:
            print("Happy Trading")

        Helper.api()
        obj = Fantastico()
        lst_of_symbols = get_tokens_from_symbols(obj)
        ws = subscribe(lst_of_symbols)
        stop = O_SETG["program"].pop("stop")
        while not is_time_past(stop):
            new_ltps = change_key(ws._ltp)
            obj.run(new_ltps)
        else:
            obj.save_dfs()
            timer(5)
            kill_tmux()
    except KeyboardInterrupt:
        print("saving ....")
        logging.info("user pressed ctrl+c")
        obj.save_dfs()
        timer(5)
    except Exception as e:
        print_exc()
        logging.error(f"{e} while running strategy")


if __name__ == "__main__":
    main()
