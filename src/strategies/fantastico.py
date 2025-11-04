from constants import STOCKS_IN_PLAY, DELIVERED, HISTORY, logging
import pandas as pd
from helper import Helper
from toolkit.kokoo import timer
import pendulum as pdlm
from traceback import print_exc
import os

COLS_DELIVERED = ["Symbol", "Qty", "Bdate", "Bprice", "Stoploss", "Ltp", "Exch"]
HIST_COLS = [
    "Symbol",
    "Qty",
    "Bdate",
    "Bprice",
    "Stoploss",
    "SDate",
    "SPrice",
    "Exch",
]
REWARD_PERC = 0.04  # 5 percent


def df_fm_file(file, columns=[], index_col="Symbol"):
    df = (
        pd.read_csv(file, index_col=index_col)
        if file.endswith("csv")
        else pd.read_excel(file, index_col=index_col, engine="xlrd")
    )
    df = df[columns] if any(columns) else df
    return df


class Fantastico:

    def __init__(self):
        self._prices = {}

        # read stocks to buy
        stocks_columns = ["Exch", "Qty"]
        self.df_stocks_in_play = df_fm_file(STOCKS_IN_PLAY, stocks_columns)
        self.df_stocks_in_play["Ltp"] = 0
        logging.info(f"FROM EXCEL \n  {self.df_stocks_in_play}  \n")
        timer(5)

        # read stocks delivered if it is not in stocks to buy
        df = df_fm_file(DELIVERED)
        lst_pos = df.index.to_list()
        self.df_stocks_in_play = self.df_stocks_in_play[
            ~self.df_stocks_in_play.index.isin(lst_pos)
        ]
        if not self.df_stocks_in_play.empty:
            logging.info(f"\n NEW STOCK TO ENTER \n {self.df_stocks_in_play}  \n")
            timer(5)

        # stocks taken delivery
        self.df_delivered = pd.read_csv(DELIVERED)
        logging.info(f"\n DELIVERED \n {self.df_delivered} \n")
        timer(5)

        # if no stocks to buy jump to processing bought stocks
        self.fn = self.process_cnc if self.df_stocks_in_play.empty else self.first_entry

    def first_entry(self):
        self.fn = self.process_cnc
        try:
            self.df_stocks_in_play["Ltp"] = self.df_stocks_in_play.index.map(
                self._prices
            )
            lst_of_dct = []
            for idx, row in self.df_stocks_in_play.iterrows():
                kwargs = dict(
                    symbol=idx,
                    exchange=row["Exch"],
                    quantity=row["Qty"],
                    side="BUY",
                )
                resp = Helper.place_order(**kwargs)
                if resp:
                    Ltp = row["Ltp"]
                    dct = {
                        "Exch": row["Exch"],
                        "Qty": int(row["Qty"]),
                        "Bdate": pdlm.now(),
                        "Bprice": Ltp,
                        "Stoploss": Ltp - (Ltp * REWARD_PERC),
                        "Ltp": Ltp,
                        "Symbol": idx,
                    }
                    lst_of_dct.append(dct)
                    logging.info(f"NEW STOCK TO ENTER, {idx}, {Ltp}, {dct['Qty']}")

            if any(lst_of_dct):
                self._append_list_to_csv(lst_of_dct)
                self.df_delivered = pd.read_csv(DELIVERED)

        except Exception as e:
            logging.error(f"{e} enter on breakout")

    def _append_list_to_csv(self, lst_of_dct):
        df = pd.DataFrame(lst_of_dct, columns=COLS_DELIVERED)
        df.to_csv(DELIVERED, mode="a", index=False, header=False)

    def save_dfs(self):
        is_empty = True if self.df_delivered.columns.empty else False
        if not is_empty:
            self.df_delivered.to_csv(DELIVERED, index=False, header=True)

    def _place_buy_order(self, **kwargs):
        resp = Helper.place_order(
            symbol=kwargs["Symbol"],
            exchange=kwargs["Exch"],
            quantity=kwargs["Qty"],
            side="BUY",
        )
        if resp:
            return True
        return False

    def _exit_on_profit(self, df: pd.DataFrame) -> bool:
        try:
            symbol = df["Symbol"].iloc[0]  # all rows are of same symbol
            total_qty = df["Qty"].sum()
            total_amount = (df["Bprice"] * df["Qty"]).sum()
            average = total_amount / total_qty

            ltp = df["Ltp"].iloc[0]
            exch = df["Exch"].iloc[0]
            target = average + (average * REWARD_PERC)
            if ltp > target:
                resp = Helper.place_order(
                    symbol=symbol,
                    exchange=exch,
                    quantity=total_qty,
                    side="SELL",
                )
                if resp:
                    df["SDate"] = pdlm.now()
                    df["SPrice"] = ltp
                    df.to_csv(HISTORY, mode="a", index=False, header=False)
                    logging.info(f"exited on profit, {symbol}, {ltp}, -{total_qty}")
                    return True

                """
                    dct = {
                        "Exch": exch,
                        "Qty": df["Qty"].iloc[0],
                        "Bdate": pdlm.now(),
                        "Bprice": ltp,
                        "Stoploss": ltp - (ltp * REWARD_PERC),
                        "Ltp": ltp,
                        "Symbol": symbol,
                    }
                    if self._place_buy_order(**dct):
                        rows_to_add.append(dct)
                        return rows_to_add
                        """

        except Exception as a:
            print(a)
            logging.error(f"{a} while exiting on profit")
        else:
            msg = f"{symbol} - LTP {ltp:.2f} Target:{target:.2f} (Avg:{average:.2f}) → Not Reached"
            logging.info(msg)
            print(msg)
            return False

    def enter_on_loss(self):
        # Create a copy with only last rows per symbol
        df_latest = (
            self.df_delivered.groupby("Symbol", as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )
        rows_to_add = []
        for _, row in df_latest.iterrows():
            ltp = row["Ltp"]
            if ltp < row["Stoploss"]:
                dct = {
                    "Exch": row["Exch"],
                    "Qty": int(row["Qty"]),
                    "Bdate": pdlm.now(),
                    "Bprice": ltp,
                    "Stoploss": ltp - (ltp * REWARD_PERC),
                    "Ltp": ltp,
                    "Symbol": row["Symbol"],
                }
                resp = self._place_buy_order(**dct)
                if resp:
                    rows_to_add.append(dct)
                    logging.info(
                        f"entered on loss, {dct['Symbol']}, {dct['Bprice']}, {dct['Qty']}"
                    )
                    return rows_to_add
            msg = f"{row['Symbol']} - LTP {ltp:.2f} below stoploss: {row['Stoploss']:.2f} Not Reached"
            print(msg)

    def append_df_to_delivered(self, rows_to_add, symbol=None):
        if rows_to_add:
            if symbol is not None:
                self.df_delivered = self.df_delivered[
                    self.df_delivered["Symbol"] != symbol
                ]
            df_new_rows = pd.DataFrame(rows_to_add, columns=COLS_DELIVERED)
            self.df_delivered = pd.concat(
                [self.df_delivered, df_new_rows], ignore_index=True
            )
            self.save_dfs()

    def delete_df_from_delivered(self, symbol):
        self.df_delivered = self.df_delivered[self.df_delivered["Symbol"] != symbol]
        self.save_dfs()

    def process_cnc(self):
        """
        Processes the stocks in play and delivered
        """
        try:
            if not self.df_delivered.empty:
                # update delivered with prices
                self.df_delivered["Ltp"] = self.df_delivered["Symbol"].map(self._prices)

                # generate unique symbols
                symbols = self.df_delivered["Symbol"].unique()
                for symbol in symbols:
                    # create unique symbol dataframes
                    df_sym = self.df_delivered[self.df_delivered["Symbol"] == symbol]
                    # try to exit on profit
                    if self._exit_on_profit(df_sym):
                        # if succeeds delete from delivered
                        self.delete_df_from_delivered(symbol=symbol)
                        timer(5)

                # averaging the position
                rows_to_add = self.enter_on_loss()
                self.append_df_to_delivered(rows_to_add)
        except Exception as e:
            logging.error(f"{e} process cnc")
            print_exc()

    def run(self, prices):
        # update df stocks in play and df_delivered with prices
        # update dataframe with price from dictionary with df.index as key
        try:
            flag = False
            self._prices = prices
            print(f"\n DELIVERED {self.df_delivered} \n")
            flag = self.fn()
            timer(2)
            return flag
        except Exception as e:
            logging.error(f"{e} in run")


"""

    def remove_me(self):
        # Create a copy with only last rows per symbol
        df_latest = (
            self.df_delivered.groupby("Symbol", as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )
        for idx, row in df_latest.iterrows():
            rows_to_drop, lst_of_dct = [], []
            Ltp = row["Ltp"]
            if Ltp >= row["Stoploss"]:
                resp = Helper.place_order(
                    symbol=row["Symbol"],
                    exchange=row["Exch"],
                    quantity=row["Qty"],
                    side="SELL",
                )
                if resp:
                    dct = {
                        "Symbol": row["Symbol"],
                        "Qty": row["Qty"],
                        "Bdate": row["Bdate"],
                        "Bprice": row["Bprice"],
                        "Stoploss": row["Stoploss"],
                        "SDate": pdlm.now(),
                        "SPrice": Ltp,
                        "Exch": row["Exch"],
                    }
                    # to be added to the history
                    lst_of_dct.append(dct)
                    # to be dropped from delivered
                    rows_to_drop.append(idx)

            if lst_of_dct and rows_to_drop:
                # if lst_of dct
                df_new = pd.DataFrame(lst_of_dct, columns=HIST_COLS)
                df_new.to_csv(HISTORY, mode="a", index=False, header=False)
                # if rows_to_drop:
                self.df_delivered.drop(rows_to_drop, inplace=True)
                self.save_dfs()

"""
