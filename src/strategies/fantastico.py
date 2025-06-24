from constants import STOCKS_IN_PLAY, DELIVERED, HISTORY, logging
import pandas as pd
from helper import Helper
from toolkit.kokoo import timer
import pendulum as pdlm

COLS_DELIVERED = ["Symbol", "Qty", "Bdate", "Bprice", "Reward", "Ltp", "Exch"]
HIST_COLS = [
    "Symbol",
    "Qty",
    "Bdate",
    "Bprice",
    "Reward",
    "SDate",
    "SPrice",
    "Exch",
]
REWARD_PERC = 0.05  # 5 percent


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
        stocks_columns = ["Exch", "Qty"]
        self.df_stocks_in_play = df_fm_file(STOCKS_IN_PLAY, stocks_columns)
        self.df_stocks_in_play["Ltp"] = 0
        print("FROM EXCEL \n", self.df_stocks_in_play, "\n")

        # drop self.df_stocks_in_play index  if it is in lst_pos
        df = df_fm_file(DELIVERED)
        lst_pos = df.index.to_list()
        self.df_stocks_in_play = self.df_stocks_in_play[
            ~self.df_stocks_in_play.index.isin(lst_pos)
        ]
        print("\n NEW STOCK TO ENTER \n", self.df_stocks_in_play, "\n")

        self.df_delivered = pd.read_csv(DELIVERED)
        print("\n DELIVERED \n", self.df_delivered, "\n")
        timer(2)

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
                    print("enter")
                    Ltp = row["Ltp"]
                    dct = {
                        "Exch": row["Exch"],
                        "Qty": int(row["Qty"]),
                        "Bdate": pdlm.now(),
                        "Bprice": Ltp,
                        "Reward": Ltp - (Ltp * REWARD_PERC),
                        "Ltp": Ltp,
                        "Symbol": idx,
                    }
                    lst_of_dct.append(dct)

            if any(lst_of_dct):
                self._save_df_to_csv(lst_of_dct)

        except Exception as e:
            print(f"{e} enter on breakout")

    def _save_df_to_csv(self, lst_of_dct):
        self.df_delivered = pd.read_csv(DELIVERED)
        if self.df_delivered.empty:
            self.df_delivered = pd.DataFrame(lst_of_dct, columns=COLS_DELIVERED)
            self.df_delivered.to_csv(DELIVERED, index=False, header=True)
        else:
            df_new = pd.DataFrame(lst_of_dct, columns=COLS_DELIVERED)
            df_new.to_csv(DELIVERED, mode="a", index=False, header=False)

    def save_dfs(self):
        is_empty = True if self.df_delivered.empty else False
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
            if Ltp >= row["Reward"]:
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
                        "Reward": row["Reward"],
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

    def exit_on_profit(self, df: pd.DataFrame) -> bool:
        symbol = df["Symbol"].iloc[0]  # all rows are of same symbol
        avg_bprice = df["Bprice"].mean()
        total_qty = df["Qty"].sum()
        ltp = df["Ltp"].iloc[-1]
        exch = df["Exch"].iloc[-1]

        if ltp > avg_bprice:
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
                return True
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
            Ltp = row["Ltp"]
            if Ltp < row["Reward"]:
                dct = {
                    "Exch": row["Exch"],
                    "Qty": int(row["Qty"]),
                    "Bdate": pdlm.now(),
                    "Bprice": Ltp,
                    "Reward": Ltp + (Ltp * REWARD_PERC),
                    "Ltp": Ltp,
                    "Symbol": row["Symbol"],
                }
                if self._place_buy_order(**dct):
                    rows_to_add.append(dct)
                    return rows_to_add

    def process_cnc(self):
        try:
            self.df_delivered = pd.read_csv(DELIVERED)
            if not self.df_delivered.empty:
                self.df_delivered["Ltp"] = self.df_delivered["Symbol"].map(self._prices)

            symbols = self.df_delivered["Symbol"].unique()
            for symbol in symbols:
                df_sym = self.df_delivered[self.df_delivered["Symbol"] == symbol]
                should_exit = self.exit_on_profit(df_sym)
                if should_exit:
                    self.df_delivered = self.df_delivered[
                        self.df_delivered["Symbol"] != symbol
                    ]
                    self.save_dfs()

            rows_to_add = self.enter_on_loss()
            if rows_to_add:
                df_new_rows = pd.DataFrame(rows_to_add, columns=COLS_DELIVERED)
                self.df_delivered = pd.concat(
                    [self.df_delivered, df_new_rows], ignore_index=True
                )
                self.save_dfs()
        except Exception as e:
            print(f"{e} process cns")

    def run(self, prices):
        # update df stocks in play and df_delivered with prices
        # update dataframe with price from dictionary with df.index as key
        try:
            self._prices = prices
            print("\n DELIVERED")
            print(self.df_delivered)
            timer(2)
            self.fn()
        except Exception as e:
            logging.error(f"{e} in run")
