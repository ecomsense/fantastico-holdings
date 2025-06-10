from constants import STOCKS_IN_PLAY, DELIVERED, HISTORY, logging
import pandas as pd
from helper import Helper
from toolkit.kokoo import timer
import pendulum as pdlm

COLS_DELIVERED = ["Symbol", "Qty", "Bdate", "Bprice", "Reward", "Ltp", "Exch"]


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

        self.fn = (
            self.exit_beyond_band
            if self.df_stocks_in_play.empty
            else self.enter_on_breakout
        )

    def enter_on_breakout(self):
        self.fn = self.exit_beyond_band
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
                        "Reward": Ltp + (Ltp * 4 / 100),
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

    def add_position(self, **kwargs):
        resp = Helper.place_order(
            symbol=kwargs["Symbol"],
            exchange=kwargs["Exch"],
            quantity=kwargs["Qty"],
            side="BUY",
        )
        if resp:
            return True
        return False

    def exit_beyond_band(self):
        try:
            self.df_delivered = pd.read_csv(DELIVERED)
            if not self.df_delivered.empty:
                self.df_delivered["Ltp"] = self.df_delivered["Symbol"].map(self._prices)
            rows_to_drop, lst_of_dct = [], []
            for idx, row in self.df_delivered.iterrows():
                if row["Ltp"] >= row["Reward"]:
                    resp = Helper.place_order(
                        symbol=row["Symbol"],
                        exchange=row["Exch"],
                        quantity=row["Qty"],
                        side="SELL",
                    )
                    if resp:
                        print("squared off")
                        Ltp = row["Ltp"]
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
                        lst_of_dct.append(dct)
                        columns = [
                            "Symbol",
                            "Qty",
                            "Bdate",
                            "Bprice",
                            "Reward",
                            "SDate",
                            "SPrice",
                            "Exch",
                        ]
                        df_new = pd.DataFrame(lst_of_dct, columns=columns)
                        df_new.to_csv(HISTORY, mode="a", index=False, header=False)

                        rows_to_drop.append(idx)
                        self.df_delivered = self.df_delivered[
                            ~self.df_delivered["Bdate"].isin(rows_to_drop)
                        ]
                        dct = {
                            "Exch": row["Exch"],
                            "Qty": int(row["Qty"]),
                            "Bdate": pdlm.now(),
                            "Bprice": Ltp,
                            "Reward": Ltp + (Ltp * 4 / 100),
                            "Ltp": Ltp,
                            "Symbol": row["Symbol"],
                        }
                        if self.add_position(**dct):
                            df_new = pd.DataFrame([dct], columns=COLS_DELIVERED)
                            self.df_delivered = pd.concat(
                                [self.df_delivered, df_new], ignore_index=True
                            )
                            self.save_dfs()
                            break
                    elif row["Ltp"] < row["Bprice"] - (row["Bprice"] * 0.04):
                        Ltp = row["Ltp"]
                        dct = {
                            "Exch": row["Exch"],
                            "Qty": int(row["Qty"]),
                            "Bdate": pdlm.now(),
                            "Bprice": Ltp,
                            "Reward": Ltp + (Ltp * 4 / 100),
                            "Ltp": Ltp,
                            "Symbol": row["Symbol"],
                        }
                        if self.add_position(**dct):
                            df_new = pd.DataFrame([dct], columns=COLS_DELIVERED)
                            self.df_delivered = pd.concat(
                                [self.df_delivered, df_new], ignore_index=True
                            )
                            self.save_dfs()
                            break

        except Exception as e:
            print(f"{e} exit beyond band")

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
