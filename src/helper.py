from traceback import print_exc
from importlib import import_module
from constants import O_CNFG, S_DATA, logging


def login():
    try:
        broker_name = O_CNFG.pop("broker", None)
        if not broker_name:
            raise ValueError("broker not specified in credential file")

        # Dynamically import the broker module
        module_path = f"stock_brokers.{broker_name}.{broker_name}"
        broker_module = import_module(module_path)

        logging.debug(f"BrokerClass: {broker_module}")
        # Get the broker class (assuming class name matches the broker name)
        BrokerClass = getattr(broker_module, broker_name.capitalize())

        # Initialize API with config
        broker_object = BrokerClass(**O_CNFG)
        if broker_object.authenticate():
            print("api connected")
            return broker_object
        __import__("sys").exit(1)
    except Exception as e:
        print(e)


def make_default_order():
    args = dict(product="CNC", tag="fantastico", order_type="MARKET", price=0)
    return args


class Helper:
    _api = None

    @classmethod
    def api(cls):
        if cls._api is None:
            cls._api = login()
        return cls._api

    @classmethod
    def orders(cls):
        return cls._api.orders

    @classmethod
    def positions(cls):
        return cls._api.positions

    @classmethod
    def holdings(cls):
        return cls._api.holdings

    @classmethod
    def place_order(cls, **kwargs):
        try:
            default_dict = make_default_order()
            kwargs.update(default_dict)
            logging.debug(str(kwargs))
            resp = cls._api.order_place(**kwargs)
            return resp
        except Exception as e:
            message = f"helper error {e} while placing order"
            logging.error(message)
            print_exc()
            __import__("sys").exit(1)


if __name__ == "__main__":
    import pandas as pd

    Helper.api()
    resp = Helper.orders()
    if resp and any(resp):
        print(resp)
        pd.DataFrame(resp).to_csv(S_DATA + "orders.csv")

    resp = Helper.positions()
    if resp and any(resp):
        print(resp)
        pd.DataFrame(resp).to_csv(S_DATA + "positions.csv")

    resp = Helper.holdings()
    if resp and any(resp):
        print(resp)
        pd.DataFrame(resp).to_csv(S_DATA + "positions.csv")

    print(Helper._api.profile)
