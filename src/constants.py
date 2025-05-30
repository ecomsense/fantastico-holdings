"""
description:
    contains all the constants
    creates yml files and necessary folders
    for project
"""

from os import path
from traceback import print_exc
from pprint import pprint
from toolkit.logger import Logger
from toolkit.fileutils import Fileutils

O_FUTL = Fileutils()
S_DATA = "../data/"
S_LOG = S_DATA + "log.txt"

STOCKS_IN_PLAY = S_DATA + "stocks.csv"
DELIVERED = S_DATA + "delivered.csv"
HISTORY = S_DATA + "history.csv"


if not O_FUTL.is_file_exists(S_LOG):
    """
    description:
        create data dir and log file
        if did not if file did not exists
    input:
         file name with full path
    """
    print("creating data dir")
    O_FUTL.add_path(S_LOG)
elif O_FUTL.is_file_not_2day(S_LOG):
    O_FUTL.nuke_file(S_LOG)


def manufacture():
    lst = [STOCKS_IN_PLAY, DELIVERED, HISTORY]
    for item in lst:
        if not O_FUTL.is_file_exists(item):
            O_FUTL.copy_file("../factory/", "../data/", item)
        else:
            print(f"found {item}")


manufacture()


def yml_to_obj(arg=None):
    """
    description:
        creates empty yml file for credentials
        and also copies project specific settings
        to data folder
    """
    if not arg:
        # return the parent folder name
        parent = path.dirname(path.abspath(__file__))
        print(f"{parent=}")
        grand_parent_path = path.dirname(parent)
        print(f"{grand_parent_path=}")
        folder = path.basename(grand_parent_path)
        """
        folder = path.basename(parent)
        """
        # reverse the words seperated by -
        lst = folder.split("-")
        file = "_".join(reversed(lst))
        file = "../../" + file + ".yml"
    else:
        file = S_DATA + arg

    flag = O_FUTL.is_file_exists(file)

    if not flag and arg:
        print(f"using default {file=}")
        O_FUTL.copy_file("../factory/", "../data/", "settings.yml")
    elif not flag and arg is None:
        print(f"fill the {file=} file and try again")
        __import__("sys").exit()

    return O_FUTL.get_lst_fm_yml(file)


def read_yml():
    try:
        O_CNFG = yml_to_obj()
        O_SETG = yml_to_obj("settings.yml")
    except Exception as e:
        print(e)
        print_exc()
        __import__("sys").exit(1)
    else:
        return O_CNFG, O_SETG


O_CNFG, O_SETG = read_yml()
print("broker credentials" + "\n" + "*****************")
pprint(O_CNFG)

print("settings " + "\n" + "*****************")
pprint(O_SETG)


def set_logger():
    """
    description:
        set custom logger's log level
        display or write to file
        based on user choice from settings
    """
    level = O_SETG["log"]["level"]
    if O_SETG["log"]["show"]:
        return Logger(level)
    return Logger(level, S_LOG)


logging = set_logger()


def create_json_file(exchange):
    json_file = f"{S_DATA}{exchange}.json"
    if not O_FUTL.is_file_exists(json_file):
        print(f"creating file {json_file}")


exchanges = ["NSE", "BSE"]
for exchange in exchanges:
    create_json_file(exchange)
