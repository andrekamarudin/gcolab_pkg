import glob
import inspect
import logging
import re
import time
from datetime import datetime
from decimal import Decimal
from functools import wraps
from math import isnan, log10
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)


class DagError(Exception):
    """
    Custom exception for DAG errors specified by our team's dags.
    """

    pass


class HelperFunctions:
    DAG_VERSION = "20240815_1030"

    def __init__(self):
        self.tools: dict = self.get_tools()
        self.emojis: dict = self.tools.get("emojis", {})

    def get_tools(self):
        tools = self.find_file("tools.yaml", sibling_folder_name="yamls")
        try:
            with open(tools, "r", encoding="utf-8") as config_file:
                return yaml.safe_load(config_file)
        except (FileNotFoundError, yaml.YAMLError) as e:
            raise DagError(f"Error loading config: {e}") from e

    @staticmethod
    def string_to_number(
        x: Union[str, int, float, Decimal, None], force: bool = False
    ) -> float:
        """
        Convert a string to a float, optionally forcing the conversion
        by removing non-numeric characters.

        Args:
            x (Union[str, int, float, Decimal, None]): A value to convert.
            force (bool): If True, attempt to clean and convert the string.
        """
        if x is None or pd.isna(x) or (isinstance(x, float) and np.isnan(x)):
            return 0.0
        if isinstance(x, (int, float, Decimal)):
            return float(x)
        try:
            if (y := str(x).replace(",", "")).lstrip("-").replace(".", "", 1).isdigit():
                return float(y)
            elif force:
                y = re.sub(r"[^\\d.-]+", "", str(x))
                return HelperFunctions.string_to_number(y, force)
            else:
                raise ValueError(
                    f"{x=} (type: {type(x)}) cannot be interpreted as float."
                )
        except Exception as e:
            raise DagError(
                f"Error in string_to_number: {e}. Input: {x} (type: {type(x)})"
            )

    @staticmethod
    def format_string(
        x: str,
        color: str = None,
        bold: bool = False,
        italic: bool = False,
    ) -> str:
        """
        Colorize a string in HTML format.
        :param x: The string to colorize.
        :param color: The color to use. If None, use green.
        :return: The colorized string in HTML format.
        """
        # Start with the base text
        formatted_text = x

        # Apply bold formatting if needed
        if bold:
            formatted_text = f"<b>{formatted_text}</b>"

        # Apply italic formatting if needed
        if italic:
            formatted_text = f"<i>{formatted_text}</i>"

        # Apply color using the font tag
        if color:
            formatted_text = f'<font color="{color}">{formatted_text}</font>'

        return formatted_text

    @staticmethod
    def colorize_number_html(
        x: Union[int, float],
        test: bool = None,
        color_if_true: str = None,
        color_if_false: str = None,
        has_percent: bool = True,
        add_plus_sign: bool = False,
    ) -> str:
        """
        Colorize a number in HTML format.
        :param x: The number to colorize.
        :param test: The condition to test the number against. If None, test is x >= 0.
        :param color_if_true: The color to use if the test is True. If None, use green.
        :param color_if_false: The color to use if the test is False. If None, use red.
        :param has_percent: Whether to display as a percentage when 0 < abs(x) < 1.
        :return: The colorized number in HTML format.
        """
        try:
            x = float(x)
        except ValueError:
            return x
        if test is None:
            test = x >= 0
        if test:
            color = color_if_true or "#008000"  # green
        else:
            color = color_if_false or "#FF0000"  # red
        x = HelperFunctions.number_to_short_string(
            x, has_percent=has_percent, add_plus_sign=add_plus_sign
        )
        return HelperFunctions.format_string(x, color)

    @staticmethod
    def df_to_md(df: pd.DataFrame) -> str:
        return f"```{df.to_markdown(index=False, tablefmt='simple')}```"

    @staticmethod
    def number_to_short_string(
        x: Union[int, float],
        has_percent: bool = True,
        add_plus_sign=False,
        force: bool = False,
    ) -> str:
        if isinstance(x, str):
            x = HelperFunctions.string_to_number(x, force=force)
        elif x is None or isnan(x):
            return ""

        try:
            x = float(x)
        except ValueError:
            return x

        plus_sign = "+" if add_plus_sign and x > 0 else ""

        if x == 0:
            return "0"
        elif abs(x) < 1:
            if has_percent:
                return f"{plus_sign}{x * 100:,.1f}%"
            else:
                decimals = int(log10(abs(x)) * -1 + 3)  # 3 sig fig for abs(n) < 1
                return f"{plus_sign}{x:,.{decimals}f}"
        elif abs(x) >= 1_000_000:
            n = x / 1_000_000
            unit = "m"
        elif abs(x) >= 1_000:
            n = x / 1_000
            unit = "k"
        else:
            n = x
            unit = ""
        decimals = 3 - len(str(int(abs(n))))  # 3 sig fig for abs(n) >= 1
        try:
            result = f"{plus_sign}{n:,.{decimals}f}{unit}"
            return result
        except ValueError as e:
            print(f"Error: {e}")
            print(f"{n=}, {decimals=}, {unit=}")
            result = f"{plus_sign}{x:,.0f}"
            return result

    @staticmethod
    def strip_and_proper(text: str) -> str:
        text = re.sub(r"\W|_", " ", text)
        return text.strip().title()

    @staticmethod
    def logit(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            local_logger = logging.getLogger(func.__module__)

            func_log = f" {func.__name__} called with: "

            args_str = "\n".join([f"{type(arg).__name__} = {arg}" for arg in args])
            kwargs_str = "\n".join(
                [f"{k}: {type(v).__name__} = {v}" for k, v in kwargs.items()]
            )
            local_logger.info(
                "\n%s\n%s\n%s\n",
                f"{func_log:=^90}",
                f"{args_str}",
                f"{kwargs_str}",
            )

            start = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start

            local_logger.info(
                "\nresult=%s\n%s",
                result,
                f"{f' {func.__name__} done in {HelperFunctions.format_time(duration)} ':#^90}",
            )
            return result

        return wrapper

    def ic(*args):
        frame = inspect.currentframe().f_back
        context = inspect.getframeinfo(frame)
        for arg in args:
            arg_name = context.code_context[0].strip()
            arg_str = arg_name[arg_name.find("(") + 1 : arg_name.rfind(")")]
            print(f"ic| {arg_str}: {arg}")

    @staticmethod
    def format_time(seconds):
        h, r = divmod(seconds, 3600)
        m, s = divmod(r, 60)
        return " ".join(
            f"{v:0f}{u}" for v, u in zip([h, m, s], ["hr", "min", "s"]) if v
        )

    @staticmethod
    def parse_datetime(date_or_datetime: Union[str, datetime]) -> datetime:
        if isinstance(date_or_datetime, datetime):
            return date_or_datetime
        try:
            dt_str = str(date_or_datetime)
        except ValueError as e:
            raise DagError(
                f"Expected str/datetime, got {type(date_or_datetime)}: {date_or_datetime}"
            ) from e
        for fmt in [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y%m%d",
            "%Y%m%d_%H%M",
            "%Y%m%d_%H%M%S",
        ]:
            try:
                result = datetime.strptime(dt_str, fmt)
                return result
            except ValueError:
                pass

    @staticmethod
    def condense_text(text, limit=500):
        text = "(None)" if text is None else str(text)
        text = re.sub(r"\s*\n+\s*", " | ", text).strip()
        text = re.sub(r"\s+", " ", text).strip()
        sql_extract = re.sub(rf"(.{{{limit}}}).*", r"\1...", text)
        return sql_extract

    @staticmethod
    def generate_hyperlink_html(text, url):
        return f'<a href="{url}">{text}</a>'

    @staticmethod
    def find_file(
        file_name: str, sibling_folder_name: str = None, start_path: Path = None
    ) -> Path:
        start_path = start_path or Path(__file__).resolve().parent
        search_paths = [start_path, start_path.parent]

        for parent in search_paths:
            if sibling_folder_name:
                search_pattern = str(parent / sibling_folder_name / "**" / file_name)
            else:
                search_pattern = str(parent / "**" / file_name)

            matches = glob.glob(search_pattern, recursive=True)
            if matches:
                return Path(matches[0])

        raise DagError(f"File not found: {file_name}")

    @staticmethod
    def back_ticks(text):
        text = re.sub(r"```", "` ` `", text)
        return f"```\n{text}\n```"

    @staticmethod
    def get_sql(file_name: str, path: Path = None):
        try:
            sql_file = HelperFunctions.find_file(file_name, start_path=path)
            return sql_file.read_text()
        except DagError as e:
            raise DagError(f"get_sql: {str(e)}")


helper = HelperFunctions()


def main():
    sql = helper.get_sql("akankhya\sql\scango_po_order_payment_daily.sql")
    print(sql)


if __name__ == "__main__":
    main()
