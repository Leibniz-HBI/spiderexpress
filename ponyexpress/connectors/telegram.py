"""Scraping Telegram Connetor for ponyexpress

Philipp Kessling
Leibniz-Institute for Media Research, 2022

ToDo:
    - should raise an exception if no data is present, thus, the calling program
      can detect an fault/non-existing data.
    - must convert telegram's number-strings to py numerics, e.g. '7.5K' should be 7500.
"""

from functools import reduce
from typing import Optional, Tuple

import pandas as pd
import requests
from loguru import logger as log
from lxml import html

message_paths = {
    # pylint: disable=C0301
    "post_id": "../@data-post",
    "views": ".//span[@class='tgme_widget_message_views']/descendant-or-self::*/text()",
    "datetime": ".//time/@datetime",
    "user": ".//a[@class='tgme_widget_message_owner_name']/descendant-or-self::*/text()",
    "from_author": ".//span[@class='tgme_widget_message_from_author']/descendant-or-self::*/text()",
    "text": "./div[contains(@class, 'tgme_widget_message_text')]/descendant-or-self::*/text()",
    "link": ".//div[contains(@class, 'tgme_widget_message_text')]//a/@href",
    #       "reply_to_user" : ".//a[@class='tgme_widget_message_reply']//span[@class='tgme_widget_message_author_name'/descendant-or-self::*/text()]",
    #       "reply_to_text" : ".//a[@class='tgme_widget_message_reply']//div[@class='tgme_widget_message_text']/descendant-or-self::*/text()",
    #       "reply_to_link" : ".//a[@class='tgme_widget_message_reply']/@href",
    "image_url": "./a[contains(@class, 'tgme_widget_message_photo_wrap')]/@style",
    "forwarded_message_url": ".//a[@class='tgme_widget_message_forwarded_from_name']/@href",
    "forwarded_message_user": ".//a[@class='tgme_widget_message_forwarded_from_name']/descendant-or-self::*/text()",
    #       "poll_question" : ".//div[@class='tgme_widget_message_poll_question']/descendant-or-self::*/text()",
    #       "poll_options_text" : ".//div[@class='tgme_widget_message_poll_option_value']/descendant-or-self::*/text()",
    #       "poll_options_percent" : ".//div[@class='tgme_widget_message_poll_option_percent']/descendant-or-self::*/text()",
    #       "video_url" : ".//video[contains(@class, 'tgme_widget_message_video')]/@src",
    #       "video_duration" : ".//time[contains(@class, 'message_video_duration')]/descendant-or-self::*/text()"
}


user_paths = {
    # pylint: disable=C0301
    "name": '//div[@class="tgme_channel_info_header_username"]/a/text()',
    "fullname": '//div[@class="tgme_channel_info_header_title"]/descendant-or-self::*/text()',
    "url": '//div[@class="tgme_channel_info_header_username"]/a/@href',
    "description": '//div[@class="tgme_channel_info_description"]/descendant-or-self::*/text()',
    "subscriber_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "subscribers"]/preceding-sibling::span/text()',
    "photos_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "photos"]/preceding-sibling::span/text()',
    "videos_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "videos"]/preceding-sibling::span/text()',
    "files_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "files"]/preceding-sibling::span/text()',
    "links_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "links"]/preceding-sibling::span/text()',
}


def parse_tg_number_string(num: str) -> float:
    """parses the telegram number format"""
    if len(num) > 0:
        if num.isdigit():
            return float(num)
        return float(num.replace("K", "E+03").replace("M", "E+06"))
    return 0.0


def get_messages(page) -> pd.DataFrame:
    """get Telegram messages from a HTML document

    Parameters
    ----------
    page:
        lxml.tree : the parsed HTML document


    Returns
    -------
    pd.DataFrame : the parsed messages, where the columns are based on the
    keys of the messages_paths-dictionary.
    """

    def extract_multiple2(tree, xpaths):
        for name, xpath in xpaths.items():
            # first post-processing steps

            parsed = tree.xpath(xpath)
            # log.debug(f"Parsing {name} with value: {parsed}.")

            if name == "link":
                data = ",".join(parsed)
            else:
                data = "".join(parsed)
                if name in ["views"]:
                    data = parse_tg_number_string(data)
                # if name == "image_url":
                #     data = re.findall(r"(?<=url\(').+(?=')", data)
            if data == "":
                data = None
            yield pd.Series(
                data=data, name=name, dtype="object" if isinstance(data, str) else float
            )

    base_xpath = "//div[@class='tgme_widget_message_bubble']"

    messages_html = page.xpath(base_xpath)
    try:
        data = pd.concat(
            [
                pd.concat(extract_multiple2(_, message_paths), axis=1)
                for _ in messages_html
            ],
            axis=0,
            ignore_index=True,
        )
    except ValueError:
        # If this code is executed, we could not obtain data from the TG-page
        return pd.DataFrame()
    data[["handle", "post_number"]] = data.post_id.str.split("/", n=1, expand=True)

    return data


def get_user(page) -> pd.DataFrame:
    """get Telegram user data from a HTML document

    Parameters
    ----------
    page:
        lxml.tree : the parsed HTML document


    Returns
    -------
    pd.DataFrame : the parsed messages, where the columns are based on the
    keys of the user_paths-dictionary.
    """

    def extract_multiple(tree, xpaths):
        for name, xpath in xpaths.items():
            data = "".join(tree.xpath(xpath))
            if "count" in name:
                data = parse_tg_number_string(data)
            if data == "":
                data = None
            yield pd.Series(
                data=data, name=name, dtype="object" if isinstance(data, str) else float
            )

    try:
        data = pd.concat(extract_multiple(page, user_paths), axis=1)
        data["name"] = data["name"].str.replace("@", "")
    except ValueError:
        # If this code is executed, we could not obtain data from the TG-page
        return pd.DataFrame()
    except AttributeError:
        return pd.DataFrame()
    # post process entries
    return data


def telegram_connector(node_names: list[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """for a list of handles scrape their public telegram channels

    That is, if there is a public channel present for the specified handles.

    Parameters
    ----------
    node_names :
        list[str] : list of handles to scrape

    Returns :
        Tuple[pd.DataFrame, pd.DataFrame] : edges, nodes
    """

    def get_node(node_name: str) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        visit_url = f"https://t.me/s/{node_name}"
        resp = requests.get(visit_url)
        log.debug(f"Visited node {node_name} with status: {resp.status_code}")

        if resp.status_code == 302:
            log.warning(f"{visit_url} throw an error")
        if resp.status_code == 200:
            html_source = html.fromstring(resp.content)
            messages = get_messages(html_source)

            nodes = get_user(html_source)
            log.debug(f"Parsed userdata for {node_name}:\n{nodes}\n")
            # except Exception:
            #     log.warning(f"Parsing failed for {node_name}")
            #     return None
            if len(messages) > 0:
                edges = messages.loc[~messages["forwarded_message_url"].isnull(), :]
                if len(edges) > 0:
                    edges["source"] = edges["handle"]
                    edges["target"] = edges["forwarded_message_url"].str.extract(
                        r"([\w_]+)(?=\/\d+)"
                    )

                log.debug(f"Parsed message for {node_name}:\n{edges}\n")
                log.debug(f"Retrieved {len(edges)} edges for {node_name}")

                return edges, nodes

        log.warning(f"parsing failed for {node_name}")
        return pd.DataFrame(), pd.DataFrame()

    def reduce_returns(
        carry: Tuple[pd.DataFrame, pd.DataFrame],
        value: Tuple[pd.DataFrame, pd.DataFrame],
    ):
        return pd.concat([carry[0], value[0]]), pd.concat([carry[1], value[1]])

    _ret_ = [get_node(_) for _ in node_names]
    _ret_ = [_ for _ in _ret_ if _ is not None]

    log.debug(f"TG Connector found: {_ret_}")

    if len(_ret_) > 0:
        return reduce(reduce_returns, _ret_)
    return pd.DataFrame(), pd.DataFrame()
