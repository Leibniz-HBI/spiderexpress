import re
import pandas as pd
from lxml import html

from ..Connector import Connector
class TelegramConnector(Connector):

    def get_layer(self, node_names: list[str]):
        pass

    def get_messages(self, channel: str, n = 20) -> None:
        # tg xpaths

        def extract_multiple2(tree, xpaths):
            for name, xpath in xpaths.items():
                # first post-processing steps
                if name != 'link':
                    data = "".join(tree.xpath(xpath))
                    if name == 'image_url':
                        data = re.findall("(?<=url\(').+(?=')", data)
                else:
                    data = ",".join(tree.xpath(xpath))
                if data == "":
                    data = None
                yield pd.Series(data = data, name = name, dtype = "object")

        base_xpath = "//div[@class='tgme_widget_message_bubble']"

        rel_xpaths = {
            "post_id"       : "../@data-post",
            "views"         : ".//span[@class='tgme_widget_message_views']/descendant-or-self::*/text()",
            "datetime"      : ".//time/@datetime",
            "user"          : ".//a[@class='tgme_widget_message_owner_name']/descendant-or-self::*/text()",
            "from_author"   : ".//span[@class='tgme_widget_message_from_author']/descendant-or-self::*/text()",
            "text"          : "./div[contains(@class, 'tgme_widget_message_text')]/descendant-or-self::*/text()",
            "link"          : ".//div[contains(@class, 'tgme_widget_message_text')]//a/@href",
    #       "reply_to_user" : ".//a[@class='tgme_widget_message_reply']//span[@class='tgme_widget_message_author_name'/descendant-or-self::*/text()]",
    #       "reply_to_text" : ".//a[@class='tgme_widget_message_reply']//div[@class='tgme_widget_message_text']/descendant-or-self::*/text()",
    #       "reply_to_link" : ".//a[@class='tgme_widget_message_reply']/@href",
            "image_url"     : "./a[contains(@class, 'tgme_widget_message_photo_wrap')]/@style",
    #       "forwarded_message_url" : ".//a[@class='tgme_widget_message_forwarded_from_name']/@href",
    #       "forwarded_message_user" : ".//a[@class='tgme_widget_message_forwarded_from_name']/descendant-or-self::*/text()",
    #       "poll_question" : ".//div[@class='tgme_widget_message_poll_question']/descendant-or-self::*/text()",
    #       "poll_options_text" : ".//div[@class='tgme_widget_message_poll_option_value']/descendant-or-self::*/text()",
    #       "poll_options_percent" : ".//div[@class='tgme_widget_message_poll_option_percent']/descendant-or-self::*/text()",
    #       "video_url" : ".//video[contains(@class, 'tgme_widget_message_video')]/@src",
    #       "video_duration" : ".//time[contains(@class, 'message_video_duration')]/descendant-or-self::*/text()"
        }
        
        messages_html = html.xpath(base_xpath)
        
        data = pd.concat(
            [pd.concat(extract_multiple2(_, rel_xpaths), axis = 1) for _ in messages_html],
            axis = 0,
            ignore_index=True
        )
        data[['handle', 'post_number']] = data.post_id.str.split("/", n = 1, expand = True)
        
        return data 

    def get_user(self, channel: str) -> None:
        
        def extract_multiple(tree, xpaths):
            for name, xpath in xpaths.items():
                data = "".join(tree.xpath(xpath))
                if data == "":
                    data = None
                yield pd.Series(data = data, name = name, dtype = "object")

        xpaths = {
            "handle": '//div[@class="tgme_channel_info_header_username"]/a/text()',
            "name": '//div[@class="tgme_channel_info_header_title"]/descendant-or-self::*/text()',
            "url": '//div[@class="tgme_channel_info_header_username"]/a/@href',
            "description": '//div[@class="tgme_channel_info_description"]/descendant-or-self::*/text()',
            "subscriber_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "subscribers"]/preceding-sibling::span/text()',
            "photos_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "photos"]/preceding-sibling::span/text()',
            "videos_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "videos"]/preceding-sibling::span/text()',
            "files_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "files"]/preceding-sibling::span/text()',
            "links_count": '//div[@class="tgme_channel_info_counter"]/span[@class="counter_type" and text() = "links"]/preceding-sibling::span/text()'
        } 
        data = pd.concat(extract_multiple(html, xpaths), axis = 1)
        # post process entries
        data['handle'] = data.handle.str.replace('@', '')
        return data