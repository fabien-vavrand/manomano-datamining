import re
import logging
import json
import pandas as pd
import numpy as np


def load_data_from_dirty_json_file(s):
    start_pattern = '{"rawResults":'
    end_pattern = ',"state":{'
    start_index = s.find(start_pattern) + len(start_pattern)
    end_index = s.rfind(end_pattern)
    s = s[start_index:end_index]

    # Replace or remove badly espaced quotes
    s = s.replace('\\\\\\"', '')
    s = s.replace('\\\\\"', '')
    s = s.replace('\\\\"', '')
    s = s.replace('\\\"', '')
    s = s.replace('\\"', '')
    s = s.replace("\\'", "'")

    # Replace non unicode characters
    s = s.replace('\\x80', '')
    s = s.replace('\\x81', '')
    s = s.replace('\\x82', 'é')
    s = s.replace('\\x83', 'â')
    s = s.replace('\\x84', '')
    s = s.replace('\\x85', 'à')
    s = s.replace('\\x87', 'ç')
    s = s.replace('\\x88', 'à')
    s = s.replace('\\x89', '')
    s = s.replace('\\x8a', 'è')
    s = s.replace('\\x8b', '_')
    s = s.replace('\\x8c', '_')
    s = s.replace('\\x8d', '')
    s = s.replace('\\x8e', '')
    s = s.replace('\\x8f', '')
    s = s.replace('\\x90', '')
    s = s.replace('\\x91', '')
    s = s.replace('\\x92', '\'')
    s = s.replace('\\x93', '') #\"
    s = s.replace('\\x94', '') #\"
    s = s.replace('\\x95', '')
    s = s.replace('\\x96', '')
    s = s.replace('\\x97', '')
    s = s.replace('\\x98', '')
    s = s.replace('\\x99', '')
    s = s.replace('\\x9c', 'oe')
    s = s.replace('\\x9d', 'oe')
    s = s.replace('\\x9e', 'u')
    s = s.replace('\\x9f', '')
    s = s.replace('\\xa0', ' ')
    s = s.replace('\\xad', '')

    # Removing emojis or special characters
    s = s.replace('\\U001000b6', '\'')
    s = s.replace('\\U0001fa91', '_')
    s = s.replace('\\U0010fc00', '_')
    s = s.replace('\\U0010fc07', '_')
    s = s.replace('\\U0010fc00', '_')
    s = s.replace('\\U0010fc01', '_')
    s = s.replace('\\U0010fc04', '_')
    s = s.replace('\\U0010fc08', '_')
    s = s.replace('\\U0010fc09', '_')
    s = s.replace('\\U0010fc14', '_')

    try:
        return json.loads(s)
    except Exception as e:
        error_position = int(re.findall(r'\(char (\d+)\)', str(e))[0])
        chars = s[(error_position - 40):(error_position + 40)]
        logging.error(chars)
        return None


def get_html_value(element, xpath):
    try:
        return str(element.xpath(xpath)[0]).strip()
    except Exception:
        return ''


def get_html_values(element, xpath):
    try:
        return [str(e).strip() for e in element.xpath(xpath)]
    except Exception:
        return []


def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]

