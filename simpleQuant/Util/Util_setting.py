# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2018 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import os
import configparser
from simpleQuant.Account.user import user_sign_in
from simpleQuant.Util.Localize import qa_path, setting_path
from simpleQuant.Util.Util_hdf5 import util_hdf5_setting



# quantaxis有一个配置目录存放在 ~/.quantaxis
# 如果配置目录不存在就创建，主要配置都保存在config.json里面
# 貌似yutian已经进行了，文件的创建步骤，他还会创建一个setting的dir
# 需要与yutian讨论具体配置文件的放置位置 author:Will 2018.5.19


CONFIGFILE_PATH = '{}{}{}'.format(setting_path, os.sep, 'config.ini')

DEFAULT_HDF5_URI = ''

DEFAULT_REDIS_URI = ''

DEFAULT_SQLLITE_URI = ''
class Util_setting():
    def __init__(self, uri=None):
        self.hdf5 = uri or self.get_config() or self.env_config() or DEFAULT_HDF5_URI
        self.redis = None
        self.sqllite = None

        # 加入配置文件地址

    def get_config(self, section='HDF5', option='uri', default_value=DEFAULT_HDF5_URI):
        """[summary]

        Keyword Arguments:
            section {str} -- [description] (default: {'MONGODB'})
            option {str} -- [description] (default: {'uri'})
            default_value {[type]} -- [description] (default: {DEFAULT_HDF5_URI})

        Returns:
            [type] -- [description]
        """

        config = configparser.ConfigParser()
        if os.path.exists(CONFIGFILE_PATH):
            config.read(CONFIGFILE_PATH)
            return self.get_or_set_section(config, section, option, default_value)

            # 排除某些IP
            # self.get_or_set_section(config, 'IPLIST', 'exclude', [{'ip': '1.1.1.1', 'port': 7709}])

        else:
            f = open(CONFIGFILE_PATH, 'w')
            config.add_section(section)
            config.set(section, option, default_value)
            config.write(f)
            f.close()
            return default_value

    def set_config(self, section='HDF5', option='uri', default_value=DEFAULT_HDF5_URI):
        """[summary]

        Keyword Arguments:
            section {str} -- [description] (default: {'MONGODB'})
            option {str} -- [description] (default: {'uri'})
            default_value {[type]} -- [description] (default: {DEFAULT_DB_URI})

        Returns:
            [type] -- [description]
        """

        config = configparser.ConfigParser()
        if os.path.exists(CONFIGFILE_PATH):
            config.read(CONFIGFILE_PATH)
            return self.get_or_set_section(config, section, option, default_value, 'set')

            # 排除某些IP
            # self.get_or_set_section(config, 'IPLIST', 'exclude', [{'ip': '1.1.1.1', 'port': 7709}])

        else:
            f = open(CONFIGFILE_PATH, 'w')
            config.add_section(section)
            config.set(section, option, default_value)
            config.write(f)
            f.close()
            return default_value

    def get_or_set_section(self, config, section, option, DEFAULT_VALUE, method='get'):
        """[summary]

        Arguments:
            config {[type]} -- [description]
            section {[type]} -- [description]
            option {[type]} -- [description]
            DEFAULT_VALUE {[type]} -- [description]

        Keyword Arguments:
            method {str} -- [description] (default: {'get'})

        Returns:
            [type] -- [description]
        """

        try:
            if isinstance(DEFAULT_VALUE,str):
                val = DEFAULT_VALUE
            else:              
                val = json.dumps(DEFAULT_VALUE)
            if method == 'get':
                return config.get(section, option)
            else:
                config.set(section, option, val)
                return val

        except configparser.NoSectionError:
            print('NO SECTION "{}" FOUND, Initialize...'.format(section))
            config.add_section(section)
            config.set(section, option, val)
            return val
        except configparser.NoOptionError:
            print('NO OPTION "{}" FOUND, Initialize...'.format(option))
            config.set(section, option, val)
            return val
        finally:
            with open(CONFIGFILE_PATH, 'w') as f:
                config.write(f)

    def env_config(self):
        return os.environ.get("MONGOURI", None)

    @property
    def client(self):
        return util_hdf5_setting(self.hdf5)
   

    def change(self, ip, port):
        self.ip = ip
        self.port = port
        global DATABASE
        DATABASE = self.client
        return self

    def login(self, user_name, password):
        user = user_sign_in(user_name, password, self.client)
        if user is not None:
            self.user_name = user_name
            self.password = password
            self.user = user
            return self.user
        else:
            return False


SETTINGS = Util_setting()
REDIS = SETTINGS.redis
HDF5=SETTINGS.hdf5
SQLLITE=SETTINGS.sqllite


def exclude_from_stock_ip_list(exclude_ip_list):
    """ 从stock_ip_list删除列表exclude_ip_list中的ip

    :param exclude_ip_list:  需要删除的ip_list
    :return: None
    """
    for exc in exclude_ip_list:
        if exc in stock_ip_list:
            stock_ip_list.remove(exc)


info_ip_list = [{'ip': '124.14.104.60', 'port': 7709},
                {'ip': '112.14.104.66', 'port': 7709},
                {'ip': '112.95.140.92', 'port': 7709},
                {'ip': '112.95.140.93', 'port': 7709},
                {'ip': '112.95.140.74', 'port': 7709},
                {'ip': '112.14.104.53', 'port': 7709}
]


stock_ip_list = [
    {'ip': '121.14.104.63','port':7721},
    {'ip': '119.147.80.148','port':7721},
    {'ip': '112.95.140.96','port':7721},
    {'ip': '221.139.150.61','port':7721}
    
]

future_ip_list = [
    {'ip': '112.74.214.43', 'port': 7727}
]

