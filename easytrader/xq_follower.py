# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

import json
import re
from datetime import datetime
from numbers import Number
from threading import Thread

from easytrader.follower import BaseFollower
from easytrader.log import logger
from easytrader.utils.misc import parse_cookies_str
import pymysql
import decimal

class XueQiuFollower(BaseFollower):
    LOGIN_PAGE = "https://www.xueqiu.com"
    LOGIN_API = "https://xueqiu.com/snowman/login"
    TRANSACTION_API = "https://xueqiu.com/service/tc/snowx/PAMID/cubes/rebalancing/history"
    PORTFOLIO_URL = "https://xueqiu.com/p/"
    WEB_REFERER = "https://www.xueqiu.com"

    def __init__(self):
        super().__init__()
        self._adjust_sell = None
        self._users = None

    def login(self, user=None, password=None, **kwargs):
        """
        雪球登陆， 需要设置 cookies
        :param cookies: 雪球登陆需要设置 cookies， 具体见
            https://smalltool.github.io/2016/08/02/cookie/
        :return:
        """
        cookies = kwargs.get("cookies")
        if cookies is None:
            raise TypeError(
                "雪球登陆需要设置 cookies， 具体见" "https://smalltool.github.io/2016/08/02/cookie/"
            )
        headers = self._generate_headers()
        self.s.headers.update(headers)

        self.s.get(self.LOGIN_PAGE)

        cookie_dict = parse_cookies_str(cookies)
        self.s.cookies.update(cookie_dict)

        logger.info("登录成功")

    def follow(  # type: ignore
        self,
        users,
        strategies,
        total_assets=None,
        initial_assets=None,
        adjust_sell=False,
        track_interval=10,
        trade_cmd_expire_seconds=120,
        cmd_cache=True,
        slippage: float = 0.0,
    ):
        """跟踪 joinquant 对应的模拟交易，支持多用户多策略
        :param users: 支持 easytrader 的用户对象，支持使用 [] 指定多个用户
        :param strategies: 雪球组合名, 类似 ZH123450
        :param total_assets: 雪球组合对应的总资产， 格式 [组合1对应资金, 组合2对应资金]
            若 strategies=['ZH000001', 'ZH000002'],
                设置 total_assets=[10000, 10000], 则表明每个组合对应的资产为 1w 元
            假设组合 ZH000001 加仓 价格为 p 股票 A 10%,
                则对应的交易指令为 买入 股票 A 价格 P 股数 1w * 10% / p 并按 100 取整
        :param adjust_sell: 是否根据用户的实际持仓数调整卖出股票数量，
            当卖出股票数大于实际持仓数时，调整为实际持仓数。目前仅在银河客户端测试通过。
            当 users 为多个时，根据第一个 user 的持仓数决定
        :type adjust_sell: bool
        :param initial_assets: 雪球组合对应的初始资产,
            格式 [ 组合1对应资金, 组合2对应资金 ]
            总资产由 初始资产 × 组合净值 算得， total_assets 会覆盖此参数
        :param track_interval: 轮训模拟交易时间，单位为秒
        :param trade_cmd_expire_seconds: 交易指令过期时间, 单位为秒
        :param cmd_cache: 是否读取存储历史执行过的指令，防止重启时重复执行已经交易过的指令
        :param slippage: 滑点，0.0 表示无滑点, 0.05 表示滑点为 5%
        """
        super().follow(
            users=users,
            strategies=strategies,
            track_interval=track_interval,
            trade_cmd_expire_seconds=trade_cmd_expire_seconds,
            cmd_cache=cmd_cache,
            slippage=slippage,
        )

        self._adjust_sell = adjust_sell

        self._users = self.warp_list(users)

        strategies = self.warp_list(strategies)
        total_assets = self.warp_list(total_assets)
        initial_assets = self.warp_list(initial_assets)

        if cmd_cache:
            self.load_expired_cmd_cache()

        self.start_trader_thread(self._users, trade_cmd_expire_seconds)

        for strategy_url, strategy_total_assets, strategy_initial_assets in zip(
            strategies, total_assets, initial_assets
        ):
            assets = self.calculate_assets(
                strategy_url, strategy_total_assets, strategy_initial_assets
            )
            try:
                strategy_id = self.extract_strategy_id(strategy_url)
                strategy_name = self.extract_strategy_name(strategy_url)
            except:
                logger.error("抽取交易id和策略名失败, 无效模拟交易url: %s", strategy_url)
                raise
            strategy_worker = Thread(
                target=self.track_strategy_worker,
                args=[strategy_id, strategy_name],
                kwargs={"interval": track_interval, "assets": assets},
            )
            strategy_worker.start()
            logger.info("开始跟踪策略: %s", strategy_name)

    def calculate_assets(self, strategy_url, total_assets=None, initial_assets=None):
        # 都设置时优先选择 total_assets
        if total_assets is None and initial_assets is not None:
            net_value = self._get_portfolio_net_value(strategy_url)
            total_assets = initial_assets * net_value
        if not isinstance(total_assets, Number):
            raise TypeError("input assets type must be number(int, float)")
        if total_assets < 1e3:
            raise ValueError("雪球总资产不能小于1000元，当前预设值 {}".format(total_assets))
        return total_assets

    @staticmethod
    def extract_strategy_id(strategy_url):
        return strategy_url

    def extract_strategy_name(self, strategy_url):
        base_url = "https://xueqiu.com/cubes/nav_daily/all.json?cube_symbol={}"
        url = base_url.format(strategy_url)
        rep = self.s.get(url)
        info_index = 0
        return rep.json()[info_index]["name"]

    def extract_transactions(self, history):
        # logger.info(history)
        if history is None:
            return []
        if history["count"] <= 0:
            return []

        transactions = []
        rebalancing_index = 0
        for i in history["list"]:
            raw_transactions = history["list"][rebalancing_index]["rebalancing_histories"]
            for transaction in raw_transactions:
                if transaction["price"] is None:
                    logger.info("该笔交易无法获取价格，疑似未成交，跳过。交易详情: %s", transaction)
                    continue
                transactions.append(transaction)
            rebalancing_index += 1
        return transactions

    def create_query_transaction_params(self, strategy):
        params = {"cube_symbol": strategy, "page": 1, "count": 20}
        return params

    # noinspection PyMethodOverriding
    def none_to_zero(self, data):
        if data is None:
            return 0
        return data

    # noinspection PyMethodOverriding
    def project_transactions(self, transactions, assets):

        logger.info("总额：" + str(assets))
        # 打开数据库连接
        db = pymysql.connect("localhost", "root", "sa123$", "xueqiu")

        sql_operation = ""
        for transaction in transactions:
            weight_diff = self.none_to_zero(transaction["target_weight"]) - self.none_to_zero(
                transaction["prev_weight_adjusted"]
            )

            initial_amount = abs(weight_diff) / 100 * assets / transaction["price"]
            crrut_amount = abs(self.none_to_zero(transaction["target_weight"])) / 100 * assets / transaction["price"]

            transaction["datetime"] = datetime.fromtimestamp(
                transaction["updated_at"] // 1000
            )

            transaction["stock_code"] = transaction["stock_symbol"].lower()
            transaction["action"] = "买入" if weight_diff > 0 else "卖出"
            transaction["amount"] = int(round(initial_amount, -2))
            if transaction["action"] == "卖出" and self._adjust_sell:
                transaction["amount"] = self._adjust_sell_amount(
                    transaction["stock_code"], transaction["amount"]
                )
            logger.info("动作：" + str(transaction["action"]))
            logger.info("股票类型：" + str(transaction["stock_label"]))
            logger.info("股票名称：" + str(transaction["stock_name"]))
            logger.info("股票号：" + str(transaction["stock_symbol"]))
            logger.info("调仓前百分比：" + str(transaction["prev_weight_adjusted"]))
            logger.info("调仓后百分比：" + str(transaction["target_weight"]))
            logger.info("百分比：" + str(abs(weight_diff)))
            logger.info("数量：" + str(int(round(initial_amount, -2))))
            logger.info("价格：" + str(transaction["price"]))
            logger.info("时间：" + str(transaction["datetime"]))
            # 使用cursor()方法获取操作游标
            cursor = db.cursor()
            # 类似于其他语言的 query 函数，execute 是 python 中的执行查询函数
            cursor.execute("SELECT * FROM xq_operation where STOCK_CODE = '"+str(transaction["stock_symbol"]) +"' and  OPERATION_TIME = '" + str(transaction["datetime"]) + "' " )
            # 使用 fetchall 函数，将结果集（多维元组）存入 rows 里面
            rows = cursor.fetchall()
            if(len(rows)<=0):
                sql_operation = """INSERT IGNORE  INTO xq_operation(
                ACCOUNT_ID,STOCK_NAME, STOCK_CODE, STOCK_OPERATION, STOCK_PRICE, STOCK_COUNT, START_REPERTORY, END_REPERTORY, OPERATION_TIME, IS_DEL)
                VALUES ('1','{}','{}','{}',{},{},{},{},'{}','0')""".format(str(transaction["stock_name"]), str(transaction["stock_symbol"]), str(transaction["action"]), transaction["price"], decimal.Decimal(int(round(initial_amount, -2))), transaction["prev_weight_adjusted"], transaction["target_weight"], str(transaction["datetime"]))
                try:
                    if(sql_operation!=""):
                       logger.info(sql_operation)
                       cursor.execute(sql_operation)  # 执行sql语句
                except Exception:
                    logger.error("发生异常1", Exception)
                    # db.rollback()  # 如果发生错误则回滚
            # 类似于其他语言的 query 函数，execute 是 python 中的执行查询函数
            cursor.execute("SELECT * FROM xq_history where STOCK_CODE = '" + str(transaction["stock_symbol"]) + "' ")
            # 使用 fetchall 函数，将结果集（多维元组）存入 rows 里面
            rowss = cursor.fetchall()
            IS_HAS = 0
            if (str(transaction["target_weight"]) is "0.00" or str(transaction["target_weight"]) is "0" or str(
                    transaction["target_weight"]) is "0.0"):
                IS_HAS = 1

            if (len(rowss) > 0):
                sql_operation = """update xq_history set  STOCK_COUNT = {}, START_REPERTORY = {}, HISTORY_TIME = '{}', IS_HAS = '{}'  where STOCK_CODE = '{}' """.format(
                     decimal.Decimal(int(round(crrut_amount, -2))), str(transaction["target_weight"]),
                    str(transaction["datetime"]), IS_HAS, str(transaction["stock_symbol"]))
                try:
                    if (sql_operation != ""):
                        logger.info(sql_operation)
                        cursor.execute(sql_operation)  # 执行sql语句
                except Exception:
                    logger.error("发生异常2", Exception)
            else:
                sql_operation = """INSERT IGNORE  INTO xq_history(
                                ACCOUNT_ID,STOCK_NAME, STOCK_CODE, STOCK_COUNT, START_REPERTORY, HISTORY_TIME, IS_HAS, IS_DEL)
                                VALUES ('1','{}','{}','{}',{},'{}',{},'0')""".format(
                    str(transaction["stock_name"]), str(transaction["stock_symbol"]), decimal.Decimal(int(round(initial_amount, -2))),
                    transaction["target_weight"], str(transaction["datetime"]),
                    IS_HAS, '0')
                try:
                    if (sql_operation != ""):
                        logger.info(sql_operation)
                        cursor.execute(sql_operation)  # 执行sql语句
                except Exception:
                    logger.error("发生异常3", Exception)
                db.commit()  # 提交到数据库执行
        cursor.execute("update xq_account set TOTAL_BALANCE = {} where  ACCOUNT_ID = '1' ".format(assets))
        db.commit()  # 提交到数据库执行
        # 关闭数据库连接
        db.close()



        # logger.info("测试：" + str(transactions))

    def _adjust_sell_amount(self, stock_code, amount):
        """
        根据实际持仓值计算雪球卖出股数
          因为雪球的交易指令是基于持仓百分比，在取近似值的情况下可能出现不精确的问题。
        导致如下情况的产生，计算出的指令为买入 1049 股，取近似值买入 1000 股。
        而卖出的指令计算出为卖出 1051 股，取近似值卖出 1100 股，超过 1000 股的买入量，
        导致卖出失败
        :param stock_code: 证券代码
        :type stock_code: str
        :param amount: 卖出股份数
        :type amount: int
        :return: 考虑实际持仓之后的卖出股份数
        :rtype: int
        """
        # stock_code = stock_code[-6:]
        user = self._users[0]

        position = user.position
        # logger.info(position)
        # logger.info(stock_code)
        try:
            stock = next(s for s in position if s["stock_code"].upper() == stock_code.upper())
        except StopIteration:
            logger.info("根据持仓调整 %s 卖出额，发现未持有股票 %s, 不做任何调整", stock_code, stock_code)
            return amount

        available_amount = stock["enable_amount"]
        if available_amount >= amount:
            return amount

        adjust_amount = available_amount // 100 * 100
        logger.info(
            "股票 %s 实际可用余额 %s, 指令卖出股数为 %s, 调整为 %s",
            stock_code,
            available_amount,
            amount,
            adjust_amount,
        )
        return adjust_amount

    def _get_portfolio_info(self, portfolio_code):
        """
        获取组合信息
        """
        url = self.PORTFOLIO_URL + portfolio_code
        portfolio_page = self.s.get(url)
        match_info = re.search(r"(?<=SNB.cubeInfo = ).*(?=;\n)", portfolio_page.text)
        if match_info is None:
            raise Exception("cant get portfolio info, portfolio url : {}".format(url))
        try:
            portfolio_info = json.loads(match_info.group())
        except Exception as e:
            raise Exception("get portfolio info error: {}".format(e))
        return portfolio_info

    def _get_portfolio_net_value(self, portfolio_code):
        """
        获取组合信息
        """
        portfolio_info = self._get_portfolio_info(portfolio_code)
        return portfolio_info["net_value"]
