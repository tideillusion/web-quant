"""
Author: Mark Sun
Email: sunyunfei201@gmail.com
----------------------------------------------------
Examples:
    from Query import Query
    q = Query()
    q.search(table='stock_daily')
    q.search('fut')
    q.search('净资产同比增长率','fina_indicator')
    q.daily(filter={'limit':5})

----------------------------------------------------
Usage:
You can use `Query().search('')` to print all tables and their parameters.
You can also perform search more precisely, for example:
    q = Query()
    q.search('date')
It will print all table whose parameters contain "date".
    q.search('存货')
It will print all table whose parameters contain "存货".
    q.search('opt_daily')
it will print all parameters of table "opt_daily".

Once you know which tables contain your keyword, you can move forward:
    q.search('存货','fina_indicator')
It will print all parameters that contain your keyword in your table.

For every method defiened in Query, it takes 2 arguments then returns a Dataframe object:
    select:list
    filter:dict
    
In ${select}, you can pass in column names, for example:
    q = Query()
    q.daily(select=['ts_code','trade_date','open','close'], filter={'limit':5})
The above command generates SQL command like this:
    SELECT `ts_code`,`trade_date`,`open`,`close` FROM daily WHERE 1 LIMIT 5;
    
In ${filter}, you can pass a dict, whose keys can be either list, tuple or int ('limit' only). For example:
    q.daily(filter = {'ts_code':['000001','000002.SZ'], 'trade_date':(20100101,20200101), 'change':('>=',0), 'limit':10})
The above command generates SQL command like this:
    SELECT * FROM daily WHERE (`ts_code` IN ("000001.SZ","000002.SZ")) AND (`trade_date` BETWEEN 20100101 AND 20200101) AND (`change` >= 0.000000) LIMIT 10;
The TS codes will be automatically completed. '000001' will be converted to '000001.SZ'.
"""

from re import match
from warnings import warn

import numpy as np
from pandas import DataFrame
from pymysql import connect


class Help:
    def __init__(self):
        self.__tables = dict(zip(
            ['stock_basic', 'stock_daily', 'fina_indicator', 'fund_basic', 'fund_daily', 'fut_basic', 'fut_daily',
             'index_basic', 'index_daily', 'opt_basic', 'opt_daily', 'stock_daily_basic','MACD','trade_cal','shibor'],
            ['股票列表', '日线行情', '财务指标数据', '公募基金列表', '场内基金日线行情', '期货合约信息表', '期货日线行情', '指数基本信息', '指数日线行情', '期权合约信息',
             '期权日线行情', '每日指标', '异同移动平均线', '交易日历','上海银行间同业拆放利率']))

        self.__outputs = [{'ts_code': 'TS代码', 'symbol': '股票代码', 'name': '股票名称', 'area': '所在地域', 'industry': '所属行业',
                           'fullname': '股票全称', 'enname': '英文全称', 'market': '市场类型 （主板/中小板/创业板/科创板）', 'exchange': '交易所代码',
                           'curr_type': '交易货币', 'list_status': '上市状态： L上市 D退市 P暂停上市', 'list_date': '上市日期',
                           'delist_date': '退市日期', 'is_hs': '是否沪深港通标的，N否 H沪股通 S深股通'},
                          {'ts_code': '股票代码', 'trade_date': '交易日期', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
                           'close': '收盘价', 'pre_close': '昨收价', 'change': '涨跌额', 'pct_chg': '涨跌幅', 'vol': '成交量 （手）',
                           'amount': '成交额 （千元）',
                           'ma5': '5日均线',
                           'ma_v_5': '5日成交量均线',
                           'ma10': '10日均线',
                           'ma_v_10': '10日成交量均线',
                           'ma15': '15日均线',
                           'ma_v_15': '15日成交量均线',
                           'ma20': '20日均线',
                           'ma_v_20': '20日成交量均线',
                           'ma30': '30日均线',
                           'ma_v_30': '30日成交量均线',
                           'ma60': '60日均线',
                           'ma_v_60': '60日成交量均线',
                           'ma90': '90日均线',
                           'ma_v_90': '90日成交量均线',
                           'ma120': '120日均线',
                           'ma_v_120': '120日成交量均线',
                           'ma180': '180日均线',
                           'ma_v_180': '180日成交量均线',
                           'ma240': '240日均线',
                           'ma_v_240': '240日成交量均线'},
                          {'ts_code': 'TS代码', 'ann_date': '公告日期', 'end_date': '报告期', 'eps': '基本每股收益',
                           'dt_eps': '稀释每股收益', 'total_revenue_ps': '每股营业总收入', 'revenue_ps': '每股营业收入',
                           'capital_rese_ps': '每股资本公积', 'surplus_rese_ps': '每股盈余公积', 'undist_profit_ps': '每股未分配利润',
                           'extra_item': '非经常性损益', 'profit_dedt': '扣除非经常性损益后的净利润', 'gross_margin': '毛利',
                           'current_ratio': '流动比率', 'quick_ratio': '速动比率', 'cash_ratio': '保守速动比率',
                           'invturn_days': '存货周转天数', 'arturn_days': '应收账款周转天数', 'inv_turn': '存货周转率',
                           'ar_turn': '应收账款周转率', 'ca_turn': '流动资产周转率', 'fa_turn': '固定资产周转率', 'assets_turn': '总资产周转率',
                           'op_income': '经营活动净收益', 'valuechange_income': '价值变动净收益', 'interst_income': '利息费用',
                           'daa': '折旧与摊销', 'ebit': '息税前利润', 'ebitda': '息税折旧摊销前利润', 'fcff': '企业自由现金流量',
                           'fcfe': '股权自由现金流量', 'current_exint': '无息流动负债', 'noncurrent_exint': '无息非流动负债',
                           'interestdebt': '带息债务', 'netdebt': '净债务', 'tangible_asset': '有形资产',
                           'working_capital': '营运资金', 'networking_capital': '营运流动资本', 'invest_capital': '全部投入资本',
                           'retained_earnings': '留存收益', 'diluted2_eps': '期末摊薄每股收益', 'bps': '每股净资产',
                           'ocfps': '每股经营活动产生的现金流量净额', 'retainedps': '每股留存收益', 'cfps': '每股现金流量净额', 'ebit_ps': '每股息税前利润',
                           'fcff_ps': '每股企业自由现金流量', 'fcfe_ps': '每股股东自由现金流量', 'netprofit_margin': '销售净利率',
                           'grossprofit_margin': '销售毛利率', 'cogs_of_sales': '销售成本率', 'expense_of_sales': '销售期间费用率',
                           'profit_to_gr': '净利润/营业总收入', 'saleexp_to_gr': '销售费用/营业总收入', 'adminexp_of_gr': '管理费用/营业总收入',
                           'finaexp_of_gr': '财务费用/营业总收入', 'impai_ttm': '资产减值损失/营业总收入', 'gc_of_gr': '营业总成本/营业总收入',
                           'op_of_gr': '营业利润/营业总收入', 'ebit_of_gr': '息税前利润/营业总收入', 'roe': '净资产收益率',
                           'roe_waa': '加权平均净资产收益率', 'roe_dt': '净资产收益率(扣除非经常损益)', 'roa': '总资产报酬率', 'npta': '总资产净利润',
                           'roic': '投入资本回报率', 'roe_yearly': '年化净资产收益率', 'roa2_yearly': '年化总资产报酬率',
                           'roe_avg': '平均净资产收益率(增发条件)', 'opincome_of_ebt': '经营活动净收益/利润总额',
                           'investincome_of_ebt': '价值变动净收益/利润总额', 'n_op_profit_of_ebt': '营业外收支净额/利润总额',
                           'tax_to_ebt': '所得税/利润总额', 'dtprofit_to_profit': '扣除非经常损益后的净利润/净利润',
                           'salescash_to_or': '销售商品提供劳务收到的现金/营业收入', 'ocf_to_or': '经营活动产生的现金流量净额/营业收入',
                           'ocf_to_opincome': '经营活动产生的现金流量净额/经营活动净收益', 'capitalized_to_da': '资本支出/折旧和摊销',
                           'debt_to_assets': '资产负债率', 'assets_to_eqt': '权益乘数', 'dp_assets_to_eqt': '权益乘数(杜邦分析)',
                           'ca_to_assets': '流动资产/总资产', 'nca_to_assets': '非流动资产/总资产',
                           'tbassets_to_totalassets': '有形资产/总资产', 'int_to_talcap': '带息债务/全部投入资本',
                           'eqt_to_talcapital': '归属于母公司的股东权益/全部投入资本', 'currentdebt_to_debt': '流动负债/负债合计',
                           'longdeb_to_debt': '非流动负债/负债合计', 'ocf_to_shortdebt': '经营活动产生的现金流量净额/流动负债',
                           'debt_to_eqt': '产权比率', 'eqt_to_debt': '归属于母公司的股东权益/负债合计',
                           'eqt_to_interestdebt': '归属于母公司的股东权益/带息债务', 'tangibleasset_to_debt': '有形资产/负债合计',
                           'tangasset_to_intdebt': '有形资产/带息债务', 'tangibleasset_to_netdebt': '有形资产/净债务',
                           'ocf_to_debt': '经营活动产生的现金流量净额/负债合计', 'ocf_to_interestdebt': '经营活动产生的现金流量净额/带息债务',
                           'ocf_to_netdebt': '经营活动产生的现金流量净额/净债务', 'ebit_to_interest': '已获利息倍数(EBIT/利息费用)',
                           'longdebt_to_workingcapital': '长期债务与营运资金比率', 'ebitda_to_debt': '息税折旧摊销前利润/负债合计',
                           'turn_days': '营业周期', 'roa_yearly': '年化总资产净利率', 'roa_dp': '总资产净利率(杜邦分析)',
                           'fixed_assets': '固定资产合计', 'profit_prefin_exp': '扣除财务费用前营业利润', 'non_op_profit': '非营业利润',
                           'op_to_ebt': '营业利润／利润总额', 'nop_to_ebt': '非营业利润／利润总额', 'ocf_to_profit': '经营活动产生的现金流量净额／营业利润',
                           'cash_to_liqdebt': '货币资金／流动负债', 'cash_to_liqdebt_withinterest': '货币资金／带息流动负债',
                           'op_to_liqdebt': '营业利润／流动负债', 'op_to_debt': '营业利润／负债合计', 'roic_yearly': '年化投入资本回报率',
                           'total_fa_trun': '固定资产合计周转率', 'profit_to_op': '利润总额／营业收入', 'q_opincome': '经营活动单季度净收益',
                           'q_investincome': '价值变动单季度净收益', 'q_dtprofit': '扣除非经常损益后的单季度净利润', 'q_eps': '每股收益(单季度)',
                           'q_netprofit_margin': '销售净利率(单季度)', 'q_gsprofit_margin': '销售毛利率(单季度)',
                           'q_exp_to_sales': '销售期间费用率(单季度)', 'q_profit_to_gr': '净利润／营业总收入(单季度)',
                           'q_saleexp_to_gr': '销售费用／营业总收入 (单季度)', 'q_adminexp_to_gr': '管理费用／营业总收入 (单季度)',
                           'q_finaexp_to_gr': '财务费用／营业总收入 (单季度)', 'q_impair_to_gr_ttm': '资产减值损失／营业总收入(单季度)',
                           'q_gc_to_gr': '营业总成本／营业总收入 (单季度)', 'q_op_to_gr': '营业利润／营业总收入(单季度)', 'q_roe': '净资产收益率(单季度)',
                           'q_dt_roe': '净资产单季度收益率(扣除非经常损益)', 'q_npta': '总资产净利润(单季度)',
                           'q_opincome_to_ebt': '经营活动净收益／利润总额(单季度)', 'q_investincome_to_ebt': '价值变动净收益／利润总额(单季度)',
                           'q_dtprofit_to_profit': '扣除非经常损益后的净利润／净利润(单季度)',
                           'q_salescash_to_or': '销售商品提供劳务收到的现金／营业收入(单季度)', 'q_ocf_to_sales': '经营活动产生的现金流量净额／营业收入(单季度)',
                           'q_ocf_to_or': '经营活动产生的现金流量净额／经营活动净收益(单季度)', 'basic_eps_yoy': '基本每股收益同比增长率(%)',
                           'dt_eps_yoy': '稀释每股收益同比增长率(%)', 'cfps_yoy': '每股经营活动产生的现金流量净额同比增长率(%)',
                           'op_yoy': '营业利润同比增长率(%)', 'ebt_yoy': '利润总额同比增长率(%)', 'netprofit_yoy': '归属母公司股东的净利润同比增长率(%)',
                           'dt_netprofit_yoy': '归属母公司股东的净利润-扣除非经常损益同比增长率(%)', 'ocf_yoy': '经营活动产生的现金流量净额同比增长率(%)',
                           'roe_yoy': '净资产收益率(摊薄)同比增长率(%)', 'bps_yoy': '每股净资产相对年初增长率(%)',
                           'assets_yoy': '资产总计相对年初增长率(%)', 'eqt_yoy': '归属母公司的股东权益相对年初增长率(%)', 'tr_yoy': '营业总收入同比增长率(%)',
                           'or_yoy': '营业收入同比增长率(%)', 'q_gr_yoy': '营业总收入同比增长率(%)(单季度)', 'q_gr_qoq': '营业总收入环比增长率(%)(单季度)',
                           'q_sales_yoy': '营业收入同比增长率(%)(单季度)', 'q_sales_qoq': '营业收入环比增长率(%)(单季度)',
                           'q_op_yoy': '营业利润同比增长率(%)(单季度)', 'q_op_qoq': '营业利润环比增长率(%)(单季度)',
                           'q_profit_yoy': '净利润同比增长率(%)(单季度)', 'q_profit_qoq': '净利润环比增长率(%)(单季度)',
                           'q_netprofit_yoy': '归属母公司股东的净利润同比增长率(%)(单季度)', 'q_netprofit_qoq': '归属母公司股东的净利润环比增长率(%)(单季度)',
                           'equity_yoy': '净资产同比增长率', 'rd_exp': '研发费用', 'update_flag': '更新标识'},
                          {'ts_code': '基金代码', 'name': '简称', 'management': '管理人', 'custodian': '托管人',
                           'fund_type': '投资类型', 'found_date': '成立日期', 'due_date': '到期日期', 'list_date': '上市时间',
                           'issue_date': '发行日期', 'delist_date': '退市日期', 'issue_amount': '发行份额(亿)', 'm_fee': '管理费',
                           'c_fee': '托管费', 'duration_year': '存续期', 'p_value': '面值', 'min_amount': '起点金额(万元)',
                           'exp_return': '预期收益率', 'benchmark': '业绩比较基准', 'status': '存续状态D摘牌 I发行 L已上市',
                           'invest_type': '投资风格', 'type': '基金类型', 'trustee': '受托人', 'purc_startdate': '日常申购起始日',
                           'redm_startdate': '日常赎回起始日', 'market': 'E场内O场外'},
                          {'ts_code': 'TS代码', 'trade_date': '交易日期', 'open': '开盘价(元)', 'high': '最高价(元)', 'low': '最低价(元)',
                           'close': '收盘价(元)', 'pre_close': '昨收盘价(元)', 'change': '涨跌额(元)', 'pct_chg': '涨跌幅(%)',
                           'vol': '成交量(手)', 'amount': '成交额(千元)'},
                          {'ts_code': '合约代码', 'symbol': '交易标识', 'exchange': '交易市场', 'name': '中文简称',
                           'fut_code': '合约产品代码', 'multiplier': '合约乘数', 'trade_unit': '交易计量单位', 'per_unit': '交易单位(每手)',
                           'quote_unit': '报价单位', 'quote_unit_desc': '最小报价单位说明', 'd_mode_desc': '交割方式说明',
                           'list_date': '上市日期', 'delist_date': '最后交易日期', 'd_month': '交割月份', 'last_ddate': '最后交割日',
                           'trade_time_desc': '交易时间说明'},
                          {'ts_code': 'TS合约代码', 'trade_date': '交易日期', 'pre_close': '昨收盘价', 'pre_settle': '昨结算价',
                           'open': '开盘价', 'high': '最高价', 'low': '最低价', 'close': '收盘价', 'settle': '结算价',
                           'change1': '涨跌1 收盘价-昨结算价', 'change2': '涨跌2 结算价-昨结算价', 'vol': '成交量(手)', 'amount': '成交金额(万元)',
                           'oi': '持仓量(手)', 'oi_chg': '持仓量变化', 'delv_settle': '交割结算价'},
                          {'ts_code': 'TS代码', 'name': '简称', 'fullname': '指数全称', 'market': '市场', 'publisher': '发布方',
                           'index_type': '指数风格', 'category': '指数类别', 'base_date': '基期', 'base_point': '基点',
                           'list_date': '发布日期', 'weight_rule': '加权方式', 'desc': '描述', 'exp_date': '终止日期'},
                          {'ts_code': 'TS指数代码', 'trade_date': '交易日', 'close': '收盘点位', 'open': '开盘点位', 'high': '最高点位',
                           'low': '最低点位', 'pre_close': '昨日收盘点', 'change': '涨跌点', 'pct_chg': '涨跌幅（%）', 'vol': '成交量（手）',
                           'amount': '成交额（千元）'},
                          {'ts_code': 'TS代码', 'exchange': '交易市场', 'name': '合约名称', 'per_unit': '合约单位',
                           'opt_code': '标准合约代码', 'opt_type': '合约类型', 'call_put': '期权类型', 'exercise_type': '行权方式',
                           'exercise_price': '行权价格', 's_month': '结算月', 'maturity_date': '到期日', 'list_price': '挂牌基准价',
                           'list_date': '开始交易日期', 'delist_date': '最后交易日期', 'last_edate': '最后行权日期',
                           'last_ddate': '最后交割日期', 'quote_unit': '报价单位', 'min_price_chg': '最小价格波幅'},
                          {'ts_code': 'TS代码', 'trade_date': '交易日期', 'exchange': '交易市场', 'pre_settle': '昨结算价',
                           'pre_close': '前收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价', 'close': '收盘价',
                           'settle': '结算价', 'vol': '成交量(手)', 'amount': '成交金额(万元)', 'oi': '持仓量(手)'},
                          {'ts_code': 'TS股票代码', 'trade_date': '交易日期', 'close': '当日收盘价', 'turnover_rate': '换手率（%）',
                           'turnover_rate_f': '换手率（自由流通股）', 'volume_ratio': '量比', 'pe': '市盈率（总市值/净利润， 亏损的PE为空）',
                           'pe_ttm': '市盈率（TTM，亏损的PE为空）', 'pb': '市净率（总市值/净资产）', 'ps': '市销率', 'ps_ttm': '市销率（TTM）',
                           'dv_ratio': '股息率 （%）', 'dv_ttm': '股息率（TTM）（%）', 'total_share': '总股本 （万股）',
                           'float_share': '流通股本 （万股）', 'free_share': '自由流通股本 （万）', 'total_mv': '总市值 （万元）',
                           'circ_mv': '流通市值（万元）'},
                          {'ts_code': 'TS股票代码', 'trade_date': '交易日期', 'close': '当日收盘价','ema12':'12日移动平均','ema26':'26日移动平均',
                           'DIF':'差离值','DEA':'9日差离平均','MACD':'异同移动平均'},
                          {'trade_date':'交易日期'},
                          {'date':'日期','on':'隔夜','1w':'1周','2w':'2周','1m':'1个月','3m':'3个月','6m':'6个月','9m':'9个月','1y':'年'}]

    def __search_dict(self, key: str, dic: dict):  # Return all indices of key in dic,value() and dic.keys()
        index = np.array([bool(match('.*%s.*' % key, i)) for i in list(dic.keys())]) + \
                np.array([bool(match('.*%s.*' % key, i)) for i in list(dic.values())])
        return index

    def search(self, keyword: str = '', table: str = None):
        if table:
            index = np.array([table == i for i in self.__tables.keys()])
            index_table = np.where(index)[0]
            if index_table.size:
                index_table = index_table[0]
            else:
                print('Could not find table "%s", please check again.' % table)
                return
            dict_output = list(self.__outputs)[index_table]
            if keyword:
                index_output = self.__search_dict(keyword, dict_output)

                if any(index_output):
                    return dict(list(zip(np.array(list(dict_output.keys()))[index_output],
                                         np.array(list(dict_output.values()))[index_output])))

                else:
                    print('"%s" not found in %s.' % (keyword, table))
                    return

            else:
                return dict_output

        else:
            index_table = self.__search_dict(keyword, self.__tables)

            index_output = np.array([any(self.__search_dict(keyword, dic)) for dic in self.__outputs])
            index = index_output
            for i in range(len(index)):
                if index[i]:
                    print('\n--------------- Table: {%s: %s} ---------------' % (
                        str(list(self.__tables)[i]), list(self.__tables.values())[i]))
                    print(list(self.__outputs)[i])

    def get_tables(self) -> dict:
        return self.__tables


class Query(Help):
    def __init__(self,host = 'db.tideillusion.xyz', user='read_only',passwd='Fintech@193.112.251.160',db = 'quant'):
        Help.__init__(self)
        self.__host = host
        self.__user = user
        self.__passwd = passwd
        self.__db = db

    def __wrap(self, string: str, wrapper: str = '"'):
        return wrapper + string + wrapper

    def __conn(self):
        db = connect(self.__host,
                     self.__user,
                     self.__passwd,
                     self.__db)
        return db

    def __sql_code(self, ts_code, table):
        sql_code = '1'
        if ts_code:
            if ts_code[0]:
                if table in ['stock_daily', 'stock_basic']:
                    sz = '03'
                    sh = '6'
                elif table in ['fund_basic','fund_daily']:
                    sz = '1'
                    sh = '5'
                elif table == 'index_daily':
                    sz = '3'
                    sh = '0'
                else:
                    ts_code = [self.__wrap(i) for i in ts_code]
                    sql_code = '(`ts_code` IN (%s))' % ','.join(ts_code)
                    return sql_code
                for index, code in enumerate(ts_code):
                    if bool(match('^[0-9]{6}', code)) & (len(code) in [6,9]):
                        pass
                    else:
                        raise Exception('Wrong length of "%s" in ts_code.' % code)
                    if len(code) == 6:
                        if match('^[%s]' % sz, code):
                            ts_code[index] += '.SZ'
                        elif match('^[%s]' % sh, code):
                            ts_code[index] += '.SH'
                    if code[0] in [d for d in sz+sh]:
                        pass
                    else:
                        raise Exception('Prefix of "%s" is not in %s.' % (code, [i for i in sz+sh]))
                ts_code = [self.__wrap(i) for i in ts_code]
                sql_code = '(`ts_code` IN (%s))' % ','.join(ts_code)
        return sql_code

    def __sql_date(self, start_date, end_date):
        if start_date - end_date > 0:
            raise Exception('%d after %d.' % (start_date, end_date))
        if start_date < 20100101:
            warn('%d ahead 20100101, use 20200101 instead.' % start_date)
            start_date = 20100101
        if end_date > 20200531:
            warn('%d after 20200531, use 20200531 instead.' % end_date)
            end_date = 20200531
        sql_date = '(`trade_date` BETWEEN %d AND %d)' % (start_date, end_date)
        return sql_date

    def __sql_filter(self, filter, table):
        filter['ts_code'] = filter.get('ts_code', [])
        filter['limit'] = filter.get('limit',10)
        keys = list(filter.keys())
        sql_in = []
        sql_between = []
        sql_range = []
        sql_limit = ''
        for index, value in enumerate(filter.values()):
            if type(value) == tuple:
                if len(value) != 2:
                    raise Exception('"%s" expects tuple of length 2, got %s.' % (keys[index], str(value)))
                elif (value[0] in ['<=', '>=', '<', '>','=']) & (type(value[1]) in [int, float]):
                    sql_range.append('(%s %s %d)' % (self.__wrap(keys[index], '`'), value[0], value[1]))
                elif (type(value[0]) in [int, float]) & (type(value[1]) in [int, float]):
                    if keys[index] == 'trade_date':
                        sql_between.append(self.__sql_date(value[0], value[1]))
                    else:
                        sql_between.append('(%s BETWEEN %s AND %s)' %
                                           (self.__wrap(keys[index], '`'), value[0], value[1]))
                else:
                    raise Exception('Expect (int/float, int/float) or (str, int/float) in "%s", got (%s, %s).' %
                                    (keys[index], str(type(value[0])), str(type(value[1]))))
            elif type(value) == list:
                if keys[index] == 'ts_code':
                    sql_in.append(self.__sql_code(value, table))
                else:
                    sql_in.append(
                        '(%s IN (%s))' % (self.__wrap(keys[index], "`"), ','.join([self.__wrap(i) for i in value])))
            elif (type(value) == int) & (keys[index] == 'limit'):
                if value > 0:
                    sql_limit = ' LIMIT %d' % value
                elif value == -1:
                    sql_limit = ''
                else:
                    raise Exception('Do you want {"limit":-1} for unlimited?')
            else:
                raise Exception('Expect tuple, list or int ("limit" only), got %s for "%s".' %
                                (type(value), keys[index]))
        return ' AND '.join(sql_in + sql_between + sql_range) + sql_limit

    def __sql_select(self, select):
        if select:
            return ','.join([self.__wrap(i, "`") for i in select])
        else:
            return '*'

    def __query(func):
        def wrapper(self, select=[], filter={}):
            table = func.__name__
            sql_command = 'SELECT %s FROM %s WHERE %s;' % (
                self.__sql_select(select), table, self.__sql_filter(filter, table))
            print('SQL command: %s' % sql_command)
            try:
                db = self.__conn()
                cursor = db.cursor()
                cursor.execute(sql_command)
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                data = list(map(list, data))
                data = DataFrame(data, columns=columns)
                return data
            except Exception as e:
                print(e)
            finally:
                try:
                    db.close()
                except Exception as e:
                    print(e)

        return wrapper

    @__query
    def stock_daily(self, select: list, filter: dict):
        pass

    @__query
    def fina_indicator(self, select: list, filter: dict):
        pass

    @__query
    def fund_basic(self, select: list, filter: dict):
        pass

    @__query
    def fund_daily(self, select: list, filter: dict):
        pass

    @__query
    def fut_basic(self, select: list, filter: dict):
        pass

    @__query
    def fut_daily(self, select: list, filter: dict):
        pass

    @__query
    def index_basic(self, select: list, filter: dict):
        pass

    @__query
    def index_daily(self, select: list, filter: dict):
        pass

    @__query
    def opt_basic(self, select: list, filter: dict):
        pass

    @__query
    def opt_daily(self, select: list, filter: dict):
        pass

    @__query
    def stock_basic(self, select: list, filter: dict):
        pass

    @__query
    def stock_daily_basic(self, select: list, filter: dict):
        pass

    @__query
    def MACD(self, select: list, filter: dict):
        pass

    @__query
    def trade_cal(self, select: list, filter: dict):
        pass

    @__query
    def shibor(self, select: list, filter: dict):
        pass
