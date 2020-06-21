import traceback
import time, datetime
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine,types, MetaData
from sqlalchemy import Column, Table, Date, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database
from multiprocessing.dummy import Pool as ThreadPool

class Data:
    def __init__(self):
        self.__engine_ts = create_engine('mysql://root:MySQL@193.112.251.160@127.0.0.1:3306/quant?charset=utf8&use_unicode=1',
                         pool_size=20, max_overflow=100)
        self.__pro = ts.pro_api('Your token') 
        if not database_exists(self.__engine_ts.url):
            create_database(self.__engine_ts.url)
        print('DataBase:%s'% database_exists(self.__engine_ts.url))
        self.last = self.read_data('trade_cal',where='',order='cal_date',attrs=['distinct cal_date']).values[-1][0]
        self.__get_new()

    def read_data(self,table,**cmd):
        where, order, attr = '', '', '*'
        if cmd['where']: where='where ' + cmd['where']
        if cmd['order']: order='order by ' + cmd['order']
        if cmd['attrs']: attr = ','.join([str(i) for i in cmd['attrs']])
        sql = '''SELECT %s FROM %s %s %s'''% (attr,table,where,order)
        df = pd.read_sql_query(sql, self.__engine_ts)
        return df
    
    def __add_pk(self,table,*pks):
        pk = ','.join([str(i) for i in pks])
        sql = '''ALTER TABLE `quant`.`%s` 
            ADD PRIMARY KEY (%s);
            '''% (table,pk)
        conn = self.__engine_ts.connect()
        conn.execute(sql)
        conn.close()
        
    def __drop_tb(self,table):
        sql = '''DROP TABLE `quant`.`%s` ;'''% (table)
        conn = self.__engine_ts.connect()
        conn.execute(sql)
        conn.close()
    
    def table_exsit(self,table):
        sql = '''SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema ='quant' and table_name ='%s';'''%(table)
        conn = self.__engine_ts.connect()
        rs = conn.execute(sql).fetchall()
        conn.close()
        return not not rs
    
    def pk_exsit(self,table):
        sql = '''SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE table_schema ='quant' and TABLE_NAME ='%s';'''%(table)
        conn = self.__engine_ts.connect()
        rs = conn.execute(sql).fetchall()
        conn.close()
        return not not rs
    
    def __get_MACD(self,code):
        if self.table_exsit('MACD'):
            s = 'INSERT IGNORE INTO MACD'
        else:
            s = 'CREATE TABLE MACD AS'

        sql = '''set @start_date='20100101'; \
            select @em12:= close from stock_daily where trade_date > @start_date  and ts_code='%s'
            group by trade_date order by trade_date limit 1; \
            select @em26:= close from stock_daily where trade_date > @start_date  and ts_code='%s'
            group by trade_date order by trade_date desc limit 1; \
            set @dea=0; \
            %s \
            SELECT *,(C.DIF-C.DEA)*2 AS MACD \
            FROM ( \
            SELECT B.*, \
            ROUND(B.ema12-B.ema26,3) AS DIF, \
            ROUND(@dea:=2/10*(B.ema12-B.ema26)+8/10*@dea,3) AS DEA \
            FROM ( \
            SELECT A.*, \
            ROUND(@em12:=2/13*A.close+(1-2/13)*@em12,3) as ema12, \
            ROUND(@em26:=2/27*A.close+(1-2/27)*@em26,3) as ema26 \
            FROM ( \
            SELECT ts_code,trade_date,close FROM stock_daily \
            WHERE ts_code = '%s' \
            AND trade_date > @start_date \
            GROUP BY trade_date \
            ORDER BY trade_date \
            ) AS A \
            ) AS B \
            ) AS C;''' % (code,code,s,code)
        print(code)
        conn = self.__engine_ts.connect()
        with conn.begin():
            conn.execute(sql)
        conn.close()
        
        
    def __get_stock_daily(self,code):
        df = ts.pro_bar(ts_code = code,adj='qfq',start_date = '20090101', 
                        ma=[5, 10, 15, 20, 30, 60, 90, 120, 180, 240], asset='E')
        try:
            df.to_sql('stock_daily', self.__engine_ts, index=False, if_exists='append', chunksize=5000,
                      dtype={'ts_code':types.NVARCHAR(length=10),'trade_date':types.INTEGER()})
        except:pass

    
    def __get_daily(self,code,tp):
        if self.table_exsit(tp):
            last = self.read_data(tp,where="ts_code='%s'"%code,order='trade_date',attrs=['distinct trade_date'])
            new = pd.DataFrame()
            if not last.empty:
                new = self.read_data('trade_cal',where="cal_date>'%s'"%last.values[-1][0],order='cal_date',attrs=['distinct cal_date'])
            if not new.empty:
                last = self.read_data(tp,where="ts_code='%s'"%code,order='trade_date',attrs=['distinct trade_date']).values[-1][0]
                new = self.read_data('trade_cal',where="cal_date>'%s'"%last,order='cal_date',attrs=['distinct cal_date']).values[0][0]
            else:
                new = 20100101
        else:
            new = 20100101
        if tp == 'stock_daily_basic':
            df = self.__pro.query('daily_basic', ts_code=code, start_date = str(new),
                           fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,\
                           dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')
        else:
            df = self.__pro.query(tp, ts_code=code, start_date = str(new))
        try:
            df.to_sql(tp, self.__engine_ts, index=False, if_exists='append', chunksize=5000,
                      dtype={'ts_code':types.NVARCHAR(length=10),'trade_date':types.INTEGER()})
            if not self.pk_exsit(tp):
                self.__add_pk(tp,'ts_code','trade_date')
        except Exception as e:
            traceback.print_exc()
        time.sleep(0.1)
    
    def __get_member(self,code):
        df = self.__pro.index_member(index_code=code)
        df.to_sql('index_member', self.__engine_ts, index=False, if_exists='append', chunksize=5000,
                      dtype={'index_code':types.NVARCHAR(length=11),'con_code':types.NVARCHAR(length=11)})
        time.sleep(0.4)

    def __get_index(self,market):
        df = self.__pro.query('index_basic', market=market,
                       fields='ts_code,name,fullname,market,publisher,category,base_date,base_point,list_date,exp_date')
        df.to_sql('index_basic', self.__engine_ts, index=False, if_exists='append', chunksize=5000,
                  dtype={'ts_code':types.NVARCHAR(length=20)})
        
    def __get_fut(self,market):
        df = self.__pro.query('fut_basic', exchange=market)
        df.to_sql('fut_basic', self.__engine_ts, index=False, if_exists='append', chunksize=5000,
                  dtype={'ts_code':types.NVARCHAR(length=20)})
   
    def __get_opt(self,market):
        df = self.__pro.query('opt_basic', exchange=market)
        df.to_sql('opt_basic', self.__engine_ts, index=False, if_exists='append', chunksize=5000,
                  dtype={'ts_code':types.NVARCHAR(length=20)})
        
    def __get_fina_indicator(self,code):
        if self.table_exsit('fina_indicator'):
            s = 'INSERT IGNORE INTO fina_indicator'
            last = self.read_data('fina_indicator',where="ts_code='%s'"%code,order='end_date',attrs=['distinct end_date'])
            new = pd.DataFrame()
            if not last.empty:
                new = self.read_data('trade_cal',where="cal_date>'%s'"%last.values[-1][0],order='cal_date',attrs=['distinct cal_date'])
            if not new.empty:
                last = self.read_data('fina_indicator',where="ts_code='%s'"%code,order='end_date',attrs=['distinct end_date']).values[-1][0]
                new = self.read_data('trade_cal',where="cal_date>'%s'"%last,order='cal_date',attrs=['distinct cal_date']).values[0][0]
            else:
                new = 20100101
        else:
            new = 20100101
            s = 'CREATE TABLE fina_indicator AS'
        df = self.__pro.query('fina_indicator',ts_code = code,start_date = str(new),update_flag=1,
                      fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,\
                      surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,\
                      cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,\
                      valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,\
                      interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,\
                      retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,\
                      grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,\
                      finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,\
                      roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,\
                      salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,\
                      ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,\
                      longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,\
                      tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,\
                      ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,\
                      profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,\
                      cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,\
                      q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,\
                      q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,\
                      q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,\
                      q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,\
                      netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,\
                      q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,\
                      q_netprofit_qoq,equity_yoy,rd_exp')                
        time.sleep(0.5)
        try:
            df.to_sql('fina_indicator2', self.__engine_ts, index=False, if_exists='replace', chunksize=5000,
                      dtype={'ts_code':types.NVARCHAR(length=25),'end_date':types.INTEGER()})

            sql = '''%s \
                SELECT * FROM fina_indicator2;'''% s
            conn = self.__engine_ts.connect()
            conn.execute(sql)
            conn.close()
            if not self.pk_exsit(tp):
                try:
                    self.__add_pk('fina_indicator','ts_code','end_date')
                except: pass     
        except: pass
     
                
    def __get_new(self,start_date='20100101'):        
        df = self.__pro.trade_cal(exchange='SSE', is_open='1', 
                                    start_date=start_date,
                                    end_date=time.strftime("%Y%m%d", time.localtime()),
                                    fields='cal_date')
        df.to_sql('trade_cal', self.__engine_ts, index=False, if_exists='replace', chunksize=5000,
                  dtype={'cal_date':types.Integer})
        self.__add_pk('trade_cal','cal_date')
        
        df = self.__pro.query('shibor',start_date = start_date)
        df.to_sql('shibor', self.__engine_ts, index=False, if_exists='replace', chunksize=5000,
                          dtype={'date':types.INTEGER()})
        self.__add_pk('shibor','date')
        
        df = self.__pro.index_classify(fields='index_code,industry_name,level,industry_code,src')
        df.to_sql('index_classify', self.__engine_ts, index=False, if_exists='replace', chunksize=5000,
                      dtype={'index_code':types.NVARCHAR(length=20)})
        self.__add_pk('index_classify','index_code')
        
        df = self.__pro.query('stock_basic', exchange='', list_status='L', 
               fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
        df.to_sql('stock_basic', self.__engine_ts, index=False, if_exists='replace', chunksize=5000,
                  dtype={'ts_code':types.NVARCHAR(length=10)})
        self.__add_pk('stock_basic','ts_code')
        
        df = self.__pro.query('fund_basic',market='O')
        df.to_sql('fund_basic', self.__engine_ts, index=False, if_exists='replace', chunksize=5000,
                  dtype={'ts_code':types.NVARCHAR(length=20)})
        self.__add_pk('fund_basic','ts_code')
        
        if self.table_exsit('index_member'):
            self.__drop_tb('index_member')
        codes['index_code'].apply(self.__get_member)
        self.__add_pk('index_member','index_code','con_code')
        codes = self.read_data('index_classify',where='',order='',attrs=['index_code'])
        
        markets = pd.Series(['MSCI','CSI','SSE','SZSE','CICC','SW','OTH'])
        if self.table_exsit('index_basic'):
            self.__drop_tb('index_basic')
        markets.apply(self.__get_index)
        self.__add_pk('index_basic','ts_code')
        
        exchanges = pd.Series(['CFFEX','DCE','CZCE','SHFE','INE'])
        if self.table_exsit('fut_basic'):
            self.__drop_tb('fut_basic')
        exchanges.apply(self.__get_fut)
        self.__add_pk('fut_basic','ts_code')
        if self.table_exsit('opt_basic'):
            self.__drop_tb('opt_basic')
        exchanges.apply(self.__get_opt)
        self.__add_pk('opt_basic','ts_code')
        
        codes = self.read_data('stock_basic',where='',order='',attrs=['ts_code'])
        
        codes['ts_code'].apply(self.__get_fina_indicator)
        self.__drop_tb('fina_indicator2')
        codes['ts_code'].apply(self.__get_daily,tp='stock_daily_basic')
        
        if self.table_exsit('stock_daily'):
            self.__drop_tb('stock_daily')
        pool = ThreadPool(5)
        conn = self.__engine_ts.connect()
        pool.map(self.__get_stock_daily,codes['ts_code'])
        pool.close()
        pool.join()
        self.__add_pk('stock_daily','ts_code','trade_date') 
        
        if self.table_exsit('MACD'):
            self.__drop_tb('MACD')
        pool = ThreadPool(50)
        conn = self.__engine_ts.connect()
        pool.map(self.__get_MACD,codes['ts_code'])
        pool.close()
        pool.join()
        self.__add_pk('MACD','ts_code','trade_date')
        
        codes = self.read_data('fund_basic',where='',order='',attrs=['ts_code'])
        codes['ts_code'].apply(self.__get_daily,tp='fund_daily')
        codes = self.read_data('fut_basic',where='',order='',attrs=['ts_code'])
        codes['ts_code'].apply(self.__get_daily,tp='fut_daily')
        codes = self.read_data('opt_basic',where='',order='',attrs=['ts_code'])
        codes['ts_code'].apply(self.__get_daily,tp='opt_daily')
        codes = self.read_data('index_basic',where='',order='',attrs=['ts_code'])
        codes['ts_code'].apply(self.__get_daily,tp='index_daily')
        

print('LAST UPDATE: ',Data().last)
