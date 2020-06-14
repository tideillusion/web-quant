from functools import partial
from os.path import join, dirname

import pandas as pd
import talib
from Query import Query
from bokeh.io import curdoc
from bokeh.layouts import column, layout, row
from bokeh.models import Button, Select, TextInput, ColumnDataSource, DataTable, TableColumn, Div, Tabs, Panel, \
    CustomJS, NumeralTickFormatter, Range1d, HoverTool, CheckboxGroup, Span, RadioGroup, Spinner
from bokeh.models.widgets import DatePicker
from bokeh.plotting import figure
from numpy import array


###################
## Plot Function ##
###################

def quant_plot(df, name, signal=None, param=None):
    # Select the datetime format for the x axis depending on the timeframe
    xaxis_dt_format = '%Y/%m/%d'

    fig = figure(sizing_mode='stretch_both',
                 tools="xpan,xwheel_zoom,tap,reset,box_select,save",
                 active_drag='xpan',
                 active_scroll='xwheel_zoom',
                 x_axis_type='linear',
                 x_range=Range1d(df.index[0] - .5, df.index[-1] + .5, bounds="auto"),
                 title=name,
                 width=1150
                 )
    fig.yaxis[0].formatter = NumeralTickFormatter(format="5.3f")
    inc = df.close > df.open
    dec = ~inc

    # Colour scheme for increasing and descending candles
    INCREASING_COLOR = '#ff0000'
    DECREASING_COLOR = '#00ff00'
    fig.background_fill_color = '#01171f'
    fig.background_fill_alpha = 0.8

    width = 0.5
    inc_source = ColumnDataSource(data=dict(
        x1=df.index[inc],
        top1=df.open[inc],
        bottom1=df.close[inc],
        high1=df.high[inc],
        low1=df.low[inc],
        trade_date1=df.trade_date[inc]
    ))

    dec_source = ColumnDataSource(data=dict(
        x2=df.index[dec],
        top2=df.open[dec],
        bottom2=df.close[dec],
        high2=df.high[dec],
        low2=df.low[dec],
        trade_date2=df.trade_date[dec]
    ))
    # Plot candles
    # high and low
    fig.segment(x0='x1', y0='high1', x1='x1', y1='low1', source=inc_source, color=INCREASING_COLOR)
    fig.segment(x0='x2', y0='high2', x1='x2', y1='low2', source=dec_source, color=DECREASING_COLOR)

    # open and close
    r1 = fig.vbar(x='x1', width=width, top='top1', bottom='bottom1', source=inc_source,
                  fill_color=INCREASING_COLOR, line_color="black")
    r2 = fig.vbar(x='x2', width=width, top='top2', bottom='bottom2', source=dec_source,
                  fill_color=DECREASING_COLOR, line_color="black")

    # Add on extra lines (e.g. moving averages) here
    # fig.line(df.index, <your data>)
    ############
    # MA lines #
    ############
    if 'ma5' in df.columns:
        fig.line(df.index, df.ma5.values, line_color='white', legend_label='MA5')
    if 'ma10' in df.columns:
        fig.line(df.index, df.ma10.values, line_color='yellow', legend_label='MA10')
    if 'ma20' in df.columns:
        fig.line(df.index, df.ma20.values, line_color='purple', legend_label='MA20')
    if 'ma60' in df.columns:
        fig.line(df.index, df.ma60.values, line_color='green', legend_label='MA60')
    if 'ma120' in df.columns:
        fig.line(df.index, df.ma120.values, line_color='red', legend_label='MA120')
    if len(df.columns) > 6:
        fig.legend.background_fill_alpha = 0.5

    # Add on a vertical line to indicate a trading signal here
    # vline = Span(location=df.index[-<your index>, dimension='height',
    #              line_color="green", line_width=2)
    # fig.renderers.extend([vline])
    ################
    # Trade Signal #
    ################
    cum_fig = None
    if signal in quant_signal_menu:
        fig.xgrid.grid_line_color = None
        fig.ygrid.grid_line_color = None

        if signal == 'MACD':
            _, holding_signal, _ = talib.MACD(array(df.close), param[0], param[1], param[2])
            quick = talib.SMA(array(df.close), param[0])
            slow = talib.SMA(array(df.close), param[1])
            fig.line(df.index, quick, line_color='white', legend_label='MA%d' % param[0])
            fig.line(df.index, slow, line_color='yellow', legend_label='MA%d' % param[1])
            fig.legend.background_fill_alpha = 0.5
            holding_signal = (holding_signal > 0)
            trade_signal = []
            flag = False
            for i in holding_signal:
                if i == flag:
                    trade_signal.append(0)
                else:
                    trade_signal.append(int(i) - .5)
                flag = i
            buy = array(trade_signal) > 0
            sell = array(trade_signal) < 0

        elif signal == 'BBANDS':
            high, middle, low = talib.BBANDS(array(df.close), timeperiod=param[0], nbdevup=param[1], nbdevdn=param[2],
                                             matype=0)
            fig.line(df.index, high, line_color='yellow', legend_label='+%1.1f\u03C3' % param[1])
            fig.line(df.index, middle, line_color='white', legend_label='MA%d' % param[0])
            fig.line(df.index, low, line_color='purple', legend_label='-%1.1f\u03C3' % param[2])
            fig.legend.background_fill_alpha = 0.5
            flag = False
            holding = False
            buy = []
            sell = []
            for h, m, l in zip(high, df.close, low):
                if (h > m) & (m > l):
                    flag = True
                if flag:
                    if h < m:
                        if holding:
                            buy.append(False)
                        else:
                            buy.append(True)
                            holding = True
                        sell.append(False)
                        flag = False
                        continue
                    if m < l:
                        if holding:
                            sell.append(True)
                            holding = False
                        else:
                            sell.append(False)
                        buy.append(False)
                        flag = False
                        continue
                buy.append(False)
                sell.append(False)
            buy = array(buy)
            sell = array(sell)

        ###############
        # Trade Lines #
        ###############
        buy_line = [Span(location=i, dimension='height', line_color="red", line_dash='dashed', line_width=2) for i in
                    df.index[buy]]
        sell_line = [Span(location=i, dimension='height', line_color="green", line_dash='dashed', line_width=2) for i in
                     df.index[sell]]
        fig.renderers.extend(buy_line + sell_line)
        cum_return = []
        flag = False
        for index, pair in enumerate(zip(buy, sell)):
            if pair[0]:
                cum_return.append(0)
                flag = True
            elif pair[1]:
                cum_return.append(df.pct_chg[index])
                flag = False
            else:
                if flag:
                    cum_return.append(df.pct_chg[index])
                else:
                    cum_return.append(0)
        cum_return = (array(cum_return) / 100 + 1).cumprod()
        cum_fig = figure(sizing_mode='stretch_both',
                         tools="pan,wheel_zoom,tap,reset,box_select,save",
                         active_drag='pan',
                         active_scroll='wheel_zoom',
                         x_axis_type='linear',
                         title='累计回报率',
                         x_range=Range1d(df.index[0] - .5, df.index[-1] + .5, bounds="auto"),
                         height=220,
                         margin=(20, 0, 0, 0))
        cum_fig.line(df.index, cum_return, color='blue')
        cum_fig.xaxis.major_label_overrides = {
            i: date.strftime(xaxis_dt_format) for i, date in
            enumerate(pd.to_datetime(df["trade_date"], format='%Y%m%d'))
        }

    # Add date labels to x axis
    fig.xaxis.major_label_overrides = {
        i: date.strftime(xaxis_dt_format) for i, date in enumerate(pd.to_datetime(df["trade_date"], format='%Y%m%d'))
    }

    # Set up the hover tooltip to display some useful data
    fig.add_tools(HoverTool(
        renderers=[r1],
        tooltips=[
            ("open", "@top1"),
            ("high", "@high1"),
            ("low", "@low1"),
            ("close", "@bottom1"),
            ("trade_date", "@trade_date1"),
        ],
        formatters={
            'trade_date1': 'datetime',
        }))

    fig.add_tools(HoverTool(
        renderers=[r2],
        tooltips=[
            ("open", "@top2"),
            ("high", "@high2"),
            ("low", "@low2"),
            ("close", "@bottom2"),
            ("trade_date", "@trade_date2")
        ],
        formatters={
            'trade_date2': 'datetime'
        }))

    # JavaScript callback function to automatically zoom the Y axis to
    # view the data properly
    source = ColumnDataSource({'Index': df.index, 'high': df.high, 'low': df.low})
    callback = CustomJS(args={'y_range': fig.y_range, 'source': source}, code='''
        clearTimeout(window._autoscale_timeout);
        var Index = source.data.Index,
            low = source.data.low,
            high = source.data.high,
            start = cb_obj.start,
            end = cb_obj.end,
            min = Infinity,
            max = -Infinity;
        for (var i=0; i < Index.length; ++i) {
            if (start <= Index[i] && Index[i] <= end) {
                max = Math.max(high[i], max);
                min = Math.min(low[i], min);
            }
        }
        var pad = (max - min) * .05;
        window._autoscale_timeout = setTimeout(function() {
            y_range.start = min - pad;
            y_range.end = max + pad;
        });
    ''')
    # Finalise the figure
    fig.x_range.js_event_callbacks = {'callback': [callback]}
    return fig, cum_fig


######################
## Global Variable  ##
######################
# Info
q = Query()
info_table_dict = q.get_tables()
info_menu = list(info_table_dict.values())
info_menu_dict = dict(zip(info_menu, list(info_table_dict.keys())))  # Map from Chinese labels to English names of table
info_table_func = q.stock_basic
info_filter = {'limit': 100, 'trade_date': (20100101, 20200531)}
info_date_available = False
info_div_date_func = lambda x: x[:4] + '/' + x[4:6] + '/' + x[6:]  # Print date
# Quant
quant_filter = {'trade_date': (20200101, 20200531), 'ts_code': ['000001']}
quant_columns = ['trade_date', 'high', 'low', 'open', 'close', 'pct_chg']
quant_ma = {x: y for x, y in list(enumerate(['ma5', 'ma10', 'ma20', 'ma60', 'ma120']))}
quant_signal_menu = ['MACD', 'BBANDS']

#############
## Widgets ##
#############
# Info
info_start_date = DatePicker(title="起始日期", min_date='2010-01-01', max_date='2020-05-31', value='2010-01-01', width=140,
                             visible=False)
info_end_date = DatePicker(title="结束日期", min_date='2010-01-01', max_date='2020-05-31', value='2020-05-31', width=140,
                           visible=False)
info_select = Select(title="请选择", value="股票列表", options=info_menu, width=150)
info_confirm = Button(label="查询", button_type="success", width=100)
info_text = TextInput(title="TS代码（多个代码请用英文,分隔）", width=150)
info_div = Div(text='请选择筛选条件，并按「查询」按钮确认。', width=600)
info_df = DataTable(source=ColumnDataSource(pd.DataFrame()), width=1150, height=500)
info_download = Button(label='下载', button_type='success', width=100, visible=False)
info_limit = Spinner(title='限制条数（-1为无限制）', width=150, value=100, visible=False, low=-1)
info_token = TextInput(title='Token', width=150)
# Quant
quant_start_date = DatePicker(title="起始日期", min_date='2010-01-01', max_date='2020-05-31', value='2020-01-01', width=140)
quant_end_date = DatePicker(title="结束日期", min_date='2010-01-01', max_date='2020-05-31', value='2020-05-31', width=140)
quant_text = TextInput(title="TS代码", width=150, value='000001')
quant_confirm = Button(label="刷新", button_type="success", width=100)
quant_option = CheckboxGroup(
    labels=["5日均线", "10日均线", "20日均线", '60日均线', '120日均线'], width=150, margin=(20, 0, 0, 0))
quant_select = Select(title='交易指标', value='无指标', options=['无指标', 'MACD', 'BBANDS'], width=150)
quant_signal = None
quant_param_MACD = [26, 12, 9]
quant_param_BBANDS = [5, 2, 2]
quant_MACD_long = Spinner(title='快变指数移动平均天数', value=26, width=150, visible=False, low=0)
quant_MACD_short = Spinner(title='慢变指数移动平均天数', value=12, width=150, visible=False, low=0)
quant_MACD_DEM = Spinner(title='差离平均天数', value=9, width=150, visible=False, low=0)
return_plot = figure(visible=False, height=220, margin=(20, 0, 0, 0))
quant_div = Div(text='', width=150)
quant_radio = RadioGroup(labels=["股票", "场内基金"], active=0, inline=True)
quant_BBANDS_timeperiod = Spinner(title='移动平均天数', value=5, width=150, visible=False, low=0)
quant_BBANDS_nbdevup = Spinner(title='上轨标准差（倍）', value=2, width=150, visible=False, low=0, step=.1)
quant_BBANDS_nbdevdn = Spinner(title='下轨标准差（倍）', value=2, width=150, visible=False, low=0, step=.1)


###############
## Callback  ##
###############

def textChange(attr, old, new: str, name):
    if name == 'info':
        info_filter['ts_code'] = new.split(',')
        divChange('info')
    elif name == 'quant':
        quant_filter['ts_code'] = [new]
    elif name == 'MACD_long':
        quant_param_MACD[0] = int(new)
    elif name == 'MACD_short':
        quant_param_MACD[1] = int(new)
    elif name == 'MACD_DEM':
        quant_param_MACD[2] = int(new)
    elif name == 'BBANDS_timeperiod':
        quant_param_BBANDS[0] = int(new)
    elif name == 'BBANDS_nbdevup':
        quant_param_BBANDS[1] = float(new)
    elif name == 'BBANDS_nbdevdn':
        quant_param_BBANDS[2] = float(new)


def selectChange(attr, old, new: str, name=None):
    if name == 'info':
        global info_table_func, info_date_available, quant_signal
        info_table_func = getattr(q, info_menu_dict[new])
        available_filter = list(q.search(table=info_menu_dict[new]).keys())
        if 'trade_date' in available_filter:
            info_date_available = True
            info_start_date.visible = True
            info_end_date.visible = True
        else:
            info_date_available = False
            info_start_date.visible = False
            info_end_date.visible = False
        divChange('info')
    elif name == 'quant':
        quant_signal = new
        return_plot.visible = False
        quant_MACD_long.visible = False
        quant_MACD_short.visible = False
        quant_MACD_DEM.visible = False
        quant_BBANDS_timeperiod.visible = False
        quant_BBANDS_nbdevup.visible = False
        quant_BBANDS_nbdevdn.visible = False
        if quant_signal in quant_signal_menu:
            return_plot.visible = True
            if quant_signal == 'MACD':
                quant_MACD_long.visible = True
                quant_MACD_short.visible = True
                quant_MACD_DEM.visible = True
            elif quant_signal == 'BBANDS':
                quant_BBANDS_timeperiod.visible = True
                quant_BBANDS_nbdevup.visible = True
                quant_BBANDS_nbdevdn.visible = True


def dateChange(attr, old, new, name):
    if name == 'info':
        info_filter['trade_date'] = (int(''.join(info_start_date.value.split('-'))),
                                     int(''.join(info_end_date.value.split('-'))))
        info_start_date.max_date = info_end_date.value
        info_end_date.min_date = info_start_date.value
        divChange('info')
    elif name == 'quant':
        quant_filter['trade_date'] = (int(''.join(quant_start_date.value.split('-'))),
                                      int(''.join(quant_end_date.value.split('-'))))
        quant_start_date.max_date = quant_end_date.value
        quant_end_date.min_date = quant_start_date.value


def divChange(status, tab='info'):
    if tab == 'info':
        if status == 'info':
            if info_filter['limit'] == -1:
                info_div.text = '<b> 上限：无限制 </b>'
            else:
                info_div.text = '<b> 上限：%s条 </b>' % info_filter['limit']
            if info_start_date.visible:
                info_div.text += '<br><b> 日期：%s至%s </b>' % (info_div_date_func(str(info_filter['trade_date'][0])),
                                                            info_div_date_func(str(info_filter['trade_date'][1])))
            if info_filter.get('ts_code', False):
                info_div.text += '<br><b> TS代码：%s </b>' % ','.join(info_filter['ts_code'])

        elif status == 'end':
            info_div.text = '<b> 查询完成！ </b>'
        else:
            info_div.text = '<b> %s </b>' % status
    elif tab == 'quant':
        quant_div.text = '<b> %s </b>' % status


def dfChange():
    try:
        if (not info_date_available) & bool(info_filter.get('trade_date', 0)):
            info_filter_without_date = info_filter.copy()
            del info_filter_without_date['trade_date']
            df = info_table_func(filter=info_filter_without_date)
        else:
            df = info_table_func(filter=info_filter)
        columns = []
        trans_dict = q.search(table=info_menu_dict[info_select.value])
        for i in trans_dict.keys():
            columns.append(TableColumn(field=i, title=trans_dict[i]))
        info_df.source.data = ColumnDataSource.from_df(df)
        info_df.columns = columns
        divChange('end')
    except Exception as e:
        divChange(e)


def tokenChange(attr, old, new):
    if new == 'fintech':
        info_download.visible = True
        info_limit.visible = True
    else:
        info_download.visible = False
        info_limit.visible = False


def limitChange(attr, old, new):
    try:
        if info_token.value == 'fintech':
            pass
        else:
            new = min(100, abs(int(new)))
        info_filter['limit'] = int(new)
        divChange('info')
    except Exception as e:
        divChange(e)


def loading(tab):
    if tab == 'info':
        divChange('查询中，请稍候...')
        curdoc().add_next_tick_callback(dfChange)
    elif tab == 'quant':
        divChange('绘制中，请稍候...', tab)
        curdoc().add_next_tick_callback(plotChange)


def plotChange():
    try:
        if quant_radio.active == 0:
            df = q.stock_daily(
                filter={'limit': -1, 'trade_date': quant_filter['trade_date'], 'ts_code': quant_filter['ts_code']},
                select=quant_columns)
            if not df.size:
                divChange('没有符合条件的结果，请检查TS代码或修改日期', tab='quant')
                return

            name = ' '.join(list(q.stock_basic(filter={'limit': 1, 'ts_code': quant_filter['ts_code']},
                                               select=['ts_code', 'name', 'area', 'industry']).values[0]))
        elif quant_radio.active == 1:
            df = q.fund_daily(
                filter={'limit': -1, 'trade_date': quant_filter['trade_date'], 'ts_code': quant_filter['ts_code']},
                select=quant_columns)
            if not df.size:
                divChange('没有符合条件的结果，请检查TS代码或修改日期', tab='quant')
                return
            name = ' '.join(list(q.fund_basic(filter={'limit': 1, 'ts_code': quant_filter['ts_code']},
                                              select=['ts_code', 'name', 'fund_type']).values[0]))

        if quant_signal == 'MACD':
            k_plot, return_plot = quant_plot(df, name, quant_signal, quant_param_MACD)
            quant_return_plot.children[:] = [return_plot]
        elif quant_signal == 'BBANDS':
            k_plot, return_plot = quant_plot(df, name, quant_signal, quant_param_BBANDS)
            quant_return_plot.children[:] = [return_plot]
        else:
            k_plot, return_plot = quant_plot(df, name, quant_signal, None)
        quant_k_plot.children[:] = [k_plot]
        divChange(status='绘制完成！', tab='quant')
    except Exception as e:
        divChange(e, tab='quant')


def maChange(attr, old, new):
    global quant_columns
    quant_columns = ['trade_date', 'high', 'low', 'open', 'close', 'pct_chg'] + [quant_ma[i] for i in new]


def radioChange(attr, old, new):
    if new:
        maChange(None, None, [])
        quant_option.active = []
        quant_option.disabled = True
    else:
        quant_option.disabled = False


###############
##   Event   ##
###############
# Info
info_text.on_change('value', partial(textChange, name='info'))
info_select.on_change('value', partial(selectChange, name='info'))
info_confirm.on_click(partial(loading, tab='info'))
info_start_date.on_change('value', partial(dateChange, name='info'))
info_end_date.on_change('value', partial(dateChange, name='info'))
info_download.js_on_click(CustomJS(args=dict(source=info_df.source),
                                   code=open(join(dirname(__file__), "download.js")).read()))
info_token.on_change('value', tokenChange)
info_limit.on_change('value', limitChange)
# Quant
quant_text.on_change('value', partial(textChange, name='quant'))
quant_start_date.on_change('value', partial(dateChange, name='quant'))
quant_end_date.on_change('value', partial(dateChange, name='quant'))
quant_confirm.on_click(partial(loading, tab='quant'))
quant_option.on_change('active', maChange)
quant_select.on_change('value', partial(selectChange, name='quant'))
quant_MACD_DEM.on_change('value', partial(textChange, name='MACD_DEM'))
quant_MACD_long.on_change('value', partial(textChange, name='MACD_long'))
quant_MACD_short.on_change('value', partial(textChange, name='MACD_short'))
quant_radio.on_change('active', radioChange)
quant_BBANDS_timeperiod.on_change('value', partial(textChange, name='BBANDS_timeperiod'))
quant_BBANDS_nbdevup.on_change('value', partial(textChange, name='BBANDS_nbdevup'))
quant_BBANDS_nbdevdn.on_change('value', partial(textChange, name='BBANDS_nbdevdn'))

#############
## Layout  ##
#############
# Info
info_layout_column = column(info_select, info_start_date, info_end_date, info_text, info_limit, info_confirm, width=500)

info_tab = Panel(child=layout([info_layout_column, column(info_div, info_token, info_download)],
                              [info_df]), title='信息查询')
# Quant
quant_k_plot = layout(children=[figure(width=1150, height=429)])
quant_return_plot = layout(children=[return_plot])
quant_tab = Panel(child=layout([row(column(quant_start_date,
                                           quant_end_date,
                                           quant_radio,
                                           quant_text,
                                           quant_confirm, width=200), column(quant_option,
                                                                             quant_div), column(quant_select,
                                                                                                quant_MACD_long,
                                                                                                quant_MACD_short,
                                                                                                quant_MACD_DEM,
                                                                                                quant_BBANDS_timeperiod,
                                                                                                quant_BBANDS_nbdevup,
                                                                                                quant_BBANDS_nbdevdn,
                                                                                                width=200),
                                    quant_return_plot),
                                quant_k_plot]), title='交易策略')
# Tabs
tabs = Tabs(tabs=[info_tab, quant_tab])
curdoc().add_root(tabs)
