import akshare as ak
import pandas as pd
import datetime
import os
import json

# ----------------------------- 辅助函数：获取概念板块映射（新版接口） -----------------------------
def get_concept_map(use_cache=True, cache_file='concept_cache.json'):
    """
    获取股票代码到概念板块名称的映射字典。
    使用 akshare 最新版接口：stock_board_concept_name_em / stock_board_concept_cons_em
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(script_dir, cache_file)

    if use_cache and os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    print("正在获取概念板块数据（首次运行较慢，请耐心等待）...")
    concept_dict = {}

    try:
        # 1. 获取所有概念板块列表
        concept_board = ak.stock_board_concept_name_em()
        total = len(concept_board)
        # 打印列名（便于调试，可删除）
        print(f"概念板块列表列名：{concept_board.columns.tolist()}")
        for idx, row in concept_board.iterrows():
            # 使用正确的列名（根据实际打印结果，应为 '板块名称' 和 '板块代码'）
            concept_name = row['板块名称']
            concept_code = row['板块代码']
            try:
                # 2. 获取该概念下的成分股
                cons_df = ak.stock_board_concept_cons_em(symbol=concept_code)
                # 提取股票代码（统一为6位字符串）
                stock_codes = cons_df['代码'].astype(str).str.zfill(6).tolist()
                for code in stock_codes:
                    if code in concept_dict:
                        concept_dict[code].append(concept_name)
                    else:
                        concept_dict[code] = [concept_name]
                # 显示进度（每50个概念打印一次）
                if (idx + 1) % 50 == 0:
                    print(f"  已处理 {idx+1}/{total} 个概念...")
            except Exception as e:
                print(f"  跳过概念 {concept_name}（{concept_code}），原因：{e}")
                continue
    except Exception as e:
        print(f"获取概念板块列表失败: {e}")
        return {}

    if use_cache:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(concept_dict, f, ensure_ascii=False, indent=2)

    print(f"概念板块数据加载完成，共包含 {len(concept_dict)} 只股票的概念信息。")
    return concept_dict

# ----------------------------- 获取实时行情 -----------------------------
def get_stock_pool():
    spot_df = ak.stock_zh_a_spot_em()
    spot_df.columns = [
        '序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额',
        '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态',
        '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅'
    ]
    # 处理涨跌幅、量比、换手率（兼容数字和字符串）
    if spot_df['涨跌幅'].dtype == 'object':
        spot_df['涨跌幅'] = spot_df['涨跌幅'].str.replace('%', '').astype(float)
    else:
        spot_df['涨跌幅'] = spot_df['涨跌幅'].astype(float)

    if spot_df['量比'].dtype == 'object':
        spot_df['量比'] = spot_df['量比'].str.replace('%', '').astype(float)
    else:
        spot_df['量比'] = spot_df['量比'].astype(float)

    if spot_df['换手率'].dtype == 'object':
        spot_df['换手率'] = spot_df['换手率'].str.replace('%', '').astype(float)
    else:
        spot_df['换手率'] = spot_df['换手率'].astype(float)

    spot_df['流通市值(亿)'] = spot_df['流通市值'] / 1e8
    return spot_df

# ----------------------------- 获取个股历史K线 -----------------------------
def get_hist_data(symbol):
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=120)
    try:
        hist = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                   start_date=start_date.strftime('%Y%m%d'),
                                   end_date=end_date.strftime('%Y%m%d'),
                                   adjust="qfq")
        if hist.empty:
            return None
        hist['MA5'] = hist['收盘'].rolling(5).mean()
        hist['MA10'] = hist['收盘'].rolling(10).mean()
        hist['MA20'] = hist['收盘'].rolling(20).mean()
        hist['MA60'] = hist['收盘'].rolling(60).mean()
        return hist
    except:
        return None

def check_volume_pattern(hist):
    if len(hist) < 5:
        return False
    recent_vol = hist['成交量'].tail(5).values
    vol_ma5 = recent_vol.mean()
    if recent_vol[-3] < recent_vol[-2] < recent_vol[-1]:
        if recent_vol[-1] > vol_ma5 * 0.8:
            return True
    return False

def check_ma_trend(hist):
    if len(hist) < 60:
        return False
    latest = hist.iloc[-1]
    prev_ma60 = hist.iloc[-2]['MA60'] if len(hist) > 1 else latest['MA60']
    cond1 = latest['MA5'] > latest['MA10'] > latest['MA20'] > latest['MA60']
    cond2 = latest['收盘'] > latest['MA20']
    cond3 = latest['MA60'] > prev_ma60
    return cond1 and cond2 and cond3

def filter_stocks():
    print("正在获取实时行情...")
    spot = get_stock_pool()

    # 筛选条件（市值已放宽至20-500亿）
    cond1 = (spot['涨跌幅'] >= 3) & (spot['涨跌幅'] <= 5)
    cond2 = spot['量比'] > 1
    cond3 = (spot['换手率'] >= 5) & (spot['换手率'] <= 10)
    cond4 = (spot['流通市值(亿)'] >= 20) & (spot['流通市值(亿)'] <= 500)
    cond_price = spot['最新价'] >= 6

    mask = cond1 & cond2 & cond3 & cond4 & cond_price
    candidates = spot.loc[mask].copy()

    print(f"初步筛选后获得 {len(candidates)} 只股票")

    # 获取概念板块映射（使用缓存）
    concept_map = get_concept_map(use_cache=True)

    final_list = []
    for idx, row in candidates.iterrows():
        symbol = row['代码']
        symbol_clean = symbol.split('.')[0].zfill(6)  # 标准化代码
        hist = get_hist_data(symbol)
        if hist is None:
            continue

        if not check_volume_pattern(hist):
            continue

        if not check_ma_trend(hist):
            continue

        latest = hist.iloc[-1]
        if latest['收盘'] > latest['MA20'] * 1.2:
            continue

        # 获取概念板块
        concept_list = concept_map.get(symbol_clean, [])
        concepts = '、'.join(concept_list) if concept_list else '无'

        final_list.append({
            '代码': symbol,
            '名称': row['名称'],
            '最新价': row['最新价'],
            '涨跌幅(%)': row['涨跌幅'],
            '量比': row['量比'],
            '换手率(%)': row['换手率'],
            '流通市值(亿)': round(row['流通市值(亿)'], 2),
            '概念板块': concepts
        })

    result = pd.DataFrame(final_list)
    return result

if __name__ == "__main__":
    df = filter_stocks()
    if df.empty:
        print("今日无符合条件的股票。")
    else:
        print("\n=== 最终筛选结果 ===")
        print(df.to_string(index=False))
        output_file = "selected_stocks.csv"
        df.to_csv(output_file, index=False, encoding='utf_8_sig')
        print(f"\n结果已保存至 {output_file}")