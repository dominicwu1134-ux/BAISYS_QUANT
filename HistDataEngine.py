import os
import akshare as ak
import pandas as pd
import datetime
import time
from sqlalchemy import create_engine, text
import tushare as ts
from concurrent.futures import ThreadPoolExecutor, as_completed


class StockSyncEngine:
    DB_URL = "postgresql+psycopg2://postgres:025874yan@127.0.0.1:5432/Corenews"

    # >>> 修改点 1: 增加 base_data_dir 参数
    def __init__(self, db_url=DB_URL, base_data_dir=None):
        self.token = "f4422b90a91c02d7dc68dd24f066988064d7307790f200243822cac3"  # 更新为你的 Tushare Token
        self.db = create_engine(db_url)
        self.global_start = "20230101"
        self.today = datetime.datetime.now().strftime("%Y%m%d")
        # 定义今天的日期对象，Timestamp类型，带00:00:00时间，并标准化
        self.today_dt = pd.to_datetime(self.today).normalize()
        self._cached_calendar = None

        # >>> 修改点 2: 将 base_data_dir 作为文件存储的基础目录
        self.base_data_dir = base_data_dir if base_data_dir else os.path.expanduser('~')
        # 确保这个基础目录存在，如果不存在则创建
        os.makedirs(self.base_data_dir, exist_ok=True)

        calendar_filename = f"tradeCalendar_{self.today}.txt"
        # >>> 修改点 3: 正确构建日历文件路径 (目录 + 文件名)
        self.calendar_file_path = os.path.join(self.base_data_dir, calendar_filename)

    def get_trade_calendar_from_akshare(self):
        # 1. 检查内存缓存
        if self._cached_calendar is not None:
            print(f"[DEBUG] 从内存缓存获取交易日历。")
            return self._cached_calendar

        # 2. 检查本地文件缓存
        if os.path.exists(self.calendar_file_path):
            try:
                print(f"[INFO] 从本地文件加载交易日历: {self.calendar_file_path}")
                df = pd.read_csv(self.calendar_file_path, parse_dates=['date'])

                # 确保df['date']是Timestamp，并进行normalize()
                # 显式转换为Timestamp并normalize，确保类型一致性
                df['date'] = pd.to_datetime(df['date']).dt.normalize()

                start_dt = pd.to_datetime(self.global_start).normalize()
                end_dt = self.today_dt

                df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
                df = df.sort_values('date').reset_index(drop=True)
                self._cached_calendar = df
                # 调试日志：检查特定日期是否在日历中
                print(f"[DEBUG] 交易日历包含 {self.today} 吗? {self.today_dt in df['date'].values}")
                return df
            except Exception as e:
                print(f"[WARNING] 从本地文件加载交易日历失败: {e}，将尝试从 Akshare 获取。")
                # 如果文件损坏或格式不对，则继续从 Akshare 获取

        # 3. 从 Akshare 获取数据
        try:
            print(f"[INFO] 从 Akshare 获取交易日历...")
            df = ak.tool_trade_date_hist_sina()
            df = df.rename(columns={'trade_date': 'date'})

            # 确保df['date']是Timestamp，并进行normalize()
            # 显式转换为Timestamp并normalize，确保类型一致性
            df['date'] = pd.to_datetime(df['date']).dt.normalize()

            start_dt = pd.to_datetime(self.global_start).normalize()
            end_dt = self.today_dt

            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
            df = df.sort_values('date').reset_index(drop=True)

            self._cached_calendar = df  # 缓存到内存

            # 写入本地文件
            try:
                df.to_csv(self.calendar_file_path, index=False, encoding='utf-8-sig')
                print(f"[INFO] 交易日历已成功写入本地文件: {self.calendar_file_path}")
            except Exception as e:
                print(f"[ERROR] 写入交易日历到本地文件失败: {e}")

            # 调试日志：检查特定日期是否在日历中
            print(f"[DEBUG] 交易日历包含 {self.today} 吗? {self.today_dt in df['date'].values}")
            return df
        except Exception as e:
            print(f"[ERROR] 从 akshare 获取交易日历失败: {e}")
            return pd.DataFrame(columns=['date'])

    def get_main_board_pool(self):
        try:
            from Ts_GetStockBasicinfo import TushareStockManager
        except ImportError:
            print("[ERROR] 无法导入 Ts_GetStockBasicinfo 模块，请检查文件路径和类名。")
            return pd.DataFrame(columns=['ts_code'])

        date_suffix = self.today
        filename = f"StockIndes_{date_suffix}.txt"
        # >>> 修改点 4: 正确构建股票字典文件路径 (目录 + 文件名)
        dict_file_path = os.path.join(self.base_data_dir, filename)

        if not os.path.exists(dict_file_path):
            print(f"[INFO] 未发现本地股票字典文件: {dict_file_path}，尝试通过 Tushare 获取。")
            try:
                manager = TushareStockManager(self.token)
                manager.get_basic_data(list_status='L', market='主板', save_path=dict_file_path)
                print(f"[INFO] 股票字典保存至：{dict_file_path}")
            except Exception as e:
                print(f"[ERROR] Tushare接口获取主板股票数据失败: {e}")
                return pd.DataFrame(columns=['ts_code'])

        try:
            dict_df = pd.read_csv(
                dict_file_path,
                encoding='utf-8-sig',
                sep='|'
            )
            if 'ts_code' not in dict_df.columns:
                print("[ERROR] 本地股票字典文件缺少 'ts_code' 列。")
                return pd.DataFrame(columns=['ts_code'])

            dict_df['ts_code'] = dict_df['ts_code'].astype(str)

            mask = dict_df.astype(str).apply(
                lambda row: row.str.contains(r'ST|st|\*ST|退市', case=False, na=False).any(), axis=1)
            dict_df = dict_df[~mask]

            def format_tushare_code_to_akshare_symbol(ts_code_str):
                if pd.isna(ts_code_str):
                    return None
                parts = ts_code_str.split('.')
                if len(parts) == 2:
                    code = parts[0]
                    market_suffix = parts[1].lower()
                    code = str(code).zfill(6)
                    return f"{market_suffix}{code}"
                return ts_code_str

            dict_df['ts_code'] = dict_df['ts_code'].apply(format_tushare_code_to_akshare_symbol)

            dict_df = dict_df[dict_df['ts_code'].str.match(r'^(sh|sz)\d{6}$', na=False)]

            dict_df = dict_df[['ts_code']].drop_duplicates(subset=['ts_code'])
            print(f"[INFO] 从本地字典文件加载 {len(dict_df)} 条股票代码（Akshare格式）。")
            return dict_df
        except Exception as e:
            print(f"[ERROR] 读取本地字典文件失败: {e}")
            return pd.DataFrame(columns=['ts_code'])

    def fetch_combined_data(self, symbol, start, end):
        """封装腾讯接口，合并前复权与不复权数据"""
        print(f"[DEBUG] 正在为 {symbol} 从 Akshare 接口获取数据，日期范围: {start} 至 {end}")
        try:
            df_qfq = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end, adjust="qfq")
            time.sleep(0.05)

            if df_qfq.empty:
                print(f"[WARNING] {symbol} 在 {start} 至 {end} 范围内无数据返回 (QFQ)。")
                if end == self.today:
                    print(f"[INFO] {symbol} {self.today} 数据可能尚未更新，稍后重试或交易结束后同步更佳。")
                return None

            expected_api_cols = ['date', 'open', 'close', 'high', 'low']
            missing_api_cols = [col for col in expected_api_cols if col not in df_qfq.columns]
            if missing_api_cols:
                print(
                    f"[ERROR] {symbol} QFQ 数据结构异常，Akshare腾讯接口缺少预期列：{missing_api_cols} (当前列: {df_qfq.columns.tolist()})")
                return None

            df_norm = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end, adjust="")
            time.sleep(0.05)

            if df_norm.empty:
                print(f"[WARNING] {symbol} 在 {start} 至 {end} 范围内无数据返回 (Normal)。")
                return None

            expected_cols_norm = ['date', 'close']
            missing_norm_cols = [col for col in expected_cols_norm if col not in df_norm.columns]
            if missing_norm_cols:
                print(
                    f"[ERROR] {symbol} Normal 数据结构异常，缺少必要列：{missing_norm_cols} (当前列: {df_norm.columns.tolist()})")
                return None

            df_norm = df_norm[['date', 'close']].rename(columns={'close': 'close_normal'})
            df = pd.merge(df_qfq, df_norm, on='date', how='inner')

            if df.empty:
                print(f"[WARNING] {symbol} 在 {start} 至 {end} 范围内合并 QFQ 和 Normal 数据后为空。")
                return None

            try:
                df['close'] = pd.to_numeric(df['close'], errors='raise')
                df['close_normal'] = pd.to_numeric(df['close_normal'], errors='raise')
                df['adj_ratio'] = df['close'] / df['close_normal'].replace(0, pd.NA)
                df.dropna(subset=['adj_ratio'], inplace=True)
            except Exception as e:
                print(f"[ERROR] {symbol} 计算 adj_ratio 失败：{e}")
                return None

            df['symbol'] = symbol
            df['date'] = pd.to_datetime(df['date'])  # 确保为Timestamp类型
            df.rename(columns={'date': 'trade_date'}, inplace=True)

            final_columns_for_db = [
                'trade_date', 'symbol', 'open', 'close', 'high', 'low',
                'close_normal', 'adj_ratio'
            ]
            df = df[final_columns_for_db]
            print(
                f"[DEBUG] 为 {symbol} 成功获取到 {len(df)} 条数据，日期范围: {df['trade_date'].min().strftime('%Y%m%d')} 至 {df['trade_date'].max().strftime('%Y%m%d')}.")
            return df

        except Exception as e:
            print(f"[CRITICAL] Akshare接口获取 {symbol} 在 {start} 至 {end} 数据时发生错误: {e}")
            return None

    def check_and_sync(self, symbol):
        print(f"\n[DEBUG] === 开始处理股票: {symbol} ===")
        query = text(
            "SELECT trade_date, adj_ratio FROM stock_daily_kline WHERE symbol = :s ORDER BY trade_date DESC LIMIT 1")
        with self.db.connect() as conn:
            local_last = conn.execute(query, {"s": symbol}).fetchone()

        print(f"[DEBUG] 本地数据库 {symbol} 最后记录: {local_last}")

        if not local_last:
            print(f"[INFO] 首次同步: {symbol}，将从 {self.global_start} 同步到 {self.today}")
            data = self.fetch_combined_data(symbol, self.global_start, self.today)
            if data is not None and not data.empty:
                print(f"[INFO] 首次同步 {symbol} 获取到 {len(data)} 条数据，准备保存。")
                self.save_to_db(data)
            else:
                print(f"[WARNING] 首次同步 {symbol} 未获取到有效数据，跳过保存。")
            print(f"[DEBUG] === 结束处理股票: {symbol} ===\n")
            return

        local_last_date_obj = local_last[0]
        last_date_str = local_last_date_obj.strftime("%Y%m%d")

        # 调试日志：对比本地最后日期和今天的日期
        print(f"[DEBUG] 本地最后日期对象: {local_last_date_obj} (类型: {type(local_last_date_obj)})")
        print(
            f"[DEBUG] 今天的日期对象 (来自self.today_dt.date()): {self.today_dt.date()} (类型: {type(self.today_dt.date())})")
        print(f"[DEBUG] 本地最后日期是否等于今天: {local_last_date_obj == self.today_dt.date()}")

        if local_last_date_obj == self.today_dt.date():
            print(f"[INFO] {symbol} 本地数据已是最新 ({self.today})。检查是否需要除权更新。")
            remote_sample = self.fetch_combined_data(symbol, self.today, self.today)
            if remote_sample is not None and not remote_sample.empty:
                remote_adj_ratio = float(remote_sample['adj_ratio'].iloc[0])
                local_adj_ratio = float(local_last[1])
                if abs(remote_adj_ratio - local_adj_ratio) > 1e-8:
                    print(
                        f"[INFO] 检测到除权: {symbol}, 远程 ({remote_adj_ratio}) != 本地 ({local_adj_ratio}), 触发全量重写...")
                    data = self.fetch_combined_data(symbol, self.global_start, self.today)
                    if data is not None and not data.empty:
                        print(f"[INFO] 全量重写 {symbol} 获取到 {len(data)} 条数据，准备保存。")
                        self.save_to_db(data, method='replace')
                    else:
                        print(f"[WARNING] 检测到除权 {symbol} 但未能获取有效数据，跳过全量重写。")
                else:
                    print(f"[INFO] {symbol} 除权比率一致，无需操作。")
            else:
                print(f"[WARNING] 无法获取 {symbol} 今日远程样本数据，无法比率校验。假定无变化。")
            print(f"[DEBUG] === 结束处理股票: {symbol} ===\n")
            return

            # 如果本地最后日期不是今天，则进行增量或修补
        print(f"[DEBUG] 本地 {symbol} 最后日期: {last_date_str} (非今天)。将进行缺口修补或增量更新。")
        print(f"[DEBUG] 尝试获取 {symbol} 在 {last_date_str} 的远程样本数据用于比对...")
        remote_sample = self.fetch_combined_data(symbol, last_date_str, last_date_str)

        if remote_sample is not None and not remote_sample.empty:
            remote_sample['trade_date'] = remote_sample['trade_date'].dt.normalize()
            print(f"[DEBUG] 远程 {symbol} 在 {last_date_str} 的样本数据:\n{remote_sample.head()}")
            if 'adj_ratio' in remote_sample.columns and not remote_sample['adj_ratio'].empty:
                remote_adj_ratio = float(remote_sample['adj_ratio'].iloc[0])
                local_adj_ratio = float(local_last[1])
                print(f"[DEBUG] {symbol} 远程 adj_ratio: {remote_adj_ratio}, 本地 adj_ratio: {local_adj_ratio}")

                if abs(remote_adj_ratio - local_adj_ratio) > 1e-8:
                    print(
                        f"[INFO] 检测到除权: {symbol}, 远程 ({remote_adj_ratio}) != 本地 ({local_adj_ratio}), 触发全量重写...")
                    data = self.fetch_combined_data(symbol, self.global_start, self.today)
                    if data is not None and not data.empty:
                        print(f"[INFO] 全量重写 {symbol} 获取到 {len(data)} 条数据，准备保存。")
                        self.save_to_db(data, method='replace')
                    else:
                        print(f"[WARNING] 检测到除权 {symbol} 但未能获取有效数据，跳过全量重写。")
                else:
                    print(f"[INFO] 比率一致: {symbol}, 执行增量/修补...")
                    self.repair_gaps(symbol)
            else:
                print(
                    f"[WARNING] 无法获取 {symbol} 在 {last_date_str} 的远程样本 adj_ratio 数据，无法进行比率校验。将尝试增量修补。")
                self.repair_gaps(symbol)
        else:
            print(
                f"[WARNING] 无法获取 {symbol} 在 {last_date_str} 的远程样本数据。可能是数据源问题或网络问题。将尝试增量修补。")
            self.repair_gaps(symbol)
        print(f"[DEBUG] === 结束处理股票: {symbol} ===\n")

    def repair_gaps(self, symbol):
        print(f"[DEBUG] 开始修补 {symbol} 的数据缺口...")
        try:
            with self.db.connect() as conn_local:
                local_dates_df = pd.read_sql(text(f"SELECT trade_date FROM stock_daily_kline WHERE symbol='{symbol}'"),
                                             conn_local)
            # local_dates_df['trade_date'] 是 Series of datetime.date
            # pd.to_datetime 会将其转换为 Timestamp Series，然后 .dt.normalize()
            local_dates = set(pd.to_datetime(local_dates_df['trade_date']).dt.normalize())
            print(f"[DEBUG] 本地 {symbol} 已有日期数量: {len(local_dates)}")
            if len(local_dates) > 0:
                print(f"[DEBUG] 本地 {symbol} 已有日期最新: {max(local_dates).strftime('%Y-%m-%d')}")

        except Exception as e:
            print(f"[ERROR] 查询本地数据或转换日期失败: {symbol}, {e}")
            local_dates = set()

        std_cal_df = self.get_trade_calendar_from_akshare()
        if std_cal_df.empty:
            print(f"[WARNING] 无法获取交易日历，跳过 {symbol} 的缺口修补")
            return

        # 核心修正：直接使用 Series 转换为 set，确保 set 中的元素是 Timestamp 类型
        std_cal_dates = set(std_cal_df['date'])
        print(f"[DEBUG] 标准交易日历日期数量: {len(std_cal_dates)}")

        missing_dates = sorted(list(std_cal_dates - local_dates))
        print(f"[DEBUG] {symbol} 发现的缺失日期数量: {len(missing_dates)}")

        # 调试日志：检查今天的日期在各个集合中的状态
        today_formatted = self.today_dt.strftime('%Y-%m-%d')
        if self.today_dt in std_cal_dates:
            print(f"[DEBUG] 今天的日期 {today_formatted} 在标准交易日历中。")
        else:
            print(f"[DEBUG] 今天的日期 {today_formatted} 不在标准交易日历中。")

        if self.today_dt in local_dates:
            print(f"[DEBUG] 今天的日期 {today_formatted} 在本地数据中。")
        else:
            print(f"[DEBUG] 今天的日期 {today_formatted} 不在本地数据中。")

        if self.today_dt in missing_dates:
            print(f"[INFO] 今天的日期 {today_formatted} 确实是缺失日期之一，将会被修补。")
        else:
            print(f"[WARNING] 今天的日期 {today_formatted} 不在缺失日期列表中，可能未被正确识别为缺口。")
            # 打印 local_dates 和 std_cal_dates 的部分内容，帮助诊断
            print(
                f"[DEBUG] local_dates (最后5个): {[d.strftime('%Y-%m-%d') for d in sorted(list(local_dates))[-5:]] if local_dates else 'N/A'}")
            print(
                f"[DEBUG] std_cal_dates (最后5个): {[d.strftime('%Y-%m-%d') for d in sorted(list(std_cal_dates))[-5:]] if std_cal_dates else 'N/A'}")

        if missing_dates:
            # missing_dates 列表中的元素是 Timestamp，可以安全地调用 .strftime()
            print(f"[DEBUG] {symbol} 缺失日期示例 (前5个): {[d.strftime('%Y-%m-%d') for d in missing_dates[:5]]}")

        else:
            print(f"[INFO] {symbol} 没有发现缺口，无需修补。")
            return

        intervals = []
        if missing_dates:
            start_p = missing_dates[0]  # start_p 是 Timestamp
            for i in range(1, len(missing_dates)):
                if (missing_dates[i] - missing_dates[i - 1]).days > 1:  # Timestamp之间可以直接days
                    intervals.append((start_p.strftime("%Y%m%d"), missing_dates[i - 1].strftime("%Y%m%d")))
                    start_p = missing_dates[i]
            intervals.append((start_p.strftime("%Y%m%d"), missing_dates[-1].strftime("%Y%m%d")))

        print(f"[INFO] 正在修补 {symbol} 的 {len(intervals)} 个数据缺口，时间区间: {intervals}...")
        for s, e in intervals:
            patch_data = self.fetch_combined_data(symbol, s, e)
            if patch_data is not None and not patch_data.empty:
                patch_data_dates = patch_data['trade_date'].dt.strftime('%Y%m%d').tolist()
                print(f"[DEBUG] {symbol} 获取到从 {s} 到 {e} 的数据，包含日期示例 (后5个): {patch_data_dates[-5:]}")
                self.save_to_db(patch_data, method='upsert')
                print(f"[INFO] 已修补 {symbol} 从 {s} 到 {e} 的数据。")
            else:
                print(f"[WARNING] 无法获取 {symbol} 在 {s} ~ {e} 的数据，跳过修补。")

    def save_to_db(self, df, method='upsert'):
        if df is None or df.empty:
            print(f"[WARNING] 尝试保存空DataFrame到数据库，已跳过。")
            return

        table_name = 'stock_daily_kline'
        symbol = df['symbol'].iloc[0]

        print(f"[DEBUG] 开始保存 {symbol} 到数据库，方法: {method}，数据量: {len(df)}")
        first_date = df['trade_date'].min().strftime('%Y-%m-%d')
        last_date = df['trade_date'].max().strftime('%Y-%m-%d')
        print(f"[DEBUG] {symbol} 数据范围: {first_date} 到 {last_date}")

        with self.db.begin() as conn:
            if method == 'replace':
                conn.execute(text(f"DELETE FROM {table_name} WHERE symbol='{symbol}'"))
                print(f"[INFO] 已删除 {symbol} 的旧数据，准备全量插入。")
                df.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"[INFO] 已全量插入 {len(df)} 条 {symbol} 的数据。")
            elif method == 'upsert':
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    # 将日期从 Timestamp 转换为 Python 的 datetime.date 对象，以匹配PostgreSQL的date类型
                    if 'trade_date' in row_dict and isinstance(row_dict['trade_date'], pd.Timestamp):
                        row_dict['trade_date'] = row_dict['trade_date'].date()

                    sql_upsert = text(f"""
                        INSERT INTO {table_name} (trade_date, symbol, open, close, high, low, close_normal, adj_ratio)
                        VALUES (:trade_date, :symbol, :open, :close, :high, :low, :close_normal, :adj_ratio)
                        ON CONFLICT (symbol, trade_date) DO UPDATE SET
                            open = EXCLUDED.open,
                            close = EXCLUDED.close,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close_normal = EXCLUDED.close_normal,
                            adj_ratio = EXCLUDED.adj_ratio
                    """)
                    try:
                        conn.execute(sql_upsert, row_dict)
                    except Exception as e:
                        print(f"[ERROR] {symbol} 在 {row_dict.get('trade_date')} UPSERT失败: {e}")
                print(f"[INFO] 已对 {symbol} 执行 UPSERT 操作，处理 {len(df)} 条数据。")
            else:
                print(f"[ERROR] 未知的保存方法: {method}")
        print(f"[DEBUG] 完成保存 {symbol} 到数据库。\n")

    def run_engine(self):
        print(f"[DEBUG] HistDataEngine 运行日期: {self.today}")
        # 在程序开始时，主动加载交易日历，确保它被缓存起来
        _ = self.get_trade_calendar_from_akshare()
        if self._cached_calendar.empty:
            print("[CRITICAL] 无法获取有效的交易日历，程序退出。")
            return

        # 再次检查今天的日期是否是交易日
        is_today_trade_day = self.today_dt in self._cached_calendar['date'].values
        print(f"[DEBUG] 交易日历中 {self.today} 是否为交易日: {is_today_trade_day}")

        if not is_today_trade_day:
            print(f"[WARNING] {self.today} 不是交易日，跳过数据同步。")
            return

        pool_df = self.get_main_board_pool()
        if pool_df is None or pool_df.empty:
            print("[CRITICAL] 无法获取股票池，程序退出。")
            return

        print(f"开始同步主板共 {len(pool_df)} 只股票，使用多线程（最大15个）。..")

        MAX_WORKERS = 15
        futures = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for _, row in pool_df.iterrows():
                akshare_symbol = row['ts_code']
                futures.append(executor.submit(self.check_and_sync, akshare_symbol))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f'[CRITICAL] 股票数据同步任务在线程中失败: {exc}')

        print("所有股票同步任务已提交并完成处理。")

    def get_synced_stock_codes_from_db(self) -> pd.DataFrame:
        """
        从数据库获取当天已同步的股票代码列表 (带前缀，如 'sz000001')。
        这个列表将作为 Corenews_Main 的基础股票池。
        """
        query = text(f"""
            SELECT DISTINCT symbol
            FROM stock_daily_kline
            WHERE trade_date = '{self.today_dt.strftime('%Y-%m-%d')}'
        """)
        try:
            with self.db.connect() as conn:
                df = pd.read_sql(query, conn)
            print(f"[DEBUG] 从数据库查询到 {len(df)} 只股票在 {self.today} 有数据。")
            return df
        except Exception as e:
            print(f"[ERROR] 从数据库获取已同步股票代码失败: {e}")
            return pd.DataFrame(columns=['symbol'])
