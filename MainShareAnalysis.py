# Modified Main Program
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Callable, Dict, Any, List
import akshare as ak
import pandas as pd
import pandas_ta as ta  # 勿删
from sqlalchemy import text, create_engine
import Industrytrending as industry
from DataManager import DatabaseWriter
from DataManager import ParallelUtils as utils
from DataManager import QuantDataPerformer
from FormatManager import Parse_Currency
from SignalManager import TASignalProcessor
from HistDataEngine import StockSyncEngine
from LoggerManager import LoggerManager
import os
import configparser
from pathlib import Path
from ConfigParser import Config
from FormatManager.ShareCodeFormatMgr import format_stock_code
from Distribution import MainCostDataManager
from DataManager.CalendarManager import  TradingCalendarAnalyzer

# ========== 新增导入：用于 Telegram 推送 ==========
import asyncio
from telegram import Bot
from telegram.error import TelegramError
# ===============================================


class StockAnalyzer:

    def __init__(self, config_file: str = "config.ini"):
        self.config_file = config_file
        self.config = Config(config_file=config_file)
        self.calendar_mgr = TradingCalendarAnalyzer()
        self.today_str = self.calendar_mgr.get_last_trading_day()
        self.temp_dir = self.config.TEMP_DATA_DIRECTORY
        os.makedirs(self.temp_dir, exist_ok=True)
        self.executor = ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS)
        self.start_time = time.time()
        self.logger = LoggerManager(
            log_dir=self.config.LOG_DIR,
            log_filename=f"Corenews_Main_{self.today_str}.log",
            level=self.config.LOG_LEVEL
        )

        try:
            self.sync_engine = StockSyncEngine()
            self.db_engine = self.sync_engine.db
        except Exception as e:
            self.logger.critical(
                f"[CRITICAL] Corenews_Main: Failed to initialize StockSyncEngine or its database engine. Error: {e}")
            raise

        # 初始化主力成本数据管理器
        self.cost_manager = MainCostDataManager(
            cache_enabled=True,
            cache_dir=os.path.join(self.config.TEMP_DATA_DIRECTORY, "cost_data_cache")
        )

    def _get_file_path(self, base_name: str, cleaned: bool = False) -> str:
        """
        生成临时数据文件的完整路径。如果 cleaned=True, 则添加 "_经清洗" 后缀。
        """
        suffix = "_经清洗" if cleaned else ""
        file_name = f"{base_name}{suffix}_{self.today_str}.txt"
        return os.path.join(self.temp_dir, file_name)

    def _load_data_from_cache(self, file_path: str) -> pd.DataFrame:
        """从缓存加载数据。"""
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, sep='|', encoding='utf-8', dtype={'股票代码': str, 'symbol': str})
                # 统一列名为 '股票代码'
                if 'symbol' in df.columns and '股票代码' not in df.columns:
                    df.rename(columns={'symbol': '股票代码'}, inplace=True)
                print(f"  - 发现缓存，加载: {os.path.basename(file_path)}")
                return df
            except Exception as e:
                self.logger.warning(f"[WARN] 加载缓存 {os.path.basename(file_path)} 失败: {e}，将重新获取。")
        return pd.DataFrame()

    def _save_data_to_cache(self, df: pd.DataFrame, file_path: str):
        """保存数据到缓存。"""
        try:
            df.to_csv(file_path, sep='|', index=False, encoding='utf-8')
        except Exception as e:
            self.logger.error(f"[ERROR] 保存数据到缓存 {os.path.basename(file_path)} 失败: {e}")

    def _safe_ak_fetch(self, fetch_func: Callable, file_base_name: str, **kwargs: Any) -> pd.DataFrame:

        # 1. 尝试从【清洗后的缓存】加载数据
        cleaned_file_path = self._get_file_path(file_base_name, cleaned=True)
        cached_df = self._load_data_from_cache(cleaned_file_path)
        if not cached_df.empty:
            return cached_df

        # 2. 如果清洗后的缓存不存在，则尝试从原始获取
        df = pd.DataFrame()
        for i in range(self.config.DATA_FETCH_RETRIES):
            try:
                print(f"  - 正在尝试第 {i + 1}/{self.config.DATA_FETCH_RETRIES} 次获取数据: {file_base_name}...")
                df = fetch_func(**kwargs)
                if df is not None and not df.empty:
                    break
                else:
                    self.logger.warning(f"[WARN] 数据返回为空或无效: {file_base_name}，重试中。")
                    time.sleep(self.config.DATA_FETCH_DELAY)
            except Exception as e:
                self.logger.error(
                    f"[ERROR] 获取 {file_base_name} 时出错: {e}，将在 {self.config.DATA_FETCH_DELAY} 秒后重试。")
                time.sleep(self.config.DATA_FETCH_DELAY)

        if df.empty:
            self.logger.critical(f"[FATAL] 所有重试均失败，返回空 DataFrame: {file_base_name}")
            return pd.DataFrame()

        # 3. 清洗数据并保存到带有 "_经清洗" 后缀的缓存文件
        cleaned_df = self._clean_and_standardize(df, file_base_name)
        if not cleaned_df.empty:
            self._save_data_to_cache(cleaned_df, cleaned_file_path)

        return cleaned_df

    def _clean_and_standardize(self, df: pd.DataFrame, df_name: str) -> pd.DataFrame:
        """通用数据清洗和列名标准化（已移除财务数据特殊逻辑）"""
        if df.empty:
            return df

        def extract_pure_code(code_str):
            if pd.isna(code_str):
                return None
            code_str = str(code_str).strip().upper()
            # 去掉 SH/SZ/BJ 前缀
            for prefix in ['SH', 'SZ', 'BJ']:
                if code_str.startswith(prefix):
                    code_str = code_str[2:]
                    break
            return code_str.zfill(6)


        alias_mappings = [
            (self.config.CODE_ALIASES, '股票代码'),
            (self.config.NAME_ALIASES, '股票简称'),
            (self.config.PRICE_ALIASES, '最新价'),
        ]

        for aliases, target_col in alias_mappings:
            for old, new in aliases.items():
                if old in df.columns and new == target_col:
                    df.rename(columns={old: new}, inplace=True)
                    break  # 找到匹配即跳出

        # --- 2. 处理股票代码 ---
        if '股票代码' in df.columns:
            df['股票代码'] = df['股票代码'].astype(str).apply(extract_pure_code)
        else:
            # 尝试从通用列生成
            code_col = next((col for col in df.columns
                             if col.lower() in ['code', 'ts_code', 'symbol']), None)
            if code_col:
                df['股票代码'] = df[code_col].astype(str).apply(extract_pure_code)
                print(f"[INFO] 已从 '{code_col}' 生成 '股票代码' 列。")
            else:
                print(f"[ERROR] {df_name} 无代码字段！列名：{df.columns.tolist()}")
                return pd.DataFrame()

        # --- 3. 处理股票简称 (ST过滤) ---
        if '股票简称' not in df.columns:
            # 尝试从常见列获取
            name_col = next((col for col in ['name', '简称', 'symbol'] if col in df.columns), None)
            if name_col:
                df['股票简称'] = df[name_col]
            else:
                df['股票简称'] = 'N/A'
                print(f"[WARN] {df_name} 无简称列，使用占位符。")

        # ST股过滤 (统一正则)
        st_pattern = r'(?:\s*(?:\*|★|※|•|·))?(?:[Ss][Tt])'
        if (df['股票简称'].dtype == 'object' and
                df['股票简称'].astype(str).str.contains(st_pattern, na=False).any()):
            st_count = df['股票简称'].astype(str).str.contains(st_pattern, na=False).sum()
            df = df[~df['股票简称'].astype(str).str.contains(st_pattern, na=False)].copy()
            print(f"[FILTER] 已过滤 {st_count} 只ST股票。")

        # --- 4. 处理最新价 ---
        if '最新价' not in df.columns:
            price_col = next((col for col in ['price', 'close'] if col in df.columns), None)
            if price_col:
                df['最新价'] = pd.to_numeric(df[price_col], errors='coerce')
                print(f"[INFO] 已从 '{price_col}' 生成 '最新价' 列。")
            else:
                df['最新价'] = 0.0
                print(f"[WARN] {df_name} 无价格列，设为默认值 0.0。")

        # --- 5. 最终通用清洗 ---
        df.dropna(subset=['股票代码'], inplace=True)
        df.drop_duplicates(subset=['股票代码'], keep='first', inplace=True)
        df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)

        return df

    def _load_industry_info_from_generated_file(self, codes_pure_digits: List[str]) -> pd.DataFrame:
        """从数据库 stock_basic_info 表读取行业信息，若无数据则尝试补全"""
        self.logger.info("正在从数据库 stock_basic_info 表加载行业信息...")

        try:
            if not self.db_engine:
                self.logger.critical("数据库引擎未初始化，无法读取行业信息。")
                return pd.DataFrame(columns=['股票代码', '行业', '股票简称'])

            # 1. 尝试查询数据
            placeholders = ','.join([f"'{code}'" for code in codes_pure_digits])
            query_sql = f"""
                SELECT ts_code, symbol, name, industry, market 
                FROM stock_basic_info 
                WHERE symbol IN ({placeholders})
            """
            with self.db_engine.connect() as conn:
                result = conn.execute(text(query_sql))
                rows = result.fetchall()
                columns = result.keys()
                db_df = pd.DataFrame(rows, columns=columns)

            # 2. 检查数据是否为空
            if db_df.empty:
                self.logger.warning("数据库中未查询到行业信息数据，正在尝试触发数据补全任务...")

                # --- 新增逻辑：尝试补数据 ---
                try:
                    # 导入服务类（确保路径正确）
                    from GetStockBasicinfo import StockBasicInfoService

                    # 创建服务实例并同步数据
                    # 注意：这里假设 config 已经在 self 中初始化
                    basic_info_service = StockBasicInfoService(self.config)
                    success = basic_info_service.sync_all_stock_basic_info()

                    if success:
                        self.logger.info("数据补全成功，正在重新查询...")
                        # 重新执行查询逻辑（递归调用或复制逻辑）
                        # 这里简单起见，再次执行查询（实际生产环境建议封装成独立函数）
                        with self.db_engine.connect() as conn2:
                            result2 = conn2.execute(text(query_sql))
                            rows2 = result2.fetchall()
                            if rows2:
                                db_df = pd.DataFrame(rows2, columns=columns)
                            else:
                                raise Exception("补全后仍无数据")
                    else:
                        raise Exception("补全任务返回失败")

                except Exception as e:
                    self.logger.error(f"数据补全过程异常: {e}，将使用空数据继续。")
                    return pd.DataFrame(columns=['股票代码', '行业', '股票简称'])

            # 3. 后续的字段映射与清洗逻辑（保持不变）...
            # ... (此处保留原代码中从 column_mapping 开始的清洗逻辑) ...

        except Exception as e:
            self.logger.error(f"从数据库读取行业信息失败: {e}，将尝试回退到空数据。")
            return pd.DataFrame(columns=['股票代码', '行业', '股票简称'])

    def _get_all_raw_data(self) -> Dict[str, pd.DataFrame]:
        """集中获取所有数据源 (包括主力研报盈利预测)，并支持缓存机制"""
        print("\n>>> 正在初始化数据获取和缓存检查...")

        data = {
            # 移除实时行情获取，直接从K线数据获取最新价格
            # 'spot_data_all': self._safe_ak_fetch(ak.stock_zh_a_spot_em, "A股实时行情"),

            'market_fund_flow_raw': self._safe_ak_fetch(ak.stock_fund_flow_individual, "5日市场资金流向",
                                                        symbol="5日排行"),
            'market_fund_flow_raw_10': self._safe_ak_fetch(ak.stock_fund_flow_individual, "10日市场资金流向",
                                                           symbol="10日排行"),
            'market_fund_flow_raw_20': self._safe_ak_fetch(ak.stock_fund_flow_individual, "20日市场资金流向",
                                                           symbol="20日排行"),
            'strong_stocks_raw': self._safe_ak_fetch(ak.stock_zt_pool_strong_em, "强势股池",
                                                     date=datetime.now().strftime('%Y%m%d')),
            'consecutive_rise_raw': self._safe_ak_fetch(ak.stock_rank_lxsz_ths, "连续上涨"),
            'ljqs_raw': self._safe_ak_fetch(ak.stock_rank_ljqs_ths, "量价齐升"),
            'cxfl_raw': self._safe_ak_fetch(ak.stock_rank_cxfl_ths, "持续放量"),
        }

        # 均线突破数据 (Akshare接口参数不同，需分开获取)
        data['xstp_10_raw'] = self._safe_ak_fetch(ak.stock_rank_xstp_ths, "向上突破10日均线", symbol="10日均线")
        data['xstp_30_raw'] = self._safe_ak_fetch(ak.stock_rank_xstp_ths, "向上突破30日均线", symbol="30日均线")
        data['xstp_60_raw'] = self._safe_ak_fetch(ak.stock_rank_xstp_ths, "向上突破60日均线", symbol="60日均线")

        # 行业板块数据
        print("\n>>> 正在获取行业板块名称并保存至本地...")
        industry_info_filename = f"行业板块信息_{self.today_str}.txt"
        industry_info_path = os.path.join(self.temp_dir, industry_info_filename)
        industry_board_df = pd.DataFrame()

        if os.path.exists(industry_info_path):
            try:
                print(f"  - 发现本地缓存文件，正在读取: {industry_info_filename}")
                industry_board_df = pd.read_csv(industry_info_path, sep='|', encoding='utf-8-sig')
            except Exception as e:
                self.logger.warning(f"  - [WARN] 读取本地缓存失败: {e}，将尝试重新获取...")
        else:
            print(f"  - 本地无有效缓存，正在通过 Akshare 接口获取...")
            try:
                industry_board_df = ak.stock_board_industry_name_em()
                if not industry_board_df.empty:
                    try:
                        industry_board_df.to_csv(industry_info_path, sep='|', index=False, encoding='utf-8-sig')
                        print(f"  - 获取成功并已保存至: {industry_info_filename}")
                    except Exception as e:
                        self.logger.error(f"  - [ERROR] 保存文件失败: {e}")
            except Exception as e:
                self.logger.error(f"  - [ERROR] 调用行业板块接口失败: {e}")

        data['top_industry_cons_df'] = self._get_top_industry_constituents(industry_board_df)
        data['industry_board_df'] = industry_board_df

        # 获取主力成本数据（使用新的管理类）
        print("\n>>> 正在获取主力成本数据...")
        main_cost_df = self.cost_manager.get_main_cost_data()
        main_cost_df = self.cost_manager.analyze_cost_data(main_cost_df)
        data['main_cost_data'] = main_cost_df

        # 打印主力成本数据摘要
        self.cost_manager.print_cost_summary(main_cost_df)

        return data

    def _safe_fetch_constituents(self, symbol: str) -> pd.DataFrame:
        """
        带重试机制获取单个行业板块的成分股。
        """
        df = pd.DataFrame()
        for i in range(self.config.DATA_FETCH_RETRIES):
            try:
                df = ak.stock_board_industry_cons_em(symbol=symbol)
                if df is not None and not df.empty:
                    return df
                else:
                    time.sleep(self.config.DATA_FETCH_DELAY)
            except Exception:
                time.sleep(self.config.DATA_FETCH_DELAY)
        return pd.DataFrame()

    def _get_top_industry_constituents(self, industry_board_df: pd.DataFrame) -> pd.DataFrame:
        """重构：获取涨幅前10板块的成分股"""
        if industry_board_df.empty or '板块名称' not in industry_board_df.columns:
            return pd.DataFrame()

        # 1. 缓存检查
        cache_name = "前十板块成分股"
        cleaned_file_path = self._get_file_path(cache_name, cleaned=True)
        cached_df = self._load_data_from_cache(cleaned_file_path)
        if not cached_df.empty:
            return cached_df

        top_industries = industry_board_df.sort_values(by='涨跌幅', ascending=False).head(10)

        # --- 修复点 1：强制构建纯 Python 字典列表，避免 Pandas Series 混入线程 ---
        # 不要直接用 to_dict('records')，以防万一
        industry_list = []
        for _, row in top_industries.iterrows():
            pure_dict = {col: row[col] for col in top_industries.columns}
            industry_list.append(pure_dict)

        def fetch_worker(row):
            try:
                # --- 修复点 2：确保能正确提取 '板块名称'，兼容 dict 和 series ---
                # 如果 row 是 pandas Series
                if isinstance(row, pd.Series):
                    industry_name = row['板块名称']
                # 如果 row 是 dict
                elif isinstance(row, dict):
                    industry_name = row['板块名称']
                else:
                    print(f"[ERROR] 无法识别的数据类型: {type(row)}")
                    return None

                print(f" - 正在获取板块成分股: {industry_name}")
                constituents_df = self._safe_fetch_constituents(symbol=industry_name)

                if constituents_df is not None and not constituents_df.empty:
                    # --- 修复点 3：这里必须使用 .str.zfill 处理 DataFrame 的整列 ---
                    # 原来的 .zfill(6) 是错的，那是给单个字符串用的
                    if '代码' in constituents_df.columns:
                        constituents_df.rename(columns={'代码': '股票代码'}, inplace=True)

                    if '股票代码' in constituents_df.columns:
                        # 关键修复：使用 .astype(str).str.zfill 处理 Series
                        constituents_df['股票代码'] = constituents_df['股票代码'].astype(str).str.zfill(6)

                    constituents_df['所属板块'] = industry_name
                    return constituents_df[['股票代码', '所属板块']].drop_duplicates()
                return None

            except Exception as e:
                # --- 修复点 4：增加异常捕获，防止某个板块出错导致整个线程池崩溃 ---
                self.logger.error(f"[WORKER ERROR] 处理板块 {row.get('板块名称', 'Unknown')} 时出错: {e}")
                return None

        results = utils.run_with_thread_pool(
            items=industry_list,
            worker_func=fetch_worker,
            max_workers=self.config.MAX_WORKERS,
            desc="获取板块成分股"
        )

        if results:
            # 过滤掉 None 结果
            valid_results = [df for df in results if df is not None and not df.empty]
            if valid_results:
                final_df = pd.concat(valid_results, ignore_index=True).drop_duplicates(subset=['股票代码'])
                self._save_data_to_cache(final_df, cleaned_file_path)
                return final_df

        return pd.DataFrame()


    def _save_ta_signals_to_txt(self, ta_signals: Dict[str, pd.DataFrame]):
        """
        将技术指标信号结果保存到独立的 TXT 文件。
        """
        print("\n>>> 正在保存技术指标信号到本地 TXT 文件...")

        save_dir = self.config.TEMP_DATA_DIRECTORY
        today_str = self.today_str

        for indicator_name, df in ta_signals.items():
            if df is None or df.empty:
                continue

            file_name = f"{indicator_name}_Signals_{today_str}.txt"
            file_path = os.path.join(save_dir, file_name)

            try:
                df.to_csv(file_path, sep='|', index=False, encoding='utf-8')
                print(f"  - 成功保存 {indicator_name} 信号文件: {file_name}")
            except Exception as e:
                self.logger.error(f"[ERROR] 保存 {indicator_name} 信号文件失败: {e}")

    def _process_xstp_and_filter(self, raw_data: Dict[str, pd.DataFrame], spot_df: pd.DataFrame) -> pd.DataFrame:
        """处理并合并均线突破数据，并进行多头排列筛选。"""
        print("正在处理并合并均线突破数据...")

        # 1. 清洗均线数据
        processed_df10 = raw_data['xstp_10_raw'].rename(columns={'最新价': '10日均线价'})
        processed_df30 = raw_data['xstp_30_raw'].rename(columns={'最新价': '30日均线价'})
        processed_df60 = raw_data['xstp_60_raw'].rename(columns={'最新价': '60日均线价'})

        # 2. 合并
        merged_df = pd.concat([
            processed_df10[['股票代码', '股票简称']].dropna(subset=['股票代码']),
            processed_df30[['股票代码', '股票简称']].dropna(subset=['股票代码']),
            processed_df60[['股票代码', '股票简称']].dropna(subset=['股票代码'])
        ]).drop_duplicates(subset=['股票代码'])

        # 3. 重新合并均线价格，确保同一行有所有数据
        xstp_base = merged_df[['股票代码', '股票简称']].drop_duplicates()
        xstp_base = pd.merge(xstp_base, processed_df10[['股票代码', '10日均线价']], on='股票代码', how='left')
        xstp_base = pd.merge(xstp_base, processed_df30[['股票代码', '30日均线价']], on='股票代码', how='left')
        xstp_base = pd.merge(xstp_base, processed_df60[['股票代码', '60日均线价']], on='股票代码', how='left')

        # 4. 合并实时价格 (此处仍然按代码合并，以便于均线计算的准确性)
        xstp_base = pd.merge(xstp_base, spot_df[['股票代码', '最新价']], on='股票代码', how='left')

        # 5. 类型转换和过滤
        cols_to_convert = [col for col in xstp_base.columns if '最新价' in col or col == '最新价']
        for col in cols_to_convert:
            xstp_base[col] = pd.to_numeric(xstp_base[col], errors='coerce')

        # 过滤条件: 1. 最新价>10日均线 2. 多头排列 (10>30 或 30>60)
        filtered_df = xstp_base[
            (xstp_base['最新价'] > xstp_base['10日均线价']) &
            (
                    (xstp_base['10日均线价'] > xstp_base['30日均线价'].fillna(float('-inf'))) |
                    (xstp_base['30日均线价'] > xstp_base['60日均线价'].fillna(float('-inf')))
            )
            ].copy()

        # 添加完全多头排列标记
        filtered_df['完全多头排列'] = filtered_df.apply(
            lambda row: '是' if row['10日均线价'] > row['30日均线价'] and row['30日均线价'] > row[
                '60日均线价'] else '否',
            axis=1
        )

        filtered_df.rename(columns={'最新价': '当前价格'}, inplace=True)
        return filtered_df.fillna('N/A')

    def _calculate_main_cost_analysis(self, main_cost_df: pd.DataFrame) -> pd.DataFrame:
        """
        基于主力成本数据进行进一步分析（此方法现在主要通过MainCostDataManager实现）
        """
        # 这个方法现在主要委托给MainCostDataManager
        return main_cost_df

    def _consolidate_data(self, processed_data: Dict[str, pd.DataFrame],
                          base_stock_codes_pure: List[str]) -> pd.DataFrame:
        """
        完全重写：稳健构建最终报告
        """
        print("\n>>> 正在汇总所有数据和信号 (技术指标作为独立列)...")

        # 基础股票代码列表（确保列存在）
        codes = [code.zfill(6) for code in base_stock_codes_pure]
        if not codes:
            print("  [ERROR] 基础股票代码列表为空！")
            return pd.DataFrame()

        df = pd.DataFrame({'股票代码': codes})

        # ==================== 1. 股票简称与最新价 ====================
        # 优先从 spot_data_all 获取
        spot = processed_data.get('spot_data_all')
        name_map = {}
        price_map = {}
        if spot is not None and not spot.empty and '股票代码' in spot.columns:
            spot['股票代码'] = spot['股票代码'].astype(str).str.zfill(6)
            if '股票简称' in spot.columns:
                name_map = spot.set_index('股票代码')['股票简称'].to_dict()
            if '最新价' in spot.columns:
                price_map = spot.set_index('股票代码')['最新价'].to_dict()
        # 若 spot 无简称，则尝试从 individual_industry 获取
        ind_info = processed_data.get('individual_industry')
        if not name_map and ind_info is not None and not ind_info.empty and '股票代码' in ind_info.columns and '股票简称' in ind_info.columns:
            ind_info['股票代码'] = ind_info['股票代码'].astype(str).str.zfill(6)
            name_map.update(ind_info.set_index('股票代码')['股票简称'].to_dict())

        df['股票简称'] = df['股票代码'].map(name_map).fillna('N/A')
        df['最新价'] = df['股票代码'].map(price_map).fillna('N/A')
        print(f"  - 股票简称填充完成，缺失数量: {(df['股票简称'] == 'N/A').sum()}")
        print(f"  - 最新价填充完成，有效数量: {(df['最新价'] != 'N/A').sum()}")
        # 如果股票简称全部为 N/A，但数据源有内容，发出警告
        if (df['股票简称'] == 'N/A').all() and name_map:
            print("  [WARN] 股票简称映射失败，请检查spot_data_all中的股票代码格式")

        # ==================== 2. 行业 ====================
        if ind_info is not None and not ind_info.empty and '股票代码' in ind_info.columns and '行业' in ind_info.columns:
            ind_info['股票代码'] = ind_info['股票代码'].astype(str).str.zfill(6)
            industry_map = ind_info.set_index('股票代码')['行业'].to_dict()
            df['行业'] = df['股票代码'].map(industry_map).fillna('N/A')
        else:
            df['行业'] = 'N/A'

        # ==================== 3. 均线突破 ====================
        xstp = processed_data.get('processed_xstp_df')
        if xstp is not None and not xstp.empty and '股票代码' in xstp.columns:
            xstp['股票代码'] = xstp['股票代码'].astype(str).str.zfill(6)
            for col in ['完全多头排列', '当前价格', '10日均线价', '30日均线价', '60日均线价']:
                if col in xstp.columns:
                    m = xstp.set_index('股票代码')[col].to_dict()
                    df[col] = df['股票代码'].map(m).fillna('N/A' if col == '完全多头排列' else 0)
                else:
                    df[col] = 'N/A' if col == '完全多头排列' else 0
        else:
            df['完全多头排列'] = '否'
            df['10日均线价'] = 0
            df['30日均线价'] = 0
            df['60日均线价'] = 0

        # ==================== 4. 资金流向 ====================
        for days, key in [('5日', 'market_fund_flow_raw'), ('10日', 'market_fund_flow_raw_10'), ('20日', 'market_fund_flow_raw_20')]:
            fdf = processed_data.get(key)
            if fdf is not None and not fdf.empty and '股票简称' in fdf.columns and '资金流入净额' in fdf.columns:
                fdf = fdf.drop_duplicates('股票简称')
                flow_map = fdf.set_index('股票简称')['资金流入净额'].to_dict()
                df[f'{days}资金流入'] = df['股票简称'].map(flow_map).fillna('0')
            else:
                df[f'{days}资金流入'] = '0'

        # 资金动能
        def calc_trend(row):
            try:
                v5 = float(str(row['5日资金流入']).replace(',', ''))
                v10 = float(str(row['10日资金流入']).replace(',', ''))
                v20 = float(str(row['20日资金流入']).replace(',', ''))
                if (v5 > v10 or v5 > v20) and v5 > 0:
                    return '动能增强'
                elif v5 > 0:
                    return '流入'
                else:
                    return ''
            except:
                return ''
        df['资金动能'] = df.apply(calc_trend, axis=1)

        # ==================== 5. 强势股、连涨、量价齐升、放量 ====================
        strong = processed_data.get('strong_stocks_raw')
        if strong is not None and not strong.empty and '股票代码' in strong.columns:
            strong_codes = set(strong['股票代码'].astype(str).str.zfill(6))
            df['强势股'] = df['股票代码'].apply(lambda x: '是' if x in strong_codes else '否')
        else:
            df['强势股'] = '否'

        rise = processed_data.get('consecutive_rise_raw')
        if rise is not None and not rise.empty and '股票代码' in rise.columns and '连涨天数' in rise.columns:
            rise['股票代码'] = rise['股票代码'].astype(str).str.zfill(6)
            rise_map = rise.set_index('股票代码')['连涨天数'].to_dict()
            df['连涨天数'] = df['股票代码'].map(rise_map).fillna(0).astype(int)
        else:
            df['连涨天数'] = 0

        ljqs = processed_data.get('ljqs_raw')
        if ljqs is not None and not ljqs.empty and '股票代码' in ljqs.columns:
            ljqs_codes = set(ljqs['股票代码'].astype(str).str.zfill(6))
            df['量价齐升'] = df['股票代码'].apply(lambda x: '是' if x in ljqs_codes else '否')
        else:
            df['量价齐升'] = '否'

        cxfl = processed_data.get('cxfl_raw')
        if cxfl is not None and not cxfl.empty and '股票代码' in cxfl.columns and '放量天数' in cxfl.columns:
            cxfl['股票代码'] = cxfl['股票代码'].astype(str).str.zfill(6)
            cxfl_map = cxfl.set_index('股票代码')['放量天数'].to_dict()
            df['放量天数'] = df['股票代码'].map(cxfl_map).fillna(0).astype(int)
        else:
            df['放量天数'] = 0

        # ==================== 6. 技术指标信号 ====================
        for indicator, signal_col in [('MACD_12269', 'MACD_12269_Signal'), ('MACD_6135', 'MACD_6135_Signal'),
                                      ('KDJ', 'KDJ_Signal'), ('CCI', 'CCI_Signal'),
                                      ('RSI', 'RSI_Signal'), ('BOLL', 'BOLL_Signal')]:
            sig_df = processed_data.get(indicator)
            if sig_df is not None and not sig_df.empty and '股票代码' in sig_df.columns and signal_col in sig_df.columns:
                sig_df['股票代码'] = sig_df['股票代码'].astype(str).str.zfill(6)
                map_dict = sig_df.set_index('股票代码')[signal_col].to_dict()
                col_name = signal_col.replace('_Signal', '') if indicator in ['MACD_12269','MACD_6135'] else signal_col
                df[col_name] = df['股票代码'].map(map_dict).fillna('')
            else:
                col_name = signal_col.replace('_Signal', '') if indicator in ['MACD_12269','MACD_6135'] else signal_col
                df[col_name] = ''

        # ==================== 7. MACD 动能和DIF ====================
        momentum = processed_data.get('MACD_DIF_MOMENTUM')
        if momentum is not None and not momentum.empty and '股票代码' in momentum.columns:
            momentum['股票代码'] = momentum['股票代码'].astype(str).str.zfill(6)
            for col in ['MACD_12269_动能', 'MACD_6135_动能', 'MACD_12269_DIF', 'MACD_6135_DIF']:
                if col in momentum.columns:
                    m_map = momentum.set_index('股票代码')[col].to_dict()
                    df[col] = df['股票代码'].map(m_map).fillna('')
                else:
                    df[col] = ''
        else:
            for col in ['MACD_12269_动能', 'MACD_6135_动能', 'MACD_12269_DIF', 'MACD_6135_DIF']:
                df[col] = ''

        # ==================== 8. TOP10 行业 ====================
        top_ind = processed_data.get('top_industry_cons_df')
        if top_ind is not None and not top_ind.empty and '股票代码' in top_ind.columns:
            top_codes = set(top_ind['股票代码'].astype(str).str.zfill(6))
            df['TOP10行业'] = df['股票代码'].apply(lambda x: '是' if x in top_codes else '否')
        else:
            df['TOP10行业'] = '否'

        # ==================== 9. 主力成本数据 ====================
        cost = processed_data.get('main_cost_data')
        if cost is not None and not cost.empty:
            if '代码' in cost.columns:
                cost.rename(columns={'代码': '股票代码'}, inplace=True)
            if '股票代码' in cost.columns:
                cost['股票代码'] = cost['股票代码'].astype(str).str.zfill(6)
                for col in ['主力成本', '成本位置', '主力控盘强度']:
                    if col in cost.columns:
                        m = cost.set_index('股票代码')[col].to_dict()
                        df[col] = df['股票代码'].map(m).fillna('N/A')
                    else:
                        df[col] = 'N/A'
                df['主力成本差价'] = 'N/A'
            else:
                df['主力成本'] = 'N/A'
                df['主力成本差价'] = 'N/A'
                df['成本位置'] = 'N/A'
                df['主力控盘强度'] = 'N/A'
        else:
            df['主力成本'] = 'N/A'
            df['主力成本差价'] = 'N/A'
            df['成本位置'] = 'N/A'
            df['主力控盘强度'] = 'N/A'

        # ==================== 10. 信号筛选 ====================
        def has_any_signal(row):
            return (row.get('完全多头排列') == '是' or
                    row.get('强势股') == '是' or
                    row.get('量价齐升') == '是' or
                    row.get('TOP10行业') == '是' or
                    row.get('MACD_12269', '') != '' or
                    row.get('MACD_6135', '') != '' or
                    row.get('KDJ_Signal', '') != '' or
                    row.get('CCI_Signal', '') != '' or
                    row.get('RSI_Signal', '') != '' or
                    row.get('BOLL_Signal', '') != '')

        before = len(df)
        df_before_filter = df.copy()
        df = df[df.apply(has_any_signal, axis=1)].copy()
        print(f"  - 信号筛选前 {before} 只，筛选后 {len(df)} 只")
        if df.empty:
            print("  [WARN] 信号筛选后为空，回退到未筛选数据（避免报告为空）")
            df = df_before_filter


        # ==================== 11. 排序和链接 ====================
        if not df.empty:
            df.sort_values(by=['连涨天数', '放量天数'], ascending=[False, False], inplace=True)
            df.reset_index(drop=True, inplace=True)
            df['完整股票代码'] = df['股票代码'].apply(format_stock_code)
            df['股票链接'] = "https://hybrid.gelonghui.com/stock-check/" + df['完整股票代码']
            df.drop(columns=['完整股票代码'], inplace=True, errors='ignore')
            if '当前价格' in df.columns and '最新价' in df.columns:
                df.drop(columns=['当前价格'], inplace=True, errors='ignore')
        else:
            print("  [WARN] 筛选后无股票")

        # ==================== 12. 最终列顺序 ====================
        base_cols = ['股票代码', '股票简称', '行业', '最新价', '主力成本', '主力成本差价', '成本位置', '主力控盘强度']
        signal_cols = [
            '强势股', '量价齐升', '连涨天数', '放量天数', 'TOP10行业',
            'MACD_12269', 'MACD_12269_动能', 'MACD_12269_DIF',
            'MACD_6135', 'MACD_6135_动能', 'MACD_6135_DIF',
            'KDJ_Signal', 'CCI_Signal', 'RSI_Signal', 'BOLL_Signal',
        ]
        report_cols = [
            '研报买入次数',
            '完全多头排列', '10日均线价', '30日均线价', '60日均线价',
            '资金动能', '5日资金流入', '10日资金流入', '20日资金流入'
        ]
        final_cols = base_cols + signal_cols + report_cols + ['股票链接']
        # 只保留存在的列
        final_cols = [col for col in final_cols if col in df.columns]
        df = df[final_cols]
        return df

    def _merge_industry_signal_to_stocks(self, stock_df: pd.DataFrame, industry_df: pd.DataFrame) -> pd.DataFrame:
        """
        将行业分析的结论('行业信号'列)，精准匹配到每一只股票上。
        """
        if industry_df is None or industry_df.empty or stock_df.empty or '行业' not in stock_df.columns:
            stock_df['所属行业信号'] = ''
            return stock_df

        print("  - 正在将行业信号映射至个股...")
        signal_map = industry_df.set_index('行业名称')['行业信号'].to_dict()
        stock_df['所属行业信号'] = stock_df['行业'].map(signal_map).fillna('')

        return stock_df

    def _generate_report(self, sheets_data: Dict[str, pd.DataFrame]):
        """生成 Excel 报告。"""
        print(f"\n>>> 正在生成 Excel 报告...")
        report_path = os.path.join(self.config.TEMP_DATA_DIRECTORY, f"审计报告_{self.today_str}.xlsx")

        try:
            writer = pd.ExcelWriter(report_path, engine='xlsxwriter')
            workbook = writer.book

            header_format = workbook.add_format(
                {'bold': True, 'text_wrap': True, 'valign': 'top', 'fg_color': '#D7E4BC', 'border': 1})
            currency_format = workbook.add_format({'num_format': '#,##0.00'})
            code_format = workbook.add_format({'num_format': '@'})

            for sheet_name, df in sheets_data.items():

                if df is None or df.empty:
                    print(f"  - 警告：工作表 '{sheet_name}' 数据为空，跳过创建。")
                    continue

                df.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)
                worksheet = writer.sheets[sheet_name]

                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                for i, col in enumerate(df.columns):
                    max_len = max(df[col].astype(str).str.len().max(), len(col))
                    col_width = min(max_len + 2, 30)

                    if col == '最新价' or '价格' in col or '价' in col or '线' in col or '均线' in col:
                        worksheet.set_column(i, i, col_width, currency_format)
                    elif '代码' in col:
                        worksheet.set_column(i, i, 10, code_format)
                    else:
                        worksheet.set_column(i, i, col_width)

            writer.close()
            print(f"  - 报告已成功生成并保存到: {report_path}")

        except Exception as e:
            self.logger.critical(f"[FATAL] 致命错误：生成 Excel 报告失败。原因: {e}")
            raise

    def _get_latest_prices_from_kline(self, hist_df_all: pd.DataFrame) -> pd.DataFrame:
        """
        从K线数据中获取最新的收盘价作为"实时价格"
        """
        if hist_df_all.empty:
            return pd.DataFrame(columns=['股票代码', '最新价'])

        # 获取每个股票的最新一条记录（按日期排序）
        latest_records = hist_df_all.sort_values('trade_date').groupby('symbol').tail(1)

        # 提取股票代码和收盘价
        latest_prices = latest_records[['symbol', 'close']].copy()
        latest_prices.columns = ['股票代码', '最新价']

        # 提取纯数字股票代码
        latest_prices['股票代码'] = latest_prices['股票代码'].astype(str).str.extract(r'(\d{6})')[0]

        return latest_prices

    # ========== 新增：超短线选股筛选函数 ==========
    def _ultra_short_filter(self, df: pd.DataFrame, industry_df: pd.DataFrame,
                             hist_all: pd.DataFrame, spot_df: pd.DataFrame) -> pd.DataFrame:
        """机构级超短线模型：趋势启动 + 资金共振 + 热点驱动"""
        if df.empty:
            return df

        # 1. 热点行业优先（核心逻辑）
        df['热点强度'] = 0
        if 'TOP10行业' in df.columns:
            df['热点强度'] += (df['TOP10行业'] == '是').astype(int) * 2

        if industry_df is not None and not industry_df.empty:
            if '行业名称' in industry_df.columns and '涨跌幅' in industry_df.columns:
                industry_df['rank'] = industry_df['涨跌幅'].rank(pct=True)
                hot_map = industry_df.set_index('行业名称')['rank'].to_dict()
                df['热点强度'] += df['行业'].map(hot_map).fillna(0)

        # 2. 启动信号（短线核心）
        start_signal = (
            (df['量价齐升'] == '是') |
            (df['MACD_12269_动能'].astype(str).str.contains('红柱加长|绿柱缩短', na=False)) |
            (df['MACD_6135_动能'].astype(str).str.contains('红柱加长|绿柱缩短', na=False)) |
            (df['KDJ_Signal'].astype(str).str.contains('金叉', na=False)) |
            (df['CCI_Signal'].astype(str).str.contains('买入', na=False))
        )

        # 3. 资金驱动（主力行为）
        flow5 = pd.to_numeric(df['5日资金流入'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        flow10 = pd.to_numeric(df['10日资金流入'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

        capital_signal = (flow5 > 0) & (flow5 >= flow10)

        # 4. 控盘 + 成本（机构思维）
        control_signal = df['主力控盘强度'].isin(['高度控盘', '中度控盘'])
        cost_signal = df['成本位置'].isin(['突破主力成本', '接近主力成本'])

        # 5. 避免追高（关键）
        price = pd.to_numeric(df['最新价'], errors='coerce')
        ma60 = pd.to_numeric(df.get('60日均线价', 0), errors='coerce')
        deviation = (price - ma60) / ma60 * 100
        risk_filter = deviation < 12  # 更严格

        # 综合筛选（机构风格：必须同时满足核心条件）
        filtered = df[
            (df['热点强度'] > 0.8) &
            start_signal &
            capital_signal &
            (control_signal | cost_signal) &
            risk_filter
        ].copy()

        print(f"[机构超短模型] 筛选后: {len(filtered)} / 原始 {len(df)}")

        # 如果为空，降级策略（防止无结果）
        if filtered.empty:
            print("[降级策略] 使用宽松模式")
            filtered = df[
                (df['热点强度'] > 0.5) &
                (start_signal | capital_signal)
            ].copy()

        return filtered

    def _rank_and_select(self, df: pd.DataFrame, top_n: int = 30) -> pd.DataFrame:
        """机构级评分模型（短线资金驱动）"""
        if df.empty:
            return df

        score = pd.Series(0, index=df.index)
        score += df.get('近3日涨停', 0) * 20
        score += (df.get('历史涨停次数', 0) > 5).astype(int) * 10
        score += (df.get('放量倍数', 1) > 1.5).astype(int) * 10


        # 1. 热点优先（最重要）
        score += df.get('热点强度', 0) * 40

        # 2. 启动信号评分
        score += (df['量价齐升'] == '是').astype(int) * 15
        score += df['MACD_12269_动能'].astype(str).str.contains('红柱加长', na=False).astype(int) * 10
        score += df['KDJ_Signal'].astype(str).str.contains('金叉', na=False).astype(int) * 10

        # 3. 资金强度
        flow5 = pd.to_numeric(df['5日资金流入'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        score += (flow5 > 0).astype(int) * 10
        score += (flow5 > flow5.quantile(0.7)).astype(int) * 10

        # 4. 控盘
        control_map = {'高度控盘': 15, '中度控盘': 10, '轻度控盘': 5}
        score += df['主力控盘强度'].map(control_map).fillna(0)

        # 5. 连板/趋势惯性（短线核心）
        score += df['连涨天数'] * 5
        score += df['放量天数'] * 3

        df['综合得分'] = score

        df_sorted = df.sort_values('综合得分', ascending=False).head(top_n)

        print(f"[评分模型] 输出 {len(df_sorted)} 只")

        return df_sorted
    # =============================================

    # ========== 新增：Telegram 推送函数 ==========
    def _send_report_to_telegram(self, report_path: str):
        """发送 Excel 报告文件到 Telegram"""
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(self.config_file)
        try:
            token = cfg.get('TELEGRAM', 'TOKEN')
            chat_id = cfg.get('TELEGRAM', 'CHAT_ID')
        except (configparser.NoSectionError, configparser.NoOptionError):
            self.logger.warning("Telegram 配置未找到，跳过推送。")
            return
        if not token or not chat_id:
            self.logger.warning("Telegram Token 或 Chat ID 为空，跳过推送。")
            return
        bot = Bot(token=token)
        try:
            asyncio.run(self._send_document(bot, chat_id, report_path))
            self.logger.info(f"报告已成功发送到 Telegram！文件: {report_path}")
        except Exception as e:
            self.logger.error(f"发送报告到 Telegram 失败: {e}")

    async def _send_document(self, bot, chat_id, file_path):
        try:
            with open(file_path, 'rb') as f:
                await bot.send_document(chat_id=chat_id, document=f, caption=f"BAISYS 量化报告 {self.today_str}")
        except Exception as e:
            self.logger.error(f"Telegram API 错误: {e}")
    # =============================================

    def _add_limit_up_features(self, df: pd.DataFrame, hist_df_all: pd.DataFrame) -> pd.DataFrame:
        #"""涨停基因（游资核心）"""
        if df.empty or hist_df_all.empty:
            df['近3日涨停'] = 0
            df['历史涨停次数'] = 0
            return df

        hist = hist_df_all.copy()

        # 🔍 DEBUG（非常重要）
        print("[DEBUG] hist columns:", hist.columns.tolist())

        # ✅ 自动识别日期字段
        date_col = None
        for col in ['trade_date', 'date', 'datetime']:
            if col in hist.columns:
                date_col = col
                break

        if date_col is None:
            print("[WARN] 未找到日期字段")
            df['近3日涨停'] = 0
            df['历史涨停次数'] = 0
            return df

        # ✅ 校验字段
        if 'high' not in hist.columns or 'close' not in hist.columns:
            print("[WARN] 缺少 high/close 字段")
            df['近3日涨停'] = 0
            df['历史涨停次数'] = 0
            return df

        # 涨停计算（简化版）
        hist['涨停'] = (hist['close'] >= hist['high'] * 0.995).astype(int)

        # 最近3天
        recent = hist.sort_values(date_col).groupby('symbol').tail(3)
        recent_map = recent.groupby('symbol')['涨停'].sum().to_dict()

        # 历史
        total_map = hist.groupby('symbol')['涨停'].sum().to_dict()

        df['近3日涨停'] = df['股票代码'].map(
            {k[-6:]: v for k, v in recent_map.items()}
        ).fillna(0)

        df['历史涨停次数'] = df['股票代码'].map(
            {k[-6:]: v for k, v in total_map.items()}
        ).fillna(0)

        return df

    def _add_volume_features(self, df: pd.DataFrame, hist_df_all: pd.DataFrame) -> pd.DataFrame:
        """盘口强度（用日K替代）"""
        if df.empty or hist_df_all.empty:
            df['放量倍数'] = 1
            return df

        hist = hist_df_all.copy()

        hist['成交额'] = hist['close'] * hist['volume']

        latest = hist.sort_values('trade_date').groupby('symbol').tail(1)
        avg5 = hist.sort_values('trade_date').groupby('symbol').tail(5).groupby('symbol')['成交额'].mean()

        ratio_map = (latest.set_index('symbol')['成交额'] / avg5).to_dict()

        df['放量倍数'] = df['股票代码'].map({k[-6:]: v for k, v in ratio_map.items()}).fillna(1)

        return df

    def _calc_market_emotion(self, hist_df_all: pd.DataFrame) -> str:
        #"""情绪周期判断"""
        if hist_df_all.empty:
            return "未知"

        hist = hist_df_all.copy()

        # 自动识别日期列
        date_col = None
        for col in ['trade_date', 'date', 'datetime']:
            if col in hist.columns:
                date_col = col
                break

        if date_col is None:
            return "未知"

        if 'high' not in hist.columns or 'close' not in hist.columns:
            return "未知"

        hist['涨停'] = (hist['close'] >= hist['high'] * 0.995).astype(int)

        latest_date = hist[date_col].max()
        today_df = hist[hist[date_col] == latest_date]

        limit_count = today_df['涨停'].sum()

        if limit_count > 80:
            return "主升期"
        elif limit_count > 30:
            return "分歧期"
        else:
            return "退潮期"
   
    def run(self):

        print(f"[INFO]  股票分析程序启动 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] 识别的业务日期(最后一个交易日)为: {self.today_str}") # 日志提示


        try:

            self.sync_engine.run_engine()
            self.sync_engine.run_engine(target_date=self.today_str)
            synced_codes_df_from_db = pd.DataFrame(columns=['symbol'])  # 初始化为空，以防查询失败

            try:
                # 确保 self.db_engine 已经被成功初始化
                if self.db_engine is None:
                    raise RuntimeError("数据库引擎未成功初始化，无法从数据库获取数据。")

                with self.db_engine.connect() as conn:
                    # 1. 查询数据库中最新的一个交易日期
                    latest_date_query = text("SELECT MAX(trade_date) FROM stock_daily_kline;")
                    latest_db_date_result = conn.execute(latest_date_query).scalar_one_or_none()
                    if latest_db_date_result is None:
                        self.logger.critical(
                            "[FATAL] 数据库中 'stock_daily_kline' 表没有K线数据，无法获取股票代码列表，流程终止。")
                        return
                    # 2. 查询在该最新交易日期有数据的股票代码
                    query_symbols = text(f"""
                                    SELECT DISTINCT symbol
                                    FROM stock_daily_kline
                                    WHERE trade_date = :latest_date
                                """)
                    synced_codes_df_from_db = pd.read_sql(query_symbols, conn,
                                                          params={'latest_date': latest_db_date_result})
                    print(
                        f">>> 已从数据库获取 {len(synced_codes_df_from_db)} 只股票代码，基于最新交易日  ")
            except Exception as e:
                self.logger.critical(f"[FATAL] 查询数据库获取股票代码失败: {e}，流程终止。")
                return  # 异常时也终止流程

            if synced_codes_df_from_db.empty:
                self.logger.critical("[FATAL] 从数据库获取已同步股票代码列表失败，流程终止。")
                return

            final_analysis_codes_prefixed = synced_codes_df_from_db['symbol'].tolist()

            final_analysis_codes_pure = [code[2:] for code in final_analysis_codes_prefixed]

            print(
                f">>> HistDataWatchDog 成功同步 {len(final_analysis_codes_prefixed)} 只股票数据到数据库，并作为分析基础。")

            # 预处理行业权重数据
            industry_analyzer = industry.IndustryFlowAnalyzer(self.config)
            industry_analysis_df = industry_analyzer.run_analysis()

            # 获取K线数据
            raw_data = self._get_all_raw_data()

            # 从K线数据获取最新价格替代实时行情
            print("\n>>> 从K线数据获取最新收盘价...")
            # 构造查询语句
            if not final_analysis_codes_prefixed:
                print("[WARN] 待分析股票代码列表为空，跳过历史数据查询。")
                hist_df_all = pd.DataFrame()
            else:
                # 构造 IN 子句
                symbols_str = ','.join([f"'{s}'" for s in final_analysis_codes_prefixed])
                query = text(f"""
                    SELECT *
                    FROM stock_daily_kline
                    WHERE symbol IN ({symbols_str})
                    ORDER BY trade_date
                """)

                hist_df_all = pd.DataFrame()  # 初始化为空
                try:
                    with self.db_engine.connect() as conn:

                        hist_df_all = pd.read_sql(query, conn)

                        if not hist_df_all.empty:
                            print(
                                f"[INFO] 数据日期范围: {hist_df_all['trade_date'].min()} 至 {hist_df_all['trade_date'].max()}")
                        else:
                            print("[ERROR] 查询结果为空！可能是股票代码不匹配或日期条件过滤了所有数据。")

                except Exception as e:
                    # except 必须紧贴 try 块
                    print(f"[ERROR] 数据库查询失败: {e}")
                    hist_df_all = pd.DataFrame()

            if hist_df_all.empty:
                print("[WARN] 由于历史数据为空，将跳过所有技术指标计算。")
                # 这里可能需要处理空数据的情况，防止后续报错
            else:
                # 正常调用信号处理
                pass

            # 从K线数据获取最新价格
            latest_prices_df = self._get_latest_prices_from_kline(hist_df_all)
            print(f"[INFO] 从K线数据获取了 {len(latest_prices_df)} 只股票的最新收盘价")

            # 将最新价格数据加入到raw_data中，替代原来的spot_data_all
            raw_data['spot_data_all'] = latest_prices_df

            signal_processor = TASignalProcessor(self)
            ta_signals = signal_processor.process_signals(

                final_analysis_codes_prefixed,
                hist_df_all,
                raw_data['spot_data_all']
            )
            self._save_ta_signals_to_txt(ta_signals)
            print(">>> 股票历史数据和技术指标分析完成。")

            # 行业信息获取，注意这里需要纯数字的代码
            industry_info_df = self._load_industry_info_from_generated_file(final_analysis_codes_pure)
            universe_codes_set_pure = set(final_analysis_codes_pure)

            def filter_df_by_universe(df, universe_set):
                if df is None or df.empty or '股票代码' not in df.columns:
                    return pd.DataFrame()
                df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
                return df[df['股票代码'].isin(universe_set)].copy()

            # 均线突破数据处理
            processed_xstp_df = self._process_xstp_and_filter(raw_data, raw_data['spot_data_all'])
            processed_xstp_df = filter_df_by_universe(processed_xstp_df, universe_codes_set_pure)

            # 过滤其他每日排名数据
            raw_data['market_fund_flow_raw'] = filter_df_by_universe(raw_data['market_fund_flow_raw'],
                                                                     universe_codes_set_pure)
            raw_data['market_fund_flow_raw_10'] = filter_df_by_universe(raw_data['market_fund_flow_raw_10'],
                                                                        universe_codes_set_pure)
            raw_data['market_fund_flow_raw_20'] = filter_df_by_universe(raw_data['market_fund_flow_raw_20'],
                                                                        universe_codes_set_pure)
            raw_data['strong_stocks_raw'] = filter_df_by_universe(raw_data['strong_stocks_raw'],
                                                                  universe_codes_set_pure)
            raw_data['consecutive_rise_raw'] = filter_df_by_universe(raw_data['consecutive_rise_raw'],
                                                                     universe_codes_set_pure)
            raw_data['ljqs_raw'] = filter_df_by_universe(raw_data['ljqs_raw'], universe_codes_set_pure)
            raw_data['cxfl_raw'] = filter_df_by_universe(raw_data['cxfl_raw'], universe_codes_set_pure)

            # 5. 合并所有数据源和信号
            processed_data = {
                **raw_data,
                **ta_signals,
                'processed_xstp_df': processed_xstp_df,
                'processed_main_report': pd.DataFrame(),  # 此时为空DataFrame
                'individual_industry': industry_info_df
            }

            # 调用 _consolidate_data 时，传入基础的纯数字股票代码列表
            consolidated_report = self._consolidate_data(processed_data, final_analysis_codes_pure)
            consolidated_report = self._merge_industry_signal_to_stocks(consolidated_report, industry_analysis_df)

            # ====== 游资增强模块 ======
            consolidated_report = self._add_limit_up_features(consolidated_report, hist_df_all)
            consolidated_report = self._add_volume_features(consolidated_report, hist_df_all)

            market_emotion = self._calc_market_emotion(hist_df_all)
            print(f"[市场情绪] 当前周期: {market_emotion}")

            if market_emotion == "退潮期":
                consolidated_report = consolidated_report.head(10)
            elif market_emotion == "主升期":
                pass


            # ========== 新增：超短线精选筛选 ==========
            if not consolidated_report.empty:
                consolidated_report = self._ultra_short_filter(
                    consolidated_report,
                    industry_analysis_df,
                    hist_df_all,
                    raw_data['spot_data_all']
                )
                if len(consolidated_report) > 30:
                    consolidated_report = self._rank_and_select(consolidated_report, top_n=30)
                    self.logger.info(f"[最终] 精选后共 {len(consolidated_report)} 只股票进入报告")
            # ========================================

            cols = list(consolidated_report.columns)
            if '所属行业信号' in cols and '行业' in cols:
                cols.remove('所属行业信号')
                idx = cols.index('行业')
                cols.insert(idx + 1, '所属行业信号')
                consolidated_report = consolidated_report[cols]

            print(">>> 正在执行最终数据清洗：剔除弱势且加速下跌的个股...")

            if not consolidated_report.empty:
                # 为了安全比较，确保 DIF 列被正确解析为数字，非数字转为 NaN
                dif_12269 = pd.to_numeric(consolidated_report.get('MACD_12269_DIF'), errors='coerce')
                dif_6135 = pd.to_numeric(consolidated_report.get('MACD_6135_DIF'), errors='coerce')
                kdj_col = consolidated_report.get('KDJ_Signal',
                                                  pd.Series([''] * len(consolidated_report),
                                                            index=consolidated_report.index))
                kdj_is_empty = kdj_col.isna() | (kdj_col.astype(str).str.strip().str.lower().isin(['', 'nan', 'none']))

                # 定义剔除条件（所有条件需同时满足）
                drop_condition = (
                        (consolidated_report.get('强势股') == '否') &
                        (consolidated_report.get('量价齐升') == '否') &
                        (consolidated_report.get('连涨天数') == 0) &
                        (consolidated_report.get('放量天数') == 0) &
                        (consolidated_report.get('MACD_12269_动能') == '加速下跌 (绿柱加长)') &
                        (consolidated_report.get('MACD_6135_动能') == '加速下跌 (绿柱加长)') &
                        (dif_12269 < 0) &
                        (dif_6135 < 0) &
                        kdj_is_empty &
                        (consolidated_report.get('5日资金流入', pd.Series(dtype=str)).astype(str).str.contains('-',
                                                                                                               na=False))
                )

                initial_count = len(consolidated_report)
                consolidated_report = consolidated_report[~drop_condition].copy()
                dropped_count = initial_count - len(consolidated_report)
                print(
                    f" 排除极度弱势特征的股票。剩余 {len(consolidated_report)} 只。")

            # 6. 准备报告数据
            sheets_data = {
                '数据汇总': consolidated_report,
                '行业深度分析': industry_analysis_df,
                '主力研报筛选': processed_data['processed_main_report'],
                '均线多头排列': processed_xstp_df,
                '5日市场资金流向': raw_data['market_fund_flow_raw'],
                '10日市场资金流向': raw_data['market_fund_flow_raw_10'],
                '20日市场资金流向': raw_data['market_fund_flow_raw_20'],
                '强势股池': raw_data['strong_stocks_raw'],
                '连续上涨': raw_data['consecutive_rise_raw'],
                '量价齐升': raw_data['ljqs_raw'],
                '持续放量': raw_data['cxfl_raw'],
                'MACD_12269金叉': ta_signals.get('MACD_12269', pd.DataFrame()),
                'MACD_6135金叉': ta_signals.get('MACD_6135', pd.DataFrame()),
                'MACD_DIF_动能状态': ta_signals.get('MACD_DIF_MOMENTUM', pd.DataFrame()),
                'KDJ超卖金叉': ta_signals.get('KDJ', pd.DataFrame()),
                'CCI专业状态': ta_signals.get('CCI', pd.DataFrame()),
                'RSI超卖': ta_signals.get('RSI', pd.DataFrame()),
                'BOLL低波': ta_signals.get('BOLL', pd.DataFrame()),
                '前十板块成分股': raw_data['top_industry_cons_df'],
                '主力成本分析': processed_data['main_cost_data'],  # 新增主力成本分析页签
                # 移除A股实时行情，因为现在从K线获取
            }

            # 7. 生成报告
            self._generate_report(sheets_data)

            # ========== 新增：推送报告到 Telegram ==========
            report_path = os.path.join(self.config.TEMP_DATA_DIRECTORY, f"审计报告_{self.today_str}.xlsx")
            self._send_report_to_telegram(report_path)
            # ============================================

            try:
              db_manager = DatabaseWriter.QuantDBManager(
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD,
                host=self.config.DB_HOST,
                port=self.config.DB_PORT,
                db_name=self.config.DB_NAME
                )

              sync_task = QuantDataPerformer.QuantDBSyncTask(db_manager)

              sync_task.sync_all(
                today_str=self.today_str,
                consolidated_report=consolidated_report,
                industry_df=industry_analysis_df,
                raw_data=raw_data
              )

              db_manager.close()
              print("数据库同步成功完成。")

            except Exception as e:
             self.logger.error(f"!!! [同步中断] 任务运行异常: {e}")

        except Exception as e:
            self.logger.critical(f"\n[FATAL] 致命错误：数据分析流程意外终止。原因: {e}")
            raise

        finally:
            end_time = time.time()
            print(f"\n>>> 流程结束。总耗时: {timedelta(seconds=end_time - self.start_time)}")

if __name__ == "__main__":
    analyzer = StockAnalyzer()
    analyzer.run()
