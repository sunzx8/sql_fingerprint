#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SQL指纹管理工具 - 提供添加、删除、合并和导出指纹的功能
"""

import argparse
import json
import os
import pickle
import re
import sys
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import pandas as pd
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 默认缓存文件路径
DEFAULT_CACHE_PATH = "/data/local_disk0/shawn/dirty_work/519/cbs_519.pkl"

def load_fingerprints(cache_path):
    """
    加载指纹缓存文件
    
    参数:
        cache_path: 指纹缓存文件路径
    
    返回:
        tuple: (指纹集合, 指纹到SQL映射字典)
    """
    try:
        with open(cache_path, 'rb') as f:
            data = pickle.load(f)
            if isinstance(data, tuple) and len(data) == 2:
                fingerprints, fingerprint_to_sql = data
                
                # 检查是否有额外添加的指纹信息
                extra_add_fingerprints = set()
                for fp, sql_list in fingerprint_to_sql.items():
                    # 检查是否有extra_add标记
                    if hasattr(sql_list, 'get') and callable(getattr(sql_list, 'get')):
                        # 如果是字典格式，检查extra_add标记
                        if sql_list.get('extra_add', False):
                            extra_add_fingerprints.add(fp)
                
                # 如果发现了extra_add标记，打印信息
                if extra_add_fingerprints:
                    logger.info(f"发现 {len(extra_add_fingerprints)} 个额外添加的指纹")
                
                # 如果fingerprint_to_sql中有字典格式，转换为列表格式
                for fp in list(fingerprint_to_sql.keys()):
                    if hasattr(fingerprint_to_sql[fp], 'get') and callable(getattr(fingerprint_to_sql[fp], 'get')):
                        # 如果是字典格式，提取sql_examples
                        if 'sql_examples' in fingerprint_to_sql[fp]:
                            fingerprint_to_sql[fp] = fingerprint_to_sql[fp]['sql_examples']
            else:
                # 兼容旧格式
                fingerprints = data
                fingerprint_to_sql = {}
        logger.info(f"已加载 {len(fingerprints)} 个指纹")
        return fingerprints, fingerprint_to_sql
    except Exception as e:
        logger.error(f"加载指纹缓存失败: {e}")
        return set(), {}

def save_fingerprints(fingerprints, fingerprint_to_sql, cache_path, backup=True):
    """
    保存指纹到缓存文件
    
    参数:
        fingerprints: 指纹集合
        fingerprint_to_sql: 指纹到SQL映射字典
        cache_path: 保存路径
        backup: 是否创建备份
    
    返回:
        bool: 是否成功保存
    """
    # 创建备份
    if backup and os.path.exists(cache_path):
        backup_path = f"{cache_path}.bak"
        try:
            import shutil
            shutil.copy2(cache_path, backup_path)
            logger.info(f"已创建备份: {backup_path}")
        except Exception as e:
            logger.error(f"创建备份失败: {e}")
            return False

    # 保存新的指纹文件
    try:
        with open(cache_path, 'wb') as f:
            pickle.dump((fingerprints, fingerprint_to_sql), f)
        logger.info(f"已保存 {len(fingerprints)} 个指纹到: {cache_path}")
        return True
    except Exception as e:
        logger.error(f"保存指纹缓存失败: {e}")
        return False

def extract_sql_from_log_content(content):
    """
    从日志内容中提取SQL语句
    
    参数:
        content: 日志内容字符串
    
    返回:
        str: 提取的SQL语句，如果无法提取则返回None
    """
    try:
        # 使用正则表达式匹配SQL语句部分
        # 格式例如: [rows:1  ] [txid:25670] UPDATE `sql_adapter_info` SET...
        sql_match = re.search(r'\[rows:[^]]*\]\s*\[txid:[^]]*\]\s*(.+)$', content)
        if sql_match:
            sql = sql_match.group(1).strip()
            # 排除BEGIN和COMMIT语句，它们不需要计算指纹
            if sql not in ['BEGIN', 'COMMIT']:
                return sql
    except Exception as e:
        logger.error(f"提取SQL时出错: {e}")
    return None

def process_single_sql(sql_text):
    """
    处理单条SQL语句，计算指纹
    
    参数:
        sql_text: SQL语句
    
    返回:
        tuple: (指纹字符串, SQL语句)
    """
    try:
        from fingerprint_matches_519_multi_caller import process_single_sql as original_process
        return original_process(sql_text)
    except ImportError:
        logger.error("无法导入指纹计算模块 fingerprint_matches_519_multi_caller")
        return None, sql_text

def read_sql_from_csv(csv_path, sql_column, annotation_column=None):
    """
    从CSV文件读取SQL语句
    
    参数:
        csv_path: CSV文件路径
        sql_column: SQL语句所在列名
        annotation_column: 标注列名，如果提供，只会读取标注为1的SQL
    
    返回:
        list: SQL语句列表
    """
    try:
        # 检查文件是否存在
        if not os.path.exists(csv_path):
            logger.error(f"错误: 找不到CSV文件 {csv_path}")
            return []
        
        # 使用分块读取大型CSV文件
        logger.info(f"正在从CSV文件 {csv_path} 读取SQL语句...")
        sql_list = []
        total_chunks = 0
        
        # 读取SQL列和标注列
        if annotation_column:
            for chunk in pd.read_csv(csv_path, usecols=[sql_column, annotation_column], chunksize=100000):
                # 仅保留标注为1的SQL
                filtered_chunk = chunk[chunk[annotation_column] == 1]
                sql_list.extend(filtered_chunk[sql_column].tolist())
                total_chunks += 1
                logger.info(f"已读取 {total_chunks} 个分块，当前共 {len(sql_list)} 条标注为1的SQL语句")
        else:
            for chunk in pd.read_csv(csv_path, usecols=[sql_column], chunksize=100000):
                filtered_chunk = chunk
                sql_list.extend(filtered_chunk[sql_column].tolist())
                total_chunks += 1
                logger.info(f"已读取 {total_chunks} 个分块，当前共 {len(sql_list)} 条SQL语句")
        
        # 去除空值和重复值
        sql_list = [sql for sql in sql_list if pd.notna(sql) and sql.strip()]
        unique_count = len(set(sql_list))
        logger.info(f"从CSV加载了 {len(sql_list)} 条SQL语句，其中唯一SQL语句 {unique_count} 条")
        
        return sql_list
    except Exception as e:
        logger.error(f"读取CSV文件时出错: {e}")
        return []

def read_sql_from_txt(txt_path):
    """
    从TXT文件读取SQL语句，每行作为一条SQL语句
    
    参数:
        txt_path: 文本文件路径
    
    返回:
        list: SQL语句列表
    """
    try:
        # 检查文件是否存在
        if not os.path.exists(txt_path):
            logger.error(f"错误: 找不到TXT文件 {txt_path}")
            return []
        
        logger.info(f"正在从TXT文件 {txt_path} 读取SQL语句...")
        sql_list = []
        
        with open(txt_path, 'r', encoding='utf-8') as f:
            for line in f:
                sql = line.strip()
                if sql:  # 如果行不为空
                    sql_list.append(sql)
        
        # 去除重复值
        unique_count = len(set(sql_list))
        logger.info(f"从TXT加载了 {len(sql_list)} 条SQL语句，其中唯一SQL语句 {unique_count} 条")
        
        return sql_list
    except Exception as e:
        logger.error(f"读取TXT文件时出错: {e}")
        return []

def add_fingerprints_from_sql_list(sql_list, input_cache, output_cache, limit_examples=100):
    """
    从SQL语句列表中添加指纹
    
    参数:
        sql_list: SQL语句列表
        input_cache: 输入指纹缓存文件
        output_cache: 输出指纹缓存文件
        limit_examples: 每个指纹最多保存的SQL示例数
    
    返回:
        bool: 是否成功添加指纹
    """
    # 加载现有指纹
    existing_fingerprints, fingerprint_to_sql = load_fingerprints(input_cache)
    
    # 记录原始指纹数量
    original_fingerprint_count = len(existing_fingerprints)
    new_fingerprint_count = 0
    
    # 使用多进程处理SQL语句
    num_processes = max(1, min(cpu_count() - 1, 16))  # 限制最大进程数为16
    logger.info(f"使用 {num_processes} 个进程并行处理SQL语句...")
    
    with Pool(processes=num_processes) as pool:
        # 使用tqdm显示进度
        for result in tqdm(pool.imap(process_single_sql, sql_list, chunksize=1000), 
                         total=len(sql_list), 
                         desc="计算SQL指纹"):
            fingerprint, sql_text = result
            if fingerprint:
                # 添加新指纹
                if fingerprint not in existing_fingerprints:
                    existing_fingerprints.add(fingerprint)
                    new_fingerprint_count += 1
                
                # 更新指纹到SQL的映射
                if fingerprint not in fingerprint_to_sql:
                    fingerprint_to_sql[fingerprint] = []
                
                # 限制每个指纹保存的SQL示例数
                if len(fingerprint_to_sql[fingerprint]) < limit_examples and sql_text not in fingerprint_to_sql[fingerprint]:
                    fingerprint_to_sql[fingerprint].append(sql_text)
    
    # 保存更新后的指纹
    save_success = save_fingerprints(existing_fingerprints, fingerprint_to_sql, output_cache)
    
    if save_success:
        logger.info(f"指纹添加完成!")
        logger.info(f"原有指纹数量: {original_fingerprint_count}")
        logger.info(f"新增指纹数量: {new_fingerprint_count}")
        logger.info(f"当前总指纹数量: {len(existing_fingerprints)}")
        return True
    else:
        logger.error("指纹添加失败")
        return False

def add_fingerprints_from_csv(csv_path, sql_column, annotation_column, input_cache, output_cache):
    """
    从CSV文件添加指纹
    
    参数:
        csv_path: CSV文件路径
        sql_column: SQL语句所在列名
        annotation_column: 标注列名，如果提供，只会读取标注为1的SQL
        input_cache: 输入指纹缓存文件
        output_cache: 输出指纹缓存文件
    
    返回:
        bool: 是否成功添加指纹
    """
    logger.info(f"正在从CSV文件 {csv_path} 添加指纹...")
    
    # 从CSV读取SQL
    sql_list = read_sql_from_csv(csv_path, sql_column, annotation_column)
    if not sql_list:
        logger.error("从CSV读取SQL失败或没有符合条件的SQL")
        return False
    
    # 添加指纹
    return add_fingerprints_from_sql_list(sql_list, input_cache, output_cache)

def add_fingerprints_from_txt(txt_path, input_cache, output_cache):
    """
    从TXT文件添加指纹
    
    参数:
        txt_path: TXT文件路径
        input_cache: 输入指纹缓存文件
        output_cache: 输出指纹缓存文件
    
    返回:
        bool: 是否成功添加指纹
    """
    logger.info(f"正在从TXT文件 {txt_path} 添加指纹...")
    
    # 从TXT读取SQL
    sql_list = read_sql_from_txt(txt_path)
    if not sql_list:
        logger.error("从TXT读取SQL失败或没有SQL")
        return False
    
    # 添加指纹
    return add_fingerprints_from_sql_list(sql_list, input_cache, output_cache)

def add_fingerprints_from_json(json_path, input_cache, output_cache):
    """
    从JSON日志文件添加指纹
    
    参数:
        json_path: JSON文件路径
        input_cache: 输入指纹缓存文件
        output_cache: 输出指纹缓存文件
    
    返回:
        bool: 是否成功添加指纹
    """
    logger.info(f"正在从JSON文件 {json_path} 添加指纹...")
    
    # 读取JSON文件并提取SQL
    sql_list = []
    
    try:
        logger.info(f"正在从JSON文件 {json_path} 读取SQL语句...")
        with open(json_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    # 解析JSON行
                    json_obj = json.loads(line.strip())
                    if '__CONTENT__' in json_obj:
                        content = json_obj['__CONTENT__']
                        sql = extract_sql_from_log_content(content)
                        if sql:
                            sql_list.append(sql)
                except json.JSONDecodeError:
                    logger.warning(f"无法解析JSON行: {line[:100]}...")
                except Exception as e:
                    logger.warning(f"处理行时出错: {e}")
        
        logger.info(f"从JSON文件中提取了 {len(sql_list)} 条SQL语句")
        
        # 去除重复的SQL语句
        unique_sql_list = list(set(sql_list))
        logger.info(f"其中唯一SQL语句 {len(unique_sql_list)} 条")
        
        # 添加指纹
        return add_fingerprints_from_sql_list(unique_sql_list, input_cache, output_cache)
    
    except Exception as e:
        logger.error(f"处理JSON文件时出错: {e}")
        return False

def remove_fingerprints(fingerprints_to_remove, input_cache, output_cache, backup=True):
    """
    从指纹缓存文件中删除指定的指纹
    
    参数:
        fingerprints_to_remove: 要删除的指纹列表或集合
        input_cache: 输入指纹缓存文件
        output_cache: 输出指纹缓存文件
        backup: 是否创建备份文件
    
    返回:
        bool: 是否成功删除指纹
    """
    if not os.path.exists(input_cache):
        logger.error(f"错误: 指纹缓存文件 {input_cache} 不存在")
        return False
    
    # 将指纹列表转换为集合，提高查找效率
    fingerprints_to_remove_set = set(fingerprints_to_remove)
    
    # 加载指纹缓存
    fingerprints, fingerprint_to_sql = load_fingerprints(input_cache)
    if not fingerprints:
        logger.error("错误: 指纹缓存加载失败或为空")
        return False
    
    # 检查要删除的指纹是否存在
    existing_fingerprints = fingerprints_to_remove_set.intersection(fingerprints)
    not_found_fingerprints = fingerprints_to_remove_set - fingerprints
    
    if not existing_fingerprints:
        logger.warning("警告: 未找到任何要删除的指纹")
        return False
    
    if not_found_fingerprints:
        logger.warning(f"警告: 未找到 {len(not_found_fingerprints)} 个指纹")
    
    # 删除指纹
    original_count = len(fingerprints)
    fingerprints -= existing_fingerprints
    
    # 同时从映射中删除
    for fp in existing_fingerprints:
        if fp in fingerprint_to_sql:
            del fingerprint_to_sql[fp]
    
    # 输出结果
    logger.info(f"删除前指纹数量: {original_count}")
    logger.info(f"删除的指纹数量: {len(existing_fingerprints)}")
    logger.info(f"删除后指纹数量: {len(fingerprints)}")
    
    # 保存结果
    success = save_fingerprints(fingerprints, fingerprint_to_sql, output_cache, backup)
    
    if success:
        logger.info(f"已成功删除 {len(existing_fingerprints)} 个指纹")
        # 输出删除的指纹列表
        if len(existing_fingerprints) <= 10:
            logger.info(f"删除的指纹: {', '.join(sorted(existing_fingerprints))}")
        else:
            first_5 = sorted(existing_fingerprints)[:5]
            logger.info(f"删除的指纹(前5个): {', '.join(first_5)}...")
    
    return success

def remove_fingerprints_from_file(fingerprints_file, input_cache, output_cache, backup=True):
    """
    从文件中读取指纹列表，然后从指纹缓存文件中删除这些指纹
    
    参数:
        fingerprints_file: 包含要删除的指纹的文件路径，每行一个指纹
        input_cache: 输入指纹缓存文件
        output_cache: 输出指纹缓存文件
        backup: 是否创建备份文件
    
    返回:
        bool: 是否成功删除指纹
    """
    if not os.path.exists(fingerprints_file):
        logger.error(f"错误: 指纹列表文件 {fingerprints_file} 不存在")
        return False
    
    try:
        with open(fingerprints_file, 'r', encoding='utf-8') as f:
            fingerprints_to_remove = [line.strip() for line in f if line.strip()]
        
        logger.info(f"从文件中读取了 {len(fingerprints_to_remove)} 个指纹")
        
        return remove_fingerprints(fingerprints_to_remove, input_cache, output_cache, backup)
    except Exception as e:
        logger.error(f"从文件中读取指纹失败: {e}")
        return False

def merge_fingerprints(cache_paths, output_cache):
    """
    合并多个指纹缓存文件
    
    参数:
        cache_paths: 指纹缓存文件路径列表
        output_cache: 输出指纹缓存文件
    
    返回:
        bool: 是否成功合并指纹
    """
    # 检查输入文件是否存在
    for cache_path in cache_paths:
        if not os.path.exists(cache_path):
            logger.error(f"错误: 指纹缓存文件 {cache_path} 不存在")
            return False
    
    # 初始化合并后的指纹集合和映射
    merged_fingerprints = set()
    merged_fingerprint_to_sql = {}
    
    # 逐个加载和合并
    for cache_path in cache_paths:
        logger.info(f"正在加载指纹缓存文件: {cache_path}")
        fingerprints, fingerprint_to_sql = load_fingerprints(cache_path)
        
        # 记录原始指纹数量
        original_count = len(merged_fingerprints)
        
        # 合并指纹集合
        merged_fingerprints.update(fingerprints)
        
        # 合并映射
        for fp, sql_list in fingerprint_to_sql.items():
            if fp not in merged_fingerprint_to_sql:
                merged_fingerprint_to_sql[fp] = []
            
            # 添加新的SQL示例，限制总数为100
            for sql in sql_list:
                if len(merged_fingerprint_to_sql[fp]) < 100 and sql not in merged_fingerprint_to_sql[fp]:
                    merged_fingerprint_to_sql[fp].append(sql)
        
        logger.info(f"合并前指纹数量: {original_count}")
        logger.info(f"从 {cache_path} 新增指纹数量: {len(merged_fingerprints) - original_count}")
        logger.info(f"当前合并后总指纹数量: {len(merged_fingerprints)}")
    
    # 保存合并结果
    success = save_fingerprints(merged_fingerprints, merged_fingerprint_to_sql, output_cache)
    
    if success:
        logger.info(f"指纹合并完成! 共合并了 {len(merged_fingerprints)} 个指纹")
        return True
    else:
        logger.error("指纹合并失败")
        return False

def export_fingerprints(cache_path, output_file, format_type="txt"):
    """
    导出指纹到文件
    
    参数:
        cache_path: 指纹缓存文件路径
        output_file: 导出文件路径
        format_type: 导出格式，支持 "txt" 或 "json"
    
    返回:
        bool: 是否成功导出指纹
    """
    if not os.path.exists(cache_path):
        logger.error(f"错误: 指纹缓存文件 {cache_path} 不存在")
        return False
    
    # 加载指纹
    fingerprints, fingerprint_to_sql = load_fingerprints(cache_path)
    if not fingerprints:
        logger.error("错误: 指纹缓存加载失败或为空")
        return False
    
    try:
        # 根据格式导出
        if format_type.lower() == "txt":
            # 以纯文本格式导出，每行一个指纹
            with open(output_file, 'w', encoding='utf-8') as f:
                for fp in sorted(fingerprints):
                    f.write(f"{fp}\n")
            logger.info(f"已将 {len(fingerprints)} 个指纹导出为文本格式: {output_file}")
            return True
        
        elif format_type.lower() == "json":
            # 以JSON格式导出，包括映射关系
            export_data = {
                "fingerprints": list(fingerprints),
                "mapping": fingerprint_to_sql
            }
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            logger.info(f"已将 {len(fingerprints)} 个指纹导出为JSON格式: {output_file}")
            return True
        
        else:
            logger.error(f"不支持的导出格式: {format_type}")
            return False
    
    except Exception as e:
        logger.error(f"导出指纹时出错: {e}")
        return False

def remove_fingerprints_by_tables(tables_file, input_cache, output_cache, backup=True, keep_only=False):
    """
    根据表名文件删除包含这些表名的指纹，或只保留包含这些表名的指纹
    
    参数:
        tables_file: 包含表名的文件路径，每行一个表名
        input_cache: 输入指纹缓存文件
        output_cache: 输出指纹缓存文件
        backup: 是否创建备份文件
        keep_only: 如果为True，只保留包含目标表名的指纹；如果为False，删除包含目标表名的指纹
    
    返回:
        bool: 是否成功处理指纹
    """
    if not os.path.exists(tables_file):
        logger.error(f"错误: 表名文件 {tables_file} 不存在")
        return False
    
    if not os.path.exists(input_cache):
        logger.error(f"错误: 指纹缓存文件 {input_cache} 不存在")
        return False
    
    # 读取表名列表
    try:
        with open(tables_file, 'r', encoding='utf-8') as f:
            target_tables = set(line.strip().lower() for line in f if line.strip())
        logger.info(f"从文件中读取了 {len(target_tables)} 个表名")
        if len(target_tables) <= 10:
            logger.info(f"目标表名: {', '.join(sorted(target_tables))}")
        else:
            first_5 = sorted(target_tables)[:5]
            logger.info(f"目标表名(前5个): {', '.join(first_5)}...")
    except Exception as e:
        logger.error(f"读取表名文件失败: {e}")
        return False
    
    # 加载指纹缓存
    fingerprints, fingerprint_to_sql = load_fingerprints(input_cache)
    if not fingerprints:
        logger.error("错误: 指纹缓存加载失败或为空")
        return False
    
    # 导入表名提取功能
    try:
        from fingerprint_matches_519_multi_caller import SQLFeatureExtractor
    except ImportError:
        logger.error("无法导入SQLFeatureExtractor，请确保fingerprint_matches_519_multi_caller.py文件存在")
        return False
    
    # 查找包含目标表名的指纹
    fingerprints_with_target_tables = set()
    total_fingerprints = len(fingerprints)
    
    logger.info("开始分析指纹中包含的表名...")
    
    for fingerprint in tqdm(fingerprints, desc="分析指纹"):
        # 获取该指纹对应的SQL示例
        sql_examples = fingerprint_to_sql.get(fingerprint, [])
        
        # 如果没有SQL示例，跳过
        if not sql_examples:
            continue
        
        # 分析每个SQL示例中的表名
        fingerprint_contains_target_table = False
        
        for sql_text in sql_examples:
            try:
                # 创建提取器并提取表名
                extractor = SQLFeatureExtractor()
                extractor.extract(sql_text)
                
                # 获取该SQL中的所有表名（转为小写进行比较）
                sql_tables = set(table.lower() for table in extractor.table_count_dict.keys())
                
                # 检查是否包含目标表名
                if sql_tables.intersection(target_tables):
                    fingerprint_contains_target_table = True
                    break
                    
            except Exception as e:
                # 如果SQL解析失败，跳过该SQL
                continue
        
        # 如果该指纹包含目标表名，记录下来
        if fingerprint_contains_target_table:
            fingerprints_with_target_tables.add(fingerprint)
    
    # 根据keep_only参数决定要删除的指纹
    if keep_only:
        # 只保留包含目标表名的指纹，删除其他所有指纹
        fingerprints_to_remove = fingerprints - fingerprints_with_target_tables
        operation_desc = "保留"
        target_fingerprints = fingerprints_with_target_tables
        operation_file_suffix = "kept_by_tables"
    else:
        # 删除包含目标表名的指纹
        fingerprints_to_remove = fingerprints_with_target_tables
        operation_desc = "删除"
        target_fingerprints = fingerprints_with_target_tables
        operation_file_suffix = "deleted_by_tables"
    
    # 输出统计信息
    logger.info(f"分析完成!")
    logger.info(f"总指纹数量: {total_fingerprints}")
    logger.info(f"包含目标表名的指纹数量: {len(target_fingerprints)}")
    
    if keep_only:
        logger.info(f"将{operation_desc} {len(target_fingerprints)} 个包含目标表名的指纹")
        logger.info(f"将删除 {len(fingerprints_to_remove)} 个不包含目标表名的指纹")
    else:
        logger.info(f"将{operation_desc} {len(target_fingerprints)} 个包含目标表名的指纹")
    
    if not target_fingerprints and not keep_only:
        logger.warning("未找到包含目标表名的指纹")
        return False
    
    if keep_only and not target_fingerprints:
        logger.warning("未找到包含目标表名的指纹，无法执行保留操作")
        return False
    
    # 显示找到的指纹中包含的表名统计
    logger.info("正在统计目标指纹包含的表名...")
    table_count_in_target_fingerprints = {}
    
    for fingerprint in target_fingerprints:
        sql_examples = fingerprint_to_sql.get(fingerprint, [])
        for sql_text in sql_examples:
            try:
                extractor = SQLFeatureExtractor()
                extractor.extract(sql_text)
                for table in extractor.table_count_dict.keys():
                    table_lower = table.lower()
                    if table_lower in target_tables:
                        table_count_in_target_fingerprints[table_lower] = table_count_in_target_fingerprints.get(table_lower, 0) + 1
            except:
                continue
    
    logger.info("目标表名在指纹中的统计:")
    for table, count in sorted(table_count_in_target_fingerprints.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  - {table}: {count}条指纹")
    
    # 删除指纹
    original_count = len(fingerprints)
    fingerprints -= fingerprints_to_remove
    
    # 同时从映射中删除
    for fp in fingerprints_to_remove:
        if fp in fingerprint_to_sql:
            del fingerprint_to_sql[fp]
    
    # 输出处理结果
    logger.info(f"处理前指纹数量: {original_count}")
    logger.info(f"处理的指纹数量: {len(fingerprints_to_remove)}")
    logger.info(f"处理后指纹数量: {len(fingerprints)}")
    
    # 保存结果
    success = save_fingerprints(fingerprints, fingerprint_to_sql, output_cache, backup)
    
    if success:
        if keep_only:
            logger.info(f"已成功保留 {len(target_fingerprints)} 个包含目标表名的指纹，删除了 {len(fingerprints_to_remove)} 个其他指纹")
        else:
            logger.info(f"已成功根据表名删除 {len(fingerprints_to_remove)} 个指纹")
        
        # 保存处理的指纹列表到文件
        if keep_only:
            operation_file_suffix = "kept_by_tables"
        else:
            operation_file_suffix = "deleted_by_tables"
        processed_fingerprints_file = f"{output_cache}_{operation_file_suffix}.txt"
        try:
            with open(processed_fingerprints_file, 'w', encoding='utf-8') as f:
                if keep_only:
                    # 保存保留的指纹列表
                    for fp in sorted(target_fingerprints):
                        f.write(f"{fp}\n")
                else:
                    # 保存删除的指纹列表
                    for fp in sorted(fingerprints_to_remove):
                        f.write(f"{fp}\n")
            logger.info(f"处理的指纹列表已保存到: {processed_fingerprints_file}")
        except Exception as e:
            logger.warning(f"保存处理的指纹列表失败: {e}")
    
    return success

def remove_fingerprints_by_regex(regex_pattern, input_cache, output_cache, backup=True, case_sensitive=False):
    """
    根据正则表达式删除包含匹配内容的指纹
    
    参数:
        regex_pattern: 正则表达式模式字符串
        input_cache: 输入指纹缓存文件
        output_cache: 输出指纹缓存文件
        backup: 是否创建备份文件
        case_sensitive: 是否区分大小写
    
    返回:
        bool: 是否成功处理指纹
    """
    if not os.path.exists(input_cache):
        logger.error(f"错误: 指纹缓存文件 {input_cache} 不存在")
        return False
    
    # 编译正则表达式
    try:
        flags = 0 if case_sensitive else re.IGNORECASE
        compiled_regex = re.compile(regex_pattern, flags)
        logger.info(f"正则表达式模式: {regex_pattern}")
        logger.info(f"区分大小写: {'是' if case_sensitive else '否'}")
    except re.error as e:
        logger.error(f"正则表达式编译失败: {e}")
        return False
    
    # 加载指纹缓存
    fingerprints, fingerprint_to_sql = load_fingerprints(input_cache)
    if not fingerprints:
        logger.error("错误: 指纹缓存加载失败或为空")
        return False
    
    # 查找包含匹配内容的指纹
    fingerprints_to_remove = set()
    total_fingerprints = len(fingerprints)
    
    logger.info("开始分析指纹中的SQL语句...")
    
    for fingerprint in tqdm(fingerprints, desc="分析指纹"):
        # 获取该指纹对应的SQL示例
        sql_examples = fingerprint_to_sql.get(fingerprint, [])
        
        # 如果没有SQL示例，跳过
        if not sql_examples:
            continue
        
        # 分析每个SQL示例是否匹配正则表达式
        fingerprint_matches_regex = False
        
        for sql_text in sql_examples:
            try:
                # 检查SQL是否匹配正则表达式
                if compiled_regex.search(sql_text):
                    fingerprint_matches_regex = True
                    break
                    
            except Exception as e:
                # 如果SQL处理失败，跳过该SQL
                continue
        
        # 如果该指纹包含匹配的SQL，记录下来
        if fingerprint_matches_regex:
            fingerprints_to_remove.add(fingerprint)
    
    # 输出统计信息
    logger.info(f"分析完成!")
    logger.info(f"总指纹数量: {total_fingerprints}")
    logger.info(f"匹配正则表达式的指纹数量: {len(fingerprints_to_remove)}")
    
    if not fingerprints_to_remove:
        logger.warning("未找到匹配正则表达式的指纹")
        return False
    
    # 显示一些匹配的SQL示例
    logger.info("正在收集匹配的SQL示例...")
    matched_sql_examples = []
    
    for fingerprint in list(fingerprints_to_remove)[:5]:  # 只显示前5个指纹的示例
        sql_examples = fingerprint_to_sql.get(fingerprint, [])
        for sql_text in sql_examples:
            if compiled_regex.search(sql_text):
                matched_sql_examples.append(sql_text[:200] + "..." if len(sql_text) > 200 else sql_text)
                break  # 每个指纹只显示一个匹配的SQL示例
    
    logger.info("匹配的SQL示例(前5个):")
    for i, sql_example in enumerate(matched_sql_examples, 1):
        logger.info(f"  {i}. {sql_example}")
    
    # 删除指纹
    original_count = len(fingerprints)
    fingerprints -= fingerprints_to_remove
    
    # 同时从映射中删除
    for fp in fingerprints_to_remove:
        if fp in fingerprint_to_sql:
            del fingerprint_to_sql[fp]
    
    # 输出处理结果
    logger.info(f"处理前指纹数量: {original_count}")
    logger.info(f"删除的指纹数量: {len(fingerprints_to_remove)}")
    logger.info(f"处理后指纹数量: {len(fingerprints)}")
    
    # 保存结果
    success = save_fingerprints(fingerprints, fingerprint_to_sql, output_cache, backup)
    
    if success:
        logger.info(f"已成功根据正则表达式删除 {len(fingerprints_to_remove)} 个指纹")
        
        # 保存删除的指纹列表到文件
        deleted_fingerprints_file = f"{output_cache}_deleted_by_regex.txt"
        try:
            with open(deleted_fingerprints_file, 'w', encoding='utf-8') as f:
                for fp in sorted(fingerprints_to_remove):
                    f.write(f"{fp}\n")
            logger.info(f"删除的指纹列表已保存到: {deleted_fingerprints_file}")
        except Exception as e:
            logger.warning(f"保存删除的指纹列表失败: {e}")
        
        # 保存匹配的SQL示例到文件
        matched_sql_file = f"{output_cache}_matched_sql_examples.txt"
        try:
            with open(matched_sql_file, 'w', encoding='utf-8') as f:
                f.write(f"正则表达式模式: {regex_pattern}\n")
                f.write(f"区分大小写: {'是' if case_sensitive else '否'}\n")
                f.write(f"匹配的指纹数量: {len(fingerprints_to_remove)}\n\n")
                
                for fingerprint in sorted(fingerprints_to_remove):
                    f.write(f"指纹: {fingerprint}\n")
                    sql_examples = fingerprint_to_sql.get(fingerprint, [])
                    for sql_text in sql_examples:
                        if compiled_regex.search(sql_text):
                            f.write(f"匹配的SQL: {sql_text}\n")
                            break
                    f.write("\n")
            logger.info(f"匹配的SQL示例已保存到: {matched_sql_file}")
        except Exception as e:
            logger.warning(f"保存匹配的SQL示例失败: {e}")
    
    return success

def main():
    parser = argparse.ArgumentParser(description="SQL指纹管理工具 - 提供添加、删除、合并和导出指纹的功能")
    subparsers = parser.add_subparsers(dest="command", help="操作命令")
    
    # 添加指纹(从CSV文件)的子命令
    add_csv_parser = subparsers.add_parser("add-csv", help="从CSV文件添加指纹")
    add_csv_parser.add_argument("--csv", required=True, help="CSV文件路径")
    add_csv_parser.add_argument("--sql-column", required=True, help="SQL语句所在列名")
    add_csv_parser.add_argument("--annotation-column", help="标注列名，如果提供，只会读取标注为1的SQL")
    add_csv_parser.add_argument("--input", default=DEFAULT_CACHE_PATH, help=f"输入指纹缓存文件路径，默认: {DEFAULT_CACHE_PATH}")
    add_csv_parser.add_argument("--output", help="输出指纹缓存文件路径，默认与输入相同")
    
    # 添加指纹(从TXT文件)的子命令
    add_txt_parser = subparsers.add_parser("add-txt", help="从TXT文件添加指纹")
    add_txt_parser.add_argument("--txt", required=True, help="TXT文件路径")
    add_txt_parser.add_argument("--input", default=DEFAULT_CACHE_PATH, help=f"输入指纹缓存文件路径，默认: {DEFAULT_CACHE_PATH}")
    add_txt_parser.add_argument("--output", help="输出指纹缓存文件路径，默认与输入相同")
    
    # 添加指纹(从JSON日志文件)的子命令
    add_json_parser = subparsers.add_parser("add-json", help="从JSON日志文件添加指纹")
    add_json_parser.add_argument("--json", required=True, help="JSON日志文件路径")
    add_json_parser.add_argument("--input", default=DEFAULT_CACHE_PATH, help=f"输入指纹缓存文件路径，默认: {DEFAULT_CACHE_PATH}")
    add_json_parser.add_argument("--output", help="输出指纹缓存文件路径，默认与输入相同")
    
    # 删除指纹的子命令
    remove_parser = subparsers.add_parser("remove", help="删除指定的指纹")
    remove_parser.add_argument("--fingerprints", "-f", nargs="+", help="要删除的指纹列表")
    remove_parser.add_argument("--file", "-i", help="包含要删除的指纹的文件路径，每行一个指纹")
    remove_parser.add_argument("--input", default=DEFAULT_CACHE_PATH, help=f"输入指纹缓存文件路径，默认: {DEFAULT_CACHE_PATH}")
    remove_parser.add_argument("--output", help="输出指纹缓存文件路径，默认与输入相同")
    remove_parser.add_argument("--no-backup", action="store_true", help="不创建备份文件")
    
    # 根据表名删除指纹的子命令
    remove_tables_parser = subparsers.add_parser("remove-by-tables", help="根据表名删除包含这些表名的指纹，或只保留包含这些表名的指纹")
    remove_tables_parser.add_argument("--tables-file", "-t", required=True, help="包含表名的文件路径，每行一个表名")
    remove_tables_parser.add_argument("--input", default=DEFAULT_CACHE_PATH, help=f"输入指纹缓存文件路径，默认: {DEFAULT_CACHE_PATH}")
    remove_tables_parser.add_argument("--output", help="输出指纹缓存文件路径，默认与输入相同")
    remove_tables_parser.add_argument("--no-backup", action="store_true", help="不创建备份文件")
    remove_tables_parser.add_argument("--keep-only", action="store_true", help="只保留包含目标表名的指纹，删除其他所有指纹")
    
    # 根据正则表达式删除指纹的子命令
    remove_regex_parser = subparsers.add_parser("remove-by-regex", help="根据正则表达式删除包含匹配内容的指纹")
    remove_regex_parser.add_argument("--pattern", "-p", required=True, help="正则表达式模式")
    remove_regex_parser.add_argument("--input", default=DEFAULT_CACHE_PATH, help=f"输入指纹缓存文件路径，默认: {DEFAULT_CACHE_PATH}")
    remove_regex_parser.add_argument("--output", help="输出指纹缓存文件路径，默认与输入相同")
    remove_regex_parser.add_argument("--no-backup", action="store_true", help="不创建备份文件")
    remove_regex_parser.add_argument("--case-sensitive", action="store_true", help="区分大小写匹配")
    
    # 合并指纹的子命令
    merge_parser = subparsers.add_parser("merge", help="合并多个指纹缓存文件")
    merge_parser.add_argument("--inputs", "-i", required=True, nargs="+", help="指纹缓存文件路径列表")
    merge_parser.add_argument("--output", "-o", required=True, help="输出指纹缓存文件路径")
    
    # 导出指纹的子命令
    export_parser = subparsers.add_parser("export", help="导出指纹到文件")
    export_parser.add_argument("--input", required=True, help="指纹缓存文件路径")
    export_parser.add_argument("--output", required=True, help="导出文件路径")
    export_parser.add_argument("--format", choices=["txt", "json"], default="txt", help="导出格式，支持 txt 或 json，默认: txt")
    
    # 查看指纹信息的子命令
    info_parser = subparsers.add_parser("info", help="查看指纹缓存文件信息")
    info_parser.add_argument("--input", default=DEFAULT_CACHE_PATH, help=f"指纹缓存文件路径，默认: {DEFAULT_CACHE_PATH}")
    info_parser.add_argument("--output-json", help="输出JSON文件路径，保存指纹和样例信息")
    
    args = parser.parse_args()
    
    # 根据子命令执行相应操作
    if args.command == "add-csv":
        # 设置默认输出路径为输入路径
        output_path = args.output if args.output else args.input
        add_fingerprints_from_csv(args.csv, args.sql_column, args.annotation_column, args.input, output_path)
    
    elif args.command == "add-txt":
        # 设置默认输出路径为输入路径
        output_path = args.output if args.output else args.input
        add_fingerprints_from_txt(args.txt, args.input, output_path)
    
    elif args.command == "add-json":
        # 设置默认输出路径为输入路径
        output_path = args.output if args.output else args.input
        add_fingerprints_from_json(args.json, args.input, output_path)
    
    elif args.command == "remove":
        # 检查是否提供了指纹列表或指纹文件
        if not args.fingerprints and not args.file:
            logger.error("错误: 必须提供要删除的指纹列表或包含指纹的文件")
            return 1
        
        # 设置默认输出路径为输入路径
        output_path = args.output if args.output else args.input
        
        if args.file:
            # 从文件读取指纹列表并删除
            remove_fingerprints_from_file(args.file, args.input, output_path, not args.no_backup)
        else:
            # 直接删除指定的指纹
            remove_fingerprints(args.fingerprints, args.input, output_path, not args.no_backup)
    
    elif args.command == "remove-by-tables":
        # 设置默认输出路径为输入路径
        output_path = args.output if args.output else args.input
        # 根据表名删除指纹或只保留指纹
        remove_fingerprints_by_tables(args.tables_file, args.input, output_path, not args.no_backup, args.keep_only)
    
    elif args.command == "remove-by-regex":
        # 设置默认输出路径为输入路径
        output_path = args.output if args.output else args.input
        # 根据正则表达式删除指纹
        remove_fingerprints_by_regex(args.pattern, args.input, output_path, not args.no_backup, args.case_sensitive)
    
    elif args.command == "merge":
        # 合并多个指纹文件
        merge_fingerprints(args.inputs, args.output)
    
    elif args.command == "export":
        # 导出指纹
        export_fingerprints(args.input, args.output, args.format)
    
    elif args.command == "info":
        # 显示指纹信息
        fingerprints, fingerprint_to_sql = load_fingerprints(args.input)
        logger.info(f"指纹缓存文件: {args.input}")
        logger.info(f"指纹总数: {len(fingerprints)}")
        logger.info(f"映射关系数量: {len(fingerprint_to_sql)}")
        
        # 计算平均SQL示例数
        total_examples = sum(len(examples) for examples in fingerprint_to_sql.values())
        avg_examples = total_examples / len(fingerprint_to_sql) if fingerprint_to_sql else 0
        logger.info(f"SQL示例总数: {total_examples}")
        logger.info(f"平均每个指纹SQL示例数: {avg_examples:.2f}")
        
        # 如果指定了输出JSON文件，则保存详细信息
        if args.output_json:
            try:
                # 准备输出数据
                output_data = {
                    "cache_file": args.input,
                    "total_fingerprints": len(fingerprints),
                    "total_mappings": len(fingerprint_to_sql),
                    "total_sql_examples": total_examples,
                    "average_examples_per_fingerprint": round(avg_examples, 2),
                    "fingerprints": {}
                }
                
                # 添加每个指纹的详细信息
                for fingerprint in sorted(fingerprints):
                    sql_examples = fingerprint_to_sql.get(fingerprint, [])
                    output_data["fingerprints"][fingerprint] = {
                        "sql_examples": sql_examples[0]
                    }
                
                # 保存到JSON文件
                with open(args.output_json, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=2)
                
                logger.info(f"指纹详细信息已保存到JSON文件: {args.output_json}")
                
            except Exception as e:
                logger.error(f"保存JSON文件失败: {e}")
    
    else:
        # 如果没有指定命令，显示帮助信息
        parser.print_help()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
