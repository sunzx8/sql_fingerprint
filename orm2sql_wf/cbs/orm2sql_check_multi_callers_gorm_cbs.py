import json
import os
import asyncio
import openai
import argparse
import re
from tqdm import tqdm
import time
import base64
from mimetypes import guess_type


CODE_ORM_MYSQL_SQL_EXTRACT = \
    "这是一段基于gorm框架的ORM代码。gorm是Go语言的优秀ORM库，支持模型关联、事务处理、钩子方法、自动迁移、自定义类型等多种功能。" \
    "请注意，这是一个ORM代码块，它一定会转换并生成SQL语句，请务必分析出所有可能的SQL语句。\n" \
    "在分析前，请先确定ORM代码操作的表名：\n" \
    "1. 在gorm中，表名可以通过多种方式确定：\n" \
    "   - 对于mysqlLocker相关操作，请使用以下确定的表名和列名映射：\n" \
    "     * mysqlLocker.tableName = dm_locks\n" \
    "     * mysqlLocker.lockColumn = lock_key\n" \
    "     * mysqlLocker.expireColumn = expired\n" \
    "     * mysqlLocker.tokenColumn = lock_token\n" \
    "     * mysqlLocker.lockTypeColumn = lock_type\n" \
    "     * mysqlLocker.lockNumsColumn = lock_nums\n" \
    "   - 请优先查看元数据(code_meta_data)中是否包含TableName()函数，该函数会显式返回表名\n" \
    "   - 检查code_meta_data或相关上下文中是否存在config文件或配置项，这些配置可能包含表名映射关系\n" \
    "   - 配置可能出现在类似'conf'、'config'、'setting'等文件或变量中，通常包含'TableName'、'table_name'或特定表名的映射\n" \
    "   - 如果找到配置文件中的表名映射，应优先使用这些映射而非默认规则\n" \
    "   - 如果没有TableName()函数和配置映射，则检查结构体名称，gorm默认使用结构体名称的蛇形复数形式作为表名\n" \
    "   - 特别注意：gorm的默认表名规则是将结构体名转换为蛇形命名并加复数形式，例如：\n" \
    "     * Users -> user\n" \
    "     * DmRdisks -> dm_rdisk\n" \
    "     * DmPhydisks -> dm_phydisk\n" \
    "     * DmRdiskAttachments -> dm_rdisk_attachment\n" \
    "     * DmRdiskBackups -> dm_rdisk_backup\n" \
    "     * DmDrgroupMembers -> dm_drgroup_member\n" \
    "     * DmPhydiskBenchmarks -> dm_phydisk_benchmark\n" \
    "   - 也可能在代码中通过Table()方法显式指定表名\n" \
    "2. 确定了表名后，再分析字段映射关系，通常在结构体的tag中定义，如`gorm:\"column:user_name\"`\n\n" \
    "请注意以下gorm特性可能会影响生成的SQL：\n" \
    "- 表名前缀/后缀：检查是否通过TablePrefix或TableSuffix设置了前缀或后缀\n" \
    "- 关联查询：Preload、Joins、Association等方法会生成不同的关联查询SQL\n" \
    "- 作用域(Scopes)：可能应用了通用查询条件\n" \
    "- 事务处理：可能包含多条相关的SQL语句\n\n" \
    "请仔细分析代码中的表结构、字段映射、查询条件和操作类型，并完成以下任务：\n" \
    "1) 分析代码可能的执行路径，并根据以下情况进行详细说明：\n" \
    " A. 如果代码会根据不同入参生成结构不同的SQL语句（不能只是sql条件列的参数值不同，一定需要是SQL结构、条件、连接方式等不同）：\n" \
    "- 请设计多种典型的入参场景（如不同条件组合、特殊标志位等）\n" \
    "- 对每个场景，给出具体的入参值\n" \
    "- 对每个场景，单独列出对应生成的完整SQL语句（包括参数绑定后的最终执行语句）\n" \
    "- 清晰说明每个场景下SQL结构的差异（如WHERE条件不同、JOIN方式不同、是否有GROUP BY等）\n" \
    "- 如果仅是参数值不同但SQL结构完全相同，请归为同一类SQL模板\n" \
    "B. 如果代码本身会执行多条SQL语句（与入参无关）：\n" \
    "- 将这些SQL语句与上述场景分开列出\n" \
    "- 按执行顺序列出所有SQL语句\n" \
    "- 说明这些SQL语句之间的关系和执行逻辑\n" \
    "- 对每条SQL语句解释其目的和作用\n" \
    "C. 如果上述两种情况同时存在（既有结构不同的SQL，又有多条SQL）：\n" \
    "- 请先按入参场景分类\n" \
    "- 在每个入参场景下，按顺序列出该场景会执行的所有SQL语句\n" \
    "- 清晰区分不同场景和不同SQL语句\n" \
    "根据代码分析，该函数应该生成 {sql_pattern_cnt} 条SQL语句。请确保你的分析包含正确数量的SQL语句（或SQL变体组）。\n" \
    "- 如果一个SQL语句有多个变体（因参数不同而结构不同），这仍然算作一条SQL语句\n" \
    "- 请仔细检查是否遗漏了某些SQL语句或错误地添加了不应该存在的SQL语句\n" \
    "请确保分析全面，考虑代码中的条件判断、循环、动态拼接等可能影响SQL结构生成的因素，输出所有可能的sql语句。" \
    "特别注意区分\"仅参数值不同\"和\"SQL结构不同\"这两种情况。\n\n" \
    "函数名称：{function_name}\n\n" \
    "ORM代码：{code_value}\n\n" \
    "调用者：{caller}\n\n" \
    "元数据：\n{code_meta_data_str}"

CODE_ORM_MYSQL_SQL_VERIFY = \
    "请检查以下从gorm ORM代码生成的SQL语句分析是否准确，并将所有SQL语句以JSON格式返回。请记住，这是一个ORM代码块，它一定会生成SQL语句，不要遗漏任何可能的SQL。\n\n" \
    "请先验证SQL语句中的表名是否正确：\n" \
    "1. 表名应该按以下优先级确定：\n" \
    "   - 对于mysqlLocker相关操作，请使用以下确定的表名和列名映射：\n" \
    "     * mysqlLocker.tableName = dm_locks\n" \
    "     * mysqlLocker.lockColumn = lock_key\n" \
    "     * mysqlLocker.expireColumn = expired\n" \
    "     * mysqlLocker.tokenColumn = lock_token\n" \
    "     * mysqlLocker.lockTypeColumn = lock_type\n" \
    "     * mysqlLocker.lockNumsColumn = lock_nums\n" \
    "   - 然后检查元数据(code_meta_data)中的TableName()函数返回值\n" \
    "   - 其次查找config文件或配置项中的表名映射\n" \
    "   - 配置通常存在于'conf'、'config'、'setting'等文件或变量中\n" \
    "   - 如果配置中定义了表名映射，这应优先于默认命名规则使用\n" \
    "   - 如果没有找到明确的表名定义，则使用结构体名称的蛇形复数形式\n" \
    "   - 特别注意：gorm的默认表名规则是将结构体名转换为蛇形命名并加复数形式，例如：\n" \
    "     * Users -> user\n" \
    "     * DmRdisks -> dm_rdisk\n" \
    "     * DmPhydisks -> dm_phydisk\n" \
    "     * DmRdiskAttachments -> dm_rdisk_attachment\n" \
    "     * DmRdiskBackups -> dm_rdisk_backup\n" \
    "     * DmDrgroupMembers -> dm_drgroup_member\n" \
    "     * DmPhydiskBenchmarks -> dm_phydisk_benchmark\n" \
    "   - 也可能在代码中通过Table()方法显式指定表名\n" \
    "   - 最后检查代码中是否通过Table()方法显式指定了表名\n" \
    "2. 检查是否应用了表名前缀或后缀配置\n" \
    "3. 确认是否正确处理了软删除条件（对有DeletedAt字段的模型）\n\n" \
    "同时，确认字段映射是否正确：\n" \
    "1. 字段映射通常在结构体的tag中定义，如`gorm:\"column:user_name\"`\n" \
    "2. 如果没有明确指定column，gorm默认使用字段名的蛇形命名作为列名\n\n" \
    "具体输出要求如下：" \
    "1. 输出应该是一个SQL语句数组，其中每个元素可以是：\n" \
    "- 一个字符串：表示单一固定的SQL语句\n" \
    "- 一个对象：表示因参数不同而产生结构变化的SQL语句，包含多个场景\n" \
    "注意，每一个SQL语句本身应该是完整可以执行的，不能包含省略号等无关内容。\n" \
    "2. 请确保输出是有效的JSON格式，结构如下：\n" \
    "[\n" \
    " \"固定的SQL语句1\",\n" \
    " {{\n" \
    "  \"type\": \"param_dependent\",\n" \
    "  \"variants\": [\n" \
    "     {{\"scenario\": \"场景1描述\", \"sql\": \"变体SQL语句1\"}},\n" \
    "     {{\"scenario\": \"场景2描述\", \"sql\": \"变体SQL语句2\"}}\n" \
    "   ]\n" \
    " }},\n" \
    " \"固定的SQL语句2\"\n" \
    "]\n" \
    "3. 这种格式能够表示：\n" \
    "- 单一SQL语句：直接作为数组元素\n" \
    "- 参数依赖的SQL变体：作为带有variants字段的对象\n" \
    "- 多条SQL语句：按执行顺序排列在数组中\n" \
    "- 混合情况：数组中可以同时包含固定SQL和参数依赖的SQL\n" \
    "4. 严格要求：\n" \
    "- SQL语句必须是完整可执行的，不能包含省略号或[其他字段]等占位符\n" \
    "- 所有表的列名必须完整列出，不能省略\n" \
    "- 所有的参数必须明确指出，不能使用\"...[其他值]\"等形式\n" \
    "- 所有SQL语句必须以分号结尾\n" \
    "- 如果SQL语句中有占位符参数，应使用问号(?)或具体的示例值代替\n" \
    "5. 对于SQL变体的重要说明：\n" \
    "- 只有当SQL语句的结构发生实质性变化时，才应被视为不同的变体\n" \
    "- 结构性变化包括：不同的查询类型(SELECT/INSERT/UPDATE/DELETE)、不同的表连接方式、不同的WHERE条件列、不同的GROUP BY/ORDER BY字段等\n" \
    "- 仅仅是参数值不同但SQL结构相同的情况，应该只列出一个代表性变体\n" \
    "- 示例：'WHERE id = 1' 和 'WHERE id = 2' 不是不同的变体，而是同一变体的不同参数\n" \
    "- 示例：'WHERE id = ?' 和 'WHERE name = ?' 是结构不同的变体，因为条件列不同\n" \
    "6. 根据ORM代码分析，该函数应该生成 {sql_pattern_cnt} 条SQL语句。请确保你的输出包含正确数量的SQL语句（或SQL变体组）。\n" \
    "- 如果一个SQL语句有多个变体（因参数不同而结构不同），这仍然算作一条SQL语句\n" \
    "- 请仔细检查是否遗漏了某些SQL语句或错误地添加了不应该存在的SQL语句\n" \
    "7. 请确保返回的是纯JSON格式，不要添加任何解释性文本。\n" \
    "8. 如果发现原始分析中的SQL语句有错误或不完整（如含有省略号、[其他字段]等占位符），请修正并补全完整的字段列表和参数。\n" \
    "9. 请确保返回的是纯JSON格式，不要添加任何解释性文本。\n\n" \
    "以下是需要检查的SQL语句分析：\n" \
    "函数定义：{code_value}\n\n" \
    "调用者信息：{caller}\n\n" \
    "相关代码上下文：{code_chain}\n\n" \
    "mysqlLocker.tableName = dm_locks\n" \
    "mysqlLocker.lockColumn = lock_key\n" \
    "mysqlLocker.expireColumn = expired\n" \
    "mysqlLocker.tokenColumn = lock_token\n" \
    "mysqlLocker.lockTypeColumn = lock_type\n" \
    "mysqlLocker.lockNumsColumn = lock_nums\n" \
    "SQL语句：{sql_statement}"

CODE_ORM_MYSQL_SQL_FORMAT = \
    "请将以下SQL语句分析结果格式化为标准JSON格式。这些SQL语句是从基于gorm的ORM代码分析得出的，请确保格式化后的结果完整表达所有SQL语句。\n\n" \
    "请注意验证SQL语句中的表名和字段是否符合gorm的命名规则：\n" \
    "1. 表名通常来自以下几种方式（按优先级排序）：\n" \
    "   - 元数据中的TableName()函数返回值\n" \
    "   - config文件或配置项中的表名映射（配置通常在'conf'、'config'、'setting'等文件中）\n" \
    "   - 结构体名称的蛇形复数形式\n" \
    "   - 代码中通过Table()方法显式指定\n" \
    "2. 可能应用了表名前缀或后缀（TablePrefix/TableSuffix）\n" \
    "3. 字段名通常来自以下几种方式：\n" \
    "   - 结构体tag中的column标签，如`gorm:\"column:user_name\"`\n" \
    "   - 结构体字段名的蛇形命名\n" \
    "1. 输出应该是一个SQL语句数组，其中每个元素可以是：\n" \
    "- 一个字符串：表示单一固定的SQL语句\n" \
    "- 一个对象：表示因参数不同而变化的SQL语句，包含多个场景\n" \
    "2. 请确保输出是有效的JSON格式，结构如下：\n" \
    "[\n" \
    " \"固定的SQL语句1\",\n" \
    " {{\n" \
    "  \"type\": \"param_dependent\",\n" \
    "  \"variants\": [\n" \
    "     {{\"scenario\": \"场景1描述\", \"sql\": \"变体SQL语句1\"}},\n" \
    "     {{\"scenario\": \"场景2描述\", \"sql\": \"变体SQL语句2\"}}\n" \
    "   ]\n" \
    " }},\n" \
    " \"固定的SQL语句2\"\n" \
    "]\n" \
    "3. 这种格式能够表示：\n" \
    "- 单一SQL语句：直接作为数组元素\n" \
    "- 参数依赖的SQL变体：作为带有variants字段的对象\n" \
    "- 多条SQL语句：按执行顺序排列在数组中\n" \
    "- 混合情况：数组中可以同时包含固定SQL和参数依赖的SQL\n" \
    "4. 严格要求：\n" \
    "- 移除所有非SQL内容（如注释、解释或描述），只保留有效的SQL语句\n" \
    "- SQL语句必须是完整可执行的，不能包含省略号或[其他字段]等占位符\n" \
    "- 所有表的列名必须完整列出，不能省略\n" \
    "- 所有的参数必须明确指出，不能使用\"...[其他值]\"等形式\n" \
    "- 所有SQL语句必须以分号结尾\n" \
    "- 如果SQL语句中有占位符参数，应使用问号(?)或具体的示例值代替" \
    "5. 请确保返回的是纯JSON格式，不要添加任何解释性文本或代码块标记。\n" \
    "6. 如果发现原始分析中的SQL语句有错误或不完整（如含有省略号、[其他字段]等占位符），请修正并补全完整的字段列表和参数。\n\n" \
    "需要格式化的内容：{sql_statement}"




async def process_json_file_async(input_file, output_file, concurrency=80):
    """处理JSON文件并将结果保存到单个文件中，包含SQL语句"""
    # 验证输入文件
    if not validate_input_file(input_file):
        print("输入文件验证失败，终止处理")
        return 0, 0
    
    # 读取输入文件
    with open(input_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 创建信号量控制并发请求数
    semaphore = asyncio.Semaphore(concurrency)
    
    # 准备所有函数信息
    all_functions = []
    if isinstance(data, dict):
        # 如果是字典类型，按原来的方式处理
        for function_name_or_path, function_info in data.items():
            # 确保function_info包含function_name
            function_info['function_name'] = function_name_or_path
            # 默认所有函数都是有效的，跳过验证阶段
            function_info['is_valid'] = True
            all_functions.append(function_info)
    elif isinstance(data, list):
        # 如果是列表类型，直接将列表项添加到all_functions
        for i, function_info in enumerate(data):
            # 确保每个项是字典类型
            if not isinstance(function_info, dict):
                print(f"警告: 索引 {i} 处的元素不是字典类型，跳过")
                continue
            # 如果没有function_name字段，使用索引作为函数名
            if 'function_name' not in function_info:
                function_info['function_name'] = f"function_{i}"
            # 默认所有函数都是有效的
            function_info['is_valid'] = True
            all_functions.append(function_info)
    
    valid_count = len(all_functions)
    invalid_count = 0

    # 为所有ORM代码的每个caller生成SQL语句
    print("开始为所有ORM代码的每个caller生成SQL语句")
    tasks = []
    task_info_map = {}
    
    for function_info in all_functions:
        function_name = function_info['function_name']
        code_value = function_info.get('code_value', '')
        sql_pattern_cnt = function_info.get('sql_pattern_cnt', None)
        
        # 获取code_meta_data的所有元素
        code_meta_data = function_info.get('code_meta_data', [])
        code_meta_data_str = ""
        for meta in code_meta_data:
            code_meta_data_str += meta['code_value'] + "\n"
        # 初始化caller_results列表
        function_info['caller_results'] = []
        
        # 处理每个caller
        callers = function_info.get('callers', [])
        if not callers:
            print(f"函数 {function_name} 没有caller，使用空caller")
            # 使用空caller创建一个任务
            caller = ""
            caller_idx = 0
            task_id = f"{function_name}/empty_caller"
            
            # 构建提示词
            prompt = CODE_ORM_MYSQL_SQL_EXTRACT.format(
                function_name=function_name,
                code_value=code_value,
                caller=caller,
                code_meta_data_str=code_meta_data_str,
                sql_pattern_cnt=sql_pattern_cnt if sql_pattern_cnt is not None else ""
            )
            
            # 创建异步任务
            task = asyncio.create_task(send_request_async(prompt, semaphore))
            tasks.append(task)
            
            # 保存任务信息
            task_info_map[task] = {
                'function_info': function_info,
                'caller': caller,
                'caller_idx': caller_idx,
                'task_id': task_id
            }
        else:
            # 为每个caller创建任务
            for caller_idx, caller_info in enumerate(callers):
                caller = caller_info.get('code_value', '')
                task_id = f"{function_name}/caller_{caller_idx}"
                
                print(f"为函数 {function_name} 的 caller_{caller_idx} 添加SQL生成任务")
                
                # 构建提示词
                prompt = CODE_ORM_MYSQL_SQL_EXTRACT.format(
                    function_name=function_name,
                    code_value=code_value,
                    caller=caller,
                    code_meta_data_str=code_meta_data_str,
                    sql_pattern_cnt=sql_pattern_cnt if sql_pattern_cnt is not None else ""
                )
                
                # 创建异步任务
                task = asyncio.create_task(send_request_async(prompt, semaphore))
                tasks.append(task)
                
                # 保存任务信息
                task_info_map[task] = {
                    'function_info': function_info,
                    'caller': caller,
                    'caller_idx': caller_idx,
                    'task_id': task_id
                }
    
    # 并发等待所有SQL生成任务完成
    if tasks:
        print(f"等待所有 {len(tasks)} 个SQL生成任务完成...")
        sql_results = await asyncio.gather(*tasks, return_exceptions=True)
    else:
        sql_results = []
    
    # 处理SQL生成结果，并开始验证任务
    verify_tasks = []
    verify_map = {}
    
    for i, sql_statement in enumerate(sql_results):
        if i >= len(tasks):
            continue
            
        task = tasks[i]
        task_info = task_info_map[task]
        function_info = task_info['function_info']
        caller = task_info['caller']
        caller_idx = task_info['caller_idx']
        task_id = task_info['task_id']
        
        # 创建caller_result条目
        caller_result = {
            'caller': caller,
            'caller_idx': caller_idx,
            'task_id': task_id,
        }
        
        # 检查是否有异常
        if isinstance(sql_statement, Exception):
            print(f"SQL生成任务 {task_id} 失败: {sql_statement}")
            caller_result['sql_statement'] = f"请求失败: {str(sql_statement)}"
            # 将结果添加到caller_results
            function_info['caller_results'].append(caller_result)
            continue
        else:
            print(f"SQL生成任务 {task_id} 完成，开始验证")
            caller_result['sql_statement'] = sql_statement
        
        # 创建验证任务
        verify_task = asyncio.create_task(
            verify_sql_async(
                sql_statement, 
                code_value=function_info.get('code_value', ''),
                code_meta_data=function_info.get('code_meta_data', []),
                caller=caller,
                semaphore=semaphore,
                sql_pattern_cnt=function_info.get('sql_pattern_cnt', None)
            )
        )
        verify_tasks.append(verify_task)
        verify_map[verify_task] = {
            'caller_result': caller_result,
            'function_info': function_info,
            'task_id': task_id
        }
    
    # 并发等待所有验证任务完成
    if verify_tasks:
        print(f"等待所有 {len(verify_tasks)} 个验证任务完成...")
        verify_results = await asyncio.gather(*verify_tasks, return_exceptions=True)
    else:
        verify_results = []
    
    # 处理验证结果，并开始格式化任务
    format_tasks = []
    format_map = {}
    
    for i, verified_sql in enumerate(verify_results):
        if i >= len(verify_tasks):
            continue
            
        task = verify_tasks[i]
        task_info = verify_map[task]
        caller_result = task_info['caller_result']
        function_info = task_info['function_info']
        task_id = task_info['task_id']
        
        # 检查是否有异常
        if isinstance(verified_sql, Exception):
            print(f"验证任务 {task_id} 失败: {verified_sql}")
            caller_result['verified_sql'] = caller_result['sql_statement']  # 使用原始SQL
        else:
            print(f"验证任务 {task_id} 完成，开始格式化")
            caller_result['verified_sql'] = verified_sql
        
        # 创建格式化任务
        format_task = asyncio.create_task(format_sql_async(caller_result['verified_sql'], semaphore))
        format_tasks.append(format_task)
        format_map[format_task] = {
            'caller_result': caller_result,
            'function_info': function_info,
            'task_id': task_id
        }
    
    # 并发等待所有格式化任务完成
    if format_tasks:
        print(f"等待所有 {len(format_tasks)} 个格式化任务完成...")
        format_results = await asyncio.gather(*format_tasks, return_exceptions=True)
    else:
        format_results = []
    
    # 处理格式化结果
    for i, sql_list in enumerate(format_results):
        if i >= len(format_tasks):
            continue
            
        task = format_tasks[i]
        task_info = format_map[task]
        caller_result = task_info['caller_result']
        function_info = task_info['function_info']
        task_id = task_info['task_id']
        
        # 检查是否有异常
        if isinstance(sql_list, Exception):
            print(f"格式化任务 {task_id} 失败: {sql_list}")
            verified_sql = caller_result['verified_sql']
            sql_list = extract_sql_statements(verified_sql)
        else:
            print(f"格式化任务 {task_id} 完成")
        
        # 如果sql_list仍然是格式不正确的字符串，尝试修复
        if isinstance(sql_list, str):
            sql_list = fix_malformed_json_array(sql_list)
        
        # 验证SQL语句完整性
        sql_list = validate_sql_completeness(sql_list)
        
        # 将SQL语句列表添加到caller_result中
        caller_result['sql_statement_list'] = sql_list
        
        # 添加SQL类型分类
        sql_types = []
        for sql in sql_list:
            sql_types.append(classify_sql(sql))
        caller_result['sql_types'] = sql_types
        
        # 验证SQL语句数量是否与预期一致
        sql_pattern_cnt = function_info.get('sql_pattern_cnt')
        if sql_pattern_cnt is not None:
            # 检查sql_statement_list长度是否与sql_pattern_cnt一致
            caller_result['sql_length_match'] = (len(sql_list) == sql_pattern_cnt)
        else:
            # 如果没有提供sql_pattern_cnt，默认为True
            caller_result['sql_length_match'] = True
        
        # 将caller_result添加到函数的caller_results列表（如果尚未添加）
        if caller_result not in function_info['caller_results']:
            function_info['caller_results'].append(caller_result)
    
    # 将结果写入输出文件
    result_dict = {}
    for function_info in all_functions:
        function_name = function_info['function_name']
        result_dict[function_name] = function_info
    
    # 检查输入文件格式，决定输出格式
    with open(input_file, 'r', encoding='utf-8') as file:
        input_data = json.load(file)
    
    # 如果输入是列表格式，输出也用列表格式
    if isinstance(input_data, list):
        output_data = list(result_dict.values())
    else:
        # 如果输入是字典格式，输出也用字典格式
        output_data = result_dict
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"处理完成，已将结果保存到 {output_file}")
    
    # 统计SQL类型
    sql_type_counts = {"SELECT": 0, "INSERT": 0, "UPDATE": 0, "DELETE": 0, "OTHER": 0}
    caller_count = 0
    
    for function_info in all_functions:
        caller_results = function_info.get('caller_results', [])
        caller_count += len(caller_results)
        
        for caller_result in caller_results:
            for sql_type in caller_result.get('sql_types', []):
                if sql_type in sql_type_counts:
                    sql_type_counts[sql_type] += 1
    
    print(f"SQL类型统计: {sql_type_counts}")
    print(f"总计处理了 {caller_count} 个调用者")
    
    return valid_count, invalid_count



def process_json_file(input_file, output_file, concurrency=80):
    """同步版本的处理函数"""
    return asyncio.run(process_json_file_async(input_file, output_file, concurrency))

# 添加输入验证
def validate_input_file(input_file):
    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # 验证必要字段
        if isinstance(data, dict):
            # 如果是字典类型，按原来的方式处理
            for function_name, function_info in data.items():
                if 'code_value' not in function_info:
                    print(f"警告: {function_name} 缺少 code_value 字段")
        elif isinstance(data, list):
            # 如果是列表类型，检查每个元素是否包含必要字段
            for i, function_info in enumerate(data):
                if not isinstance(function_info, dict):
                    print(f"警告: 索引 {i} 处的元素不是字典类型")
                    continue
                if 'code_value' not in function_info:
                    print(f"警告: 索引 {i} 处的元素缺少 code_value 字段")
        else:
            print(f"警告: 输入文件格式不是字典或列表类型，而是 {type(data)}")
            return False
            
        return True
    except Exception as e:
        print(f"输入文件验证失败: {e}")
        return False

# 添加SQL分类功能
def classify_sql(sql_statement):
    # 检查是否是字典类型（处理参数依赖的SQL变体）
    if isinstance(sql_statement, dict):
        # 如果是参数依赖的SQL，返回特殊类型
        if "type" in sql_statement and sql_statement["type"] == "param_dependent":
            return "PARAM_DEPENDENT"
        # 尝试从字典中获取第一个SQL语句进行分类
        if "sql" in sql_statement and isinstance(sql_statement["sql"], str):
            sql_lower = sql_statement["sql"].lower().strip()
        elif "variants" in sql_statement and len(sql_statement["variants"]) > 0:
            # 使用第一个变体的SQL进行分类
            first_variant = sql_statement["variants"][0]
            if "sql" in first_variant and isinstance(first_variant["sql"], str):
                sql_lower = first_variant["sql"].lower().strip()
            else:
                return "OTHER"
        else:
            return "OTHER"
    elif isinstance(sql_statement, str):
        # 原始的字符串处理逻辑
        sql_lower = sql_statement.lower().strip()
    else:
        # 处理其他类型
        return "OTHER"
    
    # 分类逻辑
    if sql_lower.startswith("select"):
        return "SELECT"
    elif sql_lower.startswith("insert"):
        return "INSERT"
    elif sql_lower.startswith("update"):
        return "UPDATE"
    elif sql_lower.startswith("delete"):
        return "DELETE"
    else:
        return "OTHER"

# 添加缺失的函数
async def send_request_async(question, semaphore):
    async with semaphore:
        client = openai.AsyncClient(
            base_url="http://62.234.167.136:8081/v1", 
            api_key="EMPTY"
        )
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = await client.chat.completions.create(
                    model="default",
                    messages=[
                        {"role": "system", "content": ""},
                        {"role": "user", "content": question},
                    ],
                    temperature=0.7,
                    max_tokens=8096,
                )
                return response.choices[0].message.content
            except Exception as e:
                retry_count += 1
                print(f"{question[:50]}... 重试 {retry_count}/{max_retries}, 错误: {e}")
                await asyncio.sleep(1)
        
        # 如果所有重试都失败，返回错误信息
        return f"请求失败: {question[:50]}..."

async def verify_sql_async(sql_statement, code_value=None, code_meta_data=None, caller=None, semaphore=None, sql_pattern_cnt=None):
    async with semaphore:
        client = openai.AsyncClient(
            base_url="http://62.234.167.136:8081/v1", 
            api_key="EMPTY"
        )
        
        # 构建提示词，使用CODE_ORM_MYSQL_SQL_VERIFY模板
        code_chain = ""
        if code_meta_data and len(code_meta_data) > 0:
            for meta in code_meta_data:
                if isinstance(meta, str):
                    code_chain += f"{meta}\n"
                elif isinstance(meta, dict) and 'code' in meta:
                    code_chain += f"{meta.get('code', '')}\n"
        
        prompt = CODE_ORM_MYSQL_SQL_VERIFY.format(
            code_value=code_value if code_value else "",
            caller=caller if caller else "",
            code_chain=code_chain,
            sql_statement=sql_statement,
            sql_pattern_cnt=sql_pattern_cnt if sql_pattern_cnt is not None else ""
        )
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = await client.chat.completions.create(
                    model="default",
                    messages=[
                        {"role": "system", "content": "你是一个SQL专家，擅长分析和修正SQL语句。"},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.7,
                    max_tokens=8096,
                )
                return response.choices[0].message.content
            except Exception as e:
                retry_count += 1
                print(f"验证SQL时出错，正在重试 {retry_count}/{max_retries}: {e}")
                await asyncio.sleep(1)
        
        # 如果所有重试都失败，返回原始SQL
        print(f"验证SQL失败，返回原始SQL")
        return sql_statement

async def format_sql_async(sql_statement, semaphore):
    async with semaphore:
        client = openai.AsyncClient(
            base_url="http://62.234.167.136:8081/v1", 
            api_key="EMPTY"
        )
        
        # 构建提示词，使用CODE_ORM_MYSQL_SQL_FORMAT模板
        prompt = CODE_ORM_MYSQL_SQL_FORMAT.format(
            sql_statement=sql_statement
        )
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = await client.chat.completions.create(
                    model="default",
                    messages=[
                        {"role": "system", "content": "你是一个SQL格式化专家，擅长将SQL语句转换为标准JSON格式。"},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.7,
                    max_tokens=8096,
                )
                
                # 尝试解析响应为JSON数组
                formatted_response = response.choices[0].message.content.strip()
                try:
                    # 检查是否包含```json标记
                    if "```json" in formatted_response:
                        # 提取json部分
                        match = re.search(r'```json\s*([\s\S]*?)```', formatted_response)
                        if match:
                            json_content = match.group(1).strip()
                            # 解析提取出的json内容
                            sql_list = json.loads(json_content)
                            return sql_list
                    
                    # 检查是否已经是JSON数组格式
                    if formatted_response.startswith('[') and formatted_response.endswith(']'):
                        sql_list = json.loads(formatted_response)
                        return sql_list
                    else:
                        # 尝试分割SQL语句
                        sql_statements = [stmt.strip() for stmt in formatted_response.split(';') if stmt.strip()]
                        sql_statements = [f"{stmt};" for stmt in sql_statements]
                        # 移除最后一个语句末尾多余的分号
                        if sql_statements and sql_statements[-1].endswith(';;'):
                            sql_statements[-1] = sql_statements[-1][:-1]
                        return sql_statements
                except json.JSONDecodeError:
                    # 如果不是有效的JSON，尝试分割SQL语句
                    sql_statements = [stmt.strip() for stmt in formatted_response.split(';') if stmt.strip()]
                    sql_statements = [f"{stmt};" for stmt in sql_statements]
                    # 移除最后一个语句末尾多余的分号
                    if sql_statements and sql_statements[-1].endswith(';;'):
                        sql_statements[-1] = sql_statements[-1][:-1]
                    return sql_statements
                
            except Exception as e:
                retry_count += 1
                print(f"格式化SQL时出错，正在重试 {retry_count}/{max_retries}: {e}")
                await asyncio.sleep(1)
        
        # 如果所有重试都失败，尝试简单分割
        print(f"格式化SQL失败，尝试简单分割")
        sql_statements = [stmt.strip() for stmt in sql_statement.split(';') if stmt.strip()]
        sql_statements = [f"{stmt};" for stmt in sql_statements]
        # 移除最后一个语句末尾多余的分号
        if sql_statements and sql_statements[-1].endswith(';;'):
            sql_statements[-1] = sql_statements[-1][:-1]
        return sql_statements

# 添加新的函数用于验证SQL语句完整性
def validate_sql_completeness(sql_list):
    """验证SQL语句是否完整，没有省略号或类似的占位符"""
    validated_list = []
    
    # 尝试修复不正确的JSON格式
    if isinstance(sql_list, str):
        sql_list = fix_malformed_json_array(sql_list)
    
    # 如果仍然是字符串，转换为列表
    if isinstance(sql_list, str):
        sql_list = [sql_list]
    
    for item in sql_list:
        if isinstance(item, str):
            # 检查字符串中是否有省略号或[其他字段]类型的占位符
            if "..." in item or "[其他" in item or "其他]" in item:
                # 尝试修复或标记为不完整
                print(f"发现不完整SQL语句: {item}")
                # 这里可以添加修复逻辑或直接标记
                validated_list.append(f"不完整SQL语句: {item}")
            else:
                validated_list.append(item)
        elif isinstance(item, dict) and "variants" in item:
            # 检查每个变体
            fixed_variants = []
            for variant in item.get("variants", []):
                sql = variant.get("sql", "")
                if "..." in sql or "[其他" in sql or "其他]" in sql:
                    print(f"发现不完整SQL变体: {sql}")
                    # 这里可以添加修复逻辑或直接标记
                    variant["sql"] = f"不完整SQL语句: {sql}"
                fixed_variants.append(variant)
            
            item["variants"] = fixed_variants
            validated_list.append(item)
        else:
            validated_list.append(item)
    
    return validated_list

def fix_malformed_json_array(json_str):
    """修复格式不正确的JSON数组字符串"""
    # 如果是字符串内的JSON数组，尝试提取并解析
    try:
        # 尝试直接解析
        return json.loads(json_str)
    except json.JSONDecodeError:
        # 如果解析失败，尝试修复常见问题
        
        # 检查是否是引号内的JSON字符串（如示例中的情况）
        if json_str.startswith('"[') and json_str.endswith(']"'):
            # 移除外层引号并转义内部引号
            inner_json = json_str[1:-1].replace('\\"', '"')
            try:
                return json.loads(inner_json)
            except json.JSONDecodeError:
                pass
        
        # 检查是否有多余的转义字符
        cleaned = json_str.replace('\\n', '\n').replace('\\"', '"')
        if cleaned != json_str:
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError:
                pass
        
        # 更彻底的修复尝试 - 提取所有可能的SQL语句
        return extract_sql_statements(json_str)

def extract_sql_statements(text):
    """从文本中提取SQL语句"""
    # 这个函数尝试从文本中提取SQL语句，适用于LLM返回了带有说明的文本而不是纯JSON
    
    # 尝试提取param_dependent格式的SQL
    param_dependent_matches = re.findall(r'{\s*"type"\s*:\s*"param_dependent"[^}]*"variants"\s*:\s*\[.*?\]\s*}', text, re.DOTALL)
    
    # 一般性SQL语句提取
    # 查找以SELECT、INSERT、UPDATE、DELETE等开头，以分号结尾的语句
    sql_matches = re.findall(r'(?:SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)[\s\S]*?;', text, re.IGNORECASE)
    
    # 合并结果
    result = []
    
    # 添加param_dependent类型
    for match in param_dependent_matches:
        try:
            # 尝试将提取的内容解析为JSON
            parsed = json.loads(match)
            result.append(parsed)
        except json.JSONDecodeError:
            # 如果解析失败，将其作为字符串添加
            result.append(match)
    
    # 添加常规SQL语句
    for match in sql_matches:
        # 检查是否已经作为param_dependent的一部分添加
        already_added = False
        for item in result:
            if isinstance(item, dict) and 'variants' in item:
                for variant in item['variants']:
                    if match in variant.get('sql', ''):
                        already_added = True
                        break
        
        if not already_added:
            result.append(match)
    
    # 如果没有找到任何SQL语句，将原始文本分割为语句
    if not result:
        statements = [stmt.strip() for stmt in text.split(';') if stmt.strip()]
        statements = [f"{stmt};" for stmt in statements if not stmt.startswith('{') and not stmt.startswith('[')]
        result.extend(statements)
    
    return result

# 添加函数用于比较两个SQL语句是否重复
def compare_sql_statements(sql1, sql2):
    """比较两个SQL语句是否实质上相同"""
    # 如果两个语句完全相同
    if sql1 == sql2:
        return True
    
    # 如果一个是字符串，一个是字典，它们不相同
    if (isinstance(sql1, str) and isinstance(sql2, dict)) or \
       (isinstance(sql1, dict) and isinstance(sql2, str)):
        return False
    
    # 如果都是字符串，进行简化比较
    if isinstance(sql1, str) and isinstance(sql2, str):
        # 移除空格、换行和分号进行比较
        simplified1 = re.sub(r'\s+', ' ', sql1).strip().rstrip(';').lower()
        simplified2 = re.sub(r'\s+', ' ', sql2).strip().rstrip(';').lower()
        return simplified1 == simplified2
    
    # 如果都是字典（变体SQL）
    if isinstance(sql1, dict) and isinstance(sql2, dict):
        # 如果类型不同
        if sql1.get('type') != sql2.get('type'):
            return False
        
        # 比较变体数量
        variants1 = sql1.get('variants', [])
        variants2 = sql2.get('variants', [])
        
        if len(variants1) != len(variants2):
            return False
        
        # 简单检查：检查是否有相同数量的变体具有相同的SQL
        sql_set1 = set()
        for variant in variants1:
            if 'sql' in variant:
                simplified = re.sub(r'\s+', ' ', variant['sql']).strip().rstrip(';').lower()
                sql_set1.add(simplified)
        
        sql_set2 = set()
        for variant in variants2:
            if 'sql' in variant:
                simplified = re.sub(r'\s+', ' ', variant['sql']).strip().rstrip(';').lower()
                sql_set2.add(simplified)
        
        # 如果两个集合有重叠，认为它们可能是相同的SQL
        return len(sql_set1.intersection(sql_set2)) > 0
    
    return False

if __name__ == '__main__':
    # 导入必要的库
    import argparse
    
    # 配置文件路径
    input_file = ''
    output_file = ''

    # 添加命令行参数支持
    parser = argparse.ArgumentParser(description='分析ORM代码有效性并生成SQL语句')
    parser.add_argument('--input', type=str, default=input_file, help='输入JSON文件路径')
    parser.add_argument('--output', type=str, default=output_file, help='输出JSON文件路径')
    parser.add_argument('--concurrency', type=int, default=80, help='并发请求数量')
    args = parser.parse_args()
    
    # 处理JSON文件
    valid_count, invalid_count = process_json_file(
        args.input, 
        args.output, 
        args.concurrency
    )
    
    print(f"统计结果: 有效ORM {valid_count}个, 无效ORM {invalid_count}个")
