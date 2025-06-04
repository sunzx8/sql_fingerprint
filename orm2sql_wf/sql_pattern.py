import json
import asyncio
import os
import openai
from pathlib import Path
import re
import argparse
# 第一轮提示词模板：SQL骨架分析
PROMPT_TEMPLATE_FIRST_ROUND = r"""
# 角色
你是一名资深数据库 DBA，擅长阅读 Go-ORM 代码并抽象 SQL 骨架。

# 任务
对给定的 ORM 代码 + 模型-表映射做三步分析：

1. 列出全部潜在 SQL 骨架  
   - 每条骨架给唯一编号 (SQL-1, SQL-2 …)  
   - 指明 查询类型 / 主表(含别名) / **必选 WHERE 条件**

2. 推断各骨架可能的可变要素  
   - SELECT 字段、JOIN、条件、GROUP BY、HAVING、ORDER BY、分页方案  
   - 标出哪些必选、可选、可重复

3. 为后续模板准备"控件清单"  
   - 给出 OPT_BLOCK 名及其内部应包含的字段 / 条件 / 子句

# 输出格式（示例）
### SQL-1
- 查询类型: SELECT  
- 主表: t_user AS u  
- 必选 WHERE: u.id = ?  
- 控件清单  
  * PROJECTION: 必选 id,name，可选 age, LOOP extra_fields  
  * FILTERS   : 可选 status=?, LOOP OR tag in (?)  
  * …

# 输入
① ORM 代码片段:
{orm_code}

② 模型-表结构映射:
{code_meta_data}

③ 调用者信息:
{caller}
"""

# 第二轮提示词模板：结果生成
PROMPT_TEMPLATE_SECOND_ROUND = r"""
你是一名资深数据库工程师，请基于以下输入，
为每个 SQL-n 骨架生成 **单一且完整** 的动态 SQL 模板，
必须严格遵守"DTMySQL 动态宏语法 & 解析约束"。


A. DTMySQL 动态宏语法（4 个核心宏）

1. OPT_BLOCK <name> [ items… ]  
   - 定义一个可整体出现/省略的块
   - <name>是块的唯一标识符，同名块只能定义一次
   - 方括号[]内放置表达式、其他宏或SQL片段

2. REQUIRED( items… )  
   - 块内基本内容必须全部出现
   - 只能在OPT_BLOCK内部使用
   - 括号内为逗号分隔的表达式列表

3. OPTIONAL( items… )  
   - 块内可选内容最多只出现一个
   - 只能在OPT_BLOCK内部使用
   - 括号内为逗号分隔的表达式列表

4. LOOP( items… )  
   - 允许0~N次重复的内容
   - 只能在OPT_BLOCK内部使用
   - 括号内为逗号分隔的表达式列表
   - 渲染端自动添加适当的分隔符


B. 解析硬规则（**严格遵守，违者解析失败**）

1. SQL关键字与宏的关系
   • SELECT / FROM / WHERE / GROUP BY / HAVING / ORDER BY / LIMIT等关键字
     **必须独立写在最外层**，不能放入宏内部
   • REQUIRED / OPTIONAL / LOOP **必须且只能在OPT_BLOCK内部**
   • 每个SQL子句(如SELECT, WHERE)内的宏应独立设计，不要跨子句引用

2. SQL基本元素的处理
   • 表名、列名等基本元素**必须使用实际标识符**，不能用注释代替
   • 不允许在FROM子句后面仅使用注释而不指定表名
   • 动态表名应当使用固定表名，不同表结构必须使用不同的SQL骨架
   • 所有SQL语句关键部分必须有实际内容，不能仅有注释

3. 表达式列表格式规范
   • 列表元素 **逗号写行尾**，禁止行首逗号
   • 列表末尾不能有多余逗号
   • 所有宏的括号内首个元素不能是 `,` / `AND` / `OR`
   • SQL关键字作为标识符时需用反引号(如 \`Order\`, \`Limit\`, \`Select\`, \`Charset\`, \`Collation\`)

4. 不同子句的特殊处理
   • SELECT子句: 字段列表是投影表达式
   • ORDER BY子句: ASC/DESC写在宏外部(如`OPTIONAL(col1, col2) DESC`)
   • GROUP BY子句: 遵循标准GROUP BY语法规则
   • WHERE子句: 条件表达式，注意逻辑运算符放宏外部

5. JOIN语句的特殊约束
   • JOIN子句中的表名和字段名**不能使用任何动态宏语法**
   • JOIN的ON条件中也不能使用动态宏
   • 如果JOIN内容是可变的，必须将其视为不同的SQL骨架(SQL-n)
   • JOIN总是作为固定部分出现在查询中

6. 宏命名与嵌套规则
   • 每个OPT_BLOCK必须有唯一名称，同名块只定义一次
   • 引用已定义的OPT_BLOCK时必须保持结构一致
   • 每个宏内至少有一个合法表达式，避免空块
   • 动态条件也需要包含占位符，不可为空

7. LIMIT与分页处理
   • 模板中使用 `LIMIT ?` 
   • 如需偏移量，在渲染时追加 `OFFSET ?`


C. 实际示例（正确格式）

1. 字段投影示例:
```sql
SELECT 
  OPT_BLOCK projection [
    REQUIRED(u.id, u.name),
    OPTIONAL(u.age, u.email, u.phone),
    LOOP(u.tag_)
  ]
FROM users u
```

2. WHERE条件示例:
```sql
SELECT * FROM users u
WHERE 
  REQUIRED(u.status = ?) AND
  OPT_BLOCK user_filters [
    OPTIONAL(u.age > ?, u.age BETWEEN ? AND ?)
  ] AND
  OPT_BLOCK tag_filters [
    LOOP(u.tag = ?)
  ]
```

3. JOIN示例（注意JOIN表和条件必须是固定的）:
```sql
SELECT u.* 
FROM users u
LEFT JOIN roles r ON u.id = r.user_id
WHERE OPT_BLOCK role_conditions [
  OPTIONAL(r.type = ?, r.status = ?)
]
```

4. 不同JOIN作为不同SQL骨架示例:
```sql
-- SQL-1: 用户查询不带角色
SELECT u.* FROM users u
WHERE u.status = ?

-- SQL-2: 用户查询带角色
SELECT u.*, r.name AS role_name 
FROM users u
LEFT JOIN roles r ON u.id = r.user_id
WHERE u.status = ?
```

5. 不正确用法示例（错误示范）:
```sql
-- 错误：FROM后面不能只用注释
SELECT * FROM /* 动态表名 */ WHERE id = ?

-- 错误：JOIN的表不能用注释或宏
SELECT * FROM users u LEFT JOIN /* 动态表名 */ j ON u.id = j.user_id

-- 错误：必须使用实际表名，不同表应该是不同SQL骨架
SELECT * FROM table_?
```

6. 正确的动态内容示例:
```sql
-- 正确：必须使用实际表名
SELECT * FROM users WHERE id = ?

-- 正确：不同表结构应该用不同SQL骨架
-- SQL-1（用户表查询）
SELECT * FROM users WHERE id = ?
-- SQL-2（订单表查询）
SELECT * FROM orders WHERE order_id = ?
```

D. 输出要求

- 每个 SQL-n 只生成 **1 个完整模板** (包含所有可能变体)
- 使用标准英文SQL，注释可使用中文
- 每个模板前标注 `-- SQL-n` 
- 不要添加额外解释文字


输入数据

1. 前置分析结果：
{first_round_result}

2. ORM 代码片段:
{orm_code}

3. 模型-表结构映射:
{code_meta_data}
"""

# 第三轮提示词模板：格式化处理
PROMPT_TEMPLATE_THIRD_ROUND = r"""
你是一名 SQL 格式化专家，请将第二轮生成的模板进行严格的"格式化+JSON列表化"处理，
确保完全符合sqlglot解析器的语法要求。

JSON结构要求:
[
  {{
    "id": "SQL-1",
    "template": "<格式化后的完整模板字符串>"
  }},
  …
]

格式化硬性规则:

1. 宏结构与命名
   • OPT_BLOCK后必须有一个空格接块名，再接一个空格和左方括号
   • 同名OPT_BLOCK只能定义一次，引用必须一致
   • 每个宏(REQUIRED/OPTIONAL/LOOP)必须包含至少一个表达式

2. 宏括号与格式
   • 所有函数宏使用圆括号()，OPT_BLOCK内容使用方括号[]
   • 括号内表达式用逗号分隔，逗号必须在行尾而非行首
   • 括号内首个元素不能是逗号、AND或OR

3. SQL语句格式
   • 保持SQL关键字(SELECT/FROM/WHERE等)在最外层
   • 不同子句间用空行分隔，增强可读性
   • 适当缩进(2或4空格)保持结构清晰

4. SQL基本元素的处理
   • FROM子句后必须有实际表名，不能只有注释
   • 表名和列名必须使用实际标识符，不能用注释替代
   • 所有SQL语句必须有完整的基本结构，不能缺少必要部分
   • 动态表名应当使用固定表名，不同表结构使用不同SQL骨架

5. JSON输出格式
   • 确保template字符串内正确处理转义字符
   • 多行SQL使用\\n连接，不使用实际换行
   • 不要在JSON外添加任何说明文字

6. 错误规避特别提示
   • 检查并修复LOOP块内可能的空内容
   • 确保所有动态条件都有占位符(?或具体值)
   • 确保字段名如果是SQL关键字已用反引号包裹
   • 确保JOIN子句中不包含任何动态宏语法
   • 检查FROM子句后是否有实际表名，不能只有注释

7. 禁止使用的模式
   • FROM /* 动态表名 */ - 不允许
   • JOIN /* 动态表名 */ - 不允许
   • table_? 或类似形式 - 不允许

输出结果必须是可直接被json.loads()解析的纯JSON，不要添加任何其他文本。

原始模板文本:
{second_round_result}
"""

async def send_request_async(question, semaphore):
    """发送API请求并获取结果"""
    async with semaphore:
        client = openai.AsyncClient(
            base_url="http://0.0.0.0:8081/v1", 
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
                    max_tokens=8096
                )
                return response.choices[0].message.content
            except Exception as e:
                retry_count += 1
                print(f"{question[:50]}... 重试 {retry_count}/{max_retries}, 错误: {e}")
                await asyncio.sleep(1)
        
        # 如果所有重试都失败，返回错误信息
        return f"请求失败: {question[:50]}..."

async def process_item(item_key, item_data, semaphore, output_dir):
    """处理单个JSON项目并执行三轮请求"""
    try:
        # 提取ORM代码和模型映射
        function_definition = item_data.get("function_definition", "")
        code_meta_data = item_data.get("code_meta_data", [])
        code_meta_data = "\n".join(code_meta_data)
        caller = item_data.get("caller", "")
        if caller == "":
            pass
        else:
            caller = caller[0]
        # 第一轮：SQL骨架分析
        first_round_prompt = PROMPT_TEMPLATE_FIRST_ROUND.format(
            orm_code=function_definition,
            code_meta_data=code_meta_data,
            caller=caller
        )
        
        print(f"处理项目: {item_key[:50]}...")
        first_round_result = await send_request_async(first_round_prompt, semaphore)
        
        # 第二轮：结果生成
        second_round_prompt = PROMPT_TEMPLATE_SECOND_ROUND.format(
            orm_code=function_definition,
            code_meta_data=code_meta_data,
            caller=caller,
            first_round_result=first_round_result
        )
        
        second_round_result = await send_request_async(second_round_prompt, semaphore)
        
        # 第三轮：格式化处理
        third_round_prompt = PROMPT_TEMPLATE_THIRD_ROUND.format(
            second_round_result=second_round_result
        )
        
        third_round_result = await send_request_async(third_round_prompt, semaphore)
        
        # 尝试解析第三轮结果为JSON
        try:
            formatted_templates = clean_and_parse_sql_templates(third_round_result)
        except json.JSONDecodeError:
            # 如果无法解析为JSON，保留原始结果
            formatted_templates = {"error": "无法解析为JSON格式", "raw_result": third_round_result}
        
        # 保存结果
        result = {
            "first_round_result": first_round_result,
            "second_round_result": second_round_result,
            "third_round_result": third_round_result,
            "formatted_templates": formatted_templates
        }
        
        return item_key, True, item_data, result
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        error_msg = f"处理项目 {item_key} 时出错:\n"
        error_msg += f"错误类型: {type(e).__name__}\n"
        error_msg += f"错误信息: {str(e)}\n"
        error_msg += f"错误详情:\n{error_trace}"
        print(error_msg)
        
        # 如果错误与JSON解析相关，尝试显示原始内容片段
        if isinstance(e, json.JSONDecodeError):
            context = str(e.doc)[:100] + "..." if len(str(e.doc)) > 100 else str(e.doc)
            print(f"JSON解析错误位置附近内容: {context}")
            
        return item_key, False, item_data, {
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": error_trace
        }

def clean_and_parse_sql_templates(raw_result):
    """
    处理第三轮输出，去除markdown代码块、转义字符，并解析为JSON对象。
    """
    # 去除markdown代码块标记
    cleaned = re.sub(r"^```json|```$", "", raw_result.strip(), flags=re.MULTILINE)
    cleaned = cleaned.strip()
    
    # 输出清理前后的内容以便调试
    print(f"清理前内容片段: {raw_result[:100]}...")
    print(f"清理后内容片段: {cleaned[:100]}...")
    
    # 如果有多余的转义字符，先尝试直接解析
    try:
        templates = json.loads(cleaned)
        print("JSON解析成功")
        return templates
        
    except json.JSONDecodeError as e:
        print(f"初次JSON解析错误: {str(e)}")
        print(f"错误位置: 行 {e.lineno}, 列 {e.colno}, 位置 {e.pos}")
        
        # 显示错误位置附近的内容
        error_context = cleaned[max(0, e.pos-30):min(len(cleaned), e.pos+30)]
        print(f"错误位置上下文: ...{error_context}...")
        
        # 替换常见的转义字符
        cleaned = cleaned.replace("\\n", "\n").replace('\\"', '"')
        print(f"替换转义字符后内容片段: {cleaned[:100]}...")
        
        # 再次尝试解析
        try:
            templates = json.loads(cleaned)
            print("第二次尝试JSON解析成功")
            return templates
            
        except Exception as e2:
            # 最后兜底，返回原始内容和错误
            print(f"第二次JSON解析仍然失败: {str(e2)}")
            
            # 尝试通过正则表达式直接提取JSON数组格式
            json_pattern = r'\[\s*\{\s*"id"\s*:\s*"[^"]+"\s*,\s*"template"\s*:\s*"[^"]+"\s*\}(?:\s*,\s*\{\s*"id"\s*:\s*"[^"]+"\s*,\s*"template"\s*:\s*"[^"]+"\s*\})*\s*\]'
            json_match = re.search(json_pattern, cleaned)
            
            if json_match:
                try:
                    final_json = json_match.group(0)
                    print(f"通过正则表达式提取到JSON: {final_json[:100]}...")
                    templates = json.loads(final_json)
                    print("正则表达式提取JSON解析成功")
                    return templates
                except Exception as e3:
                    print(f"正则表达式提取JSON后解析仍然失败: {str(e3)}")
            
            return {
                "error": "无法解析为JSON", 
                "raw": cleaned, 
                "exception": {
                    "first_attempt": str(e),
                    "second_attempt": str(e2),
                    "regex_attempt": "未尝试" if not json_match else str(e3) if 'e3' in locals() else "成功"
                }
            }

async def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='SQL模式生成工具')
    parser.add_argument('--output-dir', '-o', 
                       default='',
                       help='输出目录路径 (默认: )')
    parser.add_argument('--input-file', '-i',
                       default='',
                       help='输入JSON文件路径 (默认: )')
    parser.add_argument('--output-filename', '-f',
                       default='',
                       help='输出文件名 (默认: )')
    parser.add_argument('--max-concurrent', '-c',
                       type=int, default=80,
                       help='最大并发请求数 (默认: 80)')
    
    args = parser.parse_args()
    
    # 使用解析的参数
    output_dir = args.output_dir
    input_file = args.input_file
    output_filename = args.output_filename
    max_concurrent = args.max_concurrent
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 读取输入文件
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"读取输入文件时出错: {e}")
        return
    
    # 设置并发数
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # 创建所有任务
    tasks = []
    for item_key, item_data in data.items():
            
        task = process_item(item_key, item_data, semaphore, output_dir)
        tasks.append(task)
        # break  # 测试时只处理一个项目
    
    # 执行所有任务
    results = await asyncio.gather(*tasks)
    
    # 统计结果
    success_count = sum(1 for _, success, _, _ in results if success)
    total_count = len(tasks)
    
    print(f"处理完成: {success_count}/{total_count} 项成功")
    
    # 统计错误类型
    error_types = {}
    for key, success, _, output in results:
        if not success and "error_type" in output:
            error_type = output["error_type"]
            if error_type not in error_types:
                error_types[error_type] = []
            error_types[error_type].append(key)
    
    # 打印错误类型统计
    print("\n错误类型统计:")
    for error_type, keys in error_types.items():
        print(f"- {error_type}: {len(keys)}个错误")
        # 显示前3个示例
        for i, key in enumerate(keys[:3]):
            print(f"  示例{i+1}: {key}")
    
    # 生成汇总结果
    summary = {
        "total": total_count,
        "success": success_count,
        "success_rate": success_count / total_count if total_count > 0 else 0,
        "error_types": {error_type: len(keys) for error_type, keys in error_types.items()},
        "items": {key: success for key, success, _, _ in results}
    }
    
    # 假设 results = [(key, success, input_data, model_output), ...]
    detailed_results = []
    for key, success, input_data, model_output in results:
        detailed_results.append({
            "key": key,
            "success": success,
            "input_data": input_data,
            "model_output": model_output
        })
    path = os.path.join(output_dir, output_filename)
    # 写入详细结果
    with open(path, "w", encoding="utf-8") as f:
        json.dump(detailed_results, f, ensure_ascii=False, indent=2)
    
        

    print(f"已写入详细结果: {path}")

if __name__ == "__main__":
    asyncio.run(main())
