import typing as t
import inspect
import json
import traceback
from sqlglot import exp, parse_one
from sqlglot.tokens import TokenType
from sqlglot.expressions import Expression, Func, Insert
from sqlglot.dialects.mysql import MySQL

def clean_sql_text(sql_text: str) -> str:
    """
    清洗SQL文本，处理转义字符和格式问题
    """
    # 处理 \n 转义字符，将其替换为实际的换行符
    if "\\n" in sql_text:
        sql_text = sql_text.replace("\\n", "\n")
    
    # 将 { 替换为 [，} 替换为 ]，以适应解析器期望的语法
    sql_text = sql_text.replace("{", "[").replace("}", "]")
    
    # 移除开头和结尾的多余空白字符
    sql_text = sql_text.strip()
    
    print("清洗后的SQL文本:")
    print("-" * 50)
    print(sql_text)
    print("-" * 50)
    
    return sql_text
class OptBlockInClause:
    EXPRESSION = 0
    PROJECTION = 1
    GROUP_BY = 2
    ORDER_BY = 3


clause_name_dict = {
    OptBlockInClause.EXPRESSION: "expression",
    OptBlockInClause.PROJECTION: "projection",
    OptBlockInClause.GROUP_BY: "group_by",
    OptBlockInClause.ORDER_BY: "order_by",
}


parse_caller_to_clause_dict = {
    "_parse_projections": OptBlockInClause.PROJECTION,
    "_parse_group": OptBlockInClause.GROUP_BY,
    "_parse_order": OptBlockInClause.ORDER_BY,
}


class Replace(Insert):
    pass


class OptionalFunc(Func):
    arg_types = {"this": True}
    is_var_len_args = True


class RequiredFunc(Func):
    arg_types = {"this": True}
    is_var_len_args = True


class LoopFunc(Func):
    arg_types = {"this": True}
    is_var_len_args = True


class OptBlock(Func):
    arg_types = {"this": True, "blocks": False}


opt_func_to_class_dict = {
    "OPTIONAL": OptionalFunc,
    "REQUIRED": RequiredFunc,
    "LOOP": LoopFunc,
}


def build_optional_func(args: t.List) -> exp.Expression:
    return OptionalFunc(this=list(args))


def build_required_func(args: t.List) -> exp.Expression:
    return RequiredFunc(this=list(args))


def build_loop_func(args: t.List) -> exp.Expression:
    return LoopFunc(this=list(args))


def opt_block_sql(self: MySQL.Generator, expression: OptBlock) -> str:
    blocks = expression.args["blocks"]
    blocks_sql_list = [self.sql(b) for b in blocks]
    sql = "OPT_BLOCK " + expression.name + "[" + ", ".join(blocks_sql_list) + "]"
    return sql


def required_sql(self: MySQL.Generator, expression: RequiredFunc):
    args = expression.args["this"]
    args_sql_list = [self.sql(arg) for arg in args]
    sql = "REQUIRED (" + ", ".join(args_sql_list) + ")"
    return sql


def optional_sql(self: MySQL.Generator, expression: OptionalFunc):
    args = expression.args["this"]
    args_sql_list = [self.sql(arg) for arg in args]
    sql = "OPTIONAL (" + ", ".join(args_sql_list) + ")"
    return sql


def loop_sql(self: MySQL.Generator, expression: LoopFunc):
    args = expression.args["this"]
    args_sql_list = [self.sql(arg) for arg in args]
    sql = "LOOP (" + ", ".join(args_sql_list) + ")"
    return sql


class DTMySQL(MySQL):
    class Tokenizer(MySQL.Tokenizer):
        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
        }

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
        }

        NO_PAREN_FUNCTION_PARSERS = {
            **MySQL.Parser.NO_PAREN_FUNCTION_PARSERS,
            "OPTIONAL": lambda self: self._parse_opt_func("OPTIONAL"),
            "REQUIRED": lambda self: self._parse_opt_func("REQUIRED"),
            "LOOP": lambda self: self._parse_opt_func("LOOP"),
            "OPT_BLOCK": lambda self: self._parse_opt_block()
        }

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.opt_block_in_clause = OptBlockInClause.EXPRESSION

        def _parse_opt_func(self, func_name) -> t.Optional[exp.Expression]:
            comments = self._prev_comments

            if not self._match(TokenType.L_PAREN):
                self.raise_error(f"Expected ( after {func_name}", self._prev)

            args = list()
            if self._match(TokenType.R_PAREN):
                pass
            else:
                if self.opt_block_in_clause == OptBlockInClause.PROJECTION:
                    args.extend(self._parse_projections())
                elif self.opt_block_in_clause == OptBlockInClause.GROUP_BY:
                    args.extend(self._parse_csv(self._parse_assignment))
                elif self.opt_block_in_clause == OptBlockInClause.ORDER_BY:
                    args.extend(self._parse_csv(self._parse_ordered))
                else:
                    args.extend(self._parse_expressions())
                if not self._match(TokenType.R_PAREN):
                    self.raise_error(f"Expected ) after {func_name}", self._prev)

            func_class = opt_func_to_class_dict.get(func_name)
            if func_class is None:
                self.raise_error(f"Opt block function {func_name} is not supported", self._prev)

            return self.expression(
                func_class, comments=comments, this=args
            )

        def _parse_opt_block(self) -> t.Optional[exp.Expression]:
            comments = self._prev_comments
            name = self._parse_id_var().name

            stack = inspect.stack()
            self.opt_block_in_clause = OptBlockInClause.EXPRESSION
            for stack_info in stack:
                opt_block_in_clause = parse_caller_to_clause_dict.get(stack_info.function)
                if opt_block_in_clause is not None:
                    self.opt_block_in_clause = opt_block_in_clause
                    break

            if not self._match(TokenType.L_BRACKET):
                self.raise_error("Expected [ after OPT_BLOCK", self._prev)

            blocks = list()
            if self._match(TokenType.R_BRACKET):
                pass
            else:
                blocks.extend(self._parse_expressions())
                if not self._match(TokenType.R_BRACKET):
                    self.raise_error("Expected ] after OPT_BLOCK", self._prev)

            return self.expression(
                OptBlock, comments=comments, this=name, blocks=blocks
            )

    class Generator(MySQL.Generator):
        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            OptBlock: opt_block_sql,
            RequiredFunc: required_sql,
            OptionalFunc: optional_sql,
            LoopFunc: loop_sql,
        }


dialect = DTMySQL.__name__.lower()


def parse(sql) -> Expression:
    stmt = parse_one(sql, read=dialect)
    stmt.sql()
    return stmt


def generate(stmt: Expression) -> str:
    sql = stmt.sql(dialect=dialect)
    return sql

def verify_sql(sql_text: str) -> bool:
    try:
        stmt_exp = parse_one(sql_text, read=dialect)
        print('-' * 50)
        print('stmt_exp: ', stmt_exp)
        sql_text = stmt_exp.sql(dialect)
        print('-' * 50)
        print('sql_text: ', sql_text)
        print('-' * 50)
        print('success!')
        return True
    except Exception as e:
        print('sqlglot error: ', e)
        return False

if __name__ == "__main__":

    verify_sql_path=""
    check_templates_path=""
    total_success = 0
    total_failed = 0
    include_success_chunk=0
    full_success_chunk=0
    failed_chunk=0
    check_templates=[]
    with open(verify_sql_path, 'r') as f:
        data = json.load(f)
    for item in data:
        template_item = item
        sql_templates = item['model_output']['formatted_templates']
        full_success = True
        include_success = False
        for idx,template in enumerate(sql_templates):
            try:
                clean_sql = clean_sql_text(template['template'])
                if verify_sql(clean_sql):
                    total_success += 1
                    include_success = True
                    template_item['model_output']['formatted_templates'][idx]['success'] = True
                else:
                    total_failed += 1
                    full_success = False
                    template_item['model_output']['formatted_templates'][idx]['success'] = False
            except Exception as e:
                print('traceback.format_exc(): ', traceback.format_exc())
                print('template: ', template)
                pass
        check_templates.append(template_item)
        if full_success:
            full_success_chunk += 1
        elif include_success:
            include_success_chunk += 1
        else:
            failed_chunk += 1
    with open(check_templates_path, 'w', encoding='utf-8') as f:
        json.dump(check_templates, f, indent=4, ensure_ascii=False)
    print('total_success: ', total_success)
    print('total_failed: ', total_failed)
    print('full_success_chunk: ', full_success_chunk)
    print('include_success_chunk: ', include_success_chunk)
    print('failed_chunk: ', failed_chunk)