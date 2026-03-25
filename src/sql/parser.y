%{
#include "ast.h"
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <memory>

using namespace tinydb::sql;

// 词法分析器函数声明
void yyerror(const char* msg);
int yylex(void);

// 全局 AST 指针
extern AST* g_ast;
%}

%code top {
    // 前置声明 AST 类
    namespace tinydb {
    namespace sql {
        class AST;
        class CreateTableStmt;
    }
    }
}

%code provides {
    // 定义全局 AST 指针 - 在这里 AST 类已完整定义
    extern tinydb::sql::AST* g_ast;
}

%code requires {
    #include "ast.h"
    #include <vector>
    #include <memory>
    #include <utility>
    using namespace tinydb::sql;
}

%union {
    char* str;
    Statement* stmt;
    Expression* expr;
    SelectStmt* select_stmt;
    InsertStmt* insert_stmt;
    UpdateStmt* update_stmt;
    DeleteStmt* delete_stmt;
    CreateTableStmt* create_stmt;
    DropTableStmt* drop_stmt;
    AlterTableStmt* alter_stmt;
    BeginStmt* begin_stmt;
    CommitStmt* commit_stmt;
    RollbackStmt* rollback_stmt;
    CreateIndexStmt* create_index_stmt;
    DropIndexStmt* drop_index_stmt;
    AnalyzeStmt* analyze_stmt;
    ExplainStmt* explain_stmt;
    CreateViewStmt* create_view_stmt;
    DropViewStmt* drop_view_stmt;

    // 列表类型
    std::vector<std::unique_ptr<Expression>>* expr_list;
    std::vector<std::vector<std::unique_ptr<Expression>>>* insert_values_list;
    std::vector<std::string>* str_list;
    std::vector<std::pair<std::string, std::string>>* col_def_list;
    std::vector<CreateTableStmt::ColumnDef>* column_def_list_full;
    CreateTableStmt::ColumnDef* column_def;
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>>* assignment_list;

    // ORDER BY support
    int bool_val;
    std::pair<Expression*, bool>* order_by_item;
    std::vector<std::pair<std::unique_ptr<Expression>, bool>>* order_by_list;
    std::pair<int64_t, int64_t>* limit_offset;
}

%define parse.error verbose

/* 运算符优先级（从低到高） */
%left OR
%left AND
%left EQ NE
%left LT LE GT GE
%left PLUS MINUS
%left STAR DIV MOD
%right NOT

/* 关键字 */
%token SELECT INSERT UPDATE DELETE
%token FROM WHERE INTO VALUES SET
%token CREATE DROP TABLE VIEW INDEX ON
%token AND OR NOT NULLX TRUE FALSE
%token PRIMARY KEY UNIQUE FOREIGN REFERENCES
%token AS DISTINCT ALL STAR
%token ORDER BY ASC DESC LIMIT OFFSET
%token GROUP HAVING
%token JOIN INNER LEFT RIGHT OUTER CROSS NATURAL USING
%token IN EXISTS BETWEEN LIKE IS
%token CASE WHEN THEN ELSE END
%token IF CAST DEFAULT CHECK
%token EXPLAIN ANALYZE DESCRIBE SHOW DATABASES TABLES
%token BEGIN TRANSACTION COMMIT ROLLBACK
%token ALTER ADD COLUMN MODIFY RENAME TO TYPE

/* 数据类型 */
%token INT BIGINT SMALLINT TINYINT
%token FLOAT DOUBLE REAL DECIMAL NUMERIC
%token CHAR VARCHAR TEXT
%token DATE TIME TIMESTAMP DATETIME
%token BOOLEAN

/* 标识符和常量 */
%token <str> IDENTIFIER NUMBER STRING_LITERAL

/* 运算符 */
%token EQ NE LT GT LE GE
%token PLUS MINUS MUL DIV MOD CONCAT
%token LPAREN RPAREN COMMA SEMICOLON DOT

/* 类型定义 */
%type <stmt> statement
%type <select_stmt> select_stmt
%type <insert_stmt> insert_stmt
%type <update_stmt> update_stmt
%type <delete_stmt> delete_stmt
%type <create_stmt> create_table_stmt
%type <drop_stmt> drop_table_stmt
%type <alter_stmt> alter_table_stmt
%type <begin_stmt> begin_stmt
%type <commit_stmt> commit_stmt
%type <rollback_stmt> rollback_stmt
%type <create_index_stmt> create_index_stmt
%type <drop_index_stmt> drop_index_stmt
%type <analyze_stmt> analyze_stmt
%type <explain_stmt> explain_stmt
%type <create_view_stmt> create_view_stmt
%type <drop_view_stmt> drop_view_stmt

%type <expr> expr select_item opt_where
%type <expr> comparison_expr logical_expr
%type <expr_list> select_list value_list
%type <str_list> column_list
%type <column_def_list_full> column_def_list_full
%type <column_def> column_def
%type <insert_values_list> insert_values_list
%type <assignment_list> assignment_list

%type <str> table_name column_name data_type
%type <bool_val> opt_asc_desc
%type <order_by_item> order_by_item
%type <order_by_list> opt_order_by order_by_list
%type <limit_offset> opt_limit
%type <expr_list> opt_group_by

%destructor { free($$); } <str>
%destructor { delete $$; } <column_def_list_full>
%destructor { delete $$; } <insert_values_list>
%destructor { delete $$; } <order_by_list>
%destructor { delete $$; } <order_by_item>
%destructor { delete $$; } <limit_offset>

%start statement

%%

statement:
    select_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | insert_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | update_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | delete_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | create_table_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | drop_table_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | alter_table_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | begin_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | commit_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | rollback_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | create_index_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | drop_index_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | analyze_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | explain_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | create_view_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | create_view_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | drop_view_stmt SEMICOLON {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | drop_view_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | select_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | insert_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | update_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | delete_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | create_table_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | drop_table_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | alter_table_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | begin_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | commit_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | rollback_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | create_index_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | drop_index_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | analyze_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    | explain_stmt {
        $$ = $1;
        if (g_ast) g_ast->setStatement(std::unique_ptr<Statement>($$));
    }
    ;

/* SELECT 语句 */
select_stmt:
    SELECT DISTINCT select_list FROM table_name opt_where opt_group_by opt_order_by opt_limit {
        $$ = new SelectStmt();
        $$->setDistinct(true);
        $$->setFromTable($5);
        free($5);
        if ($3) {
            for (auto& expr : *$3) {
                $$->addSelectItem(std::move(expr));
            }
            delete $3;
        }
        if ($6) {
            $$->setWhereCondition(std::unique_ptr<Expression>($6));
        }
        if ($7) {
            // GROUP BY
            for (auto& expr : *$7) {
                $$->addGroupByItem(std::move(expr));
            }
            delete $7;
        }
        // ORDER BY
        if ($8) {
            for (auto& item : *$8) {
                $$->addOrderBy(std::move(item.first), item.second);
            }
            delete $8;
        }
        // LIMIT and OFFSET
        if ($9) {
            if ($9->first >= 0) {
                $$->setLimit($9->first);
            }
            if ($9->second >= 0) {
                $$->setOffset($9->second);
            }
            delete $9;
        }
    }
    | SELECT DISTINCT STAR FROM table_name opt_where opt_group_by opt_order_by opt_limit {
        $$ = new SelectStmt();
        $$->setDistinct(true);
        $$->addSelectItem(std::make_unique<ColumnRefExpr>("*"));
        $$->setFromTable($5);
        free($5);
        if ($6) {
            $$->setWhereCondition(std::unique_ptr<Expression>($6));
        }
        if ($7) {
            // GROUP BY
            for (auto& expr : *$7) {
                $$->addGroupByItem(std::move(expr));
            }
            delete $7;
        }
        // ORDER BY
        if ($8) {
            for (auto& item : *$8) {
                $$->addOrderBy(std::move(item.first), item.second);
            }
            delete $8;
        }
        // LIMIT and OFFSET
        if ($9) {
            if ($9->first >= 0) {
                $$->setLimit($9->first);
            }
            if ($9->second >= 0) {
                $$->setOffset($9->second);
            }
            delete $9;
        }
    }
    | SELECT select_list FROM table_name opt_where opt_group_by opt_order_by opt_limit {
        $$ = new SelectStmt();
        $$->setFromTable($4);
        free($4);
        if ($2) {
            for (auto& expr : *$2) {
                $$->addSelectItem(std::move(expr));
            }
            delete $2;
        }
        if ($5) {
            $$->setWhereCondition(std::unique_ptr<Expression>($5));
        }
        if ($6) {
            // GROUP BY
            for (auto& expr : *$6) {
                $$->addGroupByItem(std::move(expr));
            }
            delete $6;
        }
        // ORDER BY
        if ($7) {
            for (auto& item : *$7) {
                $$->addOrderBy(std::move(item.first), item.second);
            }
            delete $7;
        }
        // LIMIT and OFFSET
        if ($8) {
            if ($8->first >= 0) {
                $$->setLimit($8->first);
            }
            if ($8->second >= 0) {
                $$->setOffset($8->second);
            }
            delete $8;
        }
    }
    | SELECT STAR FROM table_name opt_where opt_group_by opt_order_by opt_limit {
        $$ = new SelectStmt();
        $$->addSelectItem(std::make_unique<ColumnRefExpr>("*"));
        $$->setFromTable($4);
        free($4);
        if ($5) {
            $$->setWhereCondition(std::unique_ptr<Expression>($5));
        }
        if ($6) {
            // GROUP BY
            for (auto& expr : *$6) {
                $$->addGroupByItem(std::move(expr));
            }
            delete $6;
        }
        // ORDER BY
        if ($7) {
            for (auto& item : *$7) {
                $$->addOrderBy(std::move(item.first), item.second);
            }
            delete $7;
        }
        // LIMIT and OFFSET
        if ($8) {
            if ($8->first >= 0) {
                $$->setLimit($8->first);
            }
            if ($8->second >= 0) {
                $$->setOffset($8->second);
            }
            delete $8;
        }
    }
    ;

select_list:
    select_item {
        $$ = new std::vector<std::unique_ptr<Expression>>();
        $$->push_back(std::unique_ptr<Expression>($1));
    }
    | select_list COMMA select_item {
        $$ = $1;
        $$->push_back(std::unique_ptr<Expression>($3));
    }
    ;

select_item:
    expr { $$ = $1; }
    | expr AS column_name {
        $$ = new AliasExpr(std::unique_ptr<Expression>($1), $3);
        free($3);
    }
    | STAR { $$ = new ColumnRefExpr("*"); }
    ;

/* INSERT 语句 */
insert_stmt:
    INSERT INTO table_name VALUES LPAREN value_list RPAREN {
        $$ = new InsertStmt();
        $$->setTable($3);
        free($3);
        if ($6) {
            for (auto& expr : *$6) {
                $$->addValue(std::move(expr));
            }
            delete $6;
        }
    }
    | INSERT INTO table_name VALUES insert_values_list {
        $$ = new InsertStmt();
        $$->setTable($3);
        free($3);
        if ($5) {
            for (auto& row : *$5) {
                $$->addRow();
                for (auto& expr : row) {
                    $$->addValue(std::move(expr));
                }
            }
            delete $5;
        }
    }
    | INSERT INTO table_name LPAREN column_list RPAREN VALUES LPAREN value_list RPAREN {
        $$ = new InsertStmt();
        $$->setTable($3);
        free($3);
        if ($5) {
            for (auto& col : *$5) {
                $$->addColumn(col);
            }
            delete $5;
        }
        if ($9) {
            for (auto& expr : *$9) {
                $$->addValue(std::move(expr));
            }
            delete $9;
        }
    }
    | INSERT INTO table_name LPAREN column_list RPAREN VALUES insert_values_list {
        $$ = new InsertStmt();
        $$->setTable($3);
        free($3);
        if ($5) {
            for (auto& col : *$5) {
                $$->addColumn(col);
            }
            delete $5;
        }
        if ($8) {
            for (auto& row : *$8) {
                $$->addRow();
                for (auto& expr : row) {
                    $$->addValue(std::move(expr));
                }
            }
            delete $8;
        }
    }
    ;

insert_values_list:
    LPAREN value_list RPAREN {
        $$ = new std::vector<std::vector<std::unique_ptr<Expression>>>();
        $$->push_back({});
        for (auto& expr : *$2) {
            $$->back().push_back(std::move(expr));
        }
        delete $2;
    }
    | insert_values_list COMMA LPAREN value_list RPAREN {
        $$ = $1;
        $$->push_back({});
        for (auto& expr : *$4) {
            $$->back().push_back(std::move(expr));
        }
        delete $4;
    }
    ;

value_list:
    expr {
        $$ = new std::vector<std::unique_ptr<Expression>>();
        $$->push_back(std::unique_ptr<Expression>($1));
    }
    | value_list COMMA expr {
        $$ = $1;
        $$->push_back(std::unique_ptr<Expression>($3));
    }
    ;

column_list:
    column_name {
        $$ = new std::vector<std::string>();
        $$->push_back($1);
        free($1);
    }
    | column_list COMMA column_name {
        $$ = $1;
        $$->push_back($3);
        free($3);
    }
    ;

/* UPDATE 语句 */
update_stmt:
    UPDATE table_name SET assignment_list opt_where {
        $$ = new UpdateStmt();
        $$->setTable($2);
        free($2);
        if ($4) {
            for (auto& assign : *$4) {
                $$->addAssignment(assign.first, std::move(assign.second));
            }
            delete $4;
        }
        if ($5) {
            $$->setWhereCondition(std::unique_ptr<Expression>($5));
        }
    }
    ;

assignment_list:
    column_name EQ expr {
        $$ = new std::vector<std::pair<std::string, std::unique_ptr<Expression>>>();
        $$->push_back({$1, std::unique_ptr<Expression>($3)});
        free($1);
    }
    | assignment_list COMMA column_name EQ expr {
        $$ = $1;
        $$->push_back({$3, std::unique_ptr<Expression>($5)});
        free($3);
    }
    ;

/* DELETE 语句 */
delete_stmt:
    DELETE FROM table_name opt_where {
        $$ = new DeleteStmt();
        $$->setTable($3);
        free($3);
        if ($4) {
            $$->setWhereCondition(std::unique_ptr<Expression>($4));
        }
    }
    | DELETE table_name opt_where {
        $$ = new DeleteStmt();
        $$->setTable($2);
        free($2);
        if ($3) {
            $$->setWhereCondition(std::unique_ptr<Expression>($3));
        }
    }
    ;

/* CREATE TABLE 语句 */
create_table_stmt:
    CREATE TABLE table_name LPAREN column_def_list_full RPAREN {
        $$ = new CreateTableStmt();
        $$->setTable($3);
        free($3);
        if ($5) {
            for (auto& col : *$5) {
                $$->addColumn(std::move(col));
            }
            delete $5;
        }
    }
    ;

column_def_list_full:
    column_def_list_full COMMA column_def {
        $$ = $1;
        $$->push_back(std::move(*$3));
        delete $3;
    }
    | column_def {
        $$ = new std::vector<CreateTableStmt::ColumnDef>();
        $$->push_back(std::move(*$1));
        delete $1;
    }
    ;

column_def:
    column_name data_type {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, false, false, nullptr, ""};
        free($1);
        free($2);
    }
    | column_name data_type PRIMARY KEY {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, true, false, false, nullptr, ""};
        free($1);
        free($2);
    }
    | column_name data_type NOT NULLX {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, true, false, nullptr, ""};
        free($1);
        free($2);
    }
    | column_name data_type NOT NULLX PRIMARY KEY {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, true, true, false, nullptr, ""};
        free($1);
        free($2);
    }
    | column_name data_type NOT NULLX UNIQUE {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, true, true, nullptr, ""};
        free($1);
        free($2);
    }
    | column_name data_type UNIQUE {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, false, true, nullptr, ""};
        free($1);
        free($2);
    }
    | column_name data_type UNIQUE NOT NULLX {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, true, true, nullptr, ""};
        free($1);
        free($2);
    }
    | column_name data_type DEFAULT NUMBER {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, false, false, std::make_unique<LiteralExpr>($4), ""};
        free($1);
        free($2);
    }
    | column_name data_type DEFAULT STRING_LITERAL {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, false, false, std::make_unique<LiteralExpr>($4), ""};
        free($1);
        free($2);
    }
    | column_name data_type DEFAULT TRUE {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, false, false, std::make_unique<LiteralExpr>("TRUE"), ""};
        free($1);
        free($2);
    }
    | column_name data_type DEFAULT FALSE {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, false, false, std::make_unique<LiteralExpr>("FALSE"), ""};
        free($1);
        free($2);
    }
    | column_name data_type CHECK LPAREN expr RPAREN {
        $$ = new CreateTableStmt::ColumnDef{$1, $2, false, false, false, nullptr, $5->toString()};
        free($1);
        free($2);
        delete $5;
    }
    ;

/* DROP TABLE 语句 */
drop_table_stmt:
    DROP TABLE table_name {
        $$ = new DropTableStmt($3);
        free($3);
    }
    | DROP TABLE IF EXISTS table_name {
        $$ = new DropTableStmt($5);
        free($5);
    }
    ;

/* ALTER TABLE 语句 */
alter_table_stmt:
    ALTER TABLE table_name ADD column_name data_type {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::ADD_COLUMN);
        $$->setColumnDef($5, $6);
        free($3);
        free($5);
        free($6);
    }
    | ALTER TABLE table_name ADD COLUMN column_name data_type {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::ADD_COLUMN);
        $$->setColumnDef($6, $7);
        free($3);
        free($6);
        free($7);
    }
    | ALTER TABLE table_name DROP column_name {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::DROP_COLUMN);
        $$->setColumnName($5);
        free($3);
        free($5);
    }
    | ALTER TABLE table_name DROP COLUMN column_name {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::DROP_COLUMN);
        $$->setColumnName($6);
        free($3);
        free($6);
    }
    | ALTER TABLE table_name MODIFY column_name data_type {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::MODIFY_COLUMN);
        $$->setColumnDef($5, $6);
        free($3);
        free($5);
        free($6);
    }
    | ALTER TABLE table_name MODIFY COLUMN column_name data_type {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::MODIFY_COLUMN);
        $$->setColumnDef($6, $7);
        free($3);
        free($6);
        free($7);
    }
    | ALTER TABLE table_name RENAME TO IDENTIFIER {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::RENAME_TABLE);
        $$->setNewTableName($6);
        free($3);
        free($6);
    }
    | ALTER TABLE table_name RENAME IDENTIFIER {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::RENAME_TABLE);
        $$->setNewTableName($5);
        free($3);
        free($5);
    }
    | ALTER TABLE table_name ALTER column_name TYPE data_type {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::ALTER_COLUMN_TYPE);
        $$->setColumnDef($5, $7);
        free($3);
        free($5);
        free($7);
    }
    | ALTER TABLE table_name ALTER COLUMN column_name TYPE data_type {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::ALTER_COLUMN_TYPE);
        $$->setColumnDef($6, $8);
        free($3);
        free($6);
        free($8);
    }
    | ALTER TABLE table_name ALTER column_name data_type {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::ALTER_COLUMN_TYPE);
        $$->setColumnDef($5, $6);
        free($3);
        free($5);
        free($6);
    }
    | ALTER TABLE table_name ALTER COLUMN column_name data_type {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::ALTER_COLUMN_TYPE);
        $$->setColumnDef($6, $7);
        free($3);
        free($6);
        free($7);
    }
    | ALTER TABLE table_name RENAME COLUMN column_name TO column_name {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::RENAME_COLUMN);
        $$->setColumnName($6);
        $$->setNewColumnName($8);
        free($3);
        free($6);
        free($8);
    }
    ;

/* 事务语句 */
begin_stmt:
    BEGIN {
        $$ = new BeginStmt();
    }
    | BEGIN TRANSACTION {
        $$ = new BeginStmt();
    }
    ;

commit_stmt:
    COMMIT {
        $$ = new CommitStmt();
    }
    | COMMIT TRANSACTION {
        $$ = new CommitStmt();
    }
    ;

rollback_stmt:
    ROLLBACK {
        $$ = new RollbackStmt();
    }
    | ROLLBACK TRANSACTION {
        $$ = new RollbackStmt();
    }
    ;

/* CREATE INDEX 语句 */
create_index_stmt:
    CREATE INDEX IDENTIFIER ON table_name LPAREN column_name RPAREN {
        $$ = new CreateIndexStmt();
        $$->setIndexName($3);
        $$->setTableName($5);
        $$->setColumnName($7);
        $$->setUnique(false);
        free($3);
        free($5);
        free($7);
    }
    | CREATE UNIQUE INDEX IDENTIFIER ON table_name LPAREN column_name RPAREN {
        $$ = new CreateIndexStmt();
        $$->setIndexName($4);
        $$->setTableName($6);
        $$->setColumnName($8);
        $$->setUnique(true);
        free($4);
        free($6);
        free($8);
    }
    ;

/* DROP INDEX 语句 */
drop_index_stmt:
    DROP INDEX IDENTIFIER {
        $$ = new DropIndexStmt($3);
        free($3);
    }
    ;

/* CREATE VIEW 语句 */
create_view_stmt:
    CREATE VIEW table_name AS select_stmt {
        $$ = new CreateViewStmt();
        $$->setViewName($3);
        free($3);
        $$->setSelectStmt(std::unique_ptr<SelectStmt>($5));
    }
    ;

/* DROP VIEW 语句 */
drop_view_stmt:
    DROP VIEW table_name {
        $$ = new DropViewStmt($3);
        free($3);
    }
    | DROP VIEW IF EXISTS table_name {
        $$ = new DropViewStmt($5, true);
        free($5);
    }
    ;

/* ANALYZE 语句 */
analyze_stmt:
    ANALYZE table_name {
        $$ = new AnalyzeStmt($2);
        free($2);
    }
    ;

/* EXPLAIN 语句 */
explain_stmt:
    EXPLAIN select_stmt {
        $$ = new ExplainStmt(std::unique_ptr<Statement>($2));
    }
    ;

/* WHERE 子句 */
opt_where:
    /* empty */ { $$ = nullptr; }
    | WHERE expr { $$ = $2; }
    ;

/* GROUP BY 子句 */
opt_group_by:
    /* empty */ { $$ = nullptr; }
    | GROUP BY value_list { $$ = $3; }
    ;

/* ORDER BY 子句 */
opt_order_by:
    /* empty */ { $$ = nullptr; }
    | ORDER BY order_by_list { $$ = $3; }
    ;

order_by_list:
    order_by_item {
        $$ = new std::vector<std::pair<std::unique_ptr<Expression>, bool>>();
        $$->push_back({std::unique_ptr<Expression>($1->first), $1->second});
        delete $1;
    }
    | order_by_list COMMA order_by_item {
        $$ = $1;
        $$->push_back({std::unique_ptr<Expression>($3->first), $3->second});
        delete $3;
    }
    ;

order_by_item:
    expr opt_asc_desc {
        $$ = new std::pair<Expression*, bool>($1, $2);
    }
    ;

opt_asc_desc:
    /* empty */ { $$ = true; }
    | ASC { $$ = true; }
    | DESC { $$ = false; }
    ;

/* LIMIT 子句 - returns pair of (limit, offset), -1 means not set */
opt_limit:
    /* empty */ {
        $$ = new std::pair<int64_t, int64_t>(-1, -1);
    }
    | LIMIT NUMBER {
        int64_t limit_val = ($2 != nullptr) ? atoll($2) : 0;
        $$ = new std::pair<int64_t, int64_t>(limit_val, -1);
        free($2);
    }
    | LIMIT NUMBER OFFSET NUMBER {
        int64_t limit_val = ($2 != nullptr) ? atoll($2) : 0;
        int64_t offset_val = ($4 != nullptr) ? atoll($4) : 0;
        $$ = new std::pair<int64_t, int64_t>(limit_val, offset_val);
        free($2);
        free($4);
    }
    | OFFSET NUMBER {
        int64_t offset_val = ($2 != nullptr) ? atoll($2) : 0;
        $$ = new std::pair<int64_t, int64_t>(-1, offset_val);
        free($2);
    }
    ;

/* 表达式 */
expr:
    comparison_expr { $$ = $1; }
    | logical_expr { $$ = $1; }
    ;

comparison_expr:
    column_name EQ expr {
        $$ = new ComparisonExpr(OpType::EQ,
            std::make_unique<ColumnRefExpr>($1),
            std::unique_ptr<Expression>($3));
        free($1);
    }
    | column_name NE expr {
        $$ = new ComparisonExpr(OpType::NE,
            std::make_unique<ColumnRefExpr>($1),
            std::unique_ptr<Expression>($3));
        free($1);
    }
    | column_name LT expr {
        $$ = new ComparisonExpr(OpType::LT,
            std::make_unique<ColumnRefExpr>($1),
            std::unique_ptr<Expression>($3));
        free($1);
    }
    | column_name GT expr {
        $$ = new ComparisonExpr(OpType::GT,
            std::make_unique<ColumnRefExpr>($1),
            std::unique_ptr<Expression>($3));
        free($1);
    }
    | column_name LE expr {
        $$ = new ComparisonExpr(OpType::LE,
            std::make_unique<ColumnRefExpr>($1),
            std::unique_ptr<Expression>($3));
        free($1);
    }
    | column_name GE expr {
        $$ = new ComparisonExpr(OpType::GE,
            std::make_unique<ColumnRefExpr>($1),
            std::unique_ptr<Expression>($3));
        free($1);
    }
    | column_name {
        $$ = new ColumnRefExpr($1);
        free($1);
    }
    | NUMBER {
        $$ = new LiteralExpr($1);
        free($1);
    }
    | STRING_LITERAL {
        $$ = new LiteralExpr($1);
        free($1);
    }
    | NULLX {
        $$ = new LiteralExpr("NULL");
    }
    | TRUE {
        $$ = new LiteralExpr("TRUE");
    }
    | FALSE {
        $$ = new LiteralExpr("FALSE");
    }
    /* 算术表达式 */
    | expr PLUS expr {
        $$ = new BinaryOpExpr(OpType::ADD,
            std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3));
    }
    | expr MINUS expr {
        $$ = new BinaryOpExpr(OpType::SUB,
            std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3));
    }
    | expr MUL expr {
        $$ = new BinaryOpExpr(OpType::MUL,
            std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3));
    }
    | expr STAR expr {
        $$ = new BinaryOpExpr(OpType::MUL,
            std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3));
    }
    | expr DIV expr {
        $$ = new BinaryOpExpr(OpType::DIV,
            std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3));
    }
    | expr MOD expr {
        $$ = new BinaryOpExpr(OpType::MOD,
            std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3));
    }
    | LPAREN expr RPAREN {
        $$ = $2;
    }
    | MINUS expr %prec NOT {
        // Unary minus for negative numbers
        $$ = new BinaryOpExpr(OpType::SUB,
            std::make_unique<LiteralExpr>("0"),
            std::unique_ptr<Expression>($2));
    }
    ;

logical_expr:
    expr AND expr {
        $$ = new LogicalExpr(OpType::AND,
            std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3));
    }
    | expr OR expr {
        $$ = new LogicalExpr(OpType::OR,
            std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3));
    }
    | NOT expr {
        $$ = new LogicalExpr(OpType::NOT,
            std::unique_ptr<Expression>($2),
            nullptr);
    }
    | expr IN LPAREN value_list RPAREN {
        auto values = new std::vector<std::unique_ptr<Expression>>();
        for (auto& v : *$4) {
            values->push_back(std::move(v));
        }
        delete $4;
        $$ = new InExpr(std::unique_ptr<Expression>($1), std::move(*values), false);
        delete values;
    }
    | expr NOT IN LPAREN value_list RPAREN {
        auto values = new std::vector<std::unique_ptr<Expression>>();
        for (auto& v : *$5) {
            values->push_back(std::move(v));
        }
        delete $5;
        $$ = new InExpr(std::unique_ptr<Expression>($1), std::move(*values), true);
        delete values;
    }
    | expr BETWEEN expr AND expr {
        $$ = new BetweenExpr(std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3),
            std::unique_ptr<Expression>($5), false);
    }
    | expr NOT BETWEEN expr AND expr {
        $$ = new BetweenExpr(std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($4),
            std::unique_ptr<Expression>($6), true);
    }
    | expr LIKE expr {
        $$ = new LikeExpr(std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($3), false);
    }
    | expr NOT LIKE expr {
        $$ = new LikeExpr(std::unique_ptr<Expression>($1),
            std::unique_ptr<Expression>($4), true);
    }
    | expr IS NULLX {
        $$ = new IsNullExpr(std::unique_ptr<Expression>($1), false);
    }
    | expr IS NOT NULLX {
        $$ = new IsNullExpr(std::unique_ptr<Expression>($1), true);
    }
    | LPAREN expr RPAREN {
        $$ = $2;
    }
    ;

/* 名称 */
table_name:
    IDENTIFIER { $$ = $1; }
    ;

column_name:
    IDENTIFIER { $$ = $1; }
    ;

data_type:
    INT { $$ = strdup("INT"); }
    | BIGINT { $$ = strdup("BIGINT"); }
    | SMALLINT { $$ = strdup("SMALLINT"); }
    | TINYINT { $$ = strdup("TINYINT"); }
    | FLOAT { $$ = strdup("FLOAT"); }
    | DOUBLE { $$ = strdup("DOUBLE"); }
    | REAL { $$ = strdup("REAL"); }
    | DECIMAL { $$ = strdup("DECIMAL"); }
    | DECIMAL LPAREN NUMBER COMMA NUMBER RPAREN {
        char buf[64];
        snprintf(buf, sizeof(buf), "DECIMAL(%s,%s)", $3, $5);
        $$ = strdup(buf);
        free($3);
        free($5);
    }
    | NUMERIC { $$ = strdup("NUMERIC"); }
    | NUMERIC LPAREN NUMBER COMMA NUMBER RPAREN {
        char buf[64];
        snprintf(buf, sizeof(buf), "NUMERIC(%s,%s)", $3, $5);
        $$ = strdup(buf);
        free($3);
        free($5);
    }
    | CHAR { $$ = strdup("CHAR"); }
    | VARCHAR { $$ = strdup("VARCHAR"); }
    | TEXT { $$ = strdup("TEXT"); }
    | DATE { $$ = strdup("DATE"); }
    | TIME { $$ = strdup("TIME"); }
    | TIMESTAMP { $$ = strdup("TIMESTAMP"); }
    | DATETIME { $$ = strdup("DATETIME"); }
    | BOOLEAN { $$ = strdup("BOOLEAN"); }
    | CHAR LPAREN NUMBER RPAREN {
        char buf[64];
        snprintf(buf, sizeof(buf), "CHAR(%s)", $3);
        $$ = strdup(buf);
        free($3);
    }
    | VARCHAR LPAREN NUMBER RPAREN {
        char buf[64];
        snprintf(buf, sizeof(buf), "VARCHAR(%s)", $3);
        $$ = strdup(buf);
        free($3);
    }
    ;

%%

void yyerror(const char* msg) {
    std::cerr << "Parse error: " << msg << std::endl;
}
