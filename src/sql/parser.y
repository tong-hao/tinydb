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

    // 列表类型
    std::vector<std::unique_ptr<Expression>>* expr_list;
    std::vector<std::string>* str_list;
    std::vector<std::pair<std::string, std::string>>* col_def_list;
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>>* assignment_list;
}

%define parse.error verbose

/* 关键字 */
%token SELECT INSERT UPDATE DELETE
%token FROM WHERE INTO VALUES SET
%token CREATE DROP TABLE INDEX ON
%token AND OR NOT NULLX TRUE FALSE
%token PRIMARY KEY UNIQUE FOREIGN REFERENCES
%token AS DISTINCT ALL STAR
%token ORDER BY ASC DESC LIMIT OFFSET
%token GROUP HAVING
%token JOIN INNER LEFT RIGHT OUTER CROSS NATURAL USING
%token IN EXISTS BETWEEN LIKE IS
%token CASE WHEN THEN ELSE END
%token IF CAST DEFAULT
%token EXPLAIN ANALYZE DESCRIBE SHOW DATABASES TABLES
%token BEGIN TRANSACTION COMMIT ROLLBACK
%token ALTER ADD COLUMN

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

%type <expr> expr select_item opt_where
%type <expr> comparison_expr logical_expr
%type <expr_list> select_list value_list
%type <str_list> column_list
%type <col_def_list> column_def_list
%type <assignment_list> assignment_list

%type <str> table_name column_name data_type

%destructor { free($$); } <str>

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
    SELECT select_list FROM table_name opt_where {
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
    }
    | SELECT STAR FROM table_name opt_where {
        $$ = new SelectStmt();
        $$->addSelectItem(std::make_unique<ColumnRefExpr>("*"));
        $$->setFromTable($4);
        free($4);
        if ($5) {
            $$->setWhereCondition(std::unique_ptr<Expression>($5));
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
    CREATE TABLE table_name LPAREN column_def_list RPAREN {
        $$ = new CreateTableStmt();
        $$->setTable($3);
        free($3);
        if ($5) {
            for (auto& col : *$5) {
                $$->addColumn(col.first, col.second);
            }
            delete $5;
        }
    }
    ;

column_def_list:
    column_name data_type {
        $$ = new std::vector<std::pair<std::string, std::string>>();
        $$->push_back({$1, $2});
        free($1);
        free($2);
    }
    | column_def_list COMMA column_name data_type {
        $$ = $1;
        $$->push_back({$3, $4});
        free($3);
        free($4);
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
    | ALTER TABLE table_name DROP column_name {
        $$ = new AlterTableStmt();
        $$->setTable($3);
        $$->setAction(AlterTableStmt::ActionType::DROP_COLUMN);
        $$->setColumnName($5);
        free($3);
        free($5);
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

/* ANALYZE 语句 */
analyze_stmt:
    ANALYZE table_name {
        $$ = new AnalyzeStmt($2);
        free($2);
    }
    ;

/* EXPLAIN 语句 */
explain_stmt:
    EXPLAIN statement {
        $$ = new ExplainStmt();
        // Note: inner statement is stored but not used in this simplified version
    }
    ;

/* WHERE 子句 */
opt_where:
    /* empty */ { $$ = nullptr; }
    | WHERE expr { $$ = $2; }
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
    | NUMERIC { $$ = strdup("NUMERIC"); }
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
