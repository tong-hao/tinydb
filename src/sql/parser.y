%{
#include "ast.h"
#include <iostream>
#include <cstdlib>
#include <cstring>

using namespace tinydb::sql;

// 词法分析器函数
int yylex(YYSTYPE* lvalp, YYLTYPE* llocp, yyscan_t scanner);
void yyerror(YYLTYPE* llocp, yyscan_t scanner, AST** ast, const char* msg);
%}

%code requires {
    #include "ast.h"
    #ifndef YYSCAN_T_DEFINED
    typedef void* yyscan_t;
    #define YYSCAN_T_DEFINED
    #endif
    using namespace tinydb::sql;
}

%union {
    char* str;
    ASTNode* node;
    Statement* stmt;
    Expression* expr;
    SelectStmt* select_stmt;
    InsertStmt* insert_stmt;
    CreateTableStmt* create_stmt;
}

%define api.pure full
%define api.push-pull push
%define api.token.prefix {TOK_}
%lex-param { yyscan_t scanner }
%parse-param { yyscan_t scanner }
%parse-param { AST** ast }

%locations

 /* 关键字 */
%token SELECT INSERT UPDATE DELETE
%token FROM WHERE INTO VALUES SET
%token CREATE DROP TABLE INDEX ON
%token AND OR NOT NULLX TRUE FALSE
%token PRIMARY KEY UNIQUE FOREIGN REFERENCES
%token AS DISTINCT ALL
%token ORDER BY ASC DESC LIMIT OFFSET
%token GROUP HAVING
%token JOIN INNER LEFT RIGHT OUTER CROSS NATURAL USING
%token IN EXISTS BETWEEN LIKE IS
%token CASE WHEN THEN ELSE END
%token IF CAST DEFAULT
%token EXPLAIN DESCRIBE SHOW DATABASES TABLES

 /* 数据类型 */
%token <str> INT BIGINT SMALLINT TINYINT
%token <str> FLOAT DOUBLE REAL DECIMAL NUMERIC
%token <str> CHAR VARCHAR TEXT
%token <str> DATE TIME TIMESTAMP DATETIME
%token <str> BOOLEAN

 /* 标识符和常量 */
%token <str> IDENTIFIER NUMBER STRING_LITERAL

 /* 运算符 */
%token EQ NE LT GT LE GE
%token PLUS MINUS STAR SLASH PERCENT CONCAT
%token LPAREN RPAREN COMMA SEMICOLON DOT

 /* 类型定义 */
%type <stmt> statement
%type <select_stmt> select_stmt
%type <insert_stmt> insert_stmt
%type <create_stmt> create_table_stmt
%type <stmt> drop_table_stmt
%type <expr> expr select_item
%type <str> table_name column_name data_type

%destructor { free($$); } <str>

%start statement_list

%%

statement_list:
    statement { if (ast) *ast = new AST(); (*ast)->setStatement(std::unique_ptr<Statement>($1)); }
    | statement SEMICOLON { if (ast) *ast = new AST(); (*ast)->setStatement(std::unique_ptr<Statement>($1)); }
    ;

statement:
    select_stmt { $$ = $1; }
    | insert_stmt { $$ = $1; }
    | create_table_stmt { $$ = $1; }
    | drop_table_stmt { $$ = $1; }
    ;

 /* SELECT 语句 */
select_stmt:
    SELECT select_list FROM table_name {
        $$ = new SelectStmt();
        // 这里需要处理 select_list
        $$->setFromTable($4);
        free($4);
    }
    | SELECT STAR FROM table_name {
        $$ = new SelectStmt();
        $$->addSelectItem(std::make_unique<ColumnRefExpr>("*"));
        $$->setFromTable($4);
        free($4);
    }
    ;

select_list:
    select_item {
        // 简化处理
    }
    | select_list COMMA select_item {
        // 简化处理
    }
    ;

select_item:
    expr { $$ = $1; }
    | expr AS IDENTIFIER { $$ = $1; free($3); }
    ;

 /* INSERT 语句 */
insert_stmt:
    INSERT INTO table_name VALUES LPAREN value_list RPAREN {
        $$ = new InsertStmt();
        $$->setTable($3);
        free($3);
    }
    | INSERT INTO table_name LPAREN column_list RPAREN VALUES LPAREN value_list RPAREN {
        $$ = new InsertStmt();
        $$->setTable($3);
        free($3);
    }
    ;

column_list:
    column_name { free($1); }
    | column_list COMMA column_name { free($3); }
    ;

value_list:
    expr
    | value_list COMMA expr
    ;

 /* CREATE TABLE 语句 */
create_table_stmt:
    CREATE TABLE table_name LPAREN column_def_list RPAREN {
        $$ = new CreateTableStmt();
        $$->setTable($3);
        free($3);
    }
    ;

column_def_list:
    column_def
    | column_def_list COMMA column_def
    ;

column_def:
    column_name data_type {
        free($1);
        free($2);
    }
    | column_name data_type column_constraint_list {
        free($1);
        free($2);
    }
    ;

column_constraint_list:
    /* empty */
    | column_constraint_list column_constraint
    ;

column_constraint:
    PRIMARY KEY
    | NOT NULLX
    | UNIQUE
    | DEFAULT expr
    ;

data_type:
    INT { $$ = $1; }
    | BIGINT { $$ = $1; }
    | SMALLINT { $$ = $1; }
    | TINYINT { $$ = $1; }
    | FLOAT { $$ = $1; }
    | DOUBLE { $$ = $1; }
    | REAL { $$ = $1; }
    | DECIMAL { $$ = $1; }
    | NUMERIC { $$ = $1; }
    | CHAR { $$ = $1; }
    | VARCHAR { $$ = $1; }
    | TEXT { $$ = $1; }
    | DATE { $$ = $1; }
    | TIME { $$ = $1; }
    | TIMESTAMP { $$ = $1; }
    | DATETIME { $$ = $1; }
    | BOOLEAN { $$ = $1; }
    | CHAR LPAREN NUMBER RPAREN { $$ = $1; free($3); }
    | VARCHAR LPAREN NUMBER RPAREN { $$ = $1; free($3); }
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

 /* 表达式 */
expr:
    column_name { $$ = new ColumnRefExpr($1); free($1); }
    | NUMBER { $$ = new LiteralExpr($1); free($1); }
    | STRING_LITERAL { $$ = new LiteralExpr($1); free($1); }
    | NULLX { $$ = new LiteralExpr("NULL"); }
    | TRUE { $$ = new LiteralExpr("TRUE"); }
    | FALSE { $$ = new LiteralExpr("FALSE"); }
    | expr PLUS expr
    | expr MINUS expr
    | expr STAR expr
    | expr SLASH expr
    | LPAREN expr RPAREN
    ;

 /* 名称 */
table_name:
    IDENTIFIER { $$ = $1; }
    ;

column_name:
    IDENTIFIER { $$ = $1; }
    | table_name DOT IDENTIFIER { 
        $$ = $3; 
        free($1); 
    }
    ;

%%

void yyerror(YYLTYPE* llocp, yyscan_t scanner, AST** ast, const char* msg) {
    std::cerr << "Parse error at line " << llocp->first_line 
              << ", column " << llocp->first_column 
              << ": " << msg << std::endl;
}
