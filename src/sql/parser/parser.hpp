/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_YY_SRC_SQL_PARSER_HPP_INCLUDED
# define YY_YY_SRC_SQL_PARSER_HPP_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif
/* "%code requires" blocks.  */
#line 33 "src/sql/parser.y"

    #include "ast.h"
    #include <vector>
    #include <memory>
    #include <utility>
    using namespace tinydb::sql;

#line 57 "src/sql/parser.hpp"

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    SELECT = 258,                  /* SELECT  */
    INSERT = 259,                  /* INSERT  */
    UPDATE = 260,                  /* UPDATE  */
    DELETE = 261,                  /* DELETE  */
    FROM = 262,                    /* FROM  */
    WHERE = 263,                   /* WHERE  */
    INTO = 264,                    /* INTO  */
    VALUES = 265,                  /* VALUES  */
    SET = 266,                     /* SET  */
    CREATE = 267,                  /* CREATE  */
    DROP = 268,                    /* DROP  */
    TABLE = 269,                   /* TABLE  */
    VIEW = 270,                    /* VIEW  */
    INDEX = 271,                   /* INDEX  */
    ON = 272,                      /* ON  */
    AND = 273,                     /* AND  */
    OR = 274,                      /* OR  */
    NOT = 275,                     /* NOT  */
    NULLX = 276,                   /* NULLX  */
    TRUE = 277,                    /* TRUE  */
    FALSE = 278,                   /* FALSE  */
    PRIMARY = 279,                 /* PRIMARY  */
    KEY = 280,                     /* KEY  */
    UNIQUE = 281,                  /* UNIQUE  */
    FOREIGN = 282,                 /* FOREIGN  */
    REFERENCES = 283,              /* REFERENCES  */
    AS = 284,                      /* AS  */
    DISTINCT = 285,                /* DISTINCT  */
    ALL = 286,                     /* ALL  */
    STAR = 287,                    /* STAR  */
    ORDER = 288,                   /* ORDER  */
    BY = 289,                      /* BY  */
    ASC = 290,                     /* ASC  */
    DESC = 291,                    /* DESC  */
    LIMIT = 292,                   /* LIMIT  */
    OFFSET = 293,                  /* OFFSET  */
    GROUP = 294,                   /* GROUP  */
    HAVING = 295,                  /* HAVING  */
    JOIN = 296,                    /* JOIN  */
    INNER = 297,                   /* INNER  */
    LEFT = 298,                    /* LEFT  */
    RIGHT = 299,                   /* RIGHT  */
    OUTER = 300,                   /* OUTER  */
    CROSS = 301,                   /* CROSS  */
    NATURAL = 302,                 /* NATURAL  */
    USING = 303,                   /* USING  */
    IN = 304,                      /* IN  */
    EXISTS = 305,                  /* EXISTS  */
    BETWEEN = 306,                 /* BETWEEN  */
    LIKE = 307,                    /* LIKE  */
    IS = 308,                      /* IS  */
    CASE = 309,                    /* CASE  */
    WHEN = 310,                    /* WHEN  */
    THEN = 311,                    /* THEN  */
    ELSE = 312,                    /* ELSE  */
    END = 313,                     /* END  */
    IF = 314,                      /* IF  */
    CAST = 315,                    /* CAST  */
    DEFAULT = 316,                 /* DEFAULT  */
    EXPLAIN = 317,                 /* EXPLAIN  */
    ANALYZE = 318,                 /* ANALYZE  */
    DESCRIBE = 319,                /* DESCRIBE  */
    SHOW = 320,                    /* SHOW  */
    DATABASES = 321,               /* DATABASES  */
    TABLES = 322,                  /* TABLES  */
    BEGIN = 323,                   /* BEGIN  */
    TRANSACTION = 324,             /* TRANSACTION  */
    COMMIT = 325,                  /* COMMIT  */
    ROLLBACK = 326,                /* ROLLBACK  */
    ALTER = 327,                   /* ALTER  */
    ADD = 328,                     /* ADD  */
    COLUMN = 329,                  /* COLUMN  */
    MODIFY = 330,                  /* MODIFY  */
    RENAME = 331,                  /* RENAME  */
    TO = 332,                      /* TO  */
    INT = 333,                     /* INT  */
    BIGINT = 334,                  /* BIGINT  */
    SMALLINT = 335,                /* SMALLINT  */
    TINYINT = 336,                 /* TINYINT  */
    FLOAT = 337,                   /* FLOAT  */
    DOUBLE = 338,                  /* DOUBLE  */
    REAL = 339,                    /* REAL  */
    DECIMAL = 340,                 /* DECIMAL  */
    NUMERIC = 341,                 /* NUMERIC  */
    CHAR = 342,                    /* CHAR  */
    VARCHAR = 343,                 /* VARCHAR  */
    TEXT = 344,                    /* TEXT  */
    DATE = 345,                    /* DATE  */
    TIME = 346,                    /* TIME  */
    TIMESTAMP = 347,               /* TIMESTAMP  */
    DATETIME = 348,                /* DATETIME  */
    BOOLEAN = 349,                 /* BOOLEAN  */
    IDENTIFIER = 350,              /* IDENTIFIER  */
    NUMBER = 351,                  /* NUMBER  */
    STRING_LITERAL = 352,          /* STRING_LITERAL  */
    EQ = 353,                      /* EQ  */
    NE = 354,                      /* NE  */
    LT = 355,                      /* LT  */
    GT = 356,                      /* GT  */
    LE = 357,                      /* LE  */
    GE = 358,                      /* GE  */
    PLUS = 359,                    /* PLUS  */
    MINUS = 360,                   /* MINUS  */
    MUL = 361,                     /* MUL  */
    DIV = 362,                     /* DIV  */
    MOD = 363,                     /* MOD  */
    CONCAT = 364,                  /* CONCAT  */
    LPAREN = 365,                  /* LPAREN  */
    RPAREN = 366,                  /* RPAREN  */
    COMMA = 367,                   /* COMMA  */
    SEMICOLON = 368,               /* SEMICOLON  */
    DOT = 369                      /* DOT  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 41 "src/sql/parser.y"

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
    std::vector<std::string>* str_list;
    std::vector<std::pair<std::string, std::string>>* col_def_list;
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>>* assignment_list;

    // ORDER BY support
    int bool_val;
    std::pair<Expression*, bool>* order_by_item;
    std::vector<std::pair<std::unique_ptr<Expression>, bool>>* order_by_list;
    std::pair<int64_t, int64_t>* limit_offset;

#line 222 "src/sql/parser.hpp"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE yylval;


int yyparse (void);

/* "%code provides" blocks.  */
#line 28 "src/sql/parser.y"

    // 定义全局 AST 指针 - 在这里 AST 类已完整定义
    extern tinydb::sql::AST* g_ast;

#line 242 "src/sql/parser.hpp"

#endif /* !YY_YY_SRC_SQL_PARSER_HPP_INCLUDED  */
