%top{
#include "parser_generated.h"
}

%{
#include <string>
#include <cstring>

// 注意：不要在这里包含 ast.h 或使用 using namespace tinydb::sql
// 以避免 OpType 枚举与 Bison token 冲突

// 声明外部 yylval
extern YYSTYPE yylval;

// Undefine Flex macros that conflict with token names
#ifdef BEGIN
#undef BEGIN
#endif

// 处理字符串转义序列
static std::string process_escapes(const char* str, size_t len) {
    std::string result;
    result.reserve(len);
    for (size_t i = 0; i < len; i++) {
        if (str[i] == '\\' && i + 1 < len) {
            switch (str[i + 1]) {
                case 'n': result += '\n'; i++; break;
                case 't': result += '\t'; i++; break;
                case 'r': result += '\r'; i++; break;
                case '\\': result += '\\'; i++; break;
                case '\'': result += '\''; i++; break;
                case '"': result += '"'; i++; break;
                default: result += str[i]; break;
            }
        } else if (str[i] == '\'' && i + 1 < len && str[i + 1] == '\'') {
            // SQL标准: '' 转义为单引号
            result += '\'';
            i++;
        } else {
            result += str[i];
        }
    }
    return result;
}
%}

%option noyywrap
%option case-insensitive

/* 定义段 */
DIGIT       [0-9]
LETTER      [a-zA-Z_]
IDENTIFIER  {LETTER}({LETTER}|{DIGIT})*
INTEGER     {DIGIT}+
FLOAT       {DIGIT}+\.{DIGIT}*
STRING      '([^']|'')*'|\"([^\"]|\\.)*\"
WHITESPACE  [ \t\r]+
NEWLINE     \n

%%

{WHITESPACE}    { /* 忽略空白字符 */ }
{NEWLINE}       { /* 忽略换行 */ }

 /* SQL 注释 */
"--".*          { /* 单行注释 */ }
"/*"([^*]|\*+[^*/])*\*+"/"  { /* 多行注释 */ }

 /* 关键字 */
"SELECT"        { return SELECT; }
"INSERT"        { return INSERT; }
"UPDATE"        { return UPDATE; }
"DELETE"        { return DELETE; }
"FROM"          { return FROM; }
"WHERE"         { return WHERE; }
"INTO"          { return INTO; }
"VALUES"        { return VALUES; }
"SET"           { return SET; }
"CREATE"        { return CREATE; }
"DROP"          { return DROP; }
"TABLE"         { return TABLE; }
"VIEW"          { return VIEW; }
"INDEX"         { return INDEX; }
"ON"            { return ON; }
"AND"           { return AND; }
"OR"            { return OR; }
"NOT"           { return NOT; }
"NULL"          { return NULLX; }
"TRUE"          { return TRUE; }
"FALSE"         { return FALSE; }
"PRIMARY"       { return PRIMARY; }
"KEY"           { return KEY; }
"UNIQUE"        { return UNIQUE; }
"FOREIGN"       { return FOREIGN; }
"REFERENCES"    { return REFERENCES; }
"AS"            { return AS; }
"DISTINCT"      { return DISTINCT; }
"ALL"           { return ALL; }
"ORDER"         { return ORDER; }
"BY"            { return BY; }
"ASC"           { return ASC; }
"DESC"          { return DESC; }
"LIMIT"         { return LIMIT; }
"OFFSET"        { return OFFSET; }
"GROUP"         { return GROUP; }
"HAVING"        { return HAVING; }
"JOIN"          { return JOIN; }
"INNER"         { return INNER; }
"LEFT"          { return LEFT; }
"RIGHT"         { return RIGHT; }
"OUTER"         { return OUTER; }
"CROSS"         { return CROSS; }
"NATURAL"       { return NATURAL; }
"USING"         { return USING; }
"IN"            { return IN; }
"EXISTS"        { return EXISTS; }
"BETWEEN"       { return BETWEEN; }
"LIKE"          { return LIKE; }
"IS"            { return IS; }
"CASE"          { return CASE; }
"WHEN"          { return WHEN; }
"THEN"          { return THEN; }
"ELSE"          { return ELSE; }
"END"           { return END; }
"IF"            { return IF; }
"CAST"          { return CAST; }
"DEFAULT"       { return DEFAULT; }
"CHECK"         { return CHECK; }
"EXPLAIN"       { return EXPLAIN; }
"DESCRIBE"      { return DESCRIBE; }
"SHOW"          { return SHOW; }
"DATABASES"     { return DATABASES; }
"TABLES"        { return TABLES; }

 /* 事务关键字 */
"BEGIN"         { return BEGIN; }
"TRANSACTION"   { return TRANSACTION; }
"COMMIT"        { return COMMIT; }
"ROLLBACK"      { return ROLLBACK; }

 /* ALTER TABLE 关键字 */
"ALTER"         { return ALTER; }
"ADD"           { return ADD; }
"COLUMN"        { return COLUMN; }
"MODIFY"        { return MODIFY; }
"RENAME"        { return RENAME; }
"TO"            { return TO; }
"TYPE"          { return TYPE; }

 /* ANALYZE */
"ANALYZE"       { return ANALYZE; }

 /* 聚合函数 */
"COUNT"         { return COUNT; }
"SUM"           { return SUM; }
"AVG"           { return AVG; }
"MAX"           { return MAX; }
"MIN"           { return MIN; }

 /* 数据类型 */
"INT"           { yylval.str = strdup(yytext); return INT; }
"INTEGER"       { yylval.str = strdup(yytext); return INT; }
"BIGINT"        { yylval.str = strdup(yytext); return BIGINT; }
"SMALLINT"      { yylval.str = strdup(yytext); return SMALLINT; }
"TINYINT"       { yylval.str = strdup(yytext); return TINYINT; }
"FLOAT"         { yylval.str = strdup(yytext); return FLOAT; }
"DOUBLE"        { yylval.str = strdup(yytext); return DOUBLE; }
"REAL"          { yylval.str = strdup(yytext); return REAL; }
"DECIMAL"       { yylval.str = strdup(yytext); return DECIMAL; }
"NUMERIC"       { yylval.str = strdup(yytext); return NUMERIC; }
"CHAR"          { yylval.str = strdup(yytext); return CHAR; }
"VARCHAR"       { yylval.str = strdup(yytext); return VARCHAR; }
"TEXT"          { yylval.str = strdup(yytext); return TEXT; }
"DATE"          { yylval.str = strdup(yytext); return DATE; }
"TIME"          { yylval.str = strdup(yytext); return TIME; }
"TIMESTAMP"     { yylval.str = strdup(yytext); return TIMESTAMP; }
"DATETIME"      { yylval.str = strdup(yytext); return DATETIME; }
"BOOLEAN"       { yylval.str = strdup(yytext); return BOOLEAN; }
"BOOL"          { yylval.str = strdup(yytext); return BOOLEAN; }

 /* 标识符 */
{IDENTIFIER}    { yylval.str = strdup(yytext); return IDENTIFIER; }

 /* 数字 */
{INTEGER}       { yylval.str = strdup(yytext); return NUMBER; }
{FLOAT}         { yylval.str = strdup(yytext); return NUMBER; }

 /* 字符串 */
{STRING}        {
                    // 去掉引号并处理转义序列
                    std::string s(yytext);
                    std::string content = s.substr(1, s.length()-2);
                    std::string processed = process_escapes(content.c_str(), content.length());
                    yylval.str = strdup(processed.c_str());
                    return STRING_LITERAL;
                }

 /* 运算符 */
"="             { return EQ; }
"<>"            { return NE; }
"!="            { return NE; }
"<"             { return LT; }
">"             { return GT; }
"<="            { return LE; }
">="            { return GE; }
"+"             { return PLUS; }
"-"             { return MINUS; }
"*"             { return STAR; }
"/"             { return DIV; }
"%"             { return MOD; }
"||"            { return CONCAT; }
"("             { return LPAREN; }
")"             { return RPAREN; }
","             { return COMMA; }
";"             { return SEMICOLON; }
"."             { return DOT; }

 /* 其他字符 */
.               { return yytext[0]; }

<<EOF>>         { return 0; }

%%

/* 额外代码段 */
