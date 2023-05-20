grammar SQL;

@header {
package cn.edu.thssdb.sql;
}

parse :
    sqlStmtList ;

sqlStmtList :
    ';'* sqlStmt ( ';'+ sqlStmt )* ';'* ;

sqlStmt :
    createTableStmt
    | createDbStmt
    | createUserStmt
    | dropDbStmt
    | dropUserStmt
    | deleteStmt
    | dropTableStmt
    | insertStmt
    | selectStmt
    | createViewStmt
    | dropViewStmt
    | grantStmt
    | revokeStmt
    | useDbStmt
    | showDbStmt
    | showTableStmt
    | quitStmt
    | updateStmt ;

createDbStmt :
    K_CREATE K_DATABASE databaseName ;

dropDbStmt :
    K_DROP K_DATABASE ( K_IF K_EXISTS )? databaseName ;

createUserStmt :
    K_CREATE K_USER userName K_IDENTIFIED K_BY password ;

dropUserStmt :
    K_DROP K_USER ( K_IF K_EXISTS )? userName ;

createTableStmt :
    K_CREATE K_TABLE tableName
        '(' columnDef ( ',' columnDef )* ( ',' tableConstraint )? ')' ;

grantStmt :
    K_GRANT authLevel ( ',' authLevel )* K_ON tableName K_TO userName ;

revokeStmt :
    K_REVOKE authLevel ( ',' authLevel )* K_ON tableName K_FROM userName ;

useDbStmt :
    K_USE databaseName;

deleteStmt :
    K_DELETE K_FROM tableName ( K_WHERE multipleCondition )? ;

dropTableStmt :
    K_DROP K_TABLE ( K_IF K_EXISTS )? tableName ;

showDbStmt :
    K_SHOW K_DATABASES;

quitStmt :
    K_QUIT;

showTableStmt :
    K_SHOW K_TABLE tableName;

insertStmt :
    K_INSERT K_INTO tableName ( '(' columnName ( ',' columnName )* ')' )?
        K_VALUES valueEntry ( ',' valueEntry )* ;

valueEntry :
    '(' literalValue ( ',' literalValue )* ')' ;

selectStmt :
    K_SELECT ( K_DISTINCT | K_ALL )? resultColumn ( ',' resultColumn )*
        K_FROM tableQuery ( ',' tableQuery )* ( K_WHERE multipleCondition )? ;

createViewStmt :
    K_CREATE K_VIEW viewName K_AS selectStmt ;

dropViewStmt :
    K_DROP K_VIEW ( K_IF K_EXISTS )? viewName ;

updateStmt :
    K_UPDATE tableName
        K_SET columnName '=' expression ( K_WHERE multipleCondition )? ;

columnDef :
    columnName typeName columnConstraint* ;

typeName :
    T_INT
    | T_LONG
    | T_FLOAT
    | T_DOUBLE
    | T_STRING '(' NUMERIC_LITERAL ')' ;

columnConstraint :
    K_PRIMARY K_KEY
    | K_NOT K_NULL ;

multipleCondition :
    condition
    | multipleCondition AND multipleCondition
    | multipleCondition OR multipleCondition ;

condition :
    expression comparator expression;

comparer :
    columnFullName
    | literalValue ;

comparator :
    EQ | NE | LE | GE | LT | GT ;

expression :
    comparer
    | expression ( MUL | DIV ) expression
    | expression ( ADD | SUB ) expression
    | '(' expression ')';

tableConstraint :
    K_PRIMARY K_KEY '(' columnName (',' columnName)* ')' ;

resultColumn
    : '*'
    | tableName '.' '*'
    | columnFullName;

tableQuery :
    tableName
    | tableName ( K_JOIN tableName )+ K_ON multipleCondition ;

authLevel :
    K_SELECT | K_INSERT | K_UPDATE | K_DELETE | K_DROP ;

literalValue :
    NUMERIC_LITERAL
    | STRING_LITERAL
    | K_NULL ;

columnFullName:
    ( tableName '.' )? columnName ;

databaseName :
    IDENTIFIER ;

tableName :
    IDENTIFIER ;

userName :
    IDENTIFIER ;

columnName :
    IDENTIFIER ;

viewName :
    IDENTIFIER;

password :
    STRING_LITERAL ;

EQ : '=';
NE : '<>';
LT : '<';
GT : '>';
LE : '<=';
GE : '>=';

ADD : '+';
SUB : '-';
MUL : '*';
DIV : '/';

AND : '&&';
OR : '||';

T_INT : I N T;
T_LONG : L O N G;
T_FLOAT : F L O A T;
T_DOUBLE : D O U B L E;
T_STRING : S T R I N G;

K_ADD : A D D;
K_ALL : A L L;
K_AS : A S;
K_BY : B Y;
K_COLUMN : C O L U M N;
K_CREATE : C R E A T E;
K_DATABASE : D A T A B A S E;
K_DATABASES : D A T A B A S E S;
K_DELETE : D E L E T E;
K_DISTINCT : D I S T I N C T;
K_DROP : D R O P;
K_EXISTS : E X I S T S;
K_FROM : F R O M;
K_GRANT : G R A N T;
K_IF : I F;
K_IDENTIFIED : I D E N T I F I E D;
K_INSERT : I N S E R T;
K_INTO : I N T O;
K_JOIN : J O I N;
K_KEY : K E Y;
K_NOT : N O T;
K_NULL : N U L L;
K_ON : O N;
K_PRIMARY : P R I M A R Y;
K_QUIT : Q U I T;
K_REVOKE : R E V O K E;
K_SELECT : S E L E C T;
K_SET : S E T;
K_SHOW : S H O W;
K_TABLE : T A B L E;
K_TO : T O;
K_UPDATE : U P D A T E;
K_USE : U S E;
K_USER : U S E R;
K_VALUES : V A L U E S;
K_VIEW : V I E W;
K_WHERE : W H E R E;

IDENTIFIER :
    [a-zA-Z_] [a-zA-Z_0-9]* ;

NUMERIC_LITERAL :
    DIGIT+ EXPONENT?
    | DIGIT+ '.' DIGIT* EXPONENT?
    | '.' DIGIT+ EXPONENT? ;

EXPONENT :
    E [-+]? DIGIT+ ;

STRING_LITERAL :
    '\'' ( ~'\'' | '\'\'' )* '\'' ;

SINGLE_LINE_COMMENT :
    '--' ~[\r\n]* -> channel(HIDDEN) ;

MULTILINE_COMMENT :
    '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN) ;

SPACES :
    [ \u000B\t\r\n] -> channel(HIDDEN) ;

fragment DIGIT : [0-9] ;
fragment A : [aA] ;
fragment B : [bB] ;
fragment C : [cC] ;
fragment D : [dD] ;
fragment E : [eE] ;
fragment F : [fF] ;
fragment G : [gG] ;
fragment H : [hH] ;
fragment I : [iI] ;
fragment J : [jJ] ;
fragment K : [kK] ;
fragment L : [lL] ;
fragment M : [mM] ;
fragment N : [nN] ;
fragment O : [oO] ;
fragment P : [pP] ;
fragment Q : [qQ] ;
fragment R : [rR] ;
fragment S : [sS] ;
fragment T : [tT] ;
fragment U : [uU] ;
fragment V : [vV] ;
fragment W : [wW] ;
fragment X : [xX] ;
fragment Y : [yY] ;
fragment Z : [zZ] ;
