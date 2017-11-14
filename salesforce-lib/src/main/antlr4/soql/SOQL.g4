/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Based on the Salesforce Object Query Language (SOQL) Reference
 * https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql.htm
 *
 * Guided by https://tomassetti.me/antlr-mega-tutorial/
 */

grammar SOQL;

/*
 * Lexer Rules
 */

fragment A          : ('A'|'a') ;
fragment B          : ('B'|'b') ;
fragment C          : ('C'|'c') ;
fragment D          : ('D'|'d') ;
fragment E          : ('E'|'e') ;
fragment F          : ('F'|'f') ;
fragment G          : ('G'|'g') ;
fragment H          : ('H'|'h') ;
fragment I          : ('I'|'i') ;
fragment J          : ('J'|'j') ;
fragment K          : ('K'|'k') ;
fragment L          : ('L'|'l') ;
fragment M          : ('M'|'m') ;
fragment N          : ('N'|'n') ;
fragment O          : ('O'|'o') ;
fragment P          : ('P'|'p') ;
fragment Q          : ('Q'|'q') ;
fragment R          : ('R'|'r') ;
fragment S          : ('S'|'s') ;
fragment T          : ('T'|'t') ;
fragment U          : ('U'|'u') ;
fragment V          : ('V'|'v') ;
fragment W          : ('W'|'w') ;
fragment X          : ('X'|'x') ;
fragment Y          : ('Y'|'y') ;
fragment Z          : ('Z'|'z') ;

fragment LETTER     : ([a-z] | [A-Z] | '$' | '_' ) ;
fragment DIGIT      : [0-9] ;
fragment LETTER_OR_DIGIT : (LETTER | DIGIT) ;

WHITESPACE	        : (' ' | '\t' | '\r' | '\n' )+ -> skip ;

SELECT              : S E L E C T ;
TYPEOF              : T Y P E O F ;
FROM                : F R O M ;
WHEN                : W H E N ;
THEN                : T H E N ;
ELSE                : E L S E ;
END                 : E N D ;
USING               : U S I N G ;
SCOPE               : S C O P E ;
WHERE               : W H E R E ;
LIKE                : L I K E ;
IN                  : I N ;
NOT                 : N O T ;
INCLUDES            : I N C L U D E S ;
EXCLUDES            : E X C L U D E S ;
NULL                : N U L L ;
AND                 : A N D ;
OR                  : O R ;
AT                  : A T ;
ABOVE               : A B O V E ;
BELOW               : B E L O W ;
ABOVE_OR_BELOW      : A B O V E '_' O R '_' B E L O W ;
WITH                : W I T H ;
DATA                : D A T A ;
CATEGORY            : C A T E G O R Y ;
GROUP               : G R O U P ;
BY                  : B Y ;
ROLLUP              : R O L L U P ;
CUBE                : C U B E ;
HAVING              : H A V I N G ;
ORDER               : O R D E R ;
ASC                 : A S C ;
DESC                : D E S C ;
NULLS               : N U L L S ;
FIRST               : F I R S T ;
LAST                : L A S T ;
LIMIT               : L I M I T ;
OFFSET              : O F F S E T ;
FOR                 : F O R ;
VIEW                : V I E W ;
REFERENCE           : R E F E R E N C E ;
UPDATE              : U P D A T E ;
TRACKING            : T R A C K I N G ;
VIEWSTAT            : V I E W S T A T ;
TRUE                : T R U E ;
FALSE               : F A L S E ;

REAL                : DIGIT+ '.' DIGIT+ ;

INTEGER             : ('+' | '-')? DIGIT+ ;

DATE_LITERAL        : DATE
                    | DATE_TIME
                    | YESTERDAY
                    | TODAY
                    | TOMORROW
                    | LAST_WEEK
                    | THIS_WEEK
                    | NEXT_WEEK
                    | LAST_MONTH
                    | THIS_MONTH
                    | NEXT_MONTH
                    | LAST_90_DAYS
                    | NEXT_90_DAYS
                    | LAST_N_DAYS_N
                    | NEXT_N_DAYS_N
                    | NEXT_N_WEEKS_N
                    | LAST_N_WEEKS_N
                    | NEXT_N_MONTHS_N
                    | LAST_N_MONTHS_N
                    | THIS_QUARTER
                    | LAST_QUARTER
                    | NEXT_QUARTER
                    | NEXT_N_QUARTERS_N
                    | LAST_N_QUARTERS_N
                    | THIS_YEAR
                    | LAST_YEAR
                    | NEXT_YEAR
                    | NEXT_N_YEARS_N
                    | LAST_N_YEARS_N
                    | THIS_FISCAL_QUARTER
                    | LAST_FISCAL_QUARTER
                    | NEXT_FISCAL_QUARTER
                    | NEXT_N_FISCAL_QUARTERS_N
                    | LAST_N_FISCAL_QUARTERS_N
                    | THIS_FISCAL_YEAR
                    | LAST_FISCAL_YEAR
                    | NEXT_FISCAL_YEAR
                    | NEXT_N_FISCAL_YEARS_N
                    | LAST_N_FISCAL_YEARS_N
                    ;

DATE                : DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT;
DATE_TIME           : DATE 'T' DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT ('.'  DIGIT DIGIT?  DIGIT?)? ( 'Z' | ( '+' | '-' ) DIGIT DIGIT ':' DIGIT DIGIT )? ;


YESTERDAY           : Y E S T E R D A Y ;
TODAY               : T O D A Y ;
TOMORROW            : T O M O R R O W ;
LAST_WEEK           : L A S T '_' W E E K ;
THIS_WEEK           : T H I S '_' W E E K ;
NEXT_WEEK           : N E X T '_' W E E K ;
LAST_MONTH          : L A S T '_' M O N T H ;
THIS_MONTH          : T H I S '_' M O N T H ;
NEXT_MONTH          : N E X T '_' M O N T H ;
LAST_90_DAYS        : L A S T '_90_' D A Y S ;
NEXT_90_DAYS        : N E X T '_90_' D A Y S ;
LAST_N_DAYS_N       : L A S T '_' N '_' D A Y S ':' INTEGER ;
NEXT_N_DAYS_N       : N E X T '_' N '_' D A Y S ':' INTEGER ;
NEXT_N_WEEKS_N      : N E X T '_' N '_' W E E K S ':' INTEGER ;
LAST_N_WEEKS_N      : L A S T '_' N '_' W E E K S ':' INTEGER ;
NEXT_N_MONTHS_N     : N E X T '_' N '_' M O N T H S ':' INTEGER ;
LAST_N_MONTHS_N     : L A S T '_' N '_' M O N T H S ':' INTEGER ;
THIS_QUARTER        : T H I S '_' Q U A R T E R ;
LAST_QUARTER        : L A S T '_' Q U A R T E R ;
NEXT_QUARTER        : N E X T '_' Q U A R T E R ;
NEXT_N_QUARTERS_N   : N E X T '_' N '_' Q U A R T E R S ':' INTEGER ;
LAST_N_QUARTERS_N   : L A S T '_' N '_' Q U A R T E R S ':' INTEGER ;
THIS_YEAR           : T H I S '_' Y E A R ;
LAST_YEAR           : L A S T '_' Y E A R ;
NEXT_YEAR           : N E X T '_' Y E A R ;
NEXT_N_YEARS_N      : N E X T '_' N '_' Y E A R S ':' INTEGER ;
LAST_N_YEARS_N      : L A S T '_' N '_' Y E A R S ':' INTEGER ;
THIS_FISCAL_QUARTER : T H I S '_' F I S C A L '_' Q U A R T E R ;
LAST_FISCAL_QUARTER : L A S T '_' F I S C A L '_' Q U A R T E R ;
NEXT_FISCAL_QUARTER : N E X T '_' F I S C A L '_' Q U A R T E R ;
NEXT_N_FISCAL_QUARTERS_N :  N E X T '_' N '_' F I S C A L '_' Q U A R T E R S ':' INTEGER ;
LAST_N_FISCAL_QUARTERS_N :  L A S T '_' N '_' F I S C A L '_' Q U A R T E R S ':' INTEGER ;
THIS_FISCAL_YEAR    : T H I S '_' F I S C A L '_' Y E A R ;
LAST_FISCAL_YEAR    : L A S T '_' F I S C A L '_' Y E A R ;
NEXT_FISCAL_YEAR    : N E X T '_' F I S C A L '_' Y E A R ;
NEXT_N_FISCAL_YEARS_N : N E X T '_' N '_' F I S C A L '_' Y E A R S ':' INTEGER ;
LAST_N_FISCAL_YEARS_N : L A S T '_' N '_' F I S C A L '_' Y E A R S ':' INTEGER ;

STRING              : '\'' ( ~['] )* '\'' ;

EL                  : '$' '{' ( ~[}] )* '}';

/*
 * IDENTIFIER must be last!!!
 */
IDENTIFIER          : LETTER LETTER_OR_DIGIT* ;

/*
 * Parser Rules
 */

statement           : SELECT (fieldList | typeOfClause)+
                      FROM objectList
                      (USING SCOPE filterScope)?
                      (WHERE conditionExpressions)?
                      (WITH ((DATA CATEGORY filteringExpression) | fieldExpression) )?
                      (GROUP BY (fieldGroupByList
                                | ROLLUP '(' fieldSubtotalGroupByList ')'
                                | CUBE '(' fieldSubtotalGroupByList ')' )
                       (HAVING havingConditionExpression)? )?
                      (ORDER BY fieldOrderByList)?
                      (LIMIT numberOfRowsToReturn)?
                      (OFFSET numberOfRowsToSkip)?
                      (FOR forClause)?
                      (UPDATE updateClause)?
                      EOF
                    ;

fieldList           : '*' | (fieldElement (',' fieldElement)*) ;

fieldElement        : subquery
                    | fieldName
                    | functionCall
                    ;

subquery            : '('
                        SELECT fieldList
                        FROM objectList
                        (WHERE conditionExpressions)?
                        (LIMIT numberOfRowsToReturn)?
                        (OFFSET numberOfRowsToSkip)?
                      ')' ;

fieldName           : field alias? ;

field               : IDENTIFIER ('.' IDENTIFIER)* ;

functionCall        : function alias? ;

alias               : IDENTIFIER ;

/*
 * Note - function names, such as MIN, MAX etc are not SOQL tokens -
 * you can use them as field aliases!
 */
functionName        : IDENTIFIER ;

function            : functionName '(' fieldElement? ')' ;

typeOfClause        : TYPEOF typeOfField (whenThenClause)+ elseClause? END;

typeOfField         : IDENTIFIER ;

whenThenClause      : WHEN whenObjectType THEN fieldList ;

whenObjectType      : IDENTIFIER ;

elseClause          : ELSE fieldList ;

objectList          : objectType ( ',' objectType )* ;

objectType          : IDENTIFIER ('.' IDENTIFIER)* ;

filterScope         : IDENTIFIER ;

conditionExpressions : conditionExpression (logicalOperator conditionExpression)* ;

conditionExpression : '(' conditionExpressions ')'
                    | fieldExpression ;

fieldExpression     : fieldElement comparisonOperator value ;

logicalOperator     : AND | OR | NOT ;

comparisonOperator  : '=' | '!=' | '<' | '>' | '<=' | '>=' | '<>' | LIKE | IN | NOT IN | INCLUDES | EXCLUDES ;

value               : REAL
                    | INTEGER
                    | DATE_LITERAL
                    | STRING
                    | NULL
                    | set
                    | subquery
                    | TRUE
                    | FALSE
                    | IDENTIFIER
                    | EL;

set                 : '(' value (',' value)* ')' ;

filteringExpression : dataCategorySelection (AND dataCategorySelection)* ;

dataCategorySelection : dataCategoryGroupName filteringSelector dataCategoryName ;

dataCategoryGroupName : IDENTIFIER ;

filteringSelector   : AT | ABOVE | BELOW | ABOVE_OR_BELOW ;

dataCategoryName    : IDENTIFIER | dataCategoryList ;

dataCategoryList    : '(' IDENTIFIER (',' IDENTIFIER) ')' ;

fieldGroupByList    : fieldElement (',' fieldElement)* ;

fieldSubtotalGroupByList : fieldElement (',' fieldElement)* ;

havingConditionExpression : conditionExpressions ;

fieldOrderByList    : fieldOrderByElement (',' fieldOrderByElement)* ;

fieldOrderByElement : fieldElement (ASC | DESC)? (NULLS (FIRST|LAST) )? ;

numberOfRowsToReturn : INTEGER ;

numberOfRowsToSkip : INTEGER ;

forClause : forElement (',' forElement)* ;

forElement : VIEW | REFERENCE | UPDATE ;

updateClause : updateElement (',' updateElement)* ;

updateElement : TRACKING | VIEWSTAT ;
