/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 1 "/home/pepephant/GleamDB/src/parser/yacc.y"

#include "ast.h"
#include "yacc.tab.h"
#include "errors.h"
#include <iostream>
#include <memory>

int yylex(YYSTYPE *yylval, YYLTYPE *yylloc);

void yyerror(YYLTYPE *locp, const char* s) {
    throw ParserError(locp->first_line, locp->first_column, std::string(s));
}

using namespace ast;

#line 87 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "yacc.tab.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_SHOW = 3,                       /* SHOW  */
  YYSYMBOL_TABLES = 4,                     /* TABLES  */
  YYSYMBOL_CREATE = 5,                     /* CREATE  */
  YYSYMBOL_TABLE = 6,                      /* TABLE  */
  YYSYMBOL_DROP = 7,                       /* DROP  */
  YYSYMBOL_DESC = 8,                       /* DESC  */
  YYSYMBOL_INSERT = 9,                     /* INSERT  */
  YYSYMBOL_INTO = 10,                      /* INTO  */
  YYSYMBOL_VALUES = 11,                    /* VALUES  */
  YYSYMBOL_DELETE = 12,                    /* DELETE  */
  YYSYMBOL_FROM = 13,                      /* FROM  */
  YYSYMBOL_ASC = 14,                       /* ASC  */
  YYSYMBOL_ORDER = 15,                     /* ORDER  */
  YYSYMBOL_BY = 16,                        /* BY  */
  YYSYMBOL_GROUP = 17,                     /* GROUP  */
  YYSYMBOL_HAVING = 18,                    /* HAVING  */
  YYSYMBOL_MAX = 19,                       /* MAX  */
  YYSYMBOL_MIN = 20,                       /* MIN  */
  YYSYMBOL_SUM = 21,                       /* SUM  */
  YYSYMBOL_COUNT = 22,                     /* COUNT  */
  YYSYMBOL_AS = 23,                        /* AS  */
  YYSYMBOL_IN = 24,                        /* IN  */
  YYSYMBOL_WHERE = 25,                     /* WHERE  */
  YYSYMBOL_UPDATE = 26,                    /* UPDATE  */
  YYSYMBOL_SET = 27,                       /* SET  */
  YYSYMBOL_SELECT = 28,                    /* SELECT  */
  YYSYMBOL_INT = 29,                       /* INT  */
  YYSYMBOL_CHAR = 30,                      /* CHAR  */
  YYSYMBOL_FLOAT = 31,                     /* FLOAT  */
  YYSYMBOL_INDEX = 32,                     /* INDEX  */
  YYSYMBOL_AND = 33,                       /* AND  */
  YYSYMBOL_JOIN = 34,                      /* JOIN  */
  YYSYMBOL_EXIT = 35,                      /* EXIT  */
  YYSYMBOL_HELP = 36,                      /* HELP  */
  YYSYMBOL_STATIC = 37,                    /* STATIC  */
  YYSYMBOL_CHECKPOINT = 38,                /* CHECKPOINT  */
  YYSYMBOL_LOAD = 39,                      /* LOAD  */
  YYSYMBOL_OFF = 40,                       /* OFF  */
  YYSYMBOL_OUTPUT_FILE = 41,               /* OUTPUT_FILE  */
  YYSYMBOL_DATETIME = 42,                  /* DATETIME  */
  YYSYMBOL_TXN_BEGIN = 43,                 /* TXN_BEGIN  */
  YYSYMBOL_TXN_COMMIT = 44,                /* TXN_COMMIT  */
  YYSYMBOL_TXN_ABORT = 45,                 /* TXN_ABORT  */
  YYSYMBOL_TXN_ROLLBACK = 46,              /* TXN_ROLLBACK  */
  YYSYMBOL_ORDER_BY = 47,                  /* ORDER_BY  */
  YYSYMBOL_ENABLE_NESTLOOP = 48,           /* ENABLE_NESTLOOP  */
  YYSYMBOL_ENABLE_SORTMERGE = 49,          /* ENABLE_SORTMERGE  */
  YYSYMBOL_LEQ = 50,                       /* LEQ  */
  YYSYMBOL_NEQ = 51,                       /* NEQ  */
  YYSYMBOL_GEQ = 52,                       /* GEQ  */
  YYSYMBOL_T_EOF = 53,                     /* T_EOF  */
  YYSYMBOL_IDENTIFIER = 54,                /* IDENTIFIER  */
  YYSYMBOL_VALUE_STRING = 55,              /* VALUE_STRING  */
  YYSYMBOL_PATH = 56,                      /* PATH  */
  YYSYMBOL_VALUE_INT = 57,                 /* VALUE_INT  */
  YYSYMBOL_VALUE_FLOAT = 58,               /* VALUE_FLOAT  */
  YYSYMBOL_VALUE_BOOL = 59,                /* VALUE_BOOL  */
  YYSYMBOL_VALUE_DATETIME = 60,            /* VALUE_DATETIME  */
  YYSYMBOL_61_ = 61,                       /* ';'  */
  YYSYMBOL_62_ = 62,                       /* '='  */
  YYSYMBOL_63_ = 63,                       /* '('  */
  YYSYMBOL_64_ = 64,                       /* ')'  */
  YYSYMBOL_65_ = 65,                       /* '*'  */
  YYSYMBOL_66_ = 66,                       /* ','  */
  YYSYMBOL_67_ = 67,                       /* '.'  */
  YYSYMBOL_68_ = 68,                       /* '<'  */
  YYSYMBOL_69_ = 69,                       /* '>'  */
  YYSYMBOL_YYACCEPT = 70,                  /* $accept  */
  YYSYMBOL_start = 71,                     /* start  */
  YYSYMBOL_stmt = 72,                      /* stmt  */
  YYSYMBOL_ckpStmt = 73,                   /* ckpStmt  */
  YYSYMBOL_loadStmt = 74,                  /* loadStmt  */
  YYSYMBOL_offStmt = 75,                   /* offStmt  */
  YYSYMBOL_txnStmt = 76,                   /* txnStmt  */
  YYSYMBOL_dbStmt = 77,                    /* dbStmt  */
  YYSYMBOL_setStmt = 78,                   /* setStmt  */
  YYSYMBOL_ddl = 79,                       /* ddl  */
  YYSYMBOL_dml = 80,                       /* dml  */
  YYSYMBOL_dql = 81,                       /* dql  */
  YYSYMBOL_aggCol = 82,                    /* aggCol  */
  YYSYMBOL_selector = 83,                  /* selector  */
  YYSYMBOL_selectors = 84,                 /* selectors  */
  YYSYMBOL_optGroupByClause = 85,          /* optGroupByClause  */
  YYSYMBOL_havingExpr = 86,                /* havingExpr  */
  YYSYMBOL_havingClause = 87,              /* havingClause  */
  YYSYMBOL_optHavingClause = 88,           /* optHavingClause  */
  YYSYMBOL_fieldList = 89,                 /* fieldList  */
  YYSYMBOL_colNameList = 90,               /* colNameList  */
  YYSYMBOL_field = 91,                     /* field  */
  YYSYMBOL_type = 92,                      /* type  */
  YYSYMBOL_valueList = 93,                 /* valueList  */
  YYSYMBOL_value = 94,                     /* value  */
  YYSYMBOL_condition = 95,                 /* condition  */
  YYSYMBOL_optWhereClause = 96,            /* optWhereClause  */
  YYSYMBOL_whereClause = 97,               /* whereClause  */
  YYSYMBOL_col = 98,                       /* col  */
  YYSYMBOL_colList = 99,                   /* colList  */
  YYSYMBOL_op = 100,                       /* op  */
  YYSYMBOL_expr = 101,                     /* expr  */
  YYSYMBOL_setClauses = 102,               /* setClauses  */
  YYSYMBOL_setClause = 103,                /* setClause  */
  YYSYMBOL_tableList = 104,                /* tableList  */
  YYSYMBOL_opt_order_clause = 105,         /* opt_order_clause  */
  YYSYMBOL_order_clause = 106,             /* order_clause  */
  YYSYMBOL_opt_asc_desc = 107,             /* opt_asc_desc  */
  YYSYMBOL_set_knob_type = 108,            /* set_knob_type  */
  YYSYMBOL_alias = 109,                    /* alias  */
  YYSYMBOL_tbName = 110,                   /* tbName  */
  YYSYMBOL_colName = 111,                  /* colName  */
  YYSYMBOL_fileName = 112                  /* fileName  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_uint8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if 1

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* 1 */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
             && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE) \
             + YYSIZEOF (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  59
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   195

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  70
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  43
/* YYNRULES -- Number of rules.  */
#define YYNRULES  105
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  198

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   315


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      63,    64,    65,     2,    66,     2,    67,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    61,
      68,    62,    69,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,    68,    68,    73,    78,    83,    88,    96,    97,    98,
      99,   100,   101,   102,   103,   107,   114,   121,   128,   132,
     136,   140,   147,   151,   158,   165,   169,   173,   177,   181,
     188,   192,   196,   203,   210,   214,   218,   222,   226,   230,
     237,   242,   246,   253,   257,   264,   268,   272,   279,   283,
     290,   291,   298,   302,   309,   313,   320,   327,   331,   335,
     339,   346,   350,   357,   361,   365,   369,   373,   380,   384,
     391,   392,   399,   403,   410,   414,   421,   425,   432,   436,
     440,   444,   448,   452,   459,   463,   467,   472,   480,   484,
     491,   498,   502,   506,   513,   517,   521,   528,   529,   530,
     534,   535,   537,   539,   541,   543
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "SHOW", "TABLES",
  "CREATE", "TABLE", "DROP", "DESC", "INSERT", "INTO", "VALUES", "DELETE",
  "FROM", "ASC", "ORDER", "BY", "GROUP", "HAVING", "MAX", "MIN", "SUM",
  "COUNT", "AS", "IN", "WHERE", "UPDATE", "SET", "SELECT", "INT", "CHAR",
  "FLOAT", "INDEX", "AND", "JOIN", "EXIT", "HELP", "STATIC", "CHECKPOINT",
  "LOAD", "OFF", "OUTPUT_FILE", "DATETIME", "TXN_BEGIN", "TXN_COMMIT",
  "TXN_ABORT", "TXN_ROLLBACK", "ORDER_BY", "ENABLE_NESTLOOP",
  "ENABLE_SORTMERGE", "LEQ", "NEQ", "GEQ", "T_EOF", "IDENTIFIER",
  "VALUE_STRING", "PATH", "VALUE_INT", "VALUE_FLOAT", "VALUE_BOOL",
  "VALUE_DATETIME", "';'", "'='", "'('", "')'", "'*'", "','", "'.'", "'<'",
  "'>'", "$accept", "start", "stmt", "ckpStmt", "loadStmt", "offStmt",
  "txnStmt", "dbStmt", "setStmt", "ddl", "dml", "dql", "aggCol",
  "selector", "selectors", "optGroupByClause", "havingExpr",
  "havingClause", "optHavingClause", "fieldList", "colNameList", "field",
  "type", "valueList", "value", "condition", "optWhereClause",
  "whereClause", "col", "colList", "op", "expr", "setClauses", "setClause",
  "tableList", "opt_order_clause", "order_clause", "opt_asc_desc",
  "set_knob_type", "alias", "tbName", "colName", "fileName", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-148)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-104)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
      57,     0,    16,    18,   -48,     6,    39,   -48,    77,    23,
    -148,  -148,    11,  -148,  -148,  -148,  -148,  -148,    68,    10,
    -148,  -148,  -148,  -148,  -148,  -148,  -148,  -148,  -148,  -148,
      62,   -48,   -48,    43,   -48,   -48,  -148,  -148,   -48,   -48,
      63,    49,  -148,  -148,    37,    59,    60,    66,    69,    27,
    -148,   108,  -148,   -12,  -148,    78,  -148,  -148,   126,  -148,
    -148,   -48,    90,    92,  -148,  -148,    93,   146,   133,   105,
    -148,   101,   107,   107,   107,    26,   109,   -48,    23,   105,
     -48,  -148,   105,   105,   105,    99,   107,  -148,  -148,     8,
    -148,   102,  -148,   103,   104,   106,   110,   111,  -148,  -148,
      -8,  -148,  -148,  -148,  -148,    -5,  -148,   -17,    31,  -148,
      64,    91,  -148,   132,   -13,   105,  -148,    91,  -148,  -148,
    -148,  -148,  -148,   -48,   -48,   149,  -148,   105,  -148,   113,
    -148,  -148,  -148,  -148,   105,  -148,  -148,  -148,  -148,  -148,
    -148,    71,  -148,   107,    84,  -148,  -148,  -148,  -148,  -148,
    -148,    84,  -148,  -148,  -148,  -148,   153,   154,  -148,   114,
    -148,  -148,    91,  -148,    54,  -148,  -148,  -148,  -148,   107,
      86,   158,   115,  -148,   116,    88,  -148,   112,    65,  -148,
     144,   165,  -148,  -148,  -148,  -148,   107,    91,    86,   107,
    -148,  -148,  -148,    32,  -148,  -148,  -148,  -148
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       4,     3,     0,    18,    19,    20,    21,     5,     0,     0,
      14,    13,     6,    10,     7,    11,     8,     9,    12,    22,
       0,     0,     0,     0,     0,     0,   103,    27,     0,     0,
       0,     0,   100,   101,     0,     0,     0,     0,     0,   104,
      42,    41,    43,     0,    34,     0,    75,   105,     0,     1,
       2,     0,     0,     0,    15,    26,     0,     0,    70,     0,
      17,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    23,     0,     0,     0,     0,     0,    31,   104,    70,
      88,     0,    24,     0,     0,     0,     0,     0,   102,    40,
      70,    91,    44,    74,    16,     0,    52,     0,     0,    54,
       0,     0,    72,    71,     0,     0,    32,     0,    36,    37,
      35,    39,    38,     0,     0,    46,    25,     0,    57,     0,
      59,    60,    56,    28,     0,    29,    65,    63,    64,    66,
      67,     0,    61,     0,     0,    82,    81,    83,    78,    79,
      80,     0,    89,    90,    93,    92,     0,    50,    53,     0,
      55,    30,     0,    73,     0,    84,    85,    69,    68,     0,
       0,    95,     0,    62,     0,     0,    76,    45,     0,    48,
      51,     0,    33,    58,    86,    87,     0,     0,     0,     0,
      77,    47,    49,    99,    94,    98,    97,    96
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -148,  -148,  -148,  -148,  -148,  -148,  -148,  -148,  -148,  -148,
    -148,    19,  -147,   117,  -148,  -148,    -6,  -148,  -148,  -148,
     100,    58,  -148,    22,  -115,    44,    -2,  -148,   -65,  -148,
      12,    38,  -148,    73,  -148,  -148,  -148,  -148,  -148,  -148,
      -4,   -64,  -148
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    51,    52,    53,   157,   179,   180,   171,   105,
     108,   106,   132,   141,   142,   112,    87,   113,    54,   177,
     151,   167,    89,    90,   100,   182,   194,   197,    44,    99,
      55,    56,    58
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      37,    77,   153,    40,    29,    91,    36,    93,    94,    95,
      97,   144,   128,   129,   130,   103,    38,    86,   107,   109,
     109,   114,    31,   178,    34,   131,   123,    62,    63,   165,
      65,    66,    30,    86,    67,    68,   165,   145,   146,   147,
     195,   178,    45,    46,    47,    48,   196,   173,    32,   148,
      35,    91,    39,    33,    78,   149,   150,    81,   124,   126,
       1,   127,     2,   107,     3,     4,     5,    57,    59,     6,
     160,    60,   191,   101,   115,    61,   104,    49,   114,   166,
      49,    64,     9,     7,     8,     9,   166,   116,    50,    70,
      69,    96,    10,    11,  -103,   133,    12,   134,   125,    71,
      13,    14,    15,    16,   176,    45,    46,    47,    48,   136,
      17,   137,   138,   139,   140,   145,   146,   147,    41,   154,
     155,   190,    72,    73,   193,    42,    43,   148,   135,    74,
     134,    76,    75,   149,   150,   161,    80,   162,    49,   136,
      49,   137,   138,   139,   140,    79,   136,   164,   137,   138,
     139,   140,   185,    82,   162,    83,    84,    85,    86,    88,
      92,    49,   111,    98,   117,   143,   156,   118,   119,   169,
     120,   172,   170,   181,   121,   122,   159,   188,   186,   183,
     184,   189,   192,   174,   110,   158,   175,   163,   152,   168,
     187,     0,     0,     0,     0,   102
};

static const yytype_int16 yycheck[] =
{
       4,    13,   117,     7,     4,    69,    54,    72,    73,    74,
      75,    24,    29,    30,    31,    79,    10,    25,    82,    83,
      84,    86,     6,   170,     6,    42,    34,    31,    32,   144,
      34,    35,    32,    25,    38,    39,   151,    50,    51,    52,
       8,   188,    19,    20,    21,    22,    14,   162,    32,    62,
      32,   115,    13,    37,    66,    68,    69,    61,    66,    64,
       3,    66,     5,   127,     7,     8,     9,    56,     0,    12,
     134,    61,   187,    77,    66,    13,    80,    54,   143,   144,
      54,    38,    28,    26,    27,    28,   151,    89,    65,    40,
      27,    65,    35,    36,    67,    64,    39,    66,   100,    62,
      43,    44,    45,    46,   169,    19,    20,    21,    22,    55,
      53,    57,    58,    59,    60,    50,    51,    52,    41,   123,
     124,   186,    63,    63,   189,    48,    49,    62,    64,    63,
      66,    23,    63,    68,    69,    64,    10,    66,    54,    55,
      54,    57,    58,    59,    60,    67,    55,    63,    57,    58,
      59,    60,    64,    63,    66,    63,    63,    11,    25,    54,
      59,    54,    63,    54,    62,    33,    17,    64,    64,    16,
      64,    57,    18,    15,    64,    64,    63,    33,    66,    64,
      64,    16,   188,   164,    84,   127,   164,   143,   115,   151,
     178,    -1,    -1,    -1,    -1,    78
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     3,     5,     7,     8,     9,    12,    26,    27,    28,
      35,    36,    39,    43,    44,    45,    46,    53,    71,    72,
      73,    74,    75,    76,    77,    78,    79,    80,    81,     4,
      32,     6,    32,    37,     6,    32,    54,   110,    10,    13,
     110,    41,    48,    49,   108,    19,    20,    21,    22,    54,
      65,    82,    83,    84,    98,   110,   111,    56,   112,     0,
      61,    13,   110,   110,    38,   110,   110,   110,   110,    27,
      40,    62,    63,    63,    63,    63,    23,    13,    66,    67,
      10,   110,    63,    63,    63,    11,    25,    96,    54,   102,
     103,   111,    59,    98,    98,    98,    65,    98,    54,   109,
     104,   110,    83,   111,   110,    89,    91,   111,    90,   111,
      90,    63,    95,    97,    98,    66,    96,    62,    64,    64,
      64,    64,    64,    34,    66,    96,    64,    66,    29,    30,
      31,    42,    92,    64,    66,    64,    55,    57,    58,    59,
      60,    93,    94,    33,    24,    50,    51,    52,    62,    68,
      69,   100,   103,    94,   110,   110,    17,    85,    91,    63,
     111,    64,    66,    95,    63,    94,    98,   101,   101,    16,
      18,    88,    57,    94,    81,    93,    98,    99,    82,    86,
      87,    15,   105,    64,    64,    64,    66,   100,    33,    16,
      98,    94,    86,    98,   106,     8,    14,   107
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    70,    71,    71,    71,    71,    71,    72,    72,    72,
      72,    72,    72,    72,    72,    73,    74,    75,    76,    76,
      76,    76,    77,    77,    78,    79,    79,    79,    79,    79,
      80,    80,    80,    81,    82,    82,    82,    82,    82,    82,
      83,    83,    83,    84,    84,    85,    85,    86,    87,    87,
      88,    88,    89,    89,    90,    90,    91,    92,    92,    92,
      92,    93,    93,    94,    94,    94,    94,    94,    95,    95,
      96,    96,    97,    97,    98,    98,    99,    99,   100,   100,
     100,   100,   100,   100,   101,   101,   101,   101,   102,   102,
     103,   104,   104,   104,   105,   105,   106,   107,   107,   107,
     108,   108,   109,   110,   111,   112
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     3,     4,     3,     1,     1,
       1,     1,     2,     4,     4,     6,     3,     2,     6,     6,
       7,     4,     5,     8,     1,     4,     4,     4,     4,     4,
       3,     1,     1,     1,     3,     3,     0,     3,     1,     3,
       0,     2,     1,     3,     1,     3,     2,     1,     4,     1,
       1,     1,     3,     1,     1,     1,     1,     1,     3,     3,
       0,     2,     1,     3,     3,     1,     1,     3,     1,     1,
       1,     1,     1,     1,     1,     1,     3,     3,     1,     3,
       3,     1,     3,     3,     3,     0,     2,     1,     1,     0,
       1,     1,     1,     1,     1,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (&yylloc, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF

/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)                                \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;        \
          (Current).first_column = YYRHSLOC (Rhs, 1).first_column;      \
          (Current).last_line    = YYRHSLOC (Rhs, N).last_line;         \
          (Current).last_column  = YYRHSLOC (Rhs, N).last_column;       \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).first_line   = (Current).last_line   =              \
            YYRHSLOC (Rhs, 0).last_line;                                \
          (Current).first_column = (Current).last_column =              \
            YYRHSLOC (Rhs, 0).last_column;                              \
        }                                                               \
    while (0)
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K])


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)


/* YYLOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

# ifndef YYLOCATION_PRINT

#  if defined YY_LOCATION_PRINT

   /* Temporary convenience wrapper in case some people defined the
      undocumented and private YY_LOCATION_PRINT macros.  */
#   define YYLOCATION_PRINT(File, Loc)  YY_LOCATION_PRINT(File, *(Loc))

#  elif defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL

/* Print *YYLOCP on YYO.  Private, do not rely on its existence. */

YY_ATTRIBUTE_UNUSED
static int
yy_location_print_ (FILE *yyo, YYLTYPE const * const yylocp)
{
  int res = 0;
  int end_col = 0 != yylocp->last_column ? yylocp->last_column - 1 : 0;
  if (0 <= yylocp->first_line)
    {
      res += YYFPRINTF (yyo, "%d", yylocp->first_line);
      if (0 <= yylocp->first_column)
        res += YYFPRINTF (yyo, ".%d", yylocp->first_column);
    }
  if (0 <= yylocp->last_line)
    {
      if (yylocp->first_line < yylocp->last_line)
        {
          res += YYFPRINTF (yyo, "-%d", yylocp->last_line);
          if (0 <= end_col)
            res += YYFPRINTF (yyo, ".%d", end_col);
        }
      else if (0 <= end_col && yylocp->first_column < end_col)
        res += YYFPRINTF (yyo, "-%d", end_col);
    }
  return res;
}

#   define YYLOCATION_PRINT  yy_location_print_

    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT(File, Loc)  YYLOCATION_PRINT(File, &(Loc))

#  else

#   define YYLOCATION_PRINT(File, Loc) ((void) 0)
    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT  YYLOCATION_PRINT

#  endif
# endif /* !defined YYLOCATION_PRINT */


# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, Location); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yylocationp);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  YYLOCATION_PRINT (yyo, yylocationp);
  YYFPRINTF (yyo, ": ");
  yy_symbol_value_print (yyo, yykind, yyvaluep, yylocationp);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, YYLTYPE *yylsp,
                 int yyrule)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)],
                       &(yylsp[(yyi + 1) - (yynrhs)]));
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
  YYLTYPE *yylloc;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
  if (!yypact_value_is_default (yyn))
    {
      /* Start YYX at -YYN if negative to avoid negative indexes in
         YYCHECK.  In other words, skip the first -YYN actions for
         this state because they are default actions.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;
      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yyx;
      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
# endif
#endif

#ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;
      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
#endif


static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
      case N:                               \
        yyformat = S;                       \
        break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yytnamerr (YY_NULLPTR, yytname[yyarg[yyi]]);
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yytname[yyarg[yyi++]]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, YYLTYPE *yylocationp)
{
  YY_USE (yyvaluep);
  YY_USE (yylocationp);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

/* Location data for the lookahead symbol.  */
static YYLTYPE yyloc_default
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  = { 1, 1, 1, 1 }
# endif
;
YYLTYPE yylloc = yyloc_default;

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

    /* The location stack: array, bottom, top.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls = yylsa;
    YYLTYPE *yylsp = yyls;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

  /* The locations where the error started and ended.  */
  YYLTYPE yyerror_range[3];

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  yylsp[0] = yylloc;
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;
        YYLTYPE *yyls1 = yyls;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yyls1, yysize * YYSIZEOF (*yylsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
        yyls = yyls1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
        YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, &yylloc);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      yyerror_range[1] = yylloc;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  *++yylsp = yylloc;

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location. */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  yyerror_range[1] = yyloc;
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* start: stmt ';'  */
#line 69 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        parse_tree = (yyvsp[-1].sv_node);
        YYACCEPT;
    }
#line 1725 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 3: /* start: HELP  */
#line 74 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
#line 1734 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 4: /* start: EXIT  */
#line 79 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
#line 1743 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 5: /* start: T_EOF  */
#line 84 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
#line 1752 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 6: /* start: offStmt  */
#line 89 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        parse_tree = (yyvsp[0].sv_node);
        YYACCEPT;
    }
#line 1761 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 15: /* ckpStmt: CREATE STATIC CHECKPOINT  */
#line 108 "/home/pepephant/GleamDB/src/parser/yacc.y"
     {
        (yyval.sv_node) = std::make_shared<CkpStmt>();
     }
#line 1769 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 16: /* loadStmt: LOAD fileName INTO tbName  */
#line 115 "/home/pepephant/GleamDB/src/parser/yacc.y"
     {
        (yyval.sv_node) = std::make_shared<LoadStmt>((yyvsp[-2].sv_str), (yyvsp[0].sv_str));
     }
#line 1777 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 17: /* offStmt: SET OUTPUT_FILE OFF  */
#line 122 "/home/pepephant/GleamDB/src/parser/yacc.y"
     {
        (yyval.sv_node) = std::make_shared<OffStmt>();
     }
#line 1785 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 18: /* txnStmt: TXN_BEGIN  */
#line 129 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnBegin>();
    }
#line 1793 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 19: /* txnStmt: TXN_COMMIT  */
#line 133 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnCommit>();
    }
#line 1801 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 20: /* txnStmt: TXN_ABORT  */
#line 137 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnAbort>();
    }
#line 1809 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 21: /* txnStmt: TXN_ROLLBACK  */
#line 141 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<TxnRollback>();
    }
#line 1817 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 22: /* dbStmt: SHOW TABLES  */
#line 148 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<ShowTables>();
    }
#line 1825 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 23: /* dbStmt: SHOW INDEX FROM tbName  */
#line 152 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<ShowIndex>((yyvsp[0].sv_str));
    }
#line 1833 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 24: /* setStmt: SET set_knob_type '=' VALUE_BOOL  */
#line 159 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<SetStmt>((yyvsp[-2].sv_setKnobType), (yyvsp[0].sv_bool));
    }
#line 1841 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 25: /* ddl: CREATE TABLE tbName '(' fieldList ')'  */
#line 166 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<CreateTable>((yyvsp[-3].sv_str), (yyvsp[-1].sv_fields));
    }
#line 1849 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 26: /* ddl: DROP TABLE tbName  */
#line 170 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DropTable>((yyvsp[0].sv_str));
    }
#line 1857 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 27: /* ddl: DESC tbName  */
#line 174 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DescTable>((yyvsp[0].sv_str));
    }
#line 1865 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 28: /* ddl: CREATE INDEX tbName '(' colNameList ')'  */
#line 178 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<CreateIndex>((yyvsp[-3].sv_str), (yyvsp[-1].sv_strs));
    }
#line 1873 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 29: /* ddl: DROP INDEX tbName '(' colNameList ')'  */
#line 182 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DropIndex>((yyvsp[-3].sv_str), (yyvsp[-1].sv_strs));
    }
#line 1881 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 30: /* dml: INSERT INTO tbName VALUES '(' valueList ')'  */
#line 189 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<InsertStmt>((yyvsp[-4].sv_str), (yyvsp[-1].sv_vals));
    }
#line 1889 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 31: /* dml: DELETE FROM tbName optWhereClause  */
#line 193 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<DeleteStmt>((yyvsp[-1].sv_str), (yyvsp[0].sv_conds));
    }
#line 1897 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 32: /* dml: UPDATE tbName SET setClauses optWhereClause  */
#line 197 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<UpdateStmt>((yyvsp[-3].sv_str), (yyvsp[-1].sv_set_clauses), (yyvsp[0].sv_conds));
    }
#line 1905 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 33: /* dql: SELECT selectors FROM tableList optWhereClause optGroupByClause optHavingClause opt_order_clause  */
#line 204 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_node) = std::make_shared<SelectStmt>((yyvsp[-6].sv_agg_cols), (yyvsp[-4].sv_strs), (yyvsp[-3].sv_conds), (yyvsp[-2].sv_groupby), (yyvsp[-1].sv_havings), (yyvsp[0].sv_orderby));
    }
#line 1913 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 34: /* aggCol: col  */
#line 211 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_col) = std::make_shared<AggCol>((yyvsp[0].sv_col));
    }
#line 1921 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 35: /* aggCol: SUM '(' col ')'  */
#line 215 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_col) = std::make_shared<AggCol>((yyvsp[-1].sv_col), AGG_SUM);
    }
#line 1929 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 36: /* aggCol: MAX '(' col ')'  */
#line 219 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_col) = std::make_shared<AggCol>((yyvsp[-1].sv_col), AGG_MAX);
    }
#line 1937 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 37: /* aggCol: MIN '(' col ')'  */
#line 223 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_col) = std::make_shared<AggCol>((yyvsp[-1].sv_col), AGG_MIN);
    }
#line 1945 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 38: /* aggCol: COUNT '(' col ')'  */
#line 227 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_col) = std::make_shared<AggCol>((yyvsp[-1].sv_col), AGG_COUNT);
    }
#line 1953 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 39: /* aggCol: COUNT '(' '*' ')'  */
#line 231 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_col) = std::make_shared<AggCol>(AGG_COUNT_STAR);
    }
#line 1961 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 40: /* selector: aggCol AS alias  */
#line 238 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyvsp[-2].sv_agg_col)->setAlias((yyvsp[0].sv_str));
        (yyval.sv_agg_col) = (yyvsp[-2].sv_agg_col);
    }
#line 1970 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 41: /* selector: aggCol  */
#line 243 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_col) = (yyvsp[0].sv_agg_col);
    }
#line 1978 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 42: /* selector: '*'  */
#line 247 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_col) = std::make_shared<AggCol>(NON_AGG_ALL);
    }
#line 1986 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 43: /* selectors: selector  */
#line 254 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_cols) = std::vector<std::shared_ptr<AggCol>>{(yyvsp[0].sv_agg_col)};
    }
#line 1994 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 44: /* selectors: selectors ',' selector  */
#line 258 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_agg_cols).push_back((yyvsp[0].sv_agg_col));
    }
#line 2002 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 45: /* optGroupByClause: GROUP BY colList  */
#line 265 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_groupby) = (yyvsp[0].sv_cols);
    }
#line 2010 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 46: /* optGroupByClause: %empty  */
#line 268 "/home/pepephant/GleamDB/src/parser/yacc.y"
                    { /* ignore*/ }
#line 2016 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 47: /* havingExpr: aggCol op value  */
#line 273 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_having) = std::make_shared<HavingExpr>((yyvsp[-2].sv_agg_col), (yyvsp[-1].sv_comp_op), (yyvsp[0].sv_val));
    }
#line 2024 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 48: /* havingClause: havingExpr  */
#line 280 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_havings) = std::vector<std::shared_ptr<HavingExpr>>{(yyvsp[0].sv_having)};
    }
#line 2032 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 49: /* havingClause: havingClause AND havingExpr  */
#line 284 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_havings).push_back((yyvsp[0].sv_having));
    }
#line 2040 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 50: /* optHavingClause: %empty  */
#line 290 "/home/pepephant/GleamDB/src/parser/yacc.y"
                      { /* ignore*/ }
#line 2046 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 51: /* optHavingClause: HAVING havingClause  */
#line 292 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_havings) = (yyvsp[0].sv_havings);
    }
#line 2054 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 52: /* fieldList: field  */
#line 299 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_fields) = std::vector<std::shared_ptr<Field>>{(yyvsp[0].sv_field)};
    }
#line 2062 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 53: /* fieldList: fieldList ',' field  */
#line 303 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_fields).push_back((yyvsp[0].sv_field));
    }
#line 2070 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 54: /* colNameList: colName  */
#line 310 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_strs) = std::vector<std::string>{(yyvsp[0].sv_str)};
    }
#line 2078 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 55: /* colNameList: colNameList ',' colName  */
#line 314 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_strs).push_back((yyvsp[0].sv_str));
    }
#line 2086 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 56: /* field: colName type  */
#line 321 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_field) = std::make_shared<ColDef>((yyvsp[-1].sv_str), (yyvsp[0].sv_type_len));
    }
#line 2094 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 57: /* type: INT  */
#line 328 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
#line 2102 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 58: /* type: CHAR '(' VALUE_INT ')'  */
#line 332 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_STRING, (yyvsp[-1].sv_int));
    }
#line 2110 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 59: /* type: FLOAT  */
#line 336 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
#line 2118 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 60: /* type: DATETIME  */
#line 340 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_type_len) = std::make_shared<TypeLen>(SV_TYPE_STRING, 20);
    }
#line 2126 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 61: /* valueList: value  */
#line 347 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_vals) = std::vector<std::shared_ptr<Value>>{(yyvsp[0].sv_val)};
    }
#line 2134 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 62: /* valueList: valueList ',' value  */
#line 351 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_vals).push_back((yyvsp[0].sv_val));
    }
#line 2142 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 63: /* value: VALUE_INT  */
#line 358 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<IntLit>((yyvsp[0].sv_int));
    }
#line 2150 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 64: /* value: VALUE_FLOAT  */
#line 362 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<FloatLit>((yyvsp[0].sv_float));
    }
#line 2158 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 65: /* value: VALUE_STRING  */
#line 366 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<StringLit>((yyvsp[0].sv_str));
    }
#line 2166 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 66: /* value: VALUE_BOOL  */
#line 370 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<BoolLit>((yyvsp[0].sv_bool));
    }
#line 2174 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 67: /* value: VALUE_DATETIME  */
#line 374 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_val) = std::make_shared<DateTimeLit>((yyvsp[0].sv_str));
    }
#line 2182 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 68: /* condition: col op expr  */
#line 381 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_cond) = std::make_shared<BinaryExpr>((yyvsp[-2].sv_col), (yyvsp[-1].sv_comp_op), (yyvsp[0].sv_expr));
    }
#line 2190 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 69: /* condition: col IN expr  */
#line 385 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_cond) = std::make_shared<BinaryExpr>((yyvsp[-2].sv_col), (yyvsp[0].sv_expr));
    }
#line 2198 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 70: /* optWhereClause: %empty  */
#line 391 "/home/pepephant/GleamDB/src/parser/yacc.y"
                      { /* ignore*/ }
#line 2204 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 71: /* optWhereClause: WHERE whereClause  */
#line 393 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_conds) = (yyvsp[0].sv_conds);
    }
#line 2212 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 72: /* whereClause: condition  */
#line 400 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_conds) = std::vector<std::shared_ptr<BinaryExpr>>{(yyvsp[0].sv_cond)};
    }
#line 2220 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 73: /* whereClause: whereClause AND condition  */
#line 404 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_conds).push_back((yyvsp[0].sv_cond));
    }
#line 2228 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 74: /* col: tbName '.' colName  */
#line 411 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>((yyvsp[-2].sv_str), (yyvsp[0].sv_str));
    }
#line 2236 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 75: /* col: colName  */
#line 415 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_col) = std::make_shared<Col>("", (yyvsp[0].sv_str));
    }
#line 2244 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 76: /* colList: col  */
#line 422 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_cols) = std::vector<std::shared_ptr<Col>>{(yyvsp[0].sv_col)};
    }
#line 2252 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 77: /* colList: colList ',' col  */
#line 426 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_cols).push_back((yyvsp[0].sv_col));
    }
#line 2260 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 78: /* op: '='  */
#line 433 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_EQ;
    }
#line 2268 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 79: /* op: '<'  */
#line 437 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_LT;
    }
#line 2276 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 80: /* op: '>'  */
#line 441 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_GT;
    }
#line 2284 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 81: /* op: NEQ  */
#line 445 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_NE;
    }
#line 2292 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 82: /* op: LEQ  */
#line 449 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_LE;
    }
#line 2300 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 83: /* op: GEQ  */
#line 453 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_comp_op) = SV_OP_GE;
    }
#line 2308 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 84: /* expr: value  */
#line 460 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_expr) = std::static_pointer_cast<Expr>((yyvsp[0].sv_val));
    }
#line 2316 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 85: /* expr: col  */
#line 464 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_expr) = std::static_pointer_cast<Expr>((yyvsp[0].sv_col));
    }
#line 2324 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 86: /* expr: '(' dql ')'  */
#line 468 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_expr) = std::make_shared<Subquery>((yyvsp[-1].sv_node));
        (yyval.sv_expr) = std::static_pointer_cast<Expr>((yyval.sv_expr));
    }
#line 2333 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 87: /* expr: '(' valueList ')'  */
#line 473 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_expr) = std::make_shared<ValueList>((yyvsp[-1].sv_vals));
        (yyval.sv_expr) = std::static_pointer_cast<Expr>((yyval.sv_expr));
    }
#line 2342 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 88: /* setClauses: setClause  */
#line 481 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_set_clauses) = std::vector<std::shared_ptr<SetClause>>{(yyvsp[0].sv_set_clause)};
    }
#line 2350 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 89: /* setClauses: setClauses ',' setClause  */
#line 485 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_set_clauses).push_back((yyvsp[0].sv_set_clause));
    }
#line 2358 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 90: /* setClause: colName '=' value  */
#line 492 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_set_clause) = std::make_shared<SetClause>((yyvsp[-2].sv_str), (yyvsp[0].sv_val));
    }
#line 2366 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 91: /* tableList: tbName  */
#line 499 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_strs) = std::vector<std::string>{(yyvsp[0].sv_str)};
    }
#line 2374 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 92: /* tableList: tableList ',' tbName  */
#line 503 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_strs).push_back((yyvsp[0].sv_str));
    }
#line 2382 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 93: /* tableList: tableList JOIN tbName  */
#line 507 "/home/pepephant/GleamDB/src/parser/yacc.y"
    {
        (yyval.sv_strs).push_back((yyvsp[0].sv_str));
    }
#line 2390 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 94: /* opt_order_clause: ORDER BY order_clause  */
#line 514 "/home/pepephant/GleamDB/src/parser/yacc.y"
    { 
        (yyval.sv_orderby) = (yyvsp[0].sv_orderby); 
    }
#line 2398 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 95: /* opt_order_clause: %empty  */
#line 517 "/home/pepephant/GleamDB/src/parser/yacc.y"
                      { /* ignore*/ }
#line 2404 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 96: /* order_clause: col opt_asc_desc  */
#line 522 "/home/pepephant/GleamDB/src/parser/yacc.y"
    { 
        (yyval.sv_orderby) = std::make_shared<OrderBy>((yyvsp[-1].sv_col), (yyvsp[0].sv_orderby_dir));
    }
#line 2412 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 97: /* opt_asc_desc: ASC  */
#line 528 "/home/pepephant/GleamDB/src/parser/yacc.y"
                 { (yyval.sv_orderby_dir) = OrderBy_ASC;     }
#line 2418 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 98: /* opt_asc_desc: DESC  */
#line 529 "/home/pepephant/GleamDB/src/parser/yacc.y"
                 { (yyval.sv_orderby_dir) = OrderBy_DESC;    }
#line 2424 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 99: /* opt_asc_desc: %empty  */
#line 530 "/home/pepephant/GleamDB/src/parser/yacc.y"
            { (yyval.sv_orderby_dir) = OrderBy_DEFAULT; }
#line 2430 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 100: /* set_knob_type: ENABLE_NESTLOOP  */
#line 534 "/home/pepephant/GleamDB/src/parser/yacc.y"
                    { (yyval.sv_setKnobType) = EnableNestLoop; }
#line 2436 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;

  case 101: /* set_knob_type: ENABLE_SORTMERGE  */
#line 535 "/home/pepephant/GleamDB/src/parser/yacc.y"
                         { (yyval.sv_setKnobType) = EnableSortMerge; }
#line 2442 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"
    break;


#line 2446 "/home/pepephant/GleamDB/src/parser/yacc.tab.cpp"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      {
        yypcontext_t yyctx
          = {yyssp, yytoken, &yylloc};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (&yylloc, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          YYNOMEM;
      }
    }

  yyerror_range[1] = yylloc;
  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yylsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  yyerror_range[2] = yylloc;
  ++yylsp;
  YYLLOC_DEFAULT (*yylsp, yyerror_range, 2);

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yylsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}

#line 544 "/home/pepephant/GleamDB/src/parser/yacc.y"

