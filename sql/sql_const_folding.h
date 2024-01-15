#ifndef SQL_CONST_FOLDING_INCLUDED
#define SQL_CONST_FOLDING_INCLUDED

/* Copyright (c) 2018, 2019, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file sql/sql_const_folding.h

  Interface to SQL expression constant folding
*/

#include "sql/field.h"                     // Field_real, Field
#include "sql/item.h"                      // Item, Item_field, Item_int
#include "sql/item_cmpfunc.h"              // Item_bool_func2, Item_cond
#include "sql/item_func.h"                 // Item_func, Item_func::LE_FUNC
#include "sql/item_timefunc.h"             // Item_date_literal

class THD;


/**
  fold_condition uses analyze_field_constant to analyze constants in
  comparisons expressions with a view to fold the expression. The analysis
  will determine the range characteristic of the constant, as represented by
  one value of this set. In the simple case, if the constant is within the range
  of the field to which it is being compared, the value used is RP_INSIDE.
*/
enum Range_placement {
  /**
    The constant is larger than the highest value in the range
  */
  RP_OUTSIDE_HIGH,
  /**
    The constant is lower than the lowest value in the range
  */
  RP_OUTSIDE_LOW,
  /**
    The constant has the value of the lowest value in the range. Only used
    for integer types and the YEAR type.
   */
  RP_ON_MIN,
  /**
    The constant has the value of the highest value in the range. Only used
    for integer types and the YEAR type.
  */
  RP_ON_MAX,
  /**
    The constant has a value within the range, but that is *not* on one of the
    borders if applicable for the type, cf. RP_ON_MIN and RP_ON_MAX.
  */
  RP_INSIDE,
  /**
    The constant has a value within the range, but was truncated. Used
    for DECIMAL if the constant has more precision in the fraction than the
    field and for floating point underflow.
  */
  RP_INSIDE_TRUNCATED,
  /**
    The YEAR type has a discontinuous range {0} U [1901, 2155]. If the constant
    is larger than 0, but less than 1901, we represent that with this value.
  */
  RP_INSIDE_YEAR_HOLE,
  /**
    The constant has been rounded down, i.e.\ to the left on the number line,
    due to restriction on field length (FLOAT or DOUBLE) or because we have a
    FLOAT type and the constant given has a mantissa with more significant
    digits than allowed for FLOAT.
  */
  RP_ROUNDED_DOWN,
  /**
    The constant has been rounded up, i.e.\ to the right on the number line, due
    to restriction on field length (FLOAT or DOUBLE) or because we have a FLOAT
    type and the constant given has a mantissa with more significant digits
    than allowed for FLOAT.
  */
  RP_ROUNDED_UP
};

/**
  Fold boolean condition {=, <>, >, >=, <, <=, <=>} involving constants and
  fields (or references to fields), possibly determining if the condition is
  always true or false based on type range of the field, or possibly simplifying
  >=, <= to = if constant lies on the range border.

  Also fold such conditions if they are present as arguments of other functions
  in the same way, except there we replace the arguments with manifest FALSE or
  field <> field to exclude NULLs if the field is nullable, cf.
  ignore_unknown().

  The constants are always converted to constants of the type of the field, no
  widening of the field type in therefore necessary after this folding for
  comparison to occur. When converting constants to decimal (when the field is a
  decimal), the constant will receive the same number of fractional decimals as
  the field. If decimal constant fraction truncation occurs, the comparison
  {GT,GE,LT,LE} logic is adjusted to preserve semantics without widening the
  field's type.

  @param      thd        session state
  @param      cond       the condition to handle
  @param[out] retcond    condition after const removal
  @param[out] cond_value resulting value of the condition
     - COND_OK:    condition must be evaluated (e.g int == 3)
     - COND_TRUE:  always true                 (e.g signed tinyint < 128)
     - COND_FALSE: always false                (e.g unsigned tinyint < 0)

  @param      manifest_result
                         For IS NOT NULL on a not nullable item, if true,
                         return item TRUE (1), else remove condition and return
                         COND_TRUE. For cmp items, this is determined by
                         Item_bool_func2::ignore_unknown.
  @returns false if success, true if error
*/
bool fold_condition(THD *thd, Item *cond, Item **retcond,
                    Item::cond_result *cond_value,
                    bool manifest_result = false);

/**
  Analyze a constant's placement within (or without) the type range of the
  field f. Also normalize the given constant to the type of the field if
  applicable.

  @param      thd       the session context
  @param      f         the field
  @param[out] const_val a pointer to an item tree pointer containing the
                        constant (at execution time). May be modified if
                        we replace or fold the constant.
  @param      func      the function of the comparison
  @param      left_has_field
                        the field is the left operand
  @param[out] place     the placement of the const_val relative to
                        the range of f
  @param[out] discount_equal
                        set to true: caller should replace GE with GT or LE
                        with LT.
  @param[out] negative  true if the constant (decimal) is has a (minus) sign
  @returns   true on error
*/
bool analyze_field_constant(THD *thd, Item_field *f, Item **const_val,
                            Item_func *func, bool left_has_field,
                            Range_placement *place, bool *discount_equal,
                            bool *negative);

#endif /* SQL_CONST_FOLDING_INCLUDED */
