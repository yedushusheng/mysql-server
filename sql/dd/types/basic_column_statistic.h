#ifndef DD__BASIC_COLUMN_STATISTICS_INCLUDED
#define DD__BASIC_COLUMN_STATISTICS_INCLUDED

#include "sql/dd/types/entity_object.h"        // Entity_object
#include "sql/dd/types/entity_object_table.h"  // Entity_object_table

typedef long my_time_t;
struct MDL_key;

namespace dd {

///////////////////////////////////////////////////////////////////////////

class Basic_column_statistic_impl;
class Primary_id_key;
class Global_name_key;
class Void_key;

namespace tables {
class Basic_column_statistics;
}

///////////////////////////////////////////////////////////////////////////
/**
  save basic column statistics info used for TDStore and SQLEngine  
*/
struct BasicColumnStat {
  String_type schema_name_ = "";
  String_type table_name_ = "";
  String_type column_name_ = "";
  /** last analyzed time */
  ulonglong last_analyzed_ = 0;
  ulonglong distinct_cnt_ = 0;
  ulonglong null_cnt_ = 0;
  String_type max_value_ = "";
  String_type min_value_ = "";
  ulonglong avg_len_ = 0;
  String_type distinct_cnt_synopsis_ = "";
  ulonglong distinct_cnt_synopsis_size_ = 0;

  void init(const BasicColumnStat *stat) {
    schema_name_ = stat->schema_name_;
    table_name_ = stat->table_name_;
    column_name_ = stat->column_name_;
    last_analyzed_ = stat->last_analyzed_;
    distinct_cnt_ = stat->distinct_cnt_;
    null_cnt_ = stat->null_cnt_;
    max_value_ = stat->max_value_;
    min_value_ = stat->min_value_;
    avg_len_ = stat->avg_len_;
    distinct_cnt_synopsis_ = stat->distinct_cnt_synopsis_;
    distinct_cnt_synopsis_size_ = stat->distinct_cnt_synopsis_size_;
  }

  void init(String_type schema_name, String_type table_name,
            String_type column_name, ulonglong last_analyzed,
            ulonglong distinct_cnt, ulonglong null_cnt, String_type max_value,
            String_type min_value, ulonglong avg_len,
            String_type distinct_cnt_synopsis,
            ulonglong distinct_cnt_synopsis_size) {
    schema_name_ = schema_name;
    table_name_ = table_name;
    column_name_ = column_name;
    last_analyzed_ = last_analyzed;
    distinct_cnt_ = distinct_cnt;
    null_cnt_ = null_cnt;
    max_value_ = max_value;
    min_value_ = min_value;
    avg_len_ = avg_len;
    distinct_cnt_synopsis_ = distinct_cnt_synopsis;
    distinct_cnt_synopsis_size_ = distinct_cnt_synopsis_size;
  }

  void clear() {
    schema_name_.clear();
    table_name_.clear();
    column_name_.clear();
    last_analyzed_ = 0;
    distinct_cnt_ = 0;
    null_cnt_ = 0;
    max_value_.clear();
    min_value_.clear();
    avg_len_ = 0;
    distinct_cnt_synopsis_.clear();
    distinct_cnt_synopsis_size_ = 0;
  }

  bool is_efficient() const {
    if (schema_name_.empty() || table_name_.empty() ||
        column_name_.empty()) {
      return false;
    }
    return true;
  }

  String_type ToString() const {
    String_type str;
    str += "schema_name: " + schema_name_;
    str += ", table_name: " + table_name_;
    str += ", column_name: " + column_name_;
    std::string last_analyzed = std::to_string(last_analyzed_);
    str += ", last_analyzed: ";
    str += String_type(last_analyzed.c_str(), last_analyzed.c_str());
    std::string distinct_cnt = std::to_string(distinct_cnt_);
    str += ", distinct_cnt: ";
    str += String_type(distinct_cnt.c_str(), strlen(distinct_cnt.c_str()));
    std::string null_cnt = std::to_string(null_cnt_);
    str += ", null_cnt: ";
    str += String_type(null_cnt.c_str(), strlen(null_cnt.c_str()));
    str += ", max_value: " + max_value_;
    str += ", min_value: " + min_value_;
    std::string avg_len = std::to_string(avg_len_);
    str += ", avg_len: ";
    str += String_type(avg_len.c_str(), strlen(avg_len.c_str()));
    str += ", distinct_cnt_synopsis: " + distinct_cnt_synopsis_;
    std::string distinct_cnt_synopsis_size =
        std::to_string(distinct_cnt_synopsis_size_);
    str += ", distinct_cnt_synopsis_size: ";
    str += String_type(distinct_cnt_synopsis_size.c_str(),
                       strlen(distinct_cnt_synopsis_size.c_str()));
    return str;
  }
};

///////////////////////////////////////////////////////////////////////////

class Basic_column_statistic : virtual public Entity_object {
 public:
  typedef Basic_column_statistic_impl Impl;
  typedef tables::Basic_column_statistics DD_table;
  typedef Global_name_key Name_key;

  virtual bool update_name_key(Name_key *key) const {
    return update_name_key(key, name());
  }

  static bool update_name_key(Name_key *key, const String_type &name);

 public:
  virtual ~Basic_column_statistic() {}

  /////////////////////////////////////////////////////////////////////////
  // catalog_id.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong catalog_id() const = 0;
  virtual void set_catalog_id(ulonglong catalog_id) = 0;

  /////////////////////////////////////////////////////////////////////////
  // schema name.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &schema_name() const = 0;
  virtual void set_schema_name(const String_type &schema_name) = 0;

  /////////////////////////////////////////////////////////////////////////
  // table name.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &table_name() const = 0;
  virtual void set_table_name(const String_type &table_name) = 0;

  /////////////////////////////////////////////////////////////////////////
  // column name.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &column_name() const = 0;
  virtual void set_column_name(const String_type &column_name) = 0;

  /////////////////////////////////////////////////////////////////////////
  // last_analyzed.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong last_analyzed() const = 0;
  virtual void set_last_analyzed(ulonglong last_analyzed) = 0;

  /////////////////////////////////////////////////////////////////////////
  // distinct_cnt.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong distinct_cnt() const = 0;
  virtual void set_distinct_cnt(ulonglong distinct_cnt) = 0;

  /////////////////////////////////////////////////////////////////////////
  // null_cnt.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong null_cnt() const = 0;
  virtual void set_null_cnt(ulonglong null_cnt) = 0;

  /////////////////////////////////////////////////////////////////////////
  // max_value.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &max_value() const = 0;
  virtual void set_max_value(const String_type &max_value) = 0;

  /////////////////////////////////////////////////////////////////////////
  // min_value.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &min_value() const = 0;
  virtual void set_min_value(const String_type &min_value) = 0;

  /////////////////////////////////////////////////////////////////////////
  // avg_len.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong avg_len() const = 0;
  virtual void set_avg_len(ulonglong avg_len) = 0;

  /////////////////////////////////////////////////////////////////////////
  // distinct_cnt_synopsis.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &distinct_cnt_synopsis() const = 0;
  virtual void set_distinct_cnt_synopsis(
      const String_type &distinct_cnt_synopsis) = 0;

  /////////////////////////////////////////////////////////////////////////
  // distinct_cnt_synopsis_size.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong distinct_cnt_synopsis_size() const = 0;
  virtual void set_distinct_cnt_synopsis_size(
      ulonglong distinct_cnt_synopsis_size) = 0;

  /////////////////////////////////////////////////////////////////////////
  // spare1.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong spare1() const = 0;
  virtual void set_spare1(ulonglong spare1) = 0;

  /////////////////////////////////////////////////////////////////////////
  // spare2.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong spare2() const = 0;
  virtual void set_spare2(ulonglong spare2) = 0;

  /////////////////////////////////////////////////////////////////////////
  // spare3.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong spare3() const = 0;
  virtual void set_spare3(ulonglong spare3) = 0;

  /////////////////////////////////////////////////////////////////////////
  // spare4.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &spare4() const = 0;
  virtual void set_spare4(const String_type &spare4) = 0;

  /////////////////////////////////////////////////////////////////////////
  // spare5.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &spare5() const = 0;
  virtual void set_spare5(const String_type &spare5) = 0;

  /////////////////////////////////////////////////////////////////////////
  // spare6.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &spare6() const = 0;
  virtual void set_spare6(const String_type &spare6) = 0;

  /*
    Create a unique name for a basic column statistic object based on the
    triplet SCHEMA_NAME TABLE_NAME COLUMN_NAME separated with the 'Unit
    Separator' character.
  */
  static String_type create_name(const String_type &schema_name,
                                 const String_type &table_name,
                                 const String_type &column_name);

  String_type create_name() const {
    return Basic_column_statistic::create_name(schema_name(), table_name(),
                                               column_name());
  }

  static void create_mdl_key(const String_type &schema_name,
                             const String_type &table_name,
                             const String_type &column_name, MDL_key *key);

  void create_mdl_key(MDL_key *key) const {
    Basic_column_statistic::create_mdl_key(schema_name(), table_name(),
                                           column_name(), key);
  }
  
  /**
    Print Debug info
  */
  virtual std::string ToString() const = 0;

  /**
    Allocate a new object graph and invoke the copy contructor for
    each object.

    @return pointer to dynamically allocated copy
  */
  virtual Basic_column_statistic *clone() const = 0;
};

///////////////////////////////////////////////////////////////////////////

}  // namespace dd

#endif  // DD__BASIC_COLUMN_STATISTICS_INCLUDED
