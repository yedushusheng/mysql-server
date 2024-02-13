#ifndef DD__BASIC_COLUMN_STATISTIC_IMPL_INCLUDED
#define DD__BASIC_COLUMN_STATISTIC_IMPL_INCLUDED

#include <memory>
#include <new>
#include <string>

#include "my_inttypes.h"
#include "sql/dd/impl/raw/raw_record.h"
#include "sql/dd/impl/types/entity_object_impl.h"  // dd::Entity_object_impl
#include "sql/dd/impl/types/weak_object_impl.h"
#include "sql/dd/object_id.h"
#include "sql/dd/string_type.h"
#include "sql/dd/types/basic_column_statistic.h"  // dd::Basic_column_statistic
#include "sql/dd/types/entity_object_table.h"

namespace dd {

///////////////////////////////////////////////////////////////////////////

class Charset;
class Object_key;
class Open_dictionary_tables_ctx;
class Raw_table;
class Weak_object;
class Object_table;

///////////////////////////////////////////////////////////////////////////

class Basic_column_statistic_impl : public Entity_object_impl,
                                    public Basic_column_statistic {
 public:
  Basic_column_statistic_impl();
  Basic_column_statistic_impl(const Basic_column_statistic_impl &);

  ~Basic_column_statistic_impl() override = default;

 public:
  void debug_print(String_type &outb) const override;

  const Object_table &object_table() const override;
  String_type basic_obj_info(const Raw_record &r) const override;
  static void register_tables(Open_dictionary_tables_ctx *otx);

  bool validate() const override;

  bool restore_attributes(const Raw_record &r) override;
  bool store_attributes(Raw_record *r) override;

 public:
  /////////////////////////////////////////////////////////////////////////
  // catalog id.
  /////////////////////////////////////////////////////////////////////////

  ulonglong catalog_id() const override { return m_catalog_id; }

  virtual void set_catalog_id(const ulonglong catalog_id) override {
    m_catalog_id = catalog_id;
  }

  /////////////////////////////////////////////////////////////////////////
  // schema name.
  /////////////////////////////////////////////////////////////////////////

  const String_type &schema_name() const override { return m_schema_name; }

  virtual void set_schema_name(const String_type &schema_name) override {
    m_schema_name = schema_name;
  }

  /////////////////////////////////////////////////////////////////////////
  // table name.
  /////////////////////////////////////////////////////////////////////////

  const String_type &table_name() const override { return m_table_name; }

  virtual void set_table_name(const String_type &table_name) override {
    m_table_name = table_name;
  }

  /////////////////////////////////////////////////////////////////////////
  // column name.
  /////////////////////////////////////////////////////////////////////////

  const String_type &column_name() const override { return m_column_name; }

  virtual void set_column_name(const String_type &column_name) override {
    m_column_name = column_name;
  }

  /////////////////////////////////////////////////////////////////////////
  // last analyzed.
  /////////////////////////////////////////////////////////////////////////

  ulonglong last_analyzed() const override { return m_last_analyzed; }

  virtual void set_last_analyzed(ulonglong last_analyzed) override {
    m_last_analyzed = last_analyzed;
  }

  /////////////////////////////////////////////////////////////////////////
  // distinct cnt.
  /////////////////////////////////////////////////////////////////////////

  ulonglong distinct_cnt() const override { return m_distinct_cnt; }

  virtual void set_distinct_cnt(ulonglong distinct_cnt) override {
    m_distinct_cnt = distinct_cnt;
  }

  /////////////////////////////////////////////////////////////////////////
  // null cnt.
  /////////////////////////////////////////////////////////////////////////

  ulonglong null_cnt() const override { return m_null_cnt; }

  virtual void set_null_cnt(ulonglong null_cnt) override {
    m_null_cnt = null_cnt;
  }

  /////////////////////////////////////////////////////////////////////////
  // max value.
  /////////////////////////////////////////////////////////////////////////

  const String_type &max_value() const override { return m_max_value; }

  virtual void set_max_value(const String_type &max_value) override {
    m_max_value = max_value;
  }

  /////////////////////////////////////////////////////////////////////////
  // min value.
  /////////////////////////////////////////////////////////////////////////

  const String_type &min_value() const override { return m_min_value; }

  virtual void set_min_value(const String_type &min_value) override {
    m_min_value = min_value;
  }

  /////////////////////////////////////////////////////////////////////////
  // avg len.
  /////////////////////////////////////////////////////////////////////////

  ulonglong avg_len() const override { return m_avg_len; }

  virtual void set_avg_len(ulonglong avg_len) override { m_avg_len = avg_len; }

  /////////////////////////////////////////////////////////////////////////
  // distinct_cnt_synopsis.
  /////////////////////////////////////////////////////////////////////////

  const String_type &distinct_cnt_synopsis() const override {
    return m_distinct_cnt_synopsis;
  }

  virtual void set_distinct_cnt_synopsis(
      const String_type &distinct_cnt_synopsis) override {
    m_distinct_cnt_synopsis = distinct_cnt_synopsis;
  }

  /////////////////////////////////////////////////////////////////////////
  // distinct_cnt_synopsis_size.
  /////////////////////////////////////////////////////////////////////////

  ulonglong distinct_cnt_synopsis_size() const override {
    return m_distinct_cnt_synopsis_size;
  }

  virtual void set_distinct_cnt_synopsis_size(
      ulonglong distinct_cnt_synopsis_size) override {
    m_distinct_cnt_synopsis_size = distinct_cnt_synopsis_size;
  }

  /////////////////////////////////////////////////////////////////////////
  // m_spare1.
  /////////////////////////////////////////////////////////////////////////

  ulonglong spare1() const override { return m_spare1; }

  virtual void set_spare1(ulonglong spare1) override { m_spare1 = spare1; }

  /////////////////////////////////////////////////////////////////////////
  // m_spare2.
  /////////////////////////////////////////////////////////////////////////

  ulonglong spare2() const override { return m_spare2; }

  virtual void set_spare2(ulonglong spare2) override { m_spare2 = spare2; }

  /////////////////////////////////////////////////////////////////////////
  // m_spare3.
  /////////////////////////////////////////////////////////////////////////

  ulonglong spare3() const override { return m_spare3; }

  virtual void set_spare3(ulonglong spare3) override { m_spare3 = spare3; }

  /////////////////////////////////////////////////////////////////////////
  // m_spare4.
  /////////////////////////////////////////////////////////////////////////

  const String_type &spare4() const override { return m_spare4; }

  virtual void set_spare4(const String_type &spare4) override {
    m_spare4 = spare4;
  }

  /////////////////////////////////////////////////////////////////////////
  // m_spare5.
  /////////////////////////////////////////////////////////////////////////

  const String_type &spare5() const override { return m_spare5; }

  virtual void set_spare5(const String_type &spare5) override {
    m_spare5 = spare5;
  }

  /////////////////////////////////////////////////////////////////////////
  // m_spare6.
  /////////////////////////////////////////////////////////////////////////

  const String_type &spare6() const override { return m_spare6; }

  virtual void set_spare6(const String_type &spare6) override {
    m_spare6 = spare6;
  }

  virtual std::string ToString() const override;
  Basic_column_statistic *clone() const override;

 public:
  Entity_object_impl *impl() override { return Entity_object_impl::impl(); }
  const Entity_object_impl *impl() const override {
    return Entity_object_impl::impl();
  }
  Object_id id() const override { return Entity_object_impl::id(); }
  bool is_persistent() const override {
    return Entity_object_impl::is_persistent();
  }
  const String_type &name() const override {
    return Entity_object_impl::name();
  }
  void set_name(const String_type &name) override {
    Entity_object_impl::set_name(name);
  }

 public:
  bool has_new_primary_key() const override;
  Object_key *create_primary_key() const override;

 private:
  // Fields
  ulonglong m_catalog_id;
  String_type m_name;
  String_type m_schema_name;
  String_type m_table_name;
  String_type m_column_name;

  ulonglong m_last_analyzed = 0;
  bool m_is_last_analyzed_null;
  ulonglong m_distinct_cnt;
  ulonglong m_null_cnt;
  String_type m_max_value;
  String_type m_min_value;
  ulonglong m_avg_len;
  String_type m_distinct_cnt_synopsis;
  ulonglong m_distinct_cnt_synopsis_size;

  // Reserved fields for data dictionary
  ulonglong m_spare1;
  ulonglong m_spare2;
  ulonglong m_spare3;
  String_type m_spare4;
  String_type m_spare5;
  String_type m_spare6;
};

///////////////////////////////////////////////////////////////////////////

}  // namespace dd

#endif  // DD__BASIC_COLUMN_STATISTIC_IMPL_INCLUDED
