#include "sql/dd/impl/types/basic_column_statistic_impl.h"

#include <ostream>
#include <string>

#include "my_sys.h"
#include "mysqld_error.h"
#include "sql/dd/impl/dictionary_impl.h"                 // Dictionary_impl
#include "sql/dd/impl/raw/object_keys.h"                 // Composite_4char_key
#include "sql/dd/impl/raw/raw_record.h"                  // raw_record
#include "sql/dd/impl/tables/basic_column_statistics.h"  // Basic_column_statistics::
#include "sql/dd/impl/transaction_impl.h"  // Open_dictionary_tables_ctx

namespace dd {
class Object_key;
}  // namespace dd

using dd::tables::Basic_column_statistics;

namespace dd {

///////////////////////////////////////////////////////////////////////////

Basic_column_statistic_impl::Basic_column_statistic_impl()
    : m_catalog_id(0),
      m_distinct_cnt(0),
      m_null_cnt(0),
      m_avg_len(0),
      m_distinct_cnt_synopsis_size(0),
      m_spare1(0),
      m_spare2(0),
      m_spare3(0) {}

///////////////////////////////////////////////////////////////////////////

Basic_column_statistic_impl::Basic_column_statistic_impl(
    const Basic_column_statistic_impl &src)
    : Entity_object_impl(src),
      m_catalog_id(src.m_catalog_id),
      m_name(src.m_name),
      m_schema_name(src.m_schema_name),
      m_table_name(src.m_table_name),
      m_column_name(src.m_column_name),
      m_last_analyzed(src.m_last_analyzed),
      m_distinct_cnt(src.m_distinct_cnt),
      m_null_cnt(src.m_null_cnt),
      m_max_value(src.m_max_value),
      m_min_value(src.m_min_value),
      m_avg_len(src.m_avg_len),
      m_distinct_cnt_synopsis(src.m_distinct_cnt_synopsis),
      m_distinct_cnt_synopsis_size(src.m_distinct_cnt_synopsis_size),
      m_spare1(src.m_spare1),
      m_spare2(src.m_spare2),
      m_spare3(src.m_spare3),
      m_spare4(src.m_spare4),
      m_spare5(src.m_spare5),
      m_spare6(src.m_spare6) {}

///////////////////////////////////////////////////////////////////////////
// Basic_column_statistic implementation.
///////////////////////////////////////////////////////////////////////////

bool Basic_column_statistic_impl::validate() const {
  if (schema_name().empty() || table_name().empty()) {
    my_error(ER_INVALID_DD_OBJECT, MYF(0),
             Basic_column_statistic::DD_table::instance().name().c_str(),
             "schema name or table name not supplied.");
    return true;
  }

  return false;
}

///////////////////////////////////////////////////////////////////////////

bool Basic_column_statistic_impl::restore_attributes(const Raw_record &r) {
  restore_name(r, Basic_column_statistics::FIELD_NAME);
  m_catalog_id = r.read_int(Basic_column_statistics::FIELD_CATALOG_ID);
  m_schema_name = r.read_str(Basic_column_statistics::FIELD_SCHEMA_NAME);
  m_table_name = r.read_str(Basic_column_statistics::FIELD_TABLE_NAME);
  m_column_name = r.read_str(Basic_column_statistics::FIELD_COLUMN_NAME);
  m_last_analyzed = r.read_int(Basic_column_statistics::FIELD_LAST_ANALYZED);
  m_distinct_cnt = r.read_int(Basic_column_statistics::FIELD_DISTINCT_CNT);
  m_null_cnt = r.read_int(Basic_column_statistics::FIELD_NULL_CNT);
  m_max_value = r.read_str(Basic_column_statistics::FIELD_MAX_VALUE);
  m_min_value = r.read_str(Basic_column_statistics::FIELD_MIN_VALUE);
  m_avg_len = r.read_int(Basic_column_statistics::FIELD_AVG_LEN);
  m_distinct_cnt_synopsis =
      r.read_str(Basic_column_statistics::FIELD_DISTINCT_CNT_SYNOPSIS);
  m_distinct_cnt_synopsis_size =
      r.read_int(Basic_column_statistics::FIELD_DISTINCT_CNT_SYNOPSIS_SIZE);
  m_spare1 = r.read_int(Basic_column_statistics::FIELD_SPARE1);
  m_spare2 = r.read_int(Basic_column_statistics::FIELD_SPARE2);
  m_spare3 = r.read_int(Basic_column_statistics::FIELD_SPARE3);
  m_spare4 = r.read_str(Basic_column_statistics::FIELD_SPARE4);
  m_spare5 = r.read_str(Basic_column_statistics::FIELD_SPARE5);
  m_spare6 = r.read_str(Basic_column_statistics::FIELD_SPARE6);

  return false;
}

///////////////////////////////////////////////////////////////////////////

bool Basic_column_statistic_impl::store_attributes(Raw_record *r) {
  return store_name(r, Basic_column_statistics::FIELD_NAME) ||
         r->store(Basic_column_statistics::FIELD_CATALOG_ID, m_catalog_id) ||
         r->store(Basic_column_statistics::FIELD_SCHEMA_NAME, m_schema_name) ||
         r->store(Basic_column_statistics::FIELD_TABLE_NAME, m_table_name) ||
         r->store(Basic_column_statistics::FIELD_COLUMN_NAME, m_column_name) ||
         r->store(Basic_column_statistics::FIELD_LAST_ANALYZED,
                  m_last_analyzed) ||
         r->store(Basic_column_statistics::FIELD_DISTINCT_CNT,
                  m_distinct_cnt) ||
         r->store(Basic_column_statistics::FIELD_NULL_CNT, m_null_cnt) ||
         r->store(Basic_column_statistics::FIELD_MAX_VALUE, m_max_value) ||
         r->store(Basic_column_statistics::FIELD_MIN_VALUE, m_min_value) ||
         r->store(Basic_column_statistics::FIELD_AVG_LEN, m_avg_len) ||
         r->store(Basic_column_statistics::FIELD_DISTINCT_CNT_SYNOPSIS,
                  m_distinct_cnt_synopsis) ||
         r->store(Basic_column_statistics::FIELD_DISTINCT_CNT_SYNOPSIS_SIZE,
                  m_distinct_cnt_synopsis_size) ||
         r->store(Basic_column_statistics::FIELD_SPARE1, m_spare1) ||
         r->store(Basic_column_statistics::FIELD_SPARE2, m_spare2) ||
         r->store(Basic_column_statistics::FIELD_SPARE3, m_spare3) ||
         r->store(Basic_column_statistics::FIELD_SPARE4, m_spare4) ||
         r->store(Basic_column_statistics::FIELD_SPARE5, m_spare5) ||
         r->store(Basic_column_statistics::FIELD_SPARE6, m_spare6);
}

///////////////////////////////////////////////////////////////////////////

void Basic_column_statistic_impl::debug_print(String_type &outb) const {
  dd::Stringstream_type ss;
  ss << "BASIC COLUMN STATISTICS OBJECT: { "
     << "m_catalog_id: " << m_catalog_id << "; "
     << "m_schema_name: " << m_schema_name << "; "
     << "m_table_name: " << m_table_name << "; "
     << "m_column_name: " << m_column_name << "; "
     << "m_last_analyzed: " << m_last_analyzed << "; "
     << "m_distinct_cnt: " << m_distinct_cnt << "; "
     << "m_null_cnt: " << m_null_cnt << "; "
     << "m_max_value: " << m_max_value << "; "
     << "m_min_value: " << m_min_value << "; "
     << "m_avg_len: " << m_avg_len << "; "
     << "m_distinct_cnt_synopsis: " << m_distinct_cnt_synopsis << "; "
     << "m_distinct_cnt_synopsis_size: " << m_distinct_cnt_synopsis_size << "; "
     << "m_spare1: " << m_spare1 << "; "
     << "m_spare2: " << m_spare2 << "; "
     << "m_spare3: " << m_spare3 << "; "
     << "m_spare4: " << m_spare4 << "; "
     << "m_spare5: " << m_spare5 << "; "
     << "m_spare6: " << m_spare6;

  ss << " }";
  outb = ss.str();
}

///////////////////////////////////////////////////////////////////////////

bool Basic_column_statistic_impl::has_new_primary_key() const { return false; }

///////////////////////////////////////////////////////////////////////////
Object_key *Basic_column_statistic_impl::create_primary_key() const {
  return new (std::nothrow)
      Global_name_key(Basic_column_statistics::INDEX_UK_NAME, name(),
                      Basic_column_statistics::name_collation());
}

///////////////////////////////////////////////////////////////////////////

String_type Basic_column_statistic_impl::basic_obj_info(
    const Raw_record &r) const {
  dd::Stringstream_type ss;
  ss << "[Basic_column_statistics] name "
     << r.read_str(tables::Basic_column_statistics::FIELD_NAME);
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////

const Object_table &Basic_column_statistic_impl::object_table() const {
  return DD_table::instance();
}

///////////////////////////////////////////////////////////////////////////

void Basic_column_statistic_impl::register_tables(
    Open_dictionary_tables_ctx *otx) {
  /**
    The requirement is that we should be able to update
    Table_stats, Index_stats and Basic_column_statistics DD tables even when
    someone holds global read lock, when we execute ANALYZE TABLE.
  */
  otx->mark_ignore_global_read_lock();
  otx->add_table<Basic_column_statistics>();
}

///////////////////////////////////////////////////////////////////////////

std::string Basic_column_statistic_impl::ToString() const {
  ardb::Buffer buffer;
  buffer.Printf(
      "m_catalog_id:%llu, name:%s, m_schema_name:%s, "
      "m_table_name:%s, m_column_name:%s, m_last_analyzed:%llu, "
      "m_distinct_cnt:%llu"
      ", m_null_cnt:%llu, m_max_value:%s, m_min_value:%s, "
      "m_avg_len:%llu, m_distinct_cnt_synopsis:%s"
      ", m_distinct_cnt_synopsis_size:%llu, m_spare1:%llu, m_spare2:%llu, "
      "m_spare3:%llu, m_spare4:%s, m_spare5:%s, m_spare6:%s",
      m_catalog_id, m_name.c_str(), m_schema_name.c_str(), m_table_name.c_str(),
      m_column_name.c_str(), m_last_analyzed, m_distinct_cnt, m_null_cnt,
      m_max_value.c_str(), m_min_value.c_str(), m_avg_len,
      m_distinct_cnt_synopsis.c_str(), m_distinct_cnt_synopsis_size, m_spare1,
      m_spare2, m_spare3, m_spare4.c_str(), m_spare5.c_str(), m_spare6.c_str());

  return buffer.AsString();
}

///////////////////////////////////////////////////////////////////////////

Basic_column_statistic *Basic_column_statistic_impl::clone() const {
  return new Basic_column_statistic_impl(*this);
}

///////////////////////////////////////////////////////////////////////////

String_type Basic_column_statistic::create_name(
    const String_type &schema_name, const String_type &table_name,
    const String_type &column_name) {
  String_type output;

  // Assembly fields schema_name+table_name+column_name as name
  output.assign(schema_name);
  output.push_back('\037');
  output.append(table_name);
  output.push_back('\037');

  /*
    Column names are always case insensitive, so convert it to lowercase.
    Lookups in the dictionary is always done using the name, so this should
    ensure that we always get back our object.
  */
  assert(column_name.length() <= NAME_LEN);
  char lowercase_name[NAME_LEN + 1];  // Max column length name + \0
  memcpy(lowercase_name, column_name.c_str(), column_name.length() + 1);
  my_casedn_str(system_charset_info, lowercase_name);
  output.append(lowercase_name, column_name.length());

  return output;
}


void Basic_column_statistic::create_mdl_key(const String_type &schema_name,
                                            const String_type &table_name,
                                            const String_type &column_name,
                                            MDL_key *mdl_key) {
  /*
    Column names are always case insensitive, so convert it to lowercase.
    Lookups in MDL is always done using this method, so this should
    ensure that we always have consistent locks.
  */
  assert(column_name.length() <= NAME_LEN);
  char lowercase_name[NAME_LEN + 1];  // Max column length name + \0
  memcpy(lowercase_name, column_name.c_str(), column_name.length() + 1);
  my_casedn_str(system_charset_info, lowercase_name);

  mdl_key->mdl_key_init(MDL_key::BASIC_COLUMN_STATISTICS, schema_name.c_str(),
                        table_name.c_str(), lowercase_name);
}
///////////////////////////////////////////////////////////////////////////

/**
  Lock a MDL key of basic column statistic for writing
  data into Distionary Data. The lock type is exclusive.

  @param thd     thread handle
  @param mdl_key MDL key for lock

  @return true on error, false on success
*/
bool Basic_column_statistic::lock_for_write(THD *thd, const MDL_key &mdl_key) {
  MDL_request mdl_request;
  MDL_REQUEST_INIT_BY_KEY(&mdl_request, &mdl_key, MDL_EXCLUSIVE,
                          MDL_TRANSACTION);

  return thd->mdl_context.acquire_lock(&mdl_request,
                                       thd->variables.lock_wait_timeout);
}

bool Basic_column_statistic::update_name_key(Global_name_key *key,
                                             const String_type &name) {
  return Basic_column_statistics::update_object_key(key, name);
}

///////////////////////////////////////////////////////////////////////////

}  // namespace dd
