#pragma once

#include <cstddef>
#pragma once

#include <atomic>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <fstream>
#include <filesystem>

extern "C" {
#include "betree.h"
}

#include "src/core/db.hpp"
#include "src/core/helper.hpp"
#include "src/core/types.hpp"

namespace ucsb::haura {

inline storage_pref_t get_storage_pref_none() {
  storage_pref_t pref;
  pref._0 = 254;
  return pref;
}

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_t = ucsb::value_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_length_t = ucsb::value_length_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using db_hints_t = ucsb::db_hints_t;
using transaction_t = ucsb::transaction_t;

class hauradb_t : public ucsb::db_t {
public:
  inline hauradb_t(): cfg_(nullptr), db_(nullptr), betree_db_(nullptr), dataset_(nullptr), created_dataset_(false) {}

  void set_config(fs::path const &config_path, fs::path const &main_dir_path,
                  std::vector<fs::path> const &storage_dir_paths,
                  db_hints_t const &hints) override;
  bool open(std::string &error) override;
  void close() override;

  std::string info() override { return {}; }

  operation_result_t upsert(key_t key, value_spanc_t value) override;
  operation_result_t update(key_t key, value_spanc_t value) override;
  operation_result_t remove(key_t key) override;
  operation_result_t read(key_t key, value_span_t value) const override;

  operation_result_t batch_upsert(keys_spanc_t keys, values_spanc_t values,
                                  value_lengths_spanc_t sizes) override;
  operation_result_t batch_read(keys_spanc_t keys,
                                values_span_t values) const override;

  operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values,
                               value_lengths_spanc_t sizes) override;

  operation_result_t range_select(key_t key, size_t length,
                                  values_span_t values) const override;
  operation_result_t scan(key_t key, size_t length,
                          value_span_t single_value) const override;

  void flush() override;

  size_t size_on_disk() const override;

  std::unique_ptr<transaction_t> create_transaction() override { return {}; }

private:
  fs::path config_path_;
  fs::path main_dir_path_;
  std::vector<fs::path> storage_dir_paths_;

  using my_cfg_t = betree_h::cfg_t;
  betree_h::cfg_t *cfg_;
  ucsb::db_t *db_;
  betree_h::database_t *betree_db_; // Pointer to Haura's db_t
  betree_h::ds_t *dataset_;

  bool created_dataset_;
};

void hauradb_t::set_config(fs::path const &config_path,
                           fs::path const &main_dir_path,
                           std::vector<fs::path> const &storage_dir_paths,
                           db_hints_t const &hints) {
  config_path_ = config_path;
  main_dir_path_ = main_dir_path;
  storage_dir_paths_ = storage_dir_paths;

  // std::cout << "Config path: " << config_path_ << std::endl;
  // std::cout << "Main dir path: " << main_dir_path_ << std::endl;
  // std::cout << "Storage dir paths: ";
  // for (auto const &path : storage_dir_paths_) {
  //   std::cout << path << " ";
  // }
  // std::cout << std::endl;

  // load the configuration from the config_path file
  // std::ifstream file(config_path_);
  // if (!file) {
  //   throw exception_t("Failed to open config file\n");
  // }
  // std::string file_content((std::istreambuf_iterator<char>(file)),
  //                          std::istreambuf_iterator<char>());
  // const char* config_cstr = file_content.c_str();
  // unsigned int config_len = file_content.size();
  betree_h::err_t *err = nullptr;
  // std::cout << config_cstr << std::endl;
  // std::cout << config_len << std::endl;
  // cfg_ = betree_h::betree_parse_configuration(&config_cstr, config_len, &err);
  cfg_ = betree_h::betree_configuration_from_env(&err);
  // Handle potential errors during configuration creation
  if (err != nullptr) {
    betree_h::betree_print_error(err);
    betree_h::betree_free_err(err);
    throw exception_t("\nFailed to create Haura configuration\n");
  }
}

bool hauradb_t::open(std::string &error) {
  if (!betree_db_) {
    betree_h::err_t *err = nullptr;
    betree_db_ = betree_h::betree_open_or_create_db(cfg_, &err);
    if (err != nullptr) {
      betree_h::betree_print_error(err);
      betree_h::betree_free_err(err);
      error = "\nFailed to open Haura database\n";
      return false;
    }
  }

  // Open the dataset
  if (!created_dataset_) {
    betree_h::err_t *err_ds = nullptr;
    int err_int = betree_h::betree_create_ds(betree_db_, "main", 4,
                                             get_storage_pref_none(), &err_ds);
    if (err_ds != nullptr || err_int != 0) {
      betree_h::betree_print_error(err_ds);
      betree_h::betree_free_err(err_ds);
      error = "\nFailed to create Haura dataset\n";
      return false;
    }
    created_dataset_ = true;
  }

  if (!dataset_) {
    betree_h::err_t *err_ds = nullptr;
    dataset_ = betree_h::betree_open_ds(betree_db_, "main", 4,
                                        get_storage_pref_none(), &err_ds);
    if (err_ds != nullptr) {
      betree_h::betree_print_error(err_ds);
      betree_h::betree_free_err(err_ds);
      error = "\nFailed to open Haura dataset\n";
      return false;
    }
  }

  // TODO: Test read, write, delete here
  // Sync database for peace of mind

  return true;
}

void hauradb_t::close() {
  if (!betree_db_)
    return;

  // int betree_close_ds(struct database_t *db, struct ds_t *ds, struct err_t **err);
  // betree_h::err_t *err_ds = nullptr;
  // int err = betree_h::betree_close_ds(betree_db_, dataset_, &err_ds);
  // if (err_ds != nullptr || err != 0) {
  //   betree_h::betree_print_error(err_ds);
  //   betree_h::betree_free_err(err_ds);
  //   throw exception_t("\nFailed to close Haura dataset\n");
  // }


  betree_h::betree_close_db(betree_db_);
  betree_db_ = nullptr;
}

void hauradb_t::flush() {
  if (!betree_db_)
    return;

  betree_h::err_t *err = nullptr;
  betree_h::betree_sync_db(betree_db_, &err);
  if (err != nullptr) {
    betree_h::betree_print_error(err);
    betree_h::betree_free_err(err);
    throw exception_t("\nFailed to flush Haura database\n");
  }
}

operation_result_t hauradb_t::upsert(key_t key, value_spanc_t value) {
  betree_h::err_t *err = nullptr;
  int res = betree_h::betree_dataset_upsert(
      dataset_, reinterpret_cast<const char *>(&key), sizeof(key),
      reinterpret_cast<const char *>(value.data()), value.size(), 0,
      get_storage_pref_none(), &err);

  if (err != nullptr) {
    betree_h::betree_print_error(err);
    betree_h::betree_free_err(err);
    return {0, operation_status_t::error_k};
  }

  return {size_t(res == 0),
          res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t hauradb_t::update(key_t key, value_spanc_t value) {
  return hauradb_t::upsert(key, value);
}

operation_result_t hauradb_t::remove(key_t key) {
  betree_h::err_t *err = nullptr;
  int res = betree_h::betree_dataset_delete(
      dataset_, reinterpret_cast<char const *>(&key), sizeof(key), &err);
  if (err != nullptr) {
    betree_h::betree_print_error(err);
    betree_h::betree_free_err(err);
    return {0, operation_status_t::error_k};
  }

  return {size_t(res == 0),
          res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t hauradb_t::read(key_t key, value_span_t value) const {
  betree_h::err_t *err = nullptr;
  betree_h::byte_slice_t betree_value;
  int res = betree_h::betree_dataset_get(dataset_,
                                         reinterpret_cast<char const *>(&key),
                                         sizeof(key), &betree_value, &err);
  if (err != nullptr) {
    betree_h::betree_print_error(err);
    betree_h::betree_free_err(err);
    return {0, operation_status_t::error_k};
  }
  if (res)
    return {0, operation_status_t::not_found_k};

  // Do we need to resize the value?
  memcpy(value.data(), betree_value.ptr, betree_value.len);
  betree_h::betree_free_byte_slice(&betree_value);

  return {1, operation_status_t::ok_k};
}

operation_result_t hauradb_t::batch_upsert(keys_spanc_t keys,
                                           values_spanc_t values,
                                           value_lengths_spanc_t sizes) {
  betree_h::err_t *err = nullptr;
  size_t offset = 0;
  for (size_t idx = 0; idx < keys.size(); ++idx) {
    int res = betree_h::betree_dataset_upsert(
        dataset_, reinterpret_cast<char const *>(&keys[idx]), sizeof(key_t),
        reinterpret_cast<char const *>(values.data() + offset), sizes[idx], 0,
        get_storage_pref_none(), &err);
    if (err != nullptr || res != 0) {
      std::cout << __LINE__;
      betree_h::betree_print_error(err);
      betree_h::betree_free_err(err);
      return {0, operation_status_t::error_k};
    }
    offset += sizes[idx];
  }

  return {keys.size(), operation_status_t::ok_k};
}

operation_result_t hauradb_t::batch_read(keys_spanc_t keys,
                                         values_span_t values) const {
  betree_h::err_t *err = nullptr;
  size_t offset = 0;
  size_t found_cnt = 0;
  for (auto key : keys) {
    betree_h::byte_slice_t betree_value;
    int res = betree_h::betree_dataset_get(dataset_,
                                           reinterpret_cast<char const *>(&key),
                                           sizeof(key), &betree_value, &err);
    if (err != nullptr) {
      betree_h::betree_print_error(err);
      betree_h::betree_free_err(err);
      return {0, operation_status_t::error_k};
    }
    if (res == 0) {
      memcpy(values.data() + offset, betree_value.ptr, betree_value.len);
      offset += betree_value.len;
      ++found_cnt;
    }
    betree_h::betree_free_byte_slice(&betree_value);
  }
  return {found_cnt, operation_status_t::ok_k};
}

operation_result_t hauradb_t::bulk_load(keys_spanc_t keys,
                                        values_spanc_t values,
                                        value_lengths_spanc_t sizes) {
  return batch_upsert(keys, values, sizes);
}

operation_result_t hauradb_t::range_select(key_t key, size_t length,
                                           values_span_t values) const {
  // TODO: because of the interface of the betree we need to first get the high
  // key and get the range with the help of that
  betree_h::err_t *err = nullptr;
  betree_h::range_iter_t *range_iter = betree_h::betree_dataset_range(
      dataset_, reinterpret_cast<char const *>(&key), sizeof(key_t),
      reinterpret_cast<char const *>(&key + length), sizeof(key_t), &err);
  if (err != nullptr) {
    betree_h::betree_print_error(err);
    betree_h::betree_free_err(err);
    return {0, operation_status_t::error_k};
  }

  size_t offset = 0;
  size_t selected_records_count = 0;
  betree_h::byte_slice_t betree_key;
  betree_h::byte_slice_t betree_value;
  while (betree_h::betree_range_iter_next(range_iter, &betree_key,
                                          &betree_value, &err) == 0) {
    memcpy(values.data() + offset, betree_value.ptr, betree_value.len);
    offset += betree_value.len;
    ++selected_records_count;
  }
  if (err != nullptr) {
    betree_h::betree_print_error(err);
    betree_h::betree_free_err(err);
  }
  betree_h::betree_free_range_iter(range_iter);

  return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t hauradb_t::scan(key_t key, size_t length,
                                   value_span_t single_value) const {
  betree_h::err_t *err = nullptr;
  betree_h::range_iter_t *range_iter = betree_h::betree_dataset_range(
      dataset_, reinterpret_cast<char const *>(&key), sizeof(key_t),
      reinterpret_cast<char const *>(&key + length), sizeof(key_t), &err);
  if (err != nullptr) {
    betree_h::betree_print_error(err);
    betree_h::betree_free_err(err);
    return {0, operation_status_t::error_k};
  }

  size_t scanned_records_count = 0;
  betree_h::byte_slice_t betree_key;
  betree_h::byte_slice_t betree_value;
  while (betree_h::betree_range_iter_next(range_iter, &betree_key,
                                          &betree_value, &err) == 0) {
    memcpy(single_value.data(), betree_value.ptr, betree_value.len);
    ++scanned_records_count;
  }
  if (err != nullptr) {
    betree_h::betree_print_error(err);
    betree_h::betree_free_err(err);
  }
  betree_h::betree_free_range_iter(range_iter);

  return {scanned_records_count, operation_status_t::ok_k};
}

size_t hauradb_t::size_on_disk() const {
  return ucsb::size_on_disk(main_dir_path_);
}

} // namespace ucsb::haura
