//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  auto it = current_reads_.find(read_ts);
  if (it == current_reads_.end()) {
    current_reads_.emplace(read_ts, 1);
    if (current_reads_.size() == 1) {
      watermark_ = read_ts;
    } else {
      watermark_ = std::min(watermark_, read_ts);
    }
    return;
  }
  it->second++;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto it = current_reads_.find(read_ts);
  if (it == current_reads_.end()) {
    return;
  }
  it->second--;
  if (it->second <= 0) {
    bool was_watermark = (it->first == watermark_);
    current_reads_.erase(it);
    if (was_watermark && !current_reads_.empty()) {
      watermark_ = current_reads_.begin()->first;
    }
  }
}

}  // namespace bustub
