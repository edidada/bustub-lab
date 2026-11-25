//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include <utility>
#include <vector>
#include "buffer/arc_replacer.h"
#include "common/macros.h"

namespace bustub {

namespace {

/** @brief Synchronously write a page's data to disk through the disk scheduler. */
void WritePageToDisk(const std::shared_ptr<DiskScheduler> &disk_scheduler, page_id_t page_id, const char *data) {
  auto promise = disk_scheduler->CreatePromise();
  auto future = promise.get_future();
  std::vector<DiskRequest> requests;
  requests.push_back({true, const_cast<char *>(data), page_id, std::move(promise)});
  disk_scheduler->Schedule(requests);
  future.get();
}

}  // namespace

ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  frame_->rwlatch_.lock_shared();
  is_valid_ = true;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;
  that.is_valid_ = false;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop();
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;
    that.is_valid_ = false;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
  }
  return *this;
}

auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

void ReadPageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  std::scoped_lock latch(*bpm_latch_);
  if (!frame_->is_dirty_) {
    return;
  }
  WritePageToDisk(disk_scheduler_, page_id_, frame_->GetData());
  frame_->is_dirty_ = false;
}

void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  is_valid_ = false;
  frame_->rwlatch_.unlock_shared();
  {
    std::scoped_lock latch(*bpm_latch_);
    frame_->pin_count_.fetch_sub(1);
    if (frame_->pin_count_.load() == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/

WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  frame_->rwlatch_.lock();
  is_valid_ = true;
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;
  that.is_valid_ = false;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;
    that.is_valid_ = false;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
  }
  return *this;
}

auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

void WritePageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  std::scoped_lock latch(*bpm_latch_);
  if (!frame_->is_dirty_) {
    return;
  }
  WritePageToDisk(disk_scheduler_, page_id_, frame_->GetData());
  frame_->is_dirty_ = false;
}

void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  is_valid_ = false;
  frame_->rwlatch_.unlock();
  {
    std::scoped_lock latch(*bpm_latch_);
    frame_->pin_count_.fetch_sub(1);
    if (frame_->pin_count_.load() == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
