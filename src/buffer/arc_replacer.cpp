// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include <optional>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

/**
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  auto evict_from = [&](std::list<frame_id_t> &list, std::list<page_id_t> &ghost,
                        ArcStatus status) -> std::optional<frame_id_t> {
    for (auto it = list.begin(); it != list.end(); ++it) {
      auto alive = alive_map_.find(*it);
      if (alive == alive_map_.end() || !alive->second->evictable_) {
        continue;
      }
      auto status_ptr = alive->second;
      const frame_id_t victim_frame_id = status_ptr->frame_id_;
      list.erase(it);
      alive_map_.erase(alive);
      curr_size_--;
      // Move the victim page into the corresponding ghost list.
      status_ptr->arc_status_ = status;
      ghost.push_back(status_ptr->page_id_);
      ghost_map_[status_ptr->page_id_] = status_ptr;
      // Keep the ghost lists bounded.
      while (mru_ghost_.size() + mfu_ghost_.size() > replacer_size_) {
        if (!mru_ghost_.empty()) {
          ghost_map_.erase(mru_ghost_.front());
          mru_ghost_.pop_front();
        } else {
          ghost_map_.erase(mfu_ghost_.front());
          mfu_ghost_.pop_front();
        }
      }
      return victim_frame_id;
    }
    return std::nullopt;
  };

  // Prefer the side that is larger than its target size.
  std::optional<frame_id_t> victim;
  if (mru_.size() >= mru_target_size_) {
    victim = evict_from(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
    if (!victim.has_value()) {
      victim = evict_from(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
    }
  } else {
    victim = evict_from(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
    if (!victim.has_value()) {
      victim = evict_from(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
    }
  }
  return victim;
}

/**
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bringing the accessed page to the back of mfu_ if it exists in any of the lists
 * or the back of mru_ if it does not.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id < 0) {
    throw Exception("ArcReplacer: invalid frame id");
  }

  // Case 1: the frame is alive (in mru_ or mfu_).
  auto alive_it = alive_map_.find(frame_id);
  if (alive_it != alive_map_.end()) {
    auto status = alive_it->second;
    if (status->arc_status_ == ArcStatus::MRU) {
      mru_.remove(frame_id);
    } else {
      mfu_.remove(frame_id);
    }
    status->arc_status_ = ArcStatus::MFU;
    status->page_id_ = page_id;
    mfu_.push_back(frame_id);
    return;
  }

  // Case 2 / 3: the page is in a ghost list.
  auto ghost_it = ghost_map_.find(page_id);
  if (ghost_it != ghost_map_.end()) {
    auto status = ghost_it->second;
    ghost_map_.erase(ghost_it);
    if (status->arc_status_ == ArcStatus::MRU_GHOST) {
      mru_ghost_.remove(page_id);
      size_t delta = mru_ghost_.empty() ? 1 : std::max<size_t>(mfu_ghost_.size() / (mru_ghost_.size() + 1), 1);
      mru_target_size_ = std::min(mru_target_size_ + delta, replacer_size_);
    } else {
      mfu_ghost_.remove(page_id);
      size_t delta = mfu_ghost_.empty() ? 0 : mru_ghost_.size() / (mfu_ghost_.size() + 1);
      mru_target_size_ = mru_target_size_ > delta ? mru_target_size_ - delta : 0;
    }
    status->frame_id_ = frame_id;
    status->arc_status_ = ArcStatus::MFU;
    status->evictable_ = false;
    alive_map_[frame_id] = status;
    mfu_.push_back(frame_id);
    return;
  }

  // Case 4: miss in all four lists.
  if (mru_.size() + mru_ghost_.size() >= replacer_size_) {
    if (mru_.size() < replacer_size_ && !mru_ghost_.empty()) {
      // Drop the LRU page of mru_ghost_ to keep room for the new page.
      ghost_map_.erase(mru_ghost_.front());
      mru_ghost_.pop_front();
    }
  }
  auto status = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  alive_map_[frame_id] = status;
  mru_.push_back(frame_id);

  // Keep the total number of entries bounded by 2 * capacity.
  while (mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size() > 2 * replacer_size_) {
    if (!mru_ghost_.empty()) {
      ghost_map_.erase(mru_ghost_.front());
      mru_ghost_.pop_front();
    } else if (!mfu_ghost_.empty()) {
      ghost_map_.erase(mfu_ghost_.front());
      mfu_ghost_.pop_front();
    } else {
      break;
    }
  }
}

/**
 * @brief Toggle whether a frame is evictable or non-evictable.
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id < 0) {
    throw Exception("ArcReplacer: invalid frame id");
  }
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }
  if (it->second->evictable_ == set_evictable) {
    return;
  }
  it->second->evictable_ = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

/**
 * @brief Remove an evictable frame from replacer (no ghost entry is created).
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id < 0) {
    throw Exception("ArcReplacer: invalid frame id");
  }
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }
  if (!it->second->evictable_) {
    throw Exception("ArcReplacer: removing a non-evictable frame");
  }
  if (it->second->arc_status_ == ArcStatus::MRU) {
    mru_.remove(frame_id);
  } else {
    mfu_.remove(frame_id);
  }
  alive_map_.erase(it);
  curr_size_--;
}

/**
 * @brief Return replacer's size, which tracks the number of evictable frames.
 */
auto ArcReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
