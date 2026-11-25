// Temporary debug harness for the P2 B+Tree delete path.
#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_utils.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeDebug, DumpAfterEachRemove) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  auto dump = [&](const char *tag) {
    fprintf(stderr, "[%s] root=%d\n", tag, tree.GetRootPageId());
    auto root = tree.GetRootPageId();
    if (root == INVALID_PAGE_ID) {
      fprintf(stderr, "  tree is empty\n");
      return;
    }
    auto it = tree.Begin();
    int count = 0;
    for (; !it.IsEnd(); ++it) {
      fprintf(stderr, "  scan key=%ld\n", static_cast<long>((*it).first.GetAsInteger()));
      if (++count > 40) {
        break;
      }
    }
    auto leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>>(root, bpm);
    while (leaf.Valid()) {
      fprintf(stderr, "  leaf size=%d keys=", (*leaf)->GetSize());
      for (int i = 0; i < (*leaf)->GetSize(); i++) {
        fprintf(stderr, "%ld ", static_cast<long>((*leaf)->KeyAt(i).GetAsInteger()));
      }
      fprintf(stderr, "\n");
      ++leaf;
    }
    fprintf(stderr, "  leaves done\n");
  };

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }
  dump("after insert");

  for (auto key : std::vector<int64_t>{1, 5, 3, 4}) {
    fprintf(stderr, "REMOVE %ld\n", static_cast<long>(key));
    index_key.SetFromInteger(key);
    tree.Remove(index_key);
    char buf[64];
    snprintf(buf, sizeof(buf), "after remove %ld", static_cast<long>(key));
    dump(buf);
    std::vector<RID> rids;
    index_key.SetFromInteger(2);
    fprintf(stderr, "  key 2 present=%d\n", static_cast<int>(tree.GetValue(index_key, &rids)));
  }
  delete bpm;
}

}  // namespace bustub
