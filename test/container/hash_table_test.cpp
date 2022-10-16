//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 100000; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 100000; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < 100000; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 200000, &res);
  EXPECT_EQ(0, res.size());

  printf("before delete value\n");
  // delete some values
  for (int i = 0; i < 100000; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  printf("before delete all value\n");
  // delete all values
  for (int i = 0; i < 100000; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, DISABLED_ImbalanceInsert) {
  auto *disk_manager = new DiskManager("test2.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 1000; i++) {
    ht.Insert(nullptr, 50, i);
    std::vector<int> res;
    ht.GetValue(nullptr, 50, &res);
    EXPECT_EQ(i + 1, res.size()) << "Failed to insert " << i << std::endl;
  }

  ht.VerifyIntegrity();
  for (int i = 0; i < 1000; ++i) {
    EXPECT_TRUE(ht.Remove(nullptr, 50, i));
  }
  ht.VerifyIntegrity();
  disk_manager->ShutDown();
  remove("test2.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ConcurrentInsertRemoveTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // HashTableDirectoryPage *htdp = ht.FetchDirectoryPage();

  for (int i = 300000; i < 400000; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, i));
  }

  std::thread t1([&ht]() {
    // insert many values
    // for (int i = 0; i < 100000; i++) {
    //   EXPECT_TRUE(ht.Insert(nullptr, i, i));
    // }
    for (int i = 0; i < 100000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    for (int i = 0; i < 100000; i++) {
      std::vector<int> res;
      ht.GetValue(nullptr, i, &res);
      if (res.size() != 1) {
        LOG_DEBUG("get value fail, i %d", i);
        return;
      }
      EXPECT_EQ(i, res[0]);
    }

    for (int i = 0; i < 100000; i++) {
      std::vector<int> res;
      ht.GetValue(nullptr, i, &res);
      if (res.size() != 1) {
        LOG_DEBUG("get value fail, i %d", i);
        return;
      }
      EXPECT_EQ(i, res[0]);
    }

    for (int i = 0; i < 100000; i++) {
      std::vector<int> res;
      ht.GetValue(nullptr, i, &res);
      if (res.size() != 1) {
        LOG_DEBUG("get value fail, i %d", i);
        return;
      }
      EXPECT_EQ(i, res[0]);
    }

    for (int i = 0; i < 100000; i++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }

    // for (int i = 0; i < 100000; i++) {
    //   EXPECT_TRUE(ht.Remove(nullptr, i, i));
    // }
  });

  std::thread t5([&ht]() {
    // insert many values
    // for (int i = 0; i < 100000; i++) {
    //   EXPECT_TRUE(ht.Insert(nullptr, i, i));
    // }

    for (int i = 300000; i < 400000; i++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }

    for (int i = 300000; i < 400000; i++) {
      EXPECT_FALSE(ht.Remove(nullptr, i, i));
    }

    // std::thread t2([&ht]() {
    //   // insert many values
    //   for (int i = 100000; i < 200000; i++) {
    //     EXPECT_TRUE(ht.Insert(nullptr, i, i));
    //   }

    //   // for (int i = 50000; i < 100000; i++) {
    //   //   std::vector<int> res;
    //   //   ht.GetValue(nullptr, i, &res);
    //   //   if (res.size() != 1) {
    //   //     LOG_DEBUG("get value fail, i %d", i);
    //   //     return;
    //   //   }
    //   //   EXPECT_EQ(i, res[0]);
    //   // }

    //   for (int i = 100000; i < 200000; i++) {
    //     EXPECT_TRUE(ht.Remove(nullptr, i, i));
    //   }
  });

  std::thread t3([&ht]() {
    // insert many values
    for (int i = 100000; i < 200000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    // for (int i = 200000; i < 250000; i++) {
    //   std::vector<int> res;
    //   ht.GetValue(nullptr, i, &res);
    //   if (res.size() != 1) {
    //     LOG_DEBUG("get value fail, i %d", i);
    //     return;
    //   }
    //   EXPECT_EQ(i, res[0]);
    // }

    for (int i = 100000; i < 200000; i++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }
    // for (int i = 150000; i < 250000; i++) {
    //   EXPECT_TRUE(ht.Remove(nullptr, i, i));
    // }
  });

  // std::thread t4([&ht]() {
  //   // insert many values
  //   for (int i = 90000; i < 120000; i++) {
  //     EXPECT_TRUE(ht.Insert(nullptr, i, i));
  //   }

  //   for (int i = 90000; i < 120000; i++) {
  //     std::vector<int> res;
  //     ht.GetValue(nullptr, i, &res);
  //     if (res.size() != 1) {
  //       LOG_DEBUG("get value fail, i %d", i);
  //       return;
  //     }
  //     EXPECT_EQ(i, res[0]);
  //   }
  //   for (int i = 90000; i < 120000; i++) {
  //     EXPECT_TRUE(ht.Remove(nullptr, i, i));
  //   }
  // });

  t1.join();
  // t2.join();
  t3.join();
  t5.join();
  // t4.join();

  // for (int i = 10000; i < 80000; i++) {
  //   EXPECT_TRUE(ht.Remove(nullptr, i, i));
  // }

  ht.VerifyIntegrity();
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ConcurrentInsertConcurrentRemoveTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // HashTableDirectoryPage *htdp = ht.FetchDirectoryPage();

  std::thread t1([&ht]() {
    // insert many values
    for (int i = 0; i < 100000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    for (int i = 0; i < 100000; i++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }
  });

  std::thread t2([&ht]() {
    // insert many values
    for (int i = 100000; i < 200000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    for (int i = 100000; i < 200000; i++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }
  });

  std::thread t3([&ht]() {
    // insert many values
    for (int i = 200000; i < 300000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    for (int i = 200000; i < 300000; i++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }
  });

  t1.join();
  t2.join();
  t3.join();

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}


TEST(HashTableTest, MultiThread) {
  auto *disk_manager = new DiskManager("test3.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  std::thread t1([&] {
    for (int i = 0; i < 10000; ++i) {
      ht.Insert(nullptr, i, i);
    }
  });
  std::thread t2([&] {
    for (int i = 10000; i < 20000; ++i) {
      ht.Insert(nullptr, i, i);
    }
  });
  t1.join();
  t2.join();
  ht.VerifyIntegrity();
  for (int i = 0; i < 10000; ++i) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size());
  }
  std::thread t3([&] {
    for (int i = 0; i < 10000; ++i) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }
  });
  std::thread t4([&] {
    for (int i = 10000; i < 15000; ++i) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }
  });
  t3.join();
  t4.join();
  ht.VerifyIntegrity();
  for (int i = 15000; i < 20000; ++i) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size());
  }
  for (int i = 10000; i < 15000; ++i) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(0, res.size());
  }
  for (int i = 15000; i < 20000; ++i) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
  }
  ht.VerifyIntegrity();
  disk_manager->ShutDown();
  remove("test3.db");
  delete disk_manager;
  delete bpm;
}
}  // namespace bustub
