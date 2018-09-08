/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <assert.h>
#include <set>

#include "common/rid.h"
#include "concurrency/transaction.h"


#define LOCK_MODE 0
#define UNLOCK_MODE 1

namespace cmudb {

struct ridLockType{
  std::mutex *ridMtx;
  std::condition_variable *ridCV;
	std::mutex q_mtx;
	std::set<txn_id_t> rd_txn_q;
	txn_id_t wr_txn_id;
  //uint32_t readers;
  //bool writer;
};

class LockManager {

public:
  LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){};

  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

  /***CUSTOM APIs*****/
  ridLockType* get_ridLock(const RID &rid, uint8_t mode);

private:
  bool strict_2PL_;
  std::mutex mapMtx;
  std::unordered_map<RID, ridLockType *> ridMap;
};

} // namespace cmudb
