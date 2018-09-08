/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

ridLockType* LockManager::get_ridLock(const RID &rid, uint8_t mode){
		ridLockType *ridLock;

		auto ridIter = ridMap.find(rid);
		if(ridIter != ridMap.end()) {
			ridLock = ridIter->second;
		} else {
			assert(mode == LOCK_MODE);
			ridLock = new ridLockType;
			if(!ridLock) 
				return nullptr;
			ridLock->ridMtx = new std::mutex;
			ridLock->ridCV 	= new std::condition_variable;
			ridLock->readers = 0;
			ridLock->writer  = false;
			ridLock->wr_txn_id = INVALID_TXN_ID; 
		}
		return ridLock;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
	ridLockType *ridLock;

	/*  Check transaction state before proceeding.
	 *  Proceed only if the transaction is in 
	 *  GROWING State, else ABORT.
	 */
	if(txn->GetState() != TransactionState::GROWING)
	{
		/* Rollback the transaction and return false */
		txn->SetState(TransactionState::ABORTED);
		return false;
	}

	
	mapMtx.lock();
	ridLock = get_ridLock(rid, LOCK_MODE);
	mapMtx.unlock();

	if(!ridLock) return false;

	ridLock->q_mtx.lock();
	if(wr_txn_id != INVALID_TXN_ID 
					&& wr_txn_id < txn->GetTransactionId()) {
		txn->SetState(TransactionState::ABORTED);
		ridLock->q_mtx.unlock();
		return false;
	}
	ridLock->q_mtx.unlock();

	std::unique_lock<std::mutex> rdLock(*ridLock->ridMtx);
	//ridLock->ridCV->wait(rdLock, [&]{return (!ridLock->writer);});
	ridLock->ridCV->wait(rdLock, 
			[&]{return (ridLock->wr_txn_id == INVALID_TXN_ID);});
	//ridLock->readers++;
	ridLock->rd_txn_q.insert(txn->GetTransactionId());
	
	txn->GetSharedLockSet()->insert(rid);
	
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
	ridLockType *ridLock;


	/*  Check transaction state before proceeding.
	 *  Proceed only if the transaction is in 
	 *  GROWING State, else ABORT.
	 */
	if(txn->GetState() != TransactionState::GROWING)
	{
		/* Rollback the transaction and return false */
		txn->SetState(TransactionState::ABORTED);		
		return false;
	}
	
	mapMtx.lock();
	ridLock = get_ridLock(rid, LOCK_MODE);
	mapMtx.unlock();

	if(!ridLock) return false;	

	std::unique_lock<std::mutex> wrLock(*ridLock->ridMtx);
	//ridLock->ridCV->wait(wrLock,
			//[&]{return (!ridLock->writer && ridLock->readers == 0);});
	ridLock->ridCV->wait(wrLock,
			[&]{return (ridLock->wr_txn_id == INVALID_TXN_ID && 
									ridLock->rd_txn_q.size() == 0);});

	//ridLock->writer = true;
	ridLock->wr_txn_id = txn->GetTransactionId();

	txn->GetExclusiveLockSet()->insert(rid);

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  return false;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
	ridLockType *ridLock;
	std::unordered_set<RID>::iterator iter;

	if(txn->GetState() == TransactionState::GROWING) {
			txn->SetState(TransactionState::SHRINKING);
	} 

	if(txn->GetState() != TransactionState::SHRINKING) {
		 txn->SetState(TransactionState::ABORTED);
		 return false;
	}
	
	mapMtx.lock();
	ridLock = get_ridLock(rid, UNLOCK_MODE);
	mapMtx.unlock();

	if(!ridLock) return false;	
	
	std::unique_lock<std::mutex> rwLock(*ridLock->ridMtx);

	iter = txn->GetExclusiveLockSet()->find(rid);
	if(iter != txn->GetExclusiveLockSet()->end()){
		//ridLock->writer = false;
		ridLock->wr_txn_id = INVALID_TXN_ID;
		txn->GetExclusiveLockSet()->erase(rid);
	} else {
		//ridLock->readers--;
	  ridLock->rd_txn_q.erase(txn->GetTransactionId());	
		txn->GetSharedLockSet()->erase(rid);
	}

	rwLock.unlock();
	ridLock->ridCV->notify_all();
  return true;
}

} // namespace cmudb
