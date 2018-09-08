/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

ridLockType* LockManager::GetRIDLock(const RID &rid, uint8_t mode){
		ridLockType *ridLock;

		auto ridIter = ridMap.find(rid);
		if(ridIter != ridMap.end()) {
			ridLock = ridIter->second;
		} else {
			assert(mode == LOCK_MODE);
			ridLock = new ridLockType;
			if(!ridLock) 
				return nullptr;
			ridLock->wr_txn_id = INVALID_TXN_ID;
		}
		return ridLock;
}

void LockManager::ReleaseAll(Transaction *txn) {
	
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
		//txn->SetState(TransactionState::ABORTED);
		return false;
	}

	
	mapMtx.lock();
	ridLock = GetRIDLock(rid, LOCK_MODE);
	mapMtx.unlock();

	if(!ridLock) return false;

	std::unique_lock<std::mutex> rdLock(ridLock->ridMtx);

	/* Implementing WAIT_DIE deadlock prevention method */
	if(ridLock->wr_txn_id != INVALID_TXN_ID &&
		 ridLock->wr_txn_id < txn->GetTransactionId())
	{
		rdLock.unlock();
		return false;
	}

	ridLock->ridCV.wait(rdLock, 
				[&]{return (ridLock->wr_txn_id == INVALID_TXN_ID);});
	
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
		//txn->SetState(TransactionState::ABORTED);		
		return false;
	}
	
	mapMtx.lock();
	ridLock = GetRIDLock(rid, LOCK_MODE);
	mapMtx.unlock();

	if(!ridLock) return false;	

	std::unique_lock<std::mutex> wrLock(ridLock->ridMtx);

	/* Implementing WAIT_DIE deadlock prevention method */
	if((ridLock->wr_txn_id != INVALID_TXN_ID &&
		  ridLock->wr_txn_id < txn->GetTransactionId()) ||
		 (ridLock->rd_txn_q.size() > 0 &&
		  *(ridLock->rd_txn_q.begin()) < txn->GetTransactionId())){
		 wrLock.unlock();
		 return false;
	}

	ridLock->ridCV.wait(wrLock, 
			    [&]{return (ridLock->wr_txn_id == INVALID_TXN_ID && 
											ridLock->rd_txn_q.size() == 0);});

	ridLock->wr_txn_id = txn->GetTransactionId();

	txn->GetExclusiveLockSet()->insert(rid);

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
	ridLockType *ridLock;
	std::unordered_set<RID>::iterator iter;

	/*  Check transaction state before proceeding.
	 *  Proceed only if the transaction is in 
	 *  GROWING State, else ABORT.
	 */
	if(txn->GetState() != TransactionState::GROWING)
	{
		/* Rollback the transaction and return false */
		//txn->SetState(TransactionState::ABORTED);		
		return false;
	}

	mapMtx.lock();
	ridLock = GetRIDLock(rid, LOCK_MODE);
	mapMtx.unlock();

	if(!ridLock) return false;	

	std::unique_lock<std::mutex> upgradeLock(ridLock->ridMtx);

	/*  Verify if it is an upgrade from shared to
	 *  exclusive, otherwise ABORT
	 */
	iter = txn->GetSharedLockSet()->find(rid);
	if(iter == txn->GetSharedLockSet()->end())
	{
		txn->SetState(TransactionState::ABORTED);
		upgradeLock.unlock();
		return false;
	}

	ridLock->rd_txn_q.erase(txn->GetTransactionId());
	txn->GetSharedLockSet()->erase(rid);
	
	/* Implementing WAIT_DIE deadlock prevention method */
	if((ridLock->wr_txn_id != INVALID_TXN_ID &&
		  ridLock->wr_txn_id < txn->GetTransactionId()) ||
		 (ridLock->rd_txn_q.size() > 0 &&
		  *(ridLock->rd_txn_q.begin()) < txn->GetTransactionId())){
		 upgradeLock.unlock();
		 return false;
	}

	ridLock->ridCV.wait(upgradeLock, 
			    [&]{return (ridLock->wr_txn_id == INVALID_TXN_ID && 
											ridLock->rd_txn_q.size() == 0);});

	ridLock->wr_txn_id = txn->GetTransactionId();

	txn->GetExclusiveLockSet()->insert(rid);

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
	ridLockType *ridLock;
	std::unordered_set<RID>::iterator iter;

	if(txn->GetState() == TransactionState::GROWING) {
		txn->SetState(TransactionState::SHRINKING);
	} 

	/* Implementing Strict Two Phase Locking */
	if(strict_2PL_ && 
		 txn->GetState() == TransactionState::SHRINKING)
	{
		return true;
	}
	
	mapMtx.lock();
	ridLock = GetRIDLock(rid, UNLOCK_MODE);
	mapMtx.unlock();

	if(!ridLock) return false;	

	
	std::unique_lock<std::mutex> releaseLock(ridLock->ridMtx);

	iter = txn->GetExclusiveLockSet()->find(rid);
	if(iter != txn->GetExclusiveLockSet()->end()){
		ridLock->wr_txn_id = INVALID_TXN_ID;
		txn->GetExclusiveLockSet()->erase(rid);
	} else {
		ridLock->rd_txn_q.erase(txn->GetTransactionId());
		txn->GetSharedLockSet()->erase(rid);
	}

	releaseLock.unlock();
	ridLock->ridCV.notify_all();
  return true;
}

} // namespace cmudb
