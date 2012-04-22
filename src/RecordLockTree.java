import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;


public class RecordLockTree extends TreeMap<Integer, RecordLock>{
	/**
	 *
	 */
	private static final long serialVersionUID = 298387451523061405L;

	// Only one transaction can have a file lock.
	private Transaction txnGrantedFileLock = null;

	// More than one transaction can be waiting to delete the file.
	private TransactionList queuedList = null;

	//public CopyOnWriteArrayList<RecordLock> grantedRecLockList = null;
	public TreeMap<Integer, LockType> grantedRecLockTypeList = null;
	//public CopyOnWriteArrayList<RecordLock> queuedRecLockList = null;
	public TreeMap<Integer, LockType> queuedRecLockTypeList = null;
	


	/*
	 * Default class ctor.
	 */
	public RecordLockTree(){
		queuedList = new TransactionList();
		grantedRecLockTypeList = new TreeMap<Integer, LockType>();
		queuedRecLockTypeList = new TreeMap<Integer, LockType>();
	}



	/*
	 * This method determines if the specified target transaction already
	 * has a file lock.
	 */
	public boolean hasQueuedFileLocks(){
		return (!queuedList.isEmpty());
	}



	/*
	 * This method determines if the specified target transaction already
	 * has a file lock.
	 */
	public boolean hasFileLock(Transaction targetTransaction){
		if (txnGrantedFileLock == null) {
			return false;
		}
		else {
//		return (txnGrantedFileLock == targetTransaction);
			return (txnGrantedFileLock.equals(targetTransaction));
		}
	}



	/*
	 * This method determines if a *file* lock can be acquired. It assumes that
	 * the lock tree has already determined if any record locks exist in the file.
	 */
	public boolean canAcquireFileLock(Transaction targetTxn){
		// Nothing has the lock, nothing is waiting for the lock, and there are no record locks.
		if (txnGrantedFileLock != null){
			if (txnGrantedFileLock.equals(targetTxn)){
				// It already has the lock.  We're good.
				return true;
			}
			else {
				// Lock is already held, can't acquire (without wounding anyway).
				attemptToWound(targetTxn);
				
				// Return false to queue this up until the either the wounded
				// dies or if older it finishes.
				return false;
			}
		}
		
		if (!grantedRecLockTypeList.isEmpty()){
			// There are record locks in place. 
			// Try wounding, then queue up until they all die or if some are older they
			// finish.
			
			// Grab the IDs of the targets because we'll be modifying the underlying granted
			// lock tree.
			CopyOnWriteArrayList<Integer> tempIds = new CopyOnWriteArrayList<Integer>();

			for (Map.Entry<Integer, LockType> entry : grantedRecLockTypeList.entrySet()) {
				// Only add transactions that are not the requesting transaction.
				if (entry.getValue().lockHolder.tid != targetTxn.tid) {
					tempIds.add(entry.getValue().lockHolder.tid);
				}
			}
			
			// Special Case: If all lock holders are from this txn, then grant the lock.
			// This is the same as a lock upgrade.
			if (tempIds.isEmpty()) {
				return true;
			}
			
			// Attempt to wound each record lock holder.
			while (!tempIds.isEmpty()) {
				Integer txnId = tempIds.remove(0);
				
				// If the target is older than the current lock holder, then the current
				// holder will be aborted.  In either case the target gets queued since
				// we need to wait for the DM to ack the abort or wait for the current
				// lock holder to finish to commit.
				grantedRecLockTypeList.get(txnId).lockHolder.wound(targetTxn);
			}
			
			// Return false so that this will be queued up to wait for the other to die or finish.
			return false;
		}
		
		return true;
	}


	/*
	 * This method gets the file lock if it can, otherwise it queues the transaction to
	 * get it when it becomes available.
	 */
	public boolean attemptAcquireFileLock(Transaction targetTxn){
		if (canAcquireFileLock(targetTxn)){
			// TODO: (jmg199) REMOVE AFTER TESTING.
			System.out.println("[Sched.RecordLockTree] Acquiring file lock.");
					
			acquireFileLock(targetTxn);
			return true;
		}
		else {
			// TODO: (jmg199) BEGIN REMOVE AFTER TESTING.
			System.out.println("[Sched.RecordLockTree] Queuing transaction id [" + targetTxn.tid + "] for file lock.");
			
			if (txnGrantedFileLock != null) {
				System.out.println("[Sched.RecordLockTree] Transaction id [" + txnGrantedFileLock.tid + "] currently holding file lock.");
			}
			else if (!grantedRecLockTypeList.isEmpty()) {
				System.out.println("[Sched.RecordLockTree] There are granted record locks in this tree which are older.");
			}
			else {
				System.out.println("[Sched.RecordLockTree] What's going on here?");
			}
			// TODO: (jmg199) END REMOVE AFTER TESTING.
			
			queuedList.add(targetTxn);
			return false;
		}
	}


	/*
	 *
	 */
	public void acquireFileLock(Transaction targetTxn){
		txnGrantedFileLock = targetTxn;
		
		// Store this file lock in the target txn.
		targetTxn.grantedFileLocks.add(this);
	}


	
	public Transaction attemptAcquireQueuedFileLock(){
		synchronized(this){
			if (queuedList.isEmpty() || (txnGrantedFileLock != null)){
				// There is nothing to be queued, or a txn already has the lock 
				// (not likely since we just gave it up).
				return null;
			}
			else {
				if (queuedRecLockTypeList.isEmpty()){
					// There are no record locks waiting.
					// The file lock is available so take it.
					acquireFileLock(queuedList.get(0));

					return queuedList.remove(0);
				}
				else {
					// There are Record Locks queued, so they need to be
					// checked to see who goes first.
					LockType currLockType;
					Transaction queuedTxn;

					for (Map.Entry<Integer, LockType> entry : queuedRecLockTypeList.entrySet()) {
						// Check the current queued lock opStart with the
						// one queued for the file lock.
						currLockType = entry.getValue();

						queuedTxn = currLockType.parentRecordLock.getOldestTxnInQueue();

						if (queuedTxn.opStart.isBefore(queuedList.get(0).opStart)){
							// Found a transaction that is queued to get a record
							// lock in this record lock tree who started before the
							// transaction-operation trying to get the file lock.
							return null;
						}
					}

					// Therefore all pending *record* locks are waiting for this
					// file lock to be granted-released.
					acquireFileLock(queuedList.get(0));

					return queuedList.remove(0);
				}
			}
		}
	}

	
	/*
	 * This method determines if a *record* lock can be acquired solely based on the
	 * state of the record lock tree. It assumes that
	 * the lock tree has already determined if any record locks exist in the file.
	 */
	public boolean isBlockedByFileLock(Transaction targetTransaction){
		if (txnGrantedFileLock != null){
			// It's only blocked if the lock holder isn't the target transaction.
			// TODO: (jmg199) REMOVE AFTER TESTING.
//			if (targetTransaction == txnGrantedFileLock){
			if (targetTransaction.equals(txnGrantedFileLock)){
				return false;
			}
			else {
				return true;
			}
		}
		else {
			if (queuedList.size() == 0){
				// There is no file lock currently, and there is no
				// transaction waiting for the file lock.
				return false;
			}
			else {
				// There is no file lock currently, but there is at least
				// one transaction waiting for the file lock. Check each
				// transaction's start time to determine which is older.
				return (targetTransaction.txnStart.isAfter(queuedList.get(0).txnStart));
			}
		}
	}

	/*
	 * This method will attempt to wound the file lock holder. The transaction
	 * start times are compared, and if the attacker is older the file lock
	 * holder is aborted.
	 */
	public void attemptToWound(Transaction attackingTxn){
		if (txnGrantedFileLock != null){
			if (attackingTxn.txnStart.isBefore(txnGrantedFileLock.txnStart)){
				// TODO: (jmg199) BEGIN REMOVE AFTER TESTING.
				System.out.println("[Sched.RecordLockTree] Aborting transaction id [" + txnGrantedFileLock.tid + "].");
				
				// Tell the scheduler to abort this txn.
				Scheduler.getSched().abort(txnGrantedFileLock);
			}
		}
	}

	/*
	 *
	 */
	public Transaction releaseFileLock(){
		// Reset the transaction granted file lock.
		txnGrantedFileLock = null;

		Transaction nextTxn = null;

		if (!queuedList.isEmpty()) {
			// See if another file lock can be granted.
			nextTxn = attemptAcquireQueuedFileLock();
		}

		return nextTxn;
	}


	/*
	 * Overridden put() method to store the tree reference in each lock
	 * for quick check of file locks.
	 * (non-Javadoc)
	 * @see java.util.TreeMap#put(java.lang.Object, java.lang.Object)
	 */
	public RecordLock put(Integer key, RecordLock value){
		// Store the parent Record Lock Tree in the Lock.
		value.parentRecLockTree = this;

		// Call the super classes put method.
		return super.put(key, value);
	}
}
