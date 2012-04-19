import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.Iterator;


public class RecordLockTree extends TreeMap<Integer, Lock>{
	/**
	 *
	 */
	private static final long serialVersionUID = 298387451523061405L;

	// Only one transaction can have a file lock.
	private Transaction txnGrantedFileLock = null;

	// More than one transaction can be waiting to delete the file.
	private TransactionList queuedList = null;

	public CopyOnWriteArrayList<Lock> grantedRecLockList = null;
	public CopyOnWriteArrayList<Lock> queuedRecLockList = null;


	/*
	 * Default class ctor.
	 */
	public RecordLockTree(){
		queuedList = new TransactionList();
		grantedRecLockList = new CopyOnWriteArrayList<Lock>();
		queuedRecLockList = new CopyOnWriteArrayList<Lock>();
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
//		return (txnGrantedFileLock == targetTransaction);
		return (txnGrantedFileLock.equals(targetTransaction));
	}



	/*
	 * This method determines if a *file* lock can be acquired. It assumes that
	 * the lock tree has already determined if any record locks exist in the file.
	 */
	public boolean canAcquireFileLock(){
		// Nothing has the lock, nothing is waiting for the lock, and there are no record locks.
		return (txnGrantedFileLock == null) && (queuedList.size() == 0) && (grantedRecLockList == 0);
	}


	/*
	 * This method gets the file lock if it can, otherwise it queues the transaction to
	 * get it when it becomes available.
	 */
	public boolean attemptAcquireFileLock(Transaction targetTxn){
		if (canAcquireFileLock()){
			acquireFileLock(targetTxn);
			return true;
		}
		else {
			queuedList.add(targetTxn);
			return false;
		}
	}


	/*
	 *
	 */
	public void acquireFileLock(Transaction targetTxn){
		txnGrantedFileLock = targetTxn;
	}


	public Transaction attemptAcquireQueuedFileLock(){
		synchronized(this){
			if (queuedList.isEmpty() || (txnGrantedFileLock != null)){
				return null;
			}
			else {
				if (queuedRecLockList.isEmpty()){
					// There are no record locks waiting.
					// The file lock is available so take it.
					acquireFileLock(queuedList.get(0));

					return queuedList.remove(0);
				}
				else {
					// There are Record Locks queued, so they need to be
					// checked to see who goes first.
					Iterator<Lock> iter = queuedRecLockList.iterator();
					Lock currLock;
					Transaction queuedTxn;

					while (iter.hasNext()){
						// Check the current queued lock opStart with the
						// one queued for the file lock.
						currLock = iter.next();

						queuedTxn = currLock.getNextQueuedTxn();

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
				// transactions opStart time to determine which got there
				// first.
				return (targetTransaction.opStart.isBefore(queuedList.get(0).opStart));
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

		// TODO: (jmg199) THIS CAN'T BE HERE. IT HAS TO BE RELOCATED.
		// There is at least one record lock queued earlier
		// than the next file lock request. Traverse all queued
		// locks to find them.
		/*
		Iterator<Lock> iter = queuedRecLockList.iterator();
		Lock currLock;
		Transaction queuedTxn;

		while (iter.hasNext()){
			// Check the current queued lock opStart with the
			// one queued for the file lock.
			currLock = iter.next();

			queuedTxn = currLock.getNextQueuedTxn();
		*/
	}


	/*
	 * Overridden put() method to store the tree reference in each lock
	 * for quick check of file locks.
	 * (non-Javadoc)
	 * @see java.util.TreeMap#put(java.lang.Object, java.lang.Object)
	 */
	public Lock put(Integer key, Lock value){
		// Store the parent Record Lock Tree in the Lock.
		value.parentRecLockTree = this;

		// Call the super classes put method.
		return super.put(key, value);
	}
}
