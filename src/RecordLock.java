import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;



/*
 * This class corresponds to a lock element. That is it is the shell
 * for the locking of a single element.
 */
public class RecordLock{
	private TreeMap<Integer, LockType> grantedList = null;

    //private CopyOnWriteArrayList<LockType> queuedList = null;
	private TreeMap<Integer, LockType> queuedList = null;
    
    public RecordLockTree parentRecLockTree = null;
    
    public int recordLockId;
    
    
    
    /* @summary
     * 	Class ctor.
     * 
     * @param	sourceLockType	The transaction is used to determine the
     * 							the id of the record for which the 
     * 							RecordLock applies.
     */
    public RecordLock(Transaction sourceTxn){
    	grantedList = new TreeMap<Integer, LockType>();
    	queuedList = new TreeMap<Integer, LockType>();

		Operation currOp = sourceTxn.get(0);
		
    	// Initialize the record lock ID.
    	if (currOp.type.equals("W")){
    		// Writes have a record object.
    		recordLockId = currOp.record.ID;
    	}
    	else {
    		recordLockId = Integer.valueOf(currOp.val);
    	}

    }



    /*
     *
     */
    public boolean attemptAcquire(Transaction sourceTxn){
    	// TODO: (jmg199) REMOVE AFTER TESTING.
    	System.out.println("[Lock] Attempting to acquire lock for txn ID [" + sourceTxn.tid + "]");
    	
    	LockType sourceLockType = new LockType(sourceTxn, this);
    	
    	if (canAcquire(sourceLockType)){
    		acquire(sourceLockType);
    		// TODO: (jmg199) REMOVE AFTER TESTING.
    		System.out.println("[Lock] Lock acquired for txn ID [" + sourceTxn.tid + "]");

    		return true;
    	}
        else{
        	queuedList.put(sourceLockType.lockHolder.tid, sourceLockType);
            
            // Add this lock to the queued record lock list at the tree level.
            // This value is used to more efficiently handle acquiring queued
            // file locks when releasing *record* locks.
            parentRecLockTree.queuedRecLockTypeList.put(sourceLockType.lockHolder.tid, sourceLockType);
            
    		// TODO: (jmg199) REMOVE AFTER TESTING.
    		System.out.println("[Lock] Queuing txn ID [" + sourceTxn.tid + "] for record lock ID [" + recordLockId + "]");
    		
            return false;
        }
    }


    /*
     * This method records the lock acquisition, but does not first
     * check to see if the source transaction is allowed to have it.
     */
    private void acquire(LockType sourceLockType){
    	// If the lock type is already in the record lock, check if it needs to
    	// be upgraded, otherwise it already has a more restrictive lock so
    	// there is nothing else to do.
    	if (grantedList.containsKey(sourceLockType.lockHolder.tid)){
    		LockType existingLockType = grantedList.get(sourceLockType.lockHolder.tid);
    		
    		if ((existingLockType.lockType.equals(LockType.LockTypeFlag.PROC_READ) || 
    			 existingLockType.lockType.equals(LockType.LockTypeFlag.TRAN_READ)
    		   ) 
    		   && (sourceLockType.lockType.equals(LockType.LockTypeFlag.PROC_WRITE) ||
    			   sourceLockType.lockType.equals(LockType.LockTypeFlag.TRAN_WRITE))) {
    			
    			// Upgrade the lock type.
    			existingLockType.lockType = sourceLockType.lockType;
    		}
    	}
    	else{
    		// Store the reference to the lock type in the granted list.
    		grantedList.put(sourceLockType.lockHolder.tid, sourceLockType);

    		// Store the reference in the grantedRecLockList in the parent Record Lock tree.
    		parentRecLockTree.grantedRecLockTypeList.put(sourceLockType.lockHolder.tid, sourceLockType);

    		// Store a reference to the granted lock in the txn for quick release.
    		sourceLockType.lockHolder.grantedLocks.put(recordLockId, this);
    	}
    }


    /*
     *
     */
    public Transaction release(Transaction sourceTxn){
    	synchronized(this){
    		// Remove the lock reference from the parent record lock tree.
    		parentRecLockTree.grantedRecLockTypeList.remove(sourceTxn.tid);
    		
    		// Remove the lock type for this txn from the lock.
    		grantedList.remove(sourceTxn.tid);

    		Transaction nextTxn = attemptAcquireForNextQueuedTxn();
    		
    		if (nextTxn == null){
    			// It may not have been able to acquire the lock because
    			// a pending file lock was queued earlier than it. So check
    			// if the file lock was waiting for the release of this lock.
    			nextTxn = parentRecLockTree.attemptAcquireQueuedFileLock();
    			
    			// TODO: (jmg199) REMOVE AFTER TESTING.
    			if (nextTxn != null) {
    				System.out.println("[Sched] TxnId [" + nextTxn.tid + "] was granted *file* lock after release of record lock ID [" + recordLockId + "]");
    			}
    		}
    		else {
    			// TODO: (jmg199) REMOVE AFTER TESTING.
    			System.out.println("[Sched] TxnId [" + nextTxn.tid + "] was granted record lock ID [" + recordLockId + "]");
    		}
    	
    		return nextTxn;
    	}
    }

    
    /*
     * 
     */
    public Transaction attemptAcquireForNextQueuedTxn(){
    	// Get the oldest Txn in the queue.
    	LockType nextLockType = getOldestInQueue();
    	
    	if (nextLockType != null){
    		if (canAcquire(nextLockType)){
    			// Acquire the lock.
    			acquire(nextLockType);

    			// Remove it from the queued list.
    			queuedList.remove(nextLockType.lockHolder.tid);

    			// Now that this has been granted remove it from the tree's
    			// queued record lock list.
    			parentRecLockTree.queuedRecLockTypeList.remove(nextLockType.lockHolder.tid);

    			// Return the next transaction so it can be scheduled.
    			return nextLockType.lockHolder;
    		}
    		else {
    			return null;
    		}
    	}
    	else{
    		return null;
    	}
    }


    /*
     *
     */
    public boolean canAcquire(LockType sourceLockType){
    	// Check if the record lock is blocked by an existing or pending
    	// file lock.
    	if (parentRecLockTree.isBlockedByFileLock(sourceLockType.lockHolder)){
    		// Record lock is blocked by a file lock.
    		
    		// Check if the transaction is old enough to wound the
    		// file lock holder. In either case return false so the
    		// txn gets queued. This allows the wounded txn time to
    		// abort.
    		parentRecLockTree.attemptToWound(sourceLockType.lockHolder);
    		
    		return false;
    	}
    	else {
    		// Check if the record lock can be granted.
    		if (grantedList.isEmpty()){
    			// No transaction currently has the lock.
    			return true;
    		}
    		else{
    			TransactionList conflictingTxns = getConflictingTxns(sourceLockType);
    			
    			if (conflictingTxns.isEmpty()) {
    				// There are transactions with locks on this record, but they are compatible.
    				return true;
    			}
    			else {
    				Iterator<Transaction> iter = conflictingTxns.iterator();
    				Transaction currTxn;

    				while (iter.hasNext()){
    					currTxn = iter.next();
    					
    					// Attempt to wound any younger transaction holding a conflicting lock, 
    					// but still return *false* because it needs to queue up until the younger txns
    					// abort.
    					currTxn.wound(sourceLockType.lockHolder);
    				}

    				return false;
    			}
    		}
    	}
    }
    
    
    
    /*
     * This method looks at the source lock type object and the existing lock
     * type objects and returns a list of transactions that are in conflict
     * with the source.
     */
    private TransactionList getConflictingTxns(LockType sourceLockType){
    	TransactionList conflictingList = new TransactionList();
    	
    	LockType currLockType;

    	for (Map.Entry<Integer, LockType> entry : grantedList.entrySet()) {
    		currLockType = entry.getValue();

    		if (sourceLockType.lockType == LockType.LockTypeFlag.PROC_READ){
    			// Process Read not compatible with Process/Transaction Write.
    			if ((currLockType.lockType == LockType.LockTypeFlag.PROC_WRITE)
    			 || (currLockType.lockType == LockType.LockTypeFlag.TRAN_WRITE)){
    				conflictingList.add(currLockType.lockHolder);
    			}
    		}
    		else if (sourceLockType.lockType == LockType.LockTypeFlag.PROC_WRITE){
    			// Process Write not compatible with Process/Transaction Write or Process Read.
    			if ((currLockType.lockType == LockType.LockTypeFlag.PROC_WRITE)
    			 || (currLockType.lockType == LockType.LockTypeFlag.TRAN_WRITE)
    			 || (currLockType.lockType == LockType.LockTypeFlag.PROC_READ)){
    				conflictingList.add(currLockType.lockHolder);
    			}
    		}
    		else if (sourceLockType.lockType == LockType.LockTypeFlag.TRAN_READ){
    			// Transaction Read not compatible with Transaction Write.
    			if (currLockType.lockType == LockType.LockTypeFlag.TRAN_WRITE){
    				conflictingList.add(currLockType.lockHolder);
    			}
    		}
    		else if (sourceLockType.lockType == LockType.LockTypeFlag.TRAN_WRITE){
    			// Transaction Write not compatible with Process/Transaction Write or Read.
    			conflictingList.add(currLockType.lockHolder);
    		}
    	}
    		
    	return conflictingList;
    }
    
    
    /*
     * 
     */
    public LockType getOldestInQueue(){
    	LockType currLockType;
    	LockType targetLockType = null;
    	
    	for (Map.Entry<Integer, LockType> entry : queuedList.entrySet()) {
    		currLockType = entry.getValue();
    		
    		if (targetLockType != null){
    			if(currLockType.lockHolder.txnStart.isBefore(targetLockType.lockHolder.txnStart)){
    				targetLockType = currLockType;
    			}
    		}
			else{
    			targetLockType = currLockType;
    		}
    	}
    	
    	return targetLockType;
    }
    
    
    /*
     * 
     */
    public Transaction getOldestTxnInQueue(){
    	LockType temp = getOldestInQueue();
    	
    	if (temp != null){
    		return temp.lockHolder;
    	}
    	
    	return null;
    }
}
