
/*
 *
 */
public class Lock{
    public enum LockType {
        PROC_READ, PROC_WRITE, TRAN_READ, TRAN_WRITE
    }

    public LockType type = null;

    private TransactionList grantedList = null;

    private TransactionList queuedList = null;
    
    public RecordLockTree parentRecLockTree = null;
    

    /*
     *
     */
    public Lock(){
    	grantedList = new TransactionList();
    	queuedList = new TransactionList();
    }


    /*
     *
     */
    public boolean attemptAcquire(Transaction sourceTxn){
    	// TODO: (jmg199) REMOVE AFTER TESTING.
    	System.out.format("[Lock] Attempting to acquire lock for txn ID [" + sourceTxn.tid + "]%n");
    	
    	if (canAcquire(sourceTxn)){
    		acquire(sourceTxn);
    		// TODO: (jmg199) REMOVE AFTER TESTING.
    		System.out.format("[Lock] Lock acquired for txn ID [" + sourceTxn.tid + "]%n");

    		return true;
    	}
        else{
            queuedList.add(sourceTxn);
            
            // Add this lock to the queued record lock list at the tree level.
            // This value is used to more efficiently handle acquiring queued
            // file locks when releasing *record* locks.
            parentRecLockTree.queuedRecLockList.add(this);
            
    		// TODO: (jmg199) REMOVE AFTER TESTING.
    		System.out.format("[Lock] Lock acquired for txn ID [" + sourceTxn.tid + "]%n");
    		
            return false;
        }
    }


    /*
     * This method records the lock acquisition, but does not first
     * check to see if the source transaction is allowed to have it.
     */
    private void acquire(Transaction sourceTxn){
    	// Store the reference to the txn in the granted list.
    	grantedList.add(sourceTxn);
    	
    	// Increment the grantedRecLockCount in the parent Record Lock tree.
    	++(parentRecLockTree.grantedRecLockCount);

    	// Store a reference to the granted lock in the txn for quick release.
    	sourceTxn.grantedLocks.add(this);
    }


    /*
     *
     */
    public Transaction release(Transaction sourceTxn){
    	synchronized(this){
    		grantedList.removeByTID(sourceTxn.tid);
    		
    		// Decrement the parent record lock tree's grantedRecLockCount.
    		--(parentRecLockTree.grantedRecLockCount);

    		Transaction nextTxn = attemptAcquireForNextQueuedTxn();
    		
    		if (nextTxn == null){
    			// It may not have been able to acquire the lock because
    			// a pending file lock was queued earlier than it. So check
    			// if the file lock was waiting for the release of this lock.
    			nextTxn = parentRecLockTree.attemptAcquireQueuedFileLock();
    		}
    	
    		return nextTxn;
    	}
    }

    
    /*
     * 
     */
    public Transaction attemptAcquireForNextQueuedTxn(){
    	if (!queuedList.isEmpty()){
    		Transaction nextTxn = queuedList.get(0);

    		if (canAcquire(nextTxn)){
    			// Acquire the lock.
    			acquire(nextTxn);

    			// Remove it from the queued list.
    			queuedList.removeByTID(nextTxn.tid);

    			// Now that this has been granted remove it from the tree's
    			// queued record lock list.
    			for (int i = 0; i < parentRecLockTree.queuedRecLockList.size(); i++){
//    				if(parentRecLockTree.queuedRecLockList.get(i) == this){
    				if(parentRecLockTree.queuedRecLockList.get(i).equals(this)){
    					// TODO: (jmg199) Should probably create an ID if I can find the time.
    					parentRecLockTree.queuedRecLockList.remove(i);
    				}
    			}

    			// Return the next transaction so it can be scheduled.
    			return nextTxn;
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
    public boolean canAcquire(Transaction sourceTxn){
    	// Check if the record lock is blocked by an existing or pending
    	// file lock.
    	if (parentRecLockTree.isBlockedByFileLock(sourceTxn)){
    		// Record lock is blocked by a file lock.
    		return false;
    	}
    	else {
    		// Check if the record lock can be granted.
    		if (grantedList.isEmpty()){
    			// No transaction currently has the lock.
    			return true;
    		}
    		else{
    			// TODO: (jmg199) Implement logic to match the compatibility table.
    			// TODO: (jmg199) For now just grant one txn at a time.
    			return false;
    		}
    	}
    }
    
    
    /*
     * 
     */
    public Transaction getNextQueuedTxn(){
    	if (!queuedList.isEmpty()){
    		return queuedList.get(0);
    	}
    	else {
    		return null;
    	}
    }
}
