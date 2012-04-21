import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;


public class Scheduler extends Thread{
	private static Scheduler schedTask = null;
	
    // Buffer size that the DM should use.
	private int buffer_size;

    // The type of search method the DM should employ.
	private String search_method;

    // The list of scheduled operations. The DM is the consumer of this list.
	public ArrayList<Operation> scheduled_ops = null;

    // The list of operations completed by the DM which Sched needs to clean up.
    public ArrayList<Operation> completed_ops = null;

    // This is a list of transactions to execute.
	private TransactionList transactions = null;

    // This is a list of transactions that are currently stalled. This is
    // due to operations not being available from the TM.
    private TransactionList stalledTxns = null;

    // TODO: (jmg199) NEED A DEAD LOCK QUEUE OF SOME SORT. NOT SURE WHAT THIS WILL BE YET.
    // **** NEED TO THINK ABOUT HOW THE SCHEDULED OPS LIST CAN GET FILLED AS FAST AS POSSIBLE
    //      (POSSIBLY HAVING MORE THAN ONE OP FROM THE SAME TXN QUEUED UP, BUT STILL BE ABLE
    //      TO DETECT DEAD LOCKS.
    private TransactionList deadlockList = null;
    private Duration deadlockThreshold = null;

    private LockTree lockTree = null;

    // This list is used to keep track of which txns still need to commit/abort.
    // TODO: (jmg199) THIS PROBABLY WON"T BE NEEDED. CLEAN UP AFTER TESTING.
    //private ArrayList<Integer> pendingTxnIds = null;

    // Our DM reference.
	private DataManager dm_task = null;

    // Our TM reference.
    private TranscationManager tm_task = null;

    // Flag indicating when the TM has completed its work.
    private boolean txnMgrDoneFlag = false;

    // Flag indicating when the DM has completed its work.
    private boolean dataMgrExitFlag = false;


    /*
     * @summary
     * This is the class ctor.
     *
     * @param   _sourceTransactions - List of transactions to execute.
     * @param   _buffer_size - Initial size of the buffer to provide to the data manager.
     * @param   _search_method - Search method that the DM should use to find the record.
     */
	public Scheduler(TranscationManager tm, TransactionList _sourceTransactions, int _buffer_size, String _search_method){
        // Init data members.
		buffer_size = _buffer_size;
		search_method = _search_method;
		transactions = _sourceTransactions;
		scheduled_ops = new ArrayList<Operation>();
        completed_ops = new ArrayList<Operation>();
        stalledTxns = new TransactionList();
        deadlockList = new TransactionList();
        lockTree = new LockTree();
        txnMgrDoneFlag = false;
        dataMgrExitFlag = false;
        tm_task = tm;
        
        schedTask = this;
	}
	
	
	
	/*
	 * This method returns a reference to our scheduler task. This is used
	 * when a wound is successful and a the scheduler needs to be notified
	 * that a transaction should be
	 */
	public static Scheduler getSched() {
		return schedTask;
	}


    /*
     * @summary
     * This method is used as the execution loop for the thread.
     */
	public void run(){
        // Create the DM if needed.
		if(dm_task == null){
			dm_task = new DataManager(scheduled_ops, completed_ops, buffer_size, search_method, this);
			System.out.println("[Sched] Started DataManager...");
			dm_task.start();
		}


        // Start the dead lock poll timer thread.
		// TODO: (jmg199) THIS WILL BE REMOVED.
        //System.out.println("[Sched] Starting poll timer...");
        //new PollTimer(20, this);


        // Check each transaction in the transaction list and try to schedule
        // the first operation for each.
        for (int index = 0; index < transactions.size(); ++index){
			System.out.println("[Sched] Trying to schedule first op for txn [" + transactions.get(index).tid +
					"] Total Ops:[" + String.valueOf(transactions.get(index).size()) + "]");
            scheduleNextOp(transactions.get(index));
        }


        // Continue working until the TM is done, and there are no more operations to execute.
		while(!txnMgrDoneFlag || !transactions.isEmpty()){
            scheduleStalledTxns();

            processCompletedOps();
            
            // Give the timer thread a chance to run.
            //try {
			//	sleep(2);
			//} catch (InterruptedException e) {
			//	// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}

            // TODO: (jmg199) CLEAN UP AFTER TESTING.
			//if(!transactions.get(0).isEmpty()){
			//	Operation currOp = transactions.get(0).get(0);
			//	transactions.get(0).remove(0);
			//	if(currOp.type.equals("B")){
			//		continue;
			//	}
			//	System.out.println("Scheduled this operation: "+currOp.toString());
			//	scheduled_ops.add(currOp);
			//}
		}

        dm_task.setSchedDoneFlag();

        while (!dataMgrExitFlag)
        {
            // Check periodically to see if DM is done.
            try {
				sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

        // Notify the transaction manager that the scheduler is exiting.
        tm_task.setSchedExitFlag();

        System.out.println("Scheduler is exiting...");
	}



    /* @summary
     * This method schedules the next operation for the specified transaction.
     * If there is no operation in its queue to be scheduled then it is put in
     * the stalled transaction queue.
     */
    private void scheduleNextOp(Transaction sourceTxn){
        // Add the first operation in the transaction if it exists.
        // Otherwise, add it to the blocked queue.
        if (sourceTxn.isEmpty()){
            // TODO: (jmg199) REMOVE AFTER TESTING.
            System.out.println("Transaction [" + String.valueOf(sourceTxn.tid) + "] has stalled.");

            // Add this transaction to the blocked queue.
            stalledTxns.add(sourceTxn);
        }
        else{
        	// Set the operation start time.
        	sourceTxn.opStart = DateTime.now();

        	if (sourceTxn.txnStart == null){
        		// If this is the first op then set the txn start time.
        		sourceTxn.txnStart = sourceTxn.opStart;
        	}

        	// Add the transaction to the deadlock list.
        	// The txn must be added even if it is unable to acquire the lock
        	// because it may be deadlocked with another transaction.
        	// TODO: (jmg199) BECAUSE OF WOUND WAIT THIS WILL GO AWAY.
        	deadlockList.add(sourceTxn);

        	// Commits and aborts do no need locks.
        	if ((!sourceTxn.get(0).type.equals("C")) && (!sourceTxn.get(0).type.equals("A"))) {
        		// Attempt to get a lock for it.
        		if (lockTree.acquireLock(sourceTxn)) {
        			scheduled_ops.add(sourceTxn.get(0));
        			
        			// TODO: (jmg199) REMOVE AFTER TESTING.
        			System.out.println("Sent op type [" + sourceTxn.get(0).type + "] for txn ID [" + sourceTxn.tid + "]");
        		}
        	}
        	else {
        		scheduled_ops.add(sourceTxn.get(0));

        		// TODO: (jmg199) REMOVE AFTER TESTING.
        		System.out.println("Sent op type [" + sourceTxn.get(0).type + "] for txn ID [" + sourceTxn.tid + "]");
        	}
        }
    }



    /* @summary
     * This method attempts to schedule operations for transactions that have stalled.
     */
    private void scheduleStalledTxns(){
    	Iterator<Transaction> iter = stalledTxns.iterator();
    	Transaction currTxn;

    	while (iter.hasNext()){
    		currTxn = iter.next();

    		if (!currTxn.isEmpty()){
    			// Now that there is an op, schedule it and remove the txn
    			// from the stalled list.
    			scheduleNextOp(currTxn);
    			stalledTxns.removeByTID(currTxn.tid);
    		}
    	}

        // TODO: (jmg199) THIS WAS THE OLD IMPLEMENTATION.  REMOVE AFTER TESTING THE ITERATOR IMPEMENTATION.
        //// List to store the transactions that need to be removed.
        //TransactionList restartedTxns = new TransactionList();

        //// Traverse the stalled transactions list and see if ops now exist to be scheduled.
        //for (int index = 0; index < stalledTxns.size(); ++index){
        //    if (!stalledTxns.get(index).isEmpty()){
        //        // Now that there is an op, schedule it and mark it for removal from the stalled queue.
        //        scheduleNextOp(stalledTxns.get(index));
        //        restartedTxns.add(stalledTxns.get(index));
        //    }
        //}

        //// Now that the entire stalled transactions list has been traversed
        //// remove the transactions that were restarted.
        //for (int index = 0; index < restartedTxns.size(); ++index){
        //    stalledTxns.remove(restartedTxns.get(index));
        //}
    }



    /* @summary
     * This method sets the transaction manager done flag which indicates that the
     * transaction manager has completed its work and is waiting to shutdown.
     */
    public void setTMDoneFlag(){
        txnMgrDoneFlag = true;
    }



    /* @summary
     * This method sets the data manager exit flag which indicates that the
     * data manager has completed its work and has exited.
     */
    public void setDMExitFlag(){
        dataMgrExitFlag = true;
    }



    /* @summary
     * This method handles the clean up after the DM completes an operation.
     */
    private void processCompletedOps(){
        Operation currOp = null;

        while(!completed_ops.isEmpty()){
            // TODO: (jmg199) REMOVE AFTER TESTING.
            System.out.println("[Sched] Completed ops size:[" + String.valueOf(completed_ops.size()) + "]");

        	Transaction parentTxn;
            // This needs to be synchronized because there is a race condition here
            // where a completed op is removed, but the parent txn is not yet
            // removed from the deadlock list when the poll timer fires.
            synchronized(this){
                currOp = completed_ops.remove(0);

                parentTxn = transactions.getByTID(currOp.tid);

                // Remove the txn from the deadlock list. It will be returned when the next op is scheduled.
                if (!deadlockList.remove(parentTxn)){
                    System.out.println("[Sched] DID NOT REMOVE THE TXN FROM THE DEADLOCK LIST!");
                }
            }

            if (deadlockThreshold == null){
            	// Establish the deadlock threshold.
            	Duration executionTime = (new Interval(parentTxn.opStart, DateTime.now())).toDuration();
            	System.out.println("[Sched] Initial execution time: " + String.valueOf(executionTime.getMillis()));

            	// Add a fudge factor to the execution time.
            	executionTime = executionTime.plus((long)(executionTime.getMillis() * 0.5));
            	System.out.println("[Sched] Fudged execution time: " + String.valueOf(executionTime.getMillis()));

            	// Multiply by the number of concurrent transactions.
            	executionTime = executionTime.withDurationAdded(executionTime.getMillis(), transactions.size());
            	System.out.println("[Sched] Final deadlock execution time: " + String.valueOf(executionTime.getMillis()));

            	// Store out newly calculated deadlockThreshold.
            	deadlockThreshold = executionTime;
            }

            if ((currOp.type.equals("C")) || (currOp.type.equals("A"))){
            	// TODO: (jmg199) REMOVE AFTER TESTING.
            	System.out.println("[Sched] Transaction ID [" + parentTxn.tid + "] ended with operation [" + currOp.type + "]");
            	
                // This transaction has committed or aborted, so release it's locks and
            	// remove it from the transaction list.
            	releaseLocks(parentTxn);

                if (!transactions.removeByTID(parentTxn.tid)){
                    System.out.println("[Sched] DID NOT REMOVE THE COMMITED/ABORTED TXN FROM THE TRANSACTIONS LIST!");
                }
                else {
                    System.out.println("[Sched] Removed txn id [" + parentTxn.tid + "].");
                }
                // Remove the operation from the transaction's operation list.
                parentTxn.remove(0);
            }
            else {
                // Remove the operation from the transaction's operation list.
                parentTxn.remove(0);

                // Schedule the next operation in the parentTxn.
                scheduleNextOp(parentTxn);
            }
        }
    }



    /* @summary
     * This method is used to check for dead lock conditions.
     */
    public void deadlockCheck(){
        // TODO: (jmg199) REMOVE AFTER TESTING.
        System.out.println("[Sched] About to check for deadlocks!");

        if (deadlockThreshold != null){
            // Traverse the deadlock queue looking for any overrun transactions.
            Iterator<Transaction> iter = deadlockList.iterator();
            Transaction currTxn;

            while (iter.hasNext()){
            	currTxn = iter.next();
            	Duration executionTime = (new Interval(currTxn.opStart, DateTime.now())).toDuration();

            	// TODO: (jmg199) REMOVE AFTER TESTING.
            	System.out.println("[Sched] Execution time for transaction [" + currTxn.tid + "]" + executionTime.toString() + ".");

            	if (executionTime.isLongerThan(deadlockThreshold)){
            		// TODO: (jmg199) REMOVE AFTER TESTING.
            		System.out.println("[Sched] Deadlock detected on transaction [" + currTxn.tid + "]!");
            		resolveDeadlock(currTxn);
            	}
            }
            //for (int index = 0; index < deadlockList.size(); ++index){
            //    //Duration executionTime = now.minus(deadlockList.get(index).opStart);
            //

            //    if (executionTime > deadlockThreshold){
            //        System.out.println("[Sched] Deadlock detected! execTime:");
            //        resolveDeadlock(deadlockList.get(index));
            //    }
            //}
        }

        //new PollTimer(20, this);
    }



    /* @summary
     * This method is used to resolve deadlocks.
     */
    private void resolveDeadlock(Transaction targetTxn){
    	// WOUND WAIT IMPLEMENTED ELSEWHERE.
        // TODO: (jmg199) IMPLEMENT WOUND-WAIT.
    }

    

    /*
     *
     */
    public void releaseLocks(Transaction sourceTxn){
    	// TODO: (jmg199) REMOVE AFTER TESTING.
    	System.out.println("[Sched] Releasing all locks.");

    	// If there are file locks, all pending record locks need to be checked
    	// since they may
    	//boolean triggerFullRecLockCheck = !sourceTxn.grantedFileLocks.isEmpty();
    	ArrayList<RecordLockTree> fullRecLockCheckList = new ArrayList<RecordLockTree>();

    	// Release the file locks.
    	Iterator<RecordLockTree> fileLockIter = sourceTxn.grantedFileLocks.iterator();
    	RecordLockTree currRecTree;
    	
    	// This variable stores the reference to a queued txn which was then granted this
    	// lock being released.
    	Transaction queuedTxnGrantedLock;
    	boolean hasPendingFileLocks = false;

    	while (fileLockIter.hasNext()){
    		currRecTree = fileLockIter.next();

    		// Must be checked before releasing the file lock.
    		hasPendingFileLocks = currRecTree.hasQueuedFileLocks();

    		queuedTxnGrantedLock = currRecTree.releaseFileLock();

    		if (queuedTxnGrantedLock != null){
    			// There was a transaction waiting for the released
    			// lock and it was able to get it.

    			// Reset the operation start time since we now know
    			// that it has the lock.
    			queuedTxnGrantedLock.opStart = DateTime.now();

    			// Send the txn's operation to the DM.
            	scheduled_ops.add(sourceTxn.get(0));
    		}
    		else if(hasPendingFileLocks){
    			// We had a pending file lock which was not granted. Therefore
    			// at least one record lock was waiting (and had precedence) for
    			// the file lock to be released. Therefore trigger a full
    			// check of all queued record lock requests in this tree.
    			fullRecLockCheckList.add(currRecTree);
    		}
    	}


    	// Now release the record locks.
    	RecordLock currRecLock;

    	for (Map.Entry<Integer, RecordLock> entry : sourceTxn.grantedLocks.entrySet()) {
    		currRecLock = entry.getValue();
    		queuedTxnGrantedLock = currRecLock.release(sourceTxn);

    		if (queuedTxnGrantedLock != null){
    			// There was a transaction waiting for the released
    			// lock and it was able to get it.

    			// Reset the operation start time since we now know
    			// that it has the lock.
    			queuedTxnGrantedLock.opStart = DateTime.now();

    			// Send the txn's operation to the DM.
            	scheduled_ops.add(sourceTxn.get(0));
    		}
    	}


    	// Now check if file locks were released that were flagged as having
    	// record locks which need to be granted.
    	if (!fullRecLockCheckList.isEmpty()){
    		Iterator<RecordLockTree> fullRecLockCheckIter = fullRecLockCheckList.iterator();
    		LockType currLockType = null;

    		while (fullRecLockCheckIter.hasNext()){
    			currRecTree = fullRecLockCheckIter.next();

    			// For each queued lock type, go back to it's parent and try to schedule the next
    			// oldest queued transaction waiting for it.
    			for (Map.Entry<Integer, LockType> entry : currRecTree.queuedRecLockTypeList.entrySet()) {
    				currLockType = entry.getValue();

    				currLockType.parentRecordLock.attemptAcquireForNextQueuedTxn();
    			}
    		}
		}
    }

    
    /*
     * This method aborts the specified transaction.
     */
    public void abort(Transaction targetTxn) {
    	// If an abort has already been made on this txn just let it go.
    	if (!targetTxn.abortedFlag) {
    		// Flag the transaction as aborted.
    		targetTxn.abortedFlag = true;

    		// Clear the op list for this transaction.
    		targetTxn.clear();

    		// Create an abort operation and insert it into the aborted txn.
    		Operation abortOp = new Operation(targetTxn.tid, "A");

    		targetTxn.add(abortOp);
    	}
    }


    /* @summary
     * This method attempts to get the necessary locks for the operation.
     */
    //private boolean getLock(Transaction targetTxn){
    //	// TODO: (jmg199) FINISH THE getLock() method.
    //
    //	return true;
    //}
}
