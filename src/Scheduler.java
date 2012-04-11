import java.util.ArrayList;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.Instant;



public class Scheduler extends Thread{
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
        // Init data memebers.
		buffer_size = _buffer_size;
		search_method = _search_method;
		transactions = _sourceTransactions;
		scheduled_ops = new ArrayList<Operation>();
        completed_ops = new ArrayList<Operation>();
        stalledTxns = new TransactionList();
        deadlockList = new TransactionList();
        txnMgrDoneFlag = false;
        dataMgrExitFlag = false;
        tm_task = tm;
	}


    /*
     * @summary
     * This method is used as the execution loop for the thread.
     */
	public void run(){
        // Create the DM if needed.
		if(dm_task == null){
			dm_task = new DataManager(scheduled_ops, completed_ops, buffer_size, search_method, this);
			System.out.println("Started DataManager...");
			dm_task.start();
		}


        // TODO: (jmg199) Start the dead lock poll timer thread.


        // Check each transaction in the transaction list and try to schedule
        // the first operation for each.
        for (int index = 0; index < transactions.size(); ++index){
            scheduleNextOp(transactions.get(index));
        }


        // Continue working until the TM is done, and there are no more operations to execute.
		while(!txnMgrDoneFlag && !transactions.isEmpty()){
            scheduleStalledTxns();

            processCompletedOps();

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
            // Check every 1/4 sec. to see if DM is done.
            try {
				sleep(250);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

        // Notify the transaction manager that the scheduler is exiting.
        tm_task.setSchedExitFlag();
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
            // Add this transaction to the blocked queue.
            stalledTxns.add(sourceTxn);
        }
        else{
            Operation nextOp = sourceTxn.get(0);

            // TODO: (jmg199) UPDATE THE TIMESTAMP!!!!
            deadlockList.add(sourceTxn);

            // TODO: (jmg199) Inspect the operation and see if we can get a lock for it.
            // if (getLock(nextOp)){
            scheduled_ops.add(nextOp);
            //}
        }
    }



    /* @summary
     * This method attempts to schedule operations for transactions that have stalled.
     */
    private void scheduleStalledTxns(){
        // List to store the transactions that need to be removed.
        ArrayList<Integer> restartedTxnIndexes = new ArrayList<Integer>();

        // Traverse the stalled transactions list and see if ops now exist to be scheduled.
        for (int index = 0; index < stalledTxns.size(); ++index){
            if (!stalledTxns.get(index).isEmpty()){
                // Now that there is an op, schedule it and mark it for removal from the stalled queue.
                scheduleNextOp(stalledTxns.get(index));
                restartedTxnIndexes.add((Integer)(index));
            }
        }

        // Now that the entire stalled transactions list has been traversed
        // remove the transactions that were restarted.
        for (int index = 0; index < restartedTxnIndexes.size(); ++index){
            stalledTxns.remove(restartedTxnIndexes.get(index));
        }
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

        for (int index = 0; index < completed_ops.size(); ++index){
            currOp = completed_ops.get(index);

            Transaction parentTxn = transactions.getByTID(currOp.tid);
            // TODO: (jmg199) REMOVE THE PARENT TRANSACTION FROM THE DEADLOCK QUEUE. (OR UPDATE THE TIMESTAMP IN THE QUEUE).

            if ((currOp.type == "C") || (currOp.type == "A")){
                // This transaction has committed or aborted, so remove it from the transaction list.
                if (!transactions.remove(parentTxn)){
                    System.out.println("DID NOT REMOVE THE COMMITED/ABORTED TXN FROM THE TRANSACTIONS LIST!");
                }
            }
            else {
                // Remove the operation from the transaction's operation list.
                parentTxn.remove(currOp);
                scheduleNextOp(parentTxn);
            }
        }
    }



    /* @summary
     * This method is used to check for dead lock conditions.
     */
    public void deadlockCheck(){
        // Create the reference date.
        Date now = new Date();

        // Traverse the deadlock queue looking for any overrun transactions.
        for (int index = 0; index < deadlockList.size(); ++index){
            deadlockList.get(index).getTimestamp();

            // TODO: (jmg199) VARIABLE TO MAKE THIS COMPILE. REPLACE TEST WITH DEADLOCK THRESHOLD TEST.
            boolean dummy = true;
            if (dummy == false){
                resolveDeadlock(deadlockList.get(index));
            }
        }
    }



    /* @summary
     * This method is used to resolve deadlocks.
     */
    private void resolveDeadlock(Transaction targetTxn){
        // TODO: (jmg199) IMPLEMENT WOUND-WAIT.
    }
}
