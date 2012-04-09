import java.util.ArrayList;
// TODO: (jmg199) CLEAN UP AFTER TESTING.
//import java.util.Map;
import java.util.Date;



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

    // This is a list of transactions that are currently blocked. This may be
    // due to operations not being available from the TM or due to an inability
    // to get a lock.
    private ArrayList<Integer> blockedTxns;

    // This list is used to keep track of which txns still need to commit/abort.
    // TODO: (jmg199) THIS PROBABLY WON"T BE NEEDED. CLEAN UP AFTER TESTING.
    //private ArrayList<Integer> pendingTxnIds = null;

    // Our DM reference.
	private DataManager dm_task = null;


    /*
     * @summary
     * This is the class ctor.
     *
     * @param   _sourceTransactions - List of transactions to execute.
     * @param   _buffer_size - Initial size of the buffer to provide to the data manager.
     * @param   _search_method - Search method that the DM should use to find the record.
     */
	public Scheduler(TransactionList _sourceTransactions, int _buffer_size, String _search_method){
        // Init data memebers.
		buffer_size = _buffer_size;
		search_method = _search_method;
		transactions = _sourceTransactions;
		scheduled_ops = new ArrayList<Operation>();
        completed_ops = new ArrayList<Operation>();
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


        // TODO: (goldswjm) Start the dead lock poll timer thread.


        // Continue working while the transaction list exists.
		while(transactions != null){
            // Check each transaction in the transaction list.
            for (int index = 0; index < transactions.size(); ++index){
                // Add the first operation in the transaction if it exists.
                // Otherwise, add it to the blocked queue.
                if (transactions.get(index).isEmpty()){
                    // Add this transaction to the blocked queue.
                    blockedTxns.add(transactions.get(index).id())
                }
                else{
                    Operation nextOp = transactions.get(index).get(0);
                    scheduled_ops.add(nextOp);
                }
            }
        }


        // Continue working while the transaction list exists.
		//while(transactions != null){
            // TODO: (goldswjm)
			//if(!transactions.get(0).isEmpty()){
			//	Operation currOp = transactions.get(0).get(0);
			//	transactions.get(0).remove(0);
			//	if(currOp.type.equals("B")){
			//		continue;
			//	}
			//	System.out.println("Scheduled this operation: "+currOp.toString());
			//	scheduled_ops.add(currOp);
			//}
		//}
	}
}
