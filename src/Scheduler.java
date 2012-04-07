import java.util.ArrayList;
import java.util.Map;


public class Scheduler extends Thread{

	public Pair current_op = null;

    // This is a list of transactions to execute.
	private ArrayList<Transaction> transactions = null;

    // This list is used to keep track of which txns still need to commit/abort.
    // TODO: (goldswjm) THIS PROBABLY WON"T BE NEEDED.
    private ArrayList<int> pendingTxnIds = null;

    // Our DM reference.
	private DataManager dm_task = null;

    /*
     * @summary
     * This is the class ctor.
     *
     * @param sourceTransactions - List of transactions to execute.
     * @param buffer - Initial size of the buffer to provide to the data manager.
     */
	public Scheduler(ArrayList<Transaction> sourceTransactions, int buffer){
        // Initialize the transactions lists.
		transactions = sourceTransactions;

		if(dm_task == null){
			dm_task = new DataManager(current_op, buffer, this);
		}
	}

	public void run(){

	}

}
