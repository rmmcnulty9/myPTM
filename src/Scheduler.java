import java.util.ArrayList;
import java.util.Map;


public class Scheduler extends Thread{
    // Buffer size that the DM should use.
	private int buffer_size;

    // The type of search method the DM should employ.
	private String search_method;

	public ArrayList<Operation> current_ops = null;

    // This is a list of transactions to execute.
	private ArrayList<Transaction> transactions = null;

    // This list is used to keep track of which txns still need to commit/abort.
    // TODO: (goldswjm) THIS PROBABLY WON"T BE NEEDED.
    private ArrayList<Integer> pendingTxnIds = null;

    // Our DM reference.
	private DataManager dm_task = null;

    /*
     * @summary
     * This is the class ctor.
     *
     * @param sourceTransactions - List of transactions to execute.
     * @param buffer - Initial size of the buffer to provide to the data manager.
     */
	public Scheduler(ArrayList<Transaction> _sourceTransactions, int _buffer_size, String _search_method){
		buffer_size = _buffer_size;
		search_method = _search_method;
		transactions = _sourceTransactions;
		current_ops = new ArrayList<Operation>();
	}


    /*
     * @summary
     * This method is used as the execution loop for the thread.
     */
	public void run(){
		if(dm_task == null){
			dm_task = new DataManager(current_ops, buffer_size, search_method, this);
			System.out.println("Started DataManager...");
			dm_task.start();
		}
		while(!transactions.isEmpty()){
			if(!transactions.get(0).isEmpty()){
				Operation o = transactions.get(0).get(0);
				transactions.get(0).remove(0);
				if(o.type.equals("B")){
					continue;
				}
				System.out.println("Scheduled this operation: "+o.toString());
				current_ops.add(o);
			}
		}
	}
}
