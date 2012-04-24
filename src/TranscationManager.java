import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;


public class TranscationManager extends Thread{

	public TransactionList transactions = new TransactionList();

	private int MAX_OPS_TO_READ = 10;
	private boolean random_read = false;
	private boolean rr_read = false;
	private Random rgen = null;
	private int buffer_size;
	private String search_method;
    private boolean schedExitFlag = false;

	private Scheduler scheduler = null;
	private boolean verbose;

	public void run(){
		if(scheduler==null){
			scheduler = new Scheduler(this, transactions, buffer_size, search_method, verbose);
			System.out.println("Started Scheduler...");
			scheduler.start();
		}

		int next_index=0;

		while(!allFilesEmpty()){

			if(rr_read){

				String op = null;
				Transaction trans = getByIndex(next_index);
				if(trans == null){
					next_index = 0;
					trans = getByIndex(next_index);
				}
				if(isOpLeftInFile(trans.tid)){
					try {
						op = trans.br.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					if (op == null){
						setOpsLeftInFile(trans.tid, false);
					}else{
//						System.out.println("[TM] Read in operation:"+op.toString());
						Operation o = new Operation(trans.tid, op);
						trans.add(o);
					}
				}
				next_index+=1;

			}else if(random_read){
				next_index = rgen.nextInt(transactions.size());
				int num_lines = rgen.nextInt(MAX_OPS_TO_READ);
				Transaction trans = getByIndex(next_index);

				if(isOpLeftInFile(trans.tid)){
					for(int i=0;i<num_lines;i++){
						String op = null;
						try {
							op = trans.br.readLine();
						} catch (IOException e) {
							e.printStackTrace();
						}
						if (op == null){
							setOpsLeftInFile(trans.tid, false);
						}else{
							trans.add(new Operation(next_index, op));
						}
					}
				}
			}
		}
		System.out.println("[TM] No more operations. Waiting on Scheduler...");
		scheduler.setTMDoneFlag();
		  while (!schedExitFlag)
	        {
	            // Check every 1/4 sec. to see if DM is done.
	            try {
					sleep(250);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	        }
	        System.out.println("Transaction Manager is exiting...");
		  
	}

	public TranscationManager(String read_method, int _buffer_size,
			String _search_method, ArrayList<String> file_list, boolean _verbose){

		verbose = _verbose;
		search_method = _search_method;
		buffer_size = _buffer_size;

		//Create transactions, one for each file
		for(int i=0;i<file_list.size();i++){
			try{
				FileInputStream fis = new FileInputStream(file_list.get(i));
				// Get the object of DataInputStream
				DataInputStream in = new DataInputStream(fis);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				//Read The initial flag B
				String flag = br.readLine();
				int mode = Integer.parseInt(flag.substring(2));
				transactions.add(new Transaction(file_list.get(i),fis, in, br, i, mode));
			}catch(IOException e){
				e.printStackTrace();
				System.exit(0);
			}
		}

		//Set flags for how to read from the transaction files
		if(read_method.equals("rr")){
			rr_read = true;
			random_read = false;
		}else{
			rr_read = false;
			random_read = true;
			rgen = new Random(Integer.parseInt(read_method));
		}
	}

    /* @summary
     * This method sets the scheduler exit flag which indicates that it has
     * exited and it is now safe for the TM to exit.
     */
    public void setSchedExitFlag(){
        schedExitFlag = true;
    }



	public Transaction getByTID(int tid){
		for(int i=0;i<transactions.size();i++){
			if(transactions.get(i).tid == tid){
				return transactions.get(i);
			}
		}
		return null;
	}

	public Transaction getByIndex(int index){
		if(index>=transactions.size()){
			return null;
		}
		return transactions.get(index);
	}

	public boolean setOpsLeftInFile(int tid, boolean val){
		for(int i=0;i<transactions.size();i++){
			if(transactions.get(i).tid == tid){
				 transactions.get(i).ops_left_in_file = val;
				 return true;
			}
		}
		return false;
	}

	public boolean isOpLeftInFile(int tid){
		for(int i=0;i<transactions.size();i++){
			if(transactions.get(i).tid == tid){
				 return transactions.get(i).ops_left_in_file;
			}
		}
		return false;
	}
	
	public boolean allFilesEmpty(){
		for(int i=0;i<transactions.size();i++){
			if(transactions.get(i).ops_left_in_file) return false;
		}
		return true;
	}

//	Not needed anymore - done in Scheduler
//
//	public boolean removeByTID(int tid){
//		for(int i=0;i<transactions.size();i++){
//			if(transactions.get(i).tid == tid){
//				transactions.remove(i);
//				return true;
//			}
//		}
//		return false;
//	}

}
