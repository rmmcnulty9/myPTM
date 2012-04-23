import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import org.joda.time.DateTime;
//import org.joda.time.Instant;

public class Transaction extends ArrayList<Operation> {

	/**
	 *
	 */
	private static final long serialVersionUID = -8187588735709506147L;


	/*
	 * TODO I believe we might only need to hold on to the BufferedReader,
	 *  but since we have the structure already I have left it
	 */
	String filename;
	FileInputStream fis;
	DataInputStream dis;
	BufferedReader br;
	public int tid;
	public int mode;
	public boolean ops_left_in_file;
	public boolean abortedFlag;

	// Operation start is used for deadlock detection.
    public DateTime opStart = null;

    // Transaction start is used for execution time reporting and deadlock threshold determination.
    public DateTime txnStart = null;
    public DateTime txnEnd = null;
    
    // List of locks that the txn has been granted.
    //public ArrayList<RecordLock> grantedLocks = null;
    public TreeMap<Integer, RecordLock> grantedLocks = null;
    public ArrayList<RecordLockTree> grantedFileLocks = null;


	public Transaction(String _filename, FileInputStream _fis, DataInputStream _dis, BufferedReader _br, int _tid, int _mode){
		filename = _filename;
		fis = _fis;
		dis = _dis;
		br = _br;
		tid = _tid;
		ops_left_in_file = true;
		mode = _mode;
		grantedLocks = new TreeMap<Integer, RecordLock>();
		grantedFileLocks = new ArrayList<RecordLockTree>();
		abortedFlag = false;
	}

	public void end(){
		try {
			br.close();
			dis.close();
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/*
	 * This method will abort the transaction if the attacking transaction
	 * is older than this transaction.
	 */
	public void wound(Transaction attackingTxn) {
		if (attackingTxn.txnStart.isBefore(txnStart)) {
			// TODO: (jmg199) REMOVE AFTER TESTING.
			System.out.println("Attacking TxnID [" + attackingTxn.tid + "] has wounded TxnID [" + tid + "].");
        			
			// The file lock requesting txn is older than the current record lock holder, so abort it.
			// Tell the scheduler to abort this transaction.
			Scheduler.getSched().abort(this);
		}	
	}
	
	

    /*
     * @summary
     * This getter returns the transaction ID as an Integer type.
     */
    public Integer id(){
        return (Integer)tid;
    }
}
