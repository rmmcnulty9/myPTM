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
	FileInputStream fis;
	DataInputStream dis;
	BufferedReader br;
	public int tid;
	public int mode;
	public boolean ops_left_in_file;

	// Operation start is used for deadlock detection.
    public DateTime opStart = null;

    // Transaction start is used for execution time reporting and deadlock threshold determination.
    public DateTime txnStart = null;
    
    // List of locks that the txn has been granted.
    public ArrayList<Lock> grantedLocks = null;
    public ArrayList<RecordLockTree> grantedFileLocks = null;


	public Transaction(FileInputStream _fis, DataInputStream _dis, BufferedReader _br, int _tid, int _mode){
		fis = _fis;
		dis = _dis;
		br = _br;
		tid = _tid;
		ops_left_in_file = true;
		mode = _mode;
		grantedLocks = new ArrayList<Lock>();
		grantedFileLocks = new ArrayList<RecordLockTree>();
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
     * @summary
     * This getter returns the transaction ID as an Integer type.
     */
    public Integer id(){
        return (Integer)tid;
    }



    /* @summary
     * This getter returns the current timestamp.
     */
    /*
    public Date getTimestamp(){
        return timestamp;
    }*/



    /* @summary
     * This sets the transaction's timestamp.
     */
    /*public void setTimestamp(Date sourceTime){
        timestamp = sourceTime;
    }*/
}
