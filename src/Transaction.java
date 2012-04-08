import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

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
	public boolean ops_left_in_file;


	public Transaction(FileInputStream _fis, DataInputStream _dis, BufferedReader _br, int _tid){
		fis = _fis;
		dis = _dis;
		br = _br;
		tid = _tid;
		ops_left_in_file = true;
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
}
