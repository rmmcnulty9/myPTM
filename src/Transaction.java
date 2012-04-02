import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class Transaction extends ArrayList<Operation> {

	FileInputStream fis;
	DataInputStream dis;
	BufferedReader br;
	public int tid;
	
	public Transaction(FileInputStream _fis, DataInputStream _dis, BufferedReader _br, int _tid){
		fis = _fis;
		dis = _dis;
		br = _br;
		tid = _tid;
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
	
}
