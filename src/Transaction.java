import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.*;

public class Transaction extends ArrayList<String> {

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
}
