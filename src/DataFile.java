import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class DataFile {
	public String filename;
	public ObjectInputStream inputStream;
	public ObjectOutputStream outputStream;
	public FileOutputStream fos;
	public DataFile(String _fn){
		filename = _fn;
		
		try {
			inputStream = new ObjectInputStream(new FileInputStream(filename));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fos = new FileOutputStream(filename);
			outputStream = new ObjectOutputStream(fos);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public boolean close(){
		try {
			inputStream.close();
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}
