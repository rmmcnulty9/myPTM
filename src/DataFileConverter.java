import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;


public class DataFileConverter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Page p;
		FileInputStream fis;
		ObjectInputStream inputStream = null;
		System.out.println("DataFileConverter <data_file>");
		ArrayList<Page> pages = new ArrayList<Page>();
		try{
			fis = new FileInputStream(args[0]);
//			inputStream = new ObjectInputStream(new BufferedInputStream(fis));
			int ctr=0;
			while(true){
				long pos = Page.PAGE_SIZE_BYTES*ctr;
				fis.getChannel().position(pos);
				inputStream = new ObjectInputStream(new BufferedInputStream(fis));
				p = (Page)inputStream.readObject();
				System.out.println("Counter: "+ctr);
				pages.add(p);
				ctr++;
			}
			
		} catch(EOFException e){
			
		}catch (Exception e1) {
		
			e1.printStackTrace();
		}

		try {
			inputStream.close();
			
			File out = new File(args[0]+"_text.txt");
			FileOutputStream fos = new FileOutputStream(out);
			// Get the object of DataInputStream
			DataOutputStream dos = new DataOutputStream(fos);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
			
			while(!pages.isEmpty()){
				p = pages.remove(0);
//				System.out.println(p);
				bw.write(p.toString());
			}
			bw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
