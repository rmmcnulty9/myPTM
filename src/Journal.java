import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;


public class Journal {

	private RandomAccessFile raf;
	
	public Journal(){
		
		File f = new File("journal.txt");
		try {
			raf = new RandomAccessFile(f,"rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	
	public boolean addEntry(int tid, int df_id, int pid, String before_image, String after_image){
		String e = tid+","+df_id+","+pid+" "+before_image+","+after_image+"\n";
		try{
			raf.seek(raf.length());
			raf.writeChars(e);
		} catch(IOException ex){
			ex.printStackTrace();
			System.exit(0);
		}
		return true;
	}

	public ArrayList<Integer> cleanByTID(int tid){
		ArrayList<Integer> df_ids = new ArrayList<Integer>();
		try{
			raf.seek(0);
			String cur = "dumby";
			while(null!=cur){
				long orig_pos = raf.getFilePointer();
				cur = raf.readLine();
				String parts[] = cur.split(",");
				if(parts.length!=5){
					System.out.println("Bad entry.");
					System.exit(0);
				}
				if(Integer.parseInt(parts[0])==tid){
					long space_size = raf.getFilePointer()-orig_pos;
					String spaces = String.format("%-"+space_size);
					raf.seek(orig_pos);
					raf.writeChars(spaces);
					//If the before image and the df_id are equal this was a deleted file
					if(parts[3].equals(parts[1])){
						df_ids.add(new Integer(parts[3]));
					}
				}
			}
		} catch(IOException ex){
			ex.printStackTrace();
			System.exit(0);
		}catch(NumberFormatException nex){
			nex.printStackTrace();
			System.exit(0);
		}
		return df_ids;
	}
	
	public ArrayList<JournalEntry> undoByTID(int tid){
		ArrayList<JournalEntry> undos = new ArrayList<JournalEntry>();
		try{
			raf.seek(0);
			String cur = "dumby";
			while(null!=cur){
				long orig_pos = raf.getFilePointer();
				cur = raf.readLine();
				String parts[] = cur.split(",");
				if(parts.length!=5){
					System.out.println("Bad entry.");
					System.exit(0);
				}
				if(Integer.parseInt(parts[0])==tid){
					long space_size = raf.getFilePointer()-orig_pos;
					String spaces = String.format("%-"+space_size);
					raf.seek(orig_pos);
					raf.writeChars(spaces);
					undos.add(new JournalEntry(parts[0],parts[1],parts[2],parts[3],parts[4]));
				}
			}
		} catch(IOException ex){
			ex.printStackTrace();
			System.exit(0);
		}catch(NumberFormatException nex){
			nex.printStackTrace();
			System.exit(0);
		}
		return undos;
	}


	public boolean addEntry(int tid, int df_id, int pid, int before_dfid, int nil) {
		String e = tid+","+df_id+","+pid+" "+before_dfid+","+nil+"\n";
		try{
			raf.seek(raf.length());
			raf.writeChars(e);
		} catch(IOException ex){
			ex.printStackTrace();
			System.exit(0);
		}
		return true;
	}
}
