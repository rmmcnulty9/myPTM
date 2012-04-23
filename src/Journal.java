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
			f.delete();
			f.createNewFile();
			
			raf = new RandomAccessFile(f,"rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	
	public boolean addEntry(int tid, int df_id, int pid, String before_image, String after_image){
		if(before_image==null)before_image = "BEFORE";
		if(after_image==null)after_image = "AFTER";
		String e = tid+","+df_id+","+pid+","+before_image+","+after_image+"\n";
		try{
			raf.seek(raf.length());
			raf.writeBytes(e);
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
				if(cur==null) continue;
				String parts[] = cur.split(",");
				
				//If the line starts with 2 spaces it is a previously cleared entry, so continue
				if(parts[0].startsWith("  ")) continue;
				
				if(parts.length!=5){
					System.out.println("Bad entry.");
					System.exit(0);
				}
				
				if(Integer.parseInt(parts[0])==tid){
					long space_size = raf.getFilePointer()-orig_pos;
					String spaces = " ";
					for(int k=0;k<space_size-2;k++) spaces+=" ";
					spaces+="\n";
					raf.seek(orig_pos);
					raf.writeBytes(spaces);
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
				if(cur==null) continue;
				String parts[] = cur.split(",");
				
				//If the line starts with 2 spaces it is a previously cleared entry, so continue
				if(parts[0].startsWith("  ")) continue;
				
				if(parts.length!=5){
					System.out.println("Bad entry.");
					System.exit(0);
				}
				if(Integer.parseInt(parts[0])==tid){
					long space_size = raf.getFilePointer()-orig_pos;
					String spaces = " ";
					for(int k=0;k<space_size-1;k++) spaces+=" ";
					raf.seek(orig_pos);
					raf.writeBytes(spaces);
					undos.add(0,new JournalEntry(parts[0],parts[1],parts[2],parts[3],parts[4]));
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
		String e = tid+","+df_id+","+pid+","+before_dfid+","+nil+"\n";
		try{
			raf.seek(raf.length());
			raf.writeBytes(e);
		} catch(IOException ex){
			ex.printStackTrace();
			System.exit(0);
		}
		return true;
	}
}
