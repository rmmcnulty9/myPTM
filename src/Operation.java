
public class Operation {
	int tid;
	String type;
	String flag;
	String filename;
	String val;
	Record record;
	String op_string;
	
	
	public Operation(int t, String op){
		tid = t;
		op_string = op;
		String op_parts[] = op.split(" ", 3);
		
		type = op_parts[0];
		
		if(type.equals("C")){
			return;
		}else if(type.equals("A")){
			return;
		}
		
		if(op_parts.length<2){
			System.out.println("Malformed operation: "+op_string);
			System.exit(0);
		}
		
		if(type.equals("B")){
			flag = op_parts[1];
			return;
		}else if(type.equals("D")){
			filename = op_parts[1];
			return;
		}
		
		if(op_parts.length<3){
			System.out.println("Malformed operation: "+op_string);
			System.exit(0);
		}
		
		if(type.equals("R")){
			filename = op_parts[1];
			val = op_parts[2];
			return;
		}else if(type.equals("W")){
			filename = op_parts[1];
			op_parts[2] = op_parts[2].substring(1, op_parts[2].length()-1);
			String rec[] = op_parts[2].split(",");
			if(rec.length!=3){
				System.out.println("Bad record.");
				System.exit(0);
			}
			record = new Record(Integer.parseInt(rec[0]),rec[1],rec[2]);
		}
		
	}
	
	public String toString(){
		
		return  op_string;
	}
	
}
