
public class Operation {
	
	String type;
	String flag;
	String filename;
	String val;
	Record record;
	
	
	public Operation(String op){
		
		String op_parts[] = op.split(" ", 2);
		
		type = op_parts[0];
		
		if(type.equals("C")){
			return;
		}else if(type.equals("A")){
			return;
		}else if(type.equals("B")){
			flag = op_parts[1];
		}else if(type.equals("R")){
			filename = op_parts[1];
			val = op_parts[2];
		}else if(type.equals("D")){
			filename = op_parts[1];
		}else if(type.equals("W")){
			String rec[] = op_parts[2].split("(*, *, *)");
			if(rec.length!=3){
				System.out.println("Bad record.");
				System.exit(0);
			}
			record = new Record(Integer.parseInt(rec[0]),rec[1],rec[2]);
		}
		
	}
	
	
}
