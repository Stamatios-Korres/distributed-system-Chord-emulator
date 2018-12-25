import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.ListIterator;

public class NodeChord_Replicas extends NodeChord {

	public NodeChord_Replicas(String id, int port, int main, int number,int RepFactor) throws InterruptedException, IOException, NoSuchAlgorithmException, UnsupportedEncodingException {
			super(id, port, main, number,RepFactor);
	}
	
	
	@Override
	public void insert_song(String[] split){
		BigInteger hash_value = new BigInteger(split[1]);
		if(hash_value.compareTo(number_id) == 1 ){
			if(number_id.compareTo(previous_id)== - 1 && hash_value.compareTo(previous_id)== 1 ){
				add_to_dht(split[1],split[2],split[3],0);
				System.out.println("Inserted ... " + split[2]);
				begin_replication(split[1],split[2],split[3]);
			}
			else{
		     	Client(next_port,String.join(",",split));
			}	
		}
		else{
			if(hash_value.compareTo(previous_id)==1){
				add_to_dht(split[1],split[2],split[3],0);
				//System.out.println(number + " Inserted " + split[2]);
				begin_replication(split[1],split[2],split[3]);
			}
			else if(number_id.compareTo(previous_id) == -1 ){
				add_to_dht(split[1],split[2],split[3],0);
				begin_replication(split[1],split[2],split[3]);
		}
			else{	
				Client(next_port,String.join(",",split));
				}
		}
	}
	
	public void left_chord(String[] split){
		BigInteger next =  new BigInteger(split[1]);
		BigInteger previous =  new BigInteger(split[2]);
		if(number_id.equals(next)){
			//System.out.println(my_port+ " Refresh previous");
			previous_id = previous;
			previous_port = Integer.parseInt(split[4]);
			Client(next_port,String.join(",",split));
			return;
		}
		if(number_id.equals(previous)){
			next_id = next;
			next_port = Integer.parseInt(split[3]);
			return;		
		}
		//System.out.println("Not me");
		Client(next_port,String.join(",",split));
	}
	

	public void depart(String[] split){
		if(my_port == Integer.parseInt(split[1])){
			System.out.println("I got to leave " + my_port + " " + number );
			send_songs_to_next();
			String  msg = "Left," + next_id + "," + previous_id + "," + next_port + "," + previous_port;
			Client(next_port,msg);
			System.out.println("Succesfully exiting Chord");
			}	
		else
			Client(next_port,String.join(",",split));
	}
		
	@Override
	public void process_request(String msg){
		String[] split = msg.split(",");
		if(split[0].equals("Pos"))
			return_pos(split);
		else if(split[0].equals("Increase"))
			increase(split);
		else if(split[0].equals("Previous_Replica"))
			Update_previous_replicas(split);
		else if(split[0].equals("Original_Replica"))
			Update_original_replicas(split);
		else if(split[0].equals("Get_Replicas"))
			add_to_dht(split[1],split[2],split[3],Integer.parseInt(split[4]));
		else if(split[0].equals("Max_Replica"))
			Got_Max_Replica(split);
		else if(split[0].equals("Update"))
			send_to_previous_songs();
		else if(split[0].equals("Delete_Replica"))
			Delete_Rep(split);
		else if(split[0].equals("Replicas"))
			Replication(split);
		else if(split[0].equals("Receive")){
			System.out.println(number +" I got new song " + split[2]);
			add_to_dht(split[1],split[2],split[3],0);
		}
		else if(split[0].equals("Found")){
			refresh(split);
			send_message_left_right();
		}
		else if(split[0].equals("Refresh")){
				refresh_left_right(split);		
		}
		else if(split[0].equals("Depart"))
			depart(split);
		else if(split[0].equals("Left"))
			left_chord(split);
		else if(split[0].equals("insert")){
			//	System.out.println("I got an insert request");
				insert_song(split);
		}	
		else if(split[0].equals("delete")){
			//		System.out.println("I got an delete request");
					delete_song(split);
		}
		else if(split[0].equals("query")){
			//	System.out.println(number + " I got a query request");
				if(split[2].equals("*")){
					System.out.println("I am the only who got query");
					query(split[1],split[2],Integer.parseInt(split[3]));
					return;
				}	
				query_chord(split);
		}
		else if(split[0].equals("Answer")){
			//System.out.println("I will print "+ number);
			print_answer(split);
		}
		else if(split[0].equals("PrintAll")){
				System.out.println(number + " PrintAll request ");
				if (my_port == Integer.parseInt(split[2]) ){
					System.out.println("* Message returned to me");
					return;
				}	
				else
					query("","*",Integer.parseInt(split[2]));
		}
		else if(split[0].equals("Update_Replica")){
			Replicas_Update(split);			
		}
		else{
			System.out.println("Unknown message so far");
			System.out.println(split[0]);
		}	
		return;
	}
	
	@Override
	public void print_answer(String[] split){
		//System.out.println("I received " + number);
		System.out.println("<"+split[1]+","+split[2]+","+split[3]+">" );
		
	}
	
	
	public void add_to_dht(String hash,String key,String value,int replicas){
		for(Pair pair1 : dht){
					if((pair1.hash).equals(hash)){
						pair1.value = value;
						pair1.key = key;
						pair1.distance = replicas;
						System.out.println(number + " Value updated " + pair1.key + " Value will now be " + pair1.value);
						return;
					}	
		}
		Pair pair2 = new Pair(hash,key,value,replicas);
		dht.add(pair2);
		System.out.println(number + " Inserted " + pair2.key);
		
		
		return;
		}
	public void Got_Max_Replica(String [] split){
		add_to_dht(split[1],split[2],split[3],RepFactor-1);		
	}
	
	
	public void Replicas_Update(String [] split){
		ListIterator<Pair> iter = dht.listIterator();
		int k = Integer.parseInt(split[1]);
		if(k!=RepFactor)
		if(k == RepFactor)
			return;
		int inserted = Integer.parseInt(split[2]);
		if(inserted  == my_port){
				return;
		}
		while(iter.hasNext()){
				Pair value = iter.next();
		    	if(value.distance == RepFactor){
		    		System.out.println(number + "Removing item " + value.key);
		    		String Msg = "Max_Replica,"+value.hash+","+value.key+","+value.value;
		    	  	Client(previous_port,Msg);
		    		iter.remove();
		    	}
		    	else{
		    		System.out.println(number + "Editing Distance " + value.key);
		    		value.distance++;
		    	}	
		}
		
		if(k!=RepFactor+1){
			k++;
			Client(next_port,"Update_Replica,"+k+","+inserted);
		}	
	}
	
	public void send_to_previous_songs(){
		String Id = "" + previous_id; // Check String comparison 
		ListIterator<Pair> iter = dht.listIterator();
		while(iter.hasNext()){
			Pair value = iter.next();
			
			// Songs that now belong to previous
		    if((value.hash).compareTo(Id) != 1 && value.distance == 0 ){ // For every Song that you send to previous probably add to your distance = 1 list 
		    	String answer = "Receive,"+value.hash + "," +value.key+ ","+value.value+ ",";
		    	Client(previous_port,answer);
				// Somehow Inform new songs !! 
				if(RepFactor == 0){
					iter.remove();
					//System.out.println("Removing from database ...");
				}
				else if(RepFactor > 0 )
					value.distance = 1;
				answer = "Increase,1,"+value.hash+","+value.key;
				Client(next_port,answer);			
		    }	    
		    else if(value.distance != 0) {
		    	String Msg = "Get_Replicas,"+value.hash+","+value.key+","+value.value+","+value.distance;
		    	//System.out.println("Sending copies back to him " +Msg);
		    	Client(previous_port,Msg);
		    	if(value.distance + 1 == RepFactor ){
		    		iter.remove();
		    	}else{
		    		System.out.println(number + " Editing Distance " + value.key);
		    		value.distance ++;
		    	 		
		    	}
		    	String answer = "Increase,1,"+value.hash+","+value.key;
		    	//System.out.println(number + " Increasing Replica Sending song ... " + value.key);
				
		    	Client(next_port,answer);
		    }
	  }    
		//System.out.println("Updating dht began I am next " + number + " and sedning to "  +next_port);
		//String answer = "Update_Replica,"+2+","+my_port;
    	//Client(next_port,answer);
	}
	
	public void increase(String [] split){
		int number1 = Integer.parseInt(split[1]);
		number1++;
		if(number1 == RepFactor){
			//System.out.println(split[3]);
			delete_from_dht(split[2]);
			return;
		}	
		else{
			ListIterator<Pair> iter = dht.listIterator();
			while(iter.hasNext()){
				Pair value = iter.next();
				if(split[2].equals(value.hash)){
					if(value.distance == RepFactor -1){
					System.out.println(number + " Deleting ... " + value.key);
					iter.remove();
				}	
				else
				add_to_dht(value.hash,value.key,value.value,value.distance+1);
				}
			}
		Client(next_port,"Increase,"+number1+","+split[2]+","+split[3]);
		}
		
	}
	
	public void query(String hash,String key,int who_asked){
		if(key.equals("*")){
			for(Pair pair1 : dht){
				if(my_port == who_asked){
					System.out.println("<"+pair1.key+","+pair1.value+","+pair1.distance+">");
				}
				else{
					String answer = "Answer,"+pair1.key+","+pair1.value+","+pair1.distance;
					Client(who_asked,answer);
				}
			}
			String line = "PrintAll,*,"+who_asked;
			Client(next_port,line);
		
		}
		else{
			for(Pair pair1 : dht){
				if((pair1.hash).equals(hash)){
					String answer = "Answer,"+pair1.key+","+pair1.value;
					Client(who_asked,answer);
				//	System.out.println("Send to requester");
					return;
				}
			}
		}
		//System.out.println(key);
		return; 
	}
	public void begin_replication(String hash,String key,String value){
		int repfactor = 0;
		String msg = "Replicas,"+repfactor+","+hash+","+key+","+value+",";
	//	System.out.println("Begining Replication ");
		if(RepFactor == 1 )
			return;
		Client(next_port,msg);
	}
	
	public void send_songs_to_next(){
		ListIterator<Pair> iter = dht.listIterator();
		while(iter.hasNext()){
			Pair value = iter.next();
			if(value.distance == 0){
				//System.out.println(number + " Orginal Removal from database " + value.key);
			    String answer = "Original_Replica,1,"+value.hash + "," +value.key+ ","+value.value+ ","+value.distance;
				Client(next_port,answer);
			}
			else{
				//System.out.println(number + " Replicas Removal from database " + value.key);
				String answer = "Previous_Replica,1,"+value.hash + "," +value.key+ ","+value.value+ ","+value.distance;
				Client(next_port,answer);
			}
			iter.remove(); 
		}
		System.out.println("Adding or Refreshing value ");


	}
	
	public void Update_previous_replicas(String[] split){
		int distance = Integer.parseInt(split[5]);
		int rep_node = Integer.parseInt(split[1]);
		if(rep_node == RepFactor )
			return;
		else{
			if(distance == RepFactor-1){
				//System.out.println(number +" distance "+ distance + " Previous insertion Song : " + split[3]);
				add_to_dht(split[2],split[3],split[4],RepFactor-1);
			}	
			else{
				//System.out.println(number + " Editing distance ... Song : " + split[3]);
				add_to_dht(split[2],split[3],split[4],rep_node);
				rep_node ++;
				distance++;
				String msg = "Previous_Replica,"+rep_node+","+split[2]+","+split[3]+","+split[4]+","+distance;
				Client(next_port,msg);
			}
		}
	}
	
	public void delete_replicas(String hash){
		String msg = "Delete_Replica,1,"+hash;
		Client(next_port,msg);
	}
	
	public void Delete_Rep(String[] split){
		int Factor = Integer.parseInt(split[1]);
		Factor ++;
		if(Factor == RepFactor){
			delete_from_dht(split[2]);
			return;
		}
		delete_from_dht(split[2]);
		//System.out.println(" Still deleting Replicas " + Factor );
		String msg = "Delete_Replica,"+Factor+","+split[2];
		Client(next_port,msg);
		
		
	}
	
	
	public void delete_song(String[] split){
			BigInteger hash_value = new BigInteger(split[1]);
			if(hash_value.compareTo(number_id) == 1 ){
				if(number_id.compareTo(previous_id)==-1 && hash_value.compareTo(previous_id)== 1){
					//System.out.println("I am Min node");
					delete_replicas(split[1]);
					delete_from_dht(split[1]);
				}
				else{
			     	//System.out.println("Case 2");
					Client(next_port,String.join(",",split));
				}	
			}
			else{
				if(hash_value.compareTo(previous_id)==1){
					delete_from_dht(split[1]);
					delete_replicas(split[1]);
				//	System.out.println("Case 3");
				}
				else if(number_id.compareTo(previous_id) == - 1 ){
					delete_from_dht(split[1]);
					delete_replicas(split[1]);
				}
				else{	
					Client(next_port,String.join(",",split));
				}
			}
		}
	
	
	public void Update_original_replicas(String[] split){
		System.out.println(number + " I have a new original song " + split[3]);
		add_to_dht(split[2],split[3],split[4],0);
		String answer = "Previous_Replica,1,"+split[2]+ "," +split[3]+ ","+split[4]+ ","+1;
		Client(next_port,answer);
		}
	
	
	public void Replication(String[] split){
		int k = Integer.parseInt(split[1]);
		add_to_dht(split[2],split[3],split[4],k+1);
		k = k+1;
		if(k == (RepFactor)-1){
			return;
		}
		else{
			String msg = "Replicas,"+k+","+split[2]+","+split[3]+","+split[4]+",";
			//System.out.println("Replica number " + k);
			Client(next_port,msg);
		}
		
	}
	

}