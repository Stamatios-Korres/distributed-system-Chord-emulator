import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.ListIterator;

public class Chain_Replication extends NodeChord_Replicas {

	public Chain_Replication(String id, int port, int main, int number, int RepFactor)
			throws InterruptedException, IOException, NoSuchAlgorithmException, UnsupportedEncodingException {
		super(id, port, main, number, RepFactor);
		
		}

	
	public void query_chord(String[] split){
		BigInteger hash_value = new BigInteger(split[1]);
		int who_asked  = Integer.parseInt(split[3]);
		int g = query1(split[1],split[2],Integer.parseInt(split[3]));
		if(g == 1)
			return;
		else if(g==2){
			Client(next_port,String.join(",",split));
			return;
		}
		if(next_port == who_asked){
			query1(split[1],split[2],Integer.parseInt(split[3]));
			System.out.println("Song not in Chord");
			return;
		}	
		if(hash_value.compareTo(number_id) == 1 ){
			if(number_id.compareTo(previous_id)==-1 && hash_value.compareTo(previous_id)== 1 ){
				//System.out.println("Case 1");
				//System.out.println("I am Min node");
				int l = query1(split[1],split[2],Integer.parseInt(split[3]));
				if(l == 0)
					System.out.println("Not in Chord");
			}
			else{
		     //	System.out.println("Case 2");
				Client(next_port,String.join(",",split));
			}	
		}
		else{
			if(hash_value.compareTo(previous_id)==1){
				int l = query1(split[1],split[2],Integer.parseInt(split[3]));
				if(l == 0)
					System.out.println("Not in Chord");

			//	System.out.println("Case 3");
			}
			else if(number_id.compareTo(previous_id) == -1 ){
				int l = query1(split[1],split[2],Integer.parseInt(split[3]));
				if(l == 0)
					System.out.println("Not in Chord");

			}
			else{	
				Client(next_port,String.join(",",split));
			}
		}
		
		
	}
	
	public int query1(String hash,String key,int who_asked){
		if(key.equals("*")){
			for(Pair pair1 : dht){
				if(my_port == who_asked){
					if(pair1.distance == RepFactor -1)
						System.out.println("<"+pair1.key+","+pair1.value+","+pair1.distance+">");
				}
				else{
					if(pair1.distance == RepFactor -1 ){
						String answer = "Answer,"+","+pair1.hash+","+pair1.key+","+pair1.value+","+pair1.distance+","+who_asked+","+1+","+number;
						Client(who_asked,answer);
					}
				}
			}
			String line = "PrintAll,*,"+who_asked;
			Client(next_port,line);			
		}
		else{
			for(Pair pair1 : dht){
				if((pair1.hash).equals(hash)){
					if(pair1.distance == RepFactor -1){
						String msg ="Answer,"+ pair1.hash+","+pair1.key+","+pair1.value+","+pair1.distance+","+who_asked+","+number;
						Client(who_asked,msg);
						Client(20000,"case found");
						return 1;
					}
					else if(pair1.distance <  RepFactor -1)
						return 2;
					String answer = "Chain_Rep,"+pair1.hash+","+pair1.key+","+pair1.value+","+pair1.distance+","+who_asked+","+1;
					//Client(20000,"");
					//Client(next_port,answer);
					//System.out.println(number + " Send Chain_Replica");
					return 0;
				}
			}
		}
		//System.out.println(key);
		return 0; 
	}
	
	public void Chain_Rep( String [] split){
		int distance = Integer.parseInt(split[6]);
		ListIterator<Pair> iter = dht.listIterator();
		while(iter.hasNext()){
			Pair value = iter.next();
			if(split[1].equals(value.hash)){
					if(distance == RepFactor-1){
						//System.out.println("I found my place " + number);
						String msg ="Answer,"+ value.hash+","+value.key+","+value.value+","+value.distance+","+split[5]+","+my_port;
						System.out.println("here");
						Client(20000,"Chain");
						//Client(Integer.parseInt(split[5]),msg);
					}	
					else{	
						//System.out.println(number + " RepNumber is " + distance + " Sending to " + next_port);
						split[6] = "" + (Integer.parseInt(split[6]) +1 );
						String msg ="Chain_Rep,"+ value.hash+","+value.key+","+value.value+","+value.distance+","+split[5]+","+split[6];
						//split[5] +1 
						Client(next_port,msg);
				}
			}	
		}
	}

	public void print_answer(String[] split){
		System.out.println("<"+split[2]+","+split[3]+","+ split[4] + " >" + " received from " + split[6]);
		}
	
	@Override
	public void process_request(String msg){
		String[] split = msg.split(",");
		if(split[0].equals("Pos"))
			return_pos(split);
		else if (split[0].equals("Chain_Rep"))
			Chain_Rep(split);
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
		else if(split[0].equals("Depart")){
			depart(split);
			System.out.println("DEPARTING");
			
		}
			
		else if(split[0].equals("Left"))
			left_chord(split);
		else if(split[0].equals("insert")){
				//System.out.println("I got an insert request");
				insert_song(split);
		}	
		else if(split[0].equals("delete")){
					//System.out.println("I got an delete request");
					delete_song(split);
		}
		else if(split[0].equals("query")){
				//System.out.println(number + " I got a query request");
				if(split[2].equals("*")){
					System.out.println("I am the only who got query");
					query1(split[1],split[2],Integer.parseInt(split[3]));
					return;
				}	
				query_chord(split);
		}
		else if(split[0].equals("Answer")){
			if(split.length == 3 ){
				for(String k : split)
					System.out.println(k);
				return;
			}
			print_answer(split);
		}
		else if(split[0].equals("PrintAll")){
				System.out.println(number + " PrintAll request ");
				if (my_port == Integer.parseInt(split[2]) ){
					//System.out.println("* Message returned to me");
					return;
				}	
				else
					query1("","*",Integer.parseInt(split[2]));
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
}
