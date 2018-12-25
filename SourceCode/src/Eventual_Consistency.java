import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;

public class Eventual_Consistency extends NodeChord_Replicas {

	public Eventual_Consistency(String id, int port, int main, int number, int RepFactor)throws InterruptedException, IOException, NoSuchAlgorithmException, UnsupportedEncodingException {
		super(id, port, main, number, RepFactor);}
		
		
		@Override
		public void query_chord(String[] split){
			int res;
			int who_asked  = Integer.parseInt(split[3]);
			BigInteger hash_value = new BigInteger(split[1]);
			if(who_asked == my_port){
				int g = query2(split[1],split[2],Integer.parseInt(split[3]));
				if(g == 1){
				//	System.out.println("I have it ");
					return;
				}
			}			
			if(hash_value.compareTo(number_id) == 1 ){
				if(number_id.compareTo(previous_id)==-1 && hash_value.compareTo(previous_id)== 1){
					if(query2(split[1],split[2],Integer.parseInt(split[3]))==0){
						System.out.println("Not found");
						return;
					}
				}
				else{
			     //	System.out.println("Case 2");
					if(next_port == who_asked){
						System.out.println(" Song not in Chord " );
						return;
					}
					Client(next_port,String.join(",",split));
				}	
			}
			else{
				if(hash_value.compareTo(previous_id)==1){
					if(query2(split[1],split[2],Integer.parseInt(split[3]))==0){
						System.out.println("Not found ");
						return;
					}

				//	System.out.println("Case 3");
				}
				else if(number_id.compareTo(previous_id) == -1 ){
					if(query2(split[1],split[2],Integer.parseInt(split[3]))==0){
					//	System.out.println("Node request " + who_asked + " my nunber " +number + " Not found  " + split[2]);
						return;
					}

				}
				else{
					if(next_port == who_asked){
					//	System.out.println(next_port +" Song not in Chord " + split[2]);
						return;
					}
					Client(next_port,String.join(",",split));
				}
			}
		}
		
		@Override
		public void print_answer(String[] split){
			//System.out.println("I received " + number);
			System.out.println(number + "  <"+split[2]+","+split[3]+","+split[4]+">" + "Received from node number : " + split[6]);
			
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
						System.out.println(String.join(",",split));
						System.out.println("I got an delete request");
						delete_song(split);
			}
			else if(split[0].equals("query")){
					if(split[2].equals("*")){
						System.out.println(number + "I am the only who got query");
						query2(split[1],split[2],Integer.parseInt(split[3]));
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
						query2("","*",Integer.parseInt(split[2]));
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

		public int query2(String hash,String key,int who_asked){
			//System.out.println("Querying ... ");
		if(key.equals("*")){
			for(Pair pair1 : dht){
					//System.out.println("case * ");
					if(my_port == who_asked){
						if(pair1.distance ==0){
						System.out.println("<"+pair1.key+","+pair1.value+","+pair1.distance+">");
						}
						//Client(20000,"");

					}
					else{
						if(pair1.distance ==0){
						String msg ="Answer,"+ pair1.hash+","+pair1.key+","+pair1.value+","+pair1.distance+",,"+my_port;
						Client(who_asked,msg) ;
						}
					}
				
			}
			String line = "PrintAll,*,"+who_asked;
			Client(next_port,line);			
		}
		else{
			for(Pair pair1 : dht){
				if((pair1.hash).equals(hash)){
					String msg ="Answer,"+ pair1.hash+","+pair1.key+","+pair1.value+","+pair1.distance+",,"+ number;
					//Client(20000,pair1.key);
					Client(who_asked,msg);
				//	System.out.println(pair1.key + " found on node "+ number );
					//System.out.println(number + "Send to requester");
					return 1;
				}
			}
		}
		//System.out.println(key);
		return 0 ; 
		}
	
}
