import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.ListIterator;
import java.io.*;
import java.util.*;

public class NodeChord extends Thread{
	
	//attributes of Nodes
	BigInteger number_id;
	ArrayList<Pair> dht;
	Server my_server;
	Client my_client;
	int my_port;
	int next_port;
	int previous_port;
	
	static int main_port;
	ServerSocket serverSocket;
	int requests;
	BigInteger max_id,min_id;
	BigInteger previous_id,next_id;
	int max_port;
	int min_port;
	int number;	
	int RepFactor;
	
	public NodeChord(String id,int port,int main,int number,int RepFactor) throws InterruptedException,IOException,NoSuchAlgorithmException, UnsupportedEncodingException{
		
		AeSimpleSHA1 code = new AeSimpleSHA1();
		number_id = new BigInteger(code.SHA1(id),16); // BigInteger -> Id
		dht =  new ArrayList<Pair>();
		my_port = port;
		main_port = main;
		this.RepFactor = RepFactor;
		this.number = number;
		serverSocket = new ServerSocket(my_port);
		serverSocket.setSoTimeout(100000);
		previous_id=next_id=number_id;
		if(this.number == 0 ){ // Admin needs ports - ids
			next_port = previous_port = main_port;
			max_id = min_id = number_id; 
			max_port = min_port = main_port;
		}
	}
	
	
	
	
	
	public void run()   {		
		insert_chord() ;
	}
	
	public void insert_chord(){
		if(number == 0)
			Server();
		else{
			String message = "Pos,next,prev,"+number_id+","+my_port+",prev_id,next_id";
			Client(main_port,message);
			Server();
		}
	}

	public String[] refresh_max_min(String[] split){
		split[1] = Integer.toString(min_port);
		split[2] = Integer.toString(max_port);
		BigInteger received =  new BigInteger(split[3]);
		int max = received.compareTo(max_id);
		if(max == 1 ){ // new max
			max_port = Integer.parseInt(split[4]);
			max_id = received;
			return split;
		}
		int min = min_id.compareTo(received);
		if(min ==1){
		//	System.out.println(number + " I am smaller");
			min_port = Integer.parseInt(split[4]);
			min_id = received;
			return split;
		}
		return split;
	}	
	public void return_pos(String[] split){
		//System.out.println(number + " Pos");
		BigInteger received =  new BigInteger(split[3]);
		int res = received.compareTo(number_id);
		if(number == 0){ // Check if 0 works
			if((received.compareTo(max_id) == 1) || (min_id.compareTo(received) ==1) ){
				split[0] = "Found";
				split[5] = "" + min_id;
				split[6] = "" + max_id;
				split = refresh_max_min(split);
				Client(Integer.parseInt(split[4]),String.join(",",split));
				return;
			}
		}	
		if(res == -1){
			if(previous_id.compareTo(received)==1){
				Client(next_port,String.join(",",split));
				//System.out.println("Wrong "+Integer.parseInt(split[4]));
			}
			else{
				split[0]="Found";
				split[1] = Integer.toString(my_port);
				split[2] = Integer.toString(previous_port);
				split[5] = "" + number_id;
				split[6] = "" + previous_id;
				Client(Integer.parseInt(split[4]),String.join(",",split));
				
			}
		}
		else if(res == 1) // No cycle smaller than max for sure 
			Client(next_port,String.join(",",split));
	}
	public void refresh(String[] split){
		next_port = Integer.parseInt(split[1]);
		previous_port = Integer.parseInt(split[2]);
		next_id = new BigInteger(split[5]);
		previous_id = new BigInteger(split[6]);
		//System.out.println("Got in " + number);
		//System.out.println(number+" I listen from " +previous_port+" and send to " + next_port );
	}
	public void send_message_left_right(){
		//System.out.println(number +" from "+ previous_port+" to "+next_port);
		String msg = "Refresh,"+ previous_id +","+ next_id+"," + my_port +","+number_id;
		Client(next_port,msg);
		
	}
	public void refresh_left_right(String[] split){
		BigInteger previous = new BigInteger(split[1]);
		BigInteger next = new BigInteger(split[2]);
		BigInteger Start = new BigInteger(split[4]);
		if(number_id.equals(next)){
			previous_port = Integer.parseInt(split[3]);
			previous_id = new BigInteger(split[4]);
		}
		if(number_id.equals(previous)){
			next_port = Integer.parseInt(split[3]);
			next_id = new BigInteger(split[4]);
			Client(next_port,String.join(",",split));
			return ;
		}
		if(Start.equals(number_id)){
			//System.out.println("Getting into Chord");
			String msg = "Update,";
			Client(next_port,msg);
			return;
		}
		Client(next_port,String.join(",",split));
	}
	public void send_songs_to_next(){
		ListIterator<Pair> iter = dht.listIterator();
		while(iter.hasNext()){
			Pair value = iter.next();
		   	String answer = "Receive,"+value.hash + "," +value.key+ ","+value.value+ ",";
			Client(next_port,answer);
			System.out.println("Removing from database " + value.key);
		    iter.remove(); 
		}
	}
	public void depart(String[] split){
		if(my_port == Integer.parseInt(split[1])){
			send_songs_to_next();
			//System.out.println(my_port+ " 2 Going to depart");
			String  msg = "Left," + next_id + "," + previous_id + "," + next_port + "," + previous_port;
			Client(next_port,msg);
			System.out.println("Succesfully exiting Chord");
			}	
		else
			Client(next_port,String.join(",",split));
	}
	public void send_to_previous_songs(){
		String Id = "" + previous_id; // Check String comparison 
		ListIterator<Pair> iter = dht.listIterator();
		while(iter.hasNext()){
			Pair value = iter.next();
		    if((value.hash).compareTo(Id) != 1){
		    	String answer = "Receive,"+value.hash + "," +value.key+ ","+value.value+ ",";
				//dht.remove(pair1);
				Client(previous_port,answer);
				System.out.println("Removing from database ...");
		    	    iter.remove();
		    }
		}
		System.out.println("My database is empty ");
	}
	
	public void left_chord(String[] split){
		BigInteger next =  new BigInteger(split[1]);
		BigInteger previous =  new BigInteger(split[2]);
		if(number_id.equals(next)){
			//System.out.println(my_port+ " Refresh previous");
			previous_id = previous;
			previous_port = Integer.parseInt(split[4]);
		//	System.out.println(number +" from "+previous_port);
			Client(next_port,String.join(",",split));
			return;
		}
		if(number_id.equals(previous)){
			//System.out.println(my_port + " next");
			next_id = next;
			next_port = Integer.parseInt(split[3]);
		//	System.out.println(number +" to "+next_port);
			return;		
		}
		//System.out.println("Not me");
		Client(next_port,String.join(",",split));
	}
	public void process_request(String msg){
		String[] split = msg.split(",");
		if(split[0].equals("Pos"))
			return_pos(split);
		else if(split[0].equals("Update"))
			send_to_previous_songs();
		else if(split[0].equals("Receive")){
			System.out.println("I got new song" + split[2]);
			add_to_dht(split[1],split[2],split[3]);
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
				if (my_port == Integer.parseInt(split[2]) )
					return;
				else
					query("","*",Integer.parseInt(split[2]));
		}
		else
			System.out.println("Unknown message so far");
		return;
	}
		
	public void print_answer(String[] split){
			//System.out.println("I received " + number);
			System.out.println("<"+split[1]+","+split[2]+">");
			
		}
	
	public void query_chord(String[] split){
		BigInteger hash_value = new BigInteger(split[1]);
		if(hash_value.compareTo(number_id) == 1 ){
			if(number_id.compareTo(previous_id)==-1 && hash_value.compareTo(previous_id)== 1){
				//System.out.println("Case 1");
				//System.out.println("I am Min node");
				query(split[1],split[2],Integer.parseInt(split[3]));
			}
			else{
		     //	System.out.println("Case 2");
				Client(next_port,String.join(",",split));
			}	
		}
		else{
			if(hash_value.compareTo(previous_id)==1){
				query(split[1],split[2],Integer.parseInt(split[3]));

			//	System.out.println("Case 3");
			}
			else if(number_id.compareTo(previous_id) == -1 ){
				query(split[1],split[2],Integer.parseInt(split[3]));

			}
			else{	
				Client(next_port,String.join(",",split));
				//System.out.println(number +  " Sending song to " + next_port);
			//	System.out.println("Case 4");
			}
		}
	}
	
	public void delete_song(String[] split){
		
		BigInteger hash_value = new BigInteger(split[1]);
			if(hash_value.compareTo(number_id) == 1 ){
				if(number_id.compareTo(previous_id)==-1 && hash_value.compareTo(previous_id)== 1 ){
					//System.out.println("I am Min node");
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
				//	System.out.println("Case 3");
				}
				else if(hash_value.compareTo(previous_id) == 1 ){
					delete_from_dht(split[1]);
				}
				else{	
					Client(next_port,String.join(",",split));
					
				}
			}
		}
	
	public void insert_song(String[] split){
			BigInteger hash_value = new BigInteger(split[1]);
			if(hash_value.compareTo(number_id) == 1 ){
				if(number_id.compareTo(previous_id)==-1){
					//System.out.println("I am Min node");
					add_to_dht(split[1],split[2],split[3]);
				}
				else{
			     	//System.out.println("Case 2");
					Client(next_port,String.join(",",split));
				}	
			}
			else{
				if(hash_value.compareTo(previous_id)==1){
					add_to_dht(split[1],split[2],split[3]);
				//	System.out.println("Case 3");
				}
				else if(number_id.compareTo(previous_id) == -1 ){
					add_to_dht(split[1],split[2],split[3]);
				}
				else{	
					Client(next_port,String.join(",",split));
					//System.out.println(number +  " Sending song to " + next_port);
				//	System.out.println("Case 4");
				}
			}
		}
	public void Server()   {
		while(true){
			try{
				
				Socket server = serverSocket.accept();
				DataInputStream in = new DataInputStream(server.getInputStream());
				String message = in.readUTF();
				process_request(message);
				server.close();
			}	
			catch(SocketTimeoutException s){
				System.out.println("Socket timed out!");
				break;
			}
			catch(IOException e) {
				e.printStackTrace();
				break;
			}
		}
	}
	public void Client(int port_num,String my_string) {
		Socket  client;
	//	while(true){
		try {
			 client = new Socket("localhost",port_num);
	         OutputStream outToServer = client.getOutputStream();
	         DataOutputStream out = new DataOutputStream(outToServer);
	         out.writeUTF(my_string);
	         client.close();
	         return;
	      }catch(IOException e) {
	         e.printStackTrace();
	      }
	//	}
	}
	public void query(String hash,String key,int who_asked){
		if(key.equals("*")){
			for(Pair pair1 : dht){
				if(my_port == who_asked){
					System.out.println("<"+pair1.key+","+pair1.value+">");
				}
				else{
					String answer = "Answer,"+pair1.key+","+pair1.value;
					Client(who_asked,answer);
				}
			}
			String line = "PrintAll,*,"+who_asked;
			if(my_port == who_asked)
				Client(next_port,line);
			else
				Client(next_port,line);			
		}
		else{
			for(Pair pair1 : dht){
				if((pair1.hash).equals(hash)){
					String answer = "Answer,"+pair1.key+","+pair1.value;
					Client(who_asked,answer);
					System.out.println("Send to requester");
					return;
				}
			}
		}
		return; 
	}
	public void add_to_dht(String hash,String key,String value){
		for(Pair pair1 : dht){
					if((pair1.hash).equals(hash)){
						pair1.value = value;
						pair1.key = key;
						return;
					}	
		}
		Pair pair2 = new Pair(hash,key,value,0);
		dht.add(pair2);
		System.out.println(number + " Inserted");
		return;
		}
	public void delete_from_dht(String hash){
			for(Pair pair1 : dht){
				if(pair1.hash.equals(hash)){
					System.out.println(number + " Deleted " + pair1.key);
					dht.remove(pair1);
					return;
				}
			}
		//	System.out.println(number + " Not found ");
		}
}
