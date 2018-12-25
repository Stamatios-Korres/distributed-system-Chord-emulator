import java.io.*;
import java.util.ArrayList;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.math.*;
import java.util.Arrays.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Emulator {
	static int RepFactor;
	String Replication_Strategy;
	static int port;
	int max_port;
	int nodes;
	Eventual_Consistency[] table;
	Chain_Replication[] table1;
	
	public Emulator(int RepFactor,String Replication_Strategy){
		this.RepFactor = RepFactor;
		this.Replication_Strategy = Replication_Strategy;	
		nodes = 0 ;
		port = 7000;
		table = new Eventual_Consistency[20];
		table1 = new Chain_Replication[20];
		
	}
	
	public int get_port(){
		return max_port;
	}
	
	//public static void main(String args[])  throws InterruptedException,IOException,NoSuchAlgorithmException,UnsupportedEncodingException {
	//}
	
	public void insert_all_songs() throws InterruptedException,IOException,NoSuchAlgorithmException,UnsupportedEncodingException {
			String file = "insert.txt";
			System.out.println("Starting insertion .... ");
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			    String line;
			    int i=0;
			     while ((line = br.readLine()) != null) {
			       	line = insert_song(line);
			     	Random rand = new Random();
			    	int  n = rand.nextInt(max_port) + nodes;
			    	n = n % (nodes);
			    	n = n + port;
			    	//System.out.println(n);
			    	Client(n,line);
			    	Thread.sleep(1);
			    	}
			     System.out.println("Done insertion .... ");
			}
		}
	
	public static void queries(int max_port,int nodes)throws InterruptedException,NoSuchAlgorithmException,FileNotFoundException, IOException{
			String file = "query.txt";
			String line;
			int nodes1 = nodes;
			AeSimpleSHA1 code = new AeSimpleSHA1();
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			     int i=0;
			     while ((line = br.readLine()) != null) {
			    	// System.out.println(i);
			    	 //i++;
			    	line = "query,hash,"+line;
			    	String split[] = line.split(",");
			     	Random rand = new Random();
			     	BigInteger hash = new BigInteger(code.SHA1(split[2]),16);
					split[1] = "" + hash;
					line = String.join(",",split);
			    	int  n = rand.nextInt(max_port) + nodes;
			    	n = n % (nodes);
			    	n = n + port;
			    	line = line + "," + n;
			    	//System.out.println(line);

			    	Client(n,line);
			    	Thread.sleep(1);
			    	}
			   
			}
			  Client(20000,"got");
			return ;
		}
		
	public static String query(String line) throws InterruptedException,NoSuchAlgorithmException,UnsupportedEncodingException{
    	line = ", hash, " + line;
		line = String.join(",",line.split(", "));
    	AeSimpleSHA1 code = new AeSimpleSHA1();
    	String split[] = line.split(",");
		//System.out.println(split[2]);
		BigInteger hash = new BigInteger(code.SHA1(split[2]),16);
		split[1] = "" + hash;
		line = String.join(",",split);
	//	System.out.println("Sending ... " + line);
		return line;
	}
	
	public void Init_Chord_Chain_Rep (int RepFactor) throws InterruptedException,IOException,NoSuchAlgorithmException,UnsupportedEncodingException{
			int port1 = port;
			int port = port1;
			int number = 0;
			for(int i=0;i<10;i++){
			//	System.out.println(i);
				nodes++;
				table1[i] = new Chain_Replication(Integer.toString(i),port,port1,number,RepFactor);
				port++;
				number++;
				table1[i].start();
				Thread.sleep(200);
			}
			max_port = port;
		//	System.out.println("Going to sleep .... ");
			Thread.sleep(2000);
		}
	
	
	
	public void Init_ChordEventual_Consistency (int RepFactor) throws InterruptedException,IOException,NoSuchAlgorithmException,UnsupportedEncodingException{
			int port = 7000;
			int port1 = port;
			int number = 0;
			for(int i=0;i<5;i++){
				nodes++;
				//System.out.println("RepFactor is " + RepFactor);
				table[i] = new Eventual_Consistency(Integer.toString(i),port,port1,number,RepFactor);
				port++;
				number++;
				table[i].start();
				Thread.sleep(20);
			}
			max_port = port;
			//Thread.sleep(2000);
			
		}
		

	
	public static String insert_song(String line) throws InterruptedException,NoSuchAlgorithmException,UnsupportedEncodingException{
    	line = "insert, hash, " + line;
		line = String.join(",",line.split(", "));
    	AeSimpleSHA1 code = new AeSimpleSHA1();
    	String split[] = line.split(",");
		//System.out.println(split[2]);
		BigInteger hash = new BigInteger(code.SHA1(split[2]),16);
		split[1] = "" + hash;
		line = String.join(",",split);
	//	System.out.println("Sending ... " + line);
		return line;
	}
	
	

	
	public static void Client(int port_num,String my_string) {
		Socket  client;
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
	}
	public static void request_transfrorm(int max_port,int nodes)throws InterruptedException,NoSuchAlgorithmException,FileNotFoundException, IOException{
		String file = "My_test";
		String line;
		AeSimpleSHA1 code = new AeSimpleSHA1();
		Thread.sleep(200);

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			  while ((line = br.readLine()) != null) {
		    	String split[] = line.split(", ");
		    	 Random rand = new Random();
	    		int  n;
		    	 if(split[0].equals("insert")){
			    	 BigInteger hash = new BigInteger(code.SHA1(split[1]),16);

		    		line = "insert,"+hash+","+split[1]+","+split[2];
		    		n = rand.nextInt(max_port) + nodes;
				    n = n % (nodes);
				    n = n + port;
				    Client(n,line);
				    Thread.sleep(1);
					//System.out.println("Sending ... " + line);

		    	}
		    	 else if(split[0].equals("query")){
			    	 BigInteger hash = new BigInteger(code.SHA1(split[1]),16);

		    		 	line ="query," +hash +","+split[1];
		    			n = rand.nextInt(max_port) + nodes; //random node to start query
				    	n = n % (nodes);
				    	n = n + port;
				    	line = line + "," + n;
				    	Client(n,line);
				    	//Thread.sleep(1);
				    	//System.out.println("Sending ... " + line);

			     }
		    	 else if(split[0].equals("delete")){
			    	 BigInteger hash = new BigInteger(code.SHA1(split[1]),16);

		    		line = "delete,"+hash;
		    		n = rand.nextInt(max_port) + nodes; //random node to start query
			    	n = n % (nodes);
			    	n = n + port;
			    	line = line + "," + n;
			    	Client(n,line);
			    	Thread.sleep(1);
		    		 
		    	 }
		    	 else if(split[0].equals("Join")){
		    		Chain_Replication new_node =  new Chain_Replication(Integer.toString(nodes),max_port,7000,nodes,RepFactor);
		    		new_node.start();
	    		 	Thread.sleep(1000);

		    		 
		    	 }
		    	 else if(split[0].equals("depart")){
	    		 		System.out.println();
	    		 		System.out.println();
	    		 		System.out.println();
	    		 		System.out.println("Going to remove node ....");
	    		 		System.out.println();
	    		 		System.out.println();
		    		 	Thread.sleep(100);
		    		    String msg = "Depart,"+split[1];
		    		 	Client(7000,msg);
			    }
		    	else
		    	 System.out.println("Unkown Request!!!");
		    	
	}
		}
	}
	
}
