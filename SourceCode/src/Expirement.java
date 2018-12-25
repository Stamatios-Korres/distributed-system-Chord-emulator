import java.io.DataInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.NoSuchAlgorithmException;
import java.text.*;
import java.util.Date;
	
public class Expirement {
	
	public static void main(String [] args)  throws InterruptedException,IOException,NoSuchAlgorithmException,UnsupportedEncodingException{
	 String [] table  = {"Eventual_Consistency","Chain_Replication"};
		Time_Listener timos = new Time_Listener();
	 	for(int i = 1;i<2 ; i+=2){
			//for(String pair : table){
	 	  	//System.out.println(i);
	 	  	Emulator k = new Emulator(i,"Eventual_Consistency");
				//if(pair.equals("Eventual_Consistency"))
					k.Init_Chord_Chain_Rep(3);
				//else 
				//k.Init_ChordEventual_Consistency(3);
			
			//k.insert_all_songs();
			timos.start();
			int j =k.get_port();
			//k.queries(j,10);
			k.request_transfrorm(j,6);
			
			
				
		}
	}
	

	
}
