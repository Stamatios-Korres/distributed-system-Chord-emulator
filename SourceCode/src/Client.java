import java.io.*;
import java.net.*;
import java.net.*;
public class Client{
	
	Socket  client;
	String my_string;
	
	public Client(int port,String my_string) throws IOException {
		client = new Socket("localhost",port);
		this.my_string = my_string;
	}
	
	public void send_request(){
	
		try {
	         OutputStream outToServer = client.getOutputStream();
	         DataOutputStream out = new DataOutputStream(outToServer);
	         out.writeUTF(my_string);
	         InputStream inFromServer = client.getInputStream();
	         DataInputStream in = new DataInputStream(inFromServer);
	         client.close();
	         return;
	      }catch(IOException e) {
	         e.printStackTrace();
	      }
	   }
	}

