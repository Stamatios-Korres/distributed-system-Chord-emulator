import java.io.*;
import java.net.*;

public class Server extends Thread {
	
	ServerSocket serverSocket;
	String id;
	public String message;
	
	public Server(int port,String id ) throws IOException{
		serverSocket = new ServerSocket(port);
		this.id = id ;
		serverSocket.setSoTimeout(10000);
	}
	
	
	public void run(){
		while(true){
			try{
				System.out.println(id + "Waiting for Client ...  ");
				Socket server = serverSocket.accept();
				DataInputStream in = new DataInputStream(server.getInputStream());
				message = in.readUTF();
				System.out.println("I received " + message);
				DataOutputStream out = new DataOutputStream(server.getOutputStream());
				out.writeUTF("Welcome");
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
}   
