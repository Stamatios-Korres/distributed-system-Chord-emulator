import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Date;

public class Time_Listener extends Thread{
	static ServerSocket serverSocket;
	
	public Time_Listener()  throws IOException {
		serverSocket = new ServerSocket(20000);
		serverSocket.setSoTimeout(100000);
	}
	
	public void run()   {		
		long start_time = System.nanoTime();
		Server(start_time);
	}

	public static void Server(long start_time)  {
		long end_time;
		double difference;
		int i=0;
		int count=0;
	    while(true){
			try{
				
				Socket server;
				server = serverSocket.accept();
				i++;
				DataInputStream in = new DataInputStream(server.getInputStream());
				String message = in.readUTF();
				end_time = System.nanoTime();
				difference = (end_time - start_time)/(1e6*1000);
				count++;
				if(!message.equals("got"))
				//	System.out.println("Found" + " " + message + " " + count);
				if(count == 500)
					System.out.println(i + " Time difference for quering " +difference);
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
