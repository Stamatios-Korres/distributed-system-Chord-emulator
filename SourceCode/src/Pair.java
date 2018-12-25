import java.io.*;

public class Pair {
	String hash;
	String key;
	String value;
	int distance;
	int version;
	public Pair(String hash,String key,String value,int distance){
		this.hash = hash;
		this.key = key;
		this.value = value;
		this.distance = distance;
	}
}
