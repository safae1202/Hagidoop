package interfaces;

public class HdfsKV extends KV {
	public HdfsKV(String type, String fileName) {
		super();
		this.k = type;
		this.v = fileName;
	}
}
