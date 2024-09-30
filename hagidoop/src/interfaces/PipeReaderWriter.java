package interfaces;

import java.util.Queue;
import java.util.LinkedList;


public class PipeReaderWriter implements ReaderWriter {
	public Queue<KV> queue;
	
	public PipeReaderWriter() {
		this.queue = new LinkedList<>();
	}

	@Override
	public void write(KV record) {
		this.queue.add(record);
	}

	@Override
	public KV read() {
		return this.queue.poll();
	}
}
