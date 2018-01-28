package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.Deflater;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

public final class DefaultProducer implements Producer {

	private KeyValue properties;

	/** 还剩多少个生产者没有完成 */
	private static short producerID = 0;

	/** 增生产者 */
	private static synchronized short active(String STORE_PATH) {

//		// 初始化
//		if (producerID == 0) {
//
//			/** 创建目录 */
//			File dataDir = new File(STORE_PATH);
//			if (!dataDir.exists()) {
//				dataDir.mkdirs();
//			}
//
//		}

		return producerID++;
	}

	private final String STORE_PATH;
	private final short pid;

	public DefaultProducer(KeyValue properties) {

		this.properties = properties;
		this.STORE_PATH = properties.getString("STORE_PATH") + File.separator;
		this.pid = active(STORE_PATH);

	}

	@Override
	public final BytesMessage createBytesMessageToTopic(final String topic, final byte[] body) {
		ProducerMessage msg = new ProducerMessage(body);
		msg.tag = topic;
		return msg;
	}

	@Override
	public final BytesMessage createBytesMessageToQueue(final String queue, final byte[] body) {
		ProducerMessage msg = new ProducerMessage(body);
		msg.tag = queue;
		return msg;
	}

	@Override
	public void start() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	/** 每个线程可以复用的Buffer缓存 */
	// private final Wrapper wrap = new Wrapper(MAX_MESSAGE_SIZE);
	private final static boolean[] dirExists = new boolean[100];

	// 0- 89 topic 90 -99 queue
	private final Writer[] writers = new Writer[100];
	private final Deflater deflater = new Deflater(1);//Deflater.BEST_SPEED);
	private final byte[] cmp = new byte[Global.CACHE_SIZE]; // 预留两个字节

	
	@Override
	public final void send(final Message msg) {

		ProducerMessage dmsg = (ProducerMessage) msg;
		String tag = dmsg.tag;

		// 优化 map
		int idx;
		if (tag.charAt(0) == 'T') {

			// TOPIC_ 0 - 89
			int len = tag.length();
			if (len == 7) {
				idx = tag.charAt(6) - '0';
			} else {
				idx = (tag.charAt(6) - '0') * 10 + (tag.charAt(7) - '0');
			}

		} else {

			// QUEUE_ 90 - 99 : tag.charAt(6) - '0' + 90 ;
			idx = tag.charAt(6) + 42;
		}

		// 判断文件映射是否存在
		if (writers[idx] == null) {

			final String dir = STORE_PATH + tag + File.separator;
			if (!dirExists[idx]) {
				/** 创建目录 */
				File dataDir = new File(dir);
				if (!dataDir.exists()) {
					dataDir.mkdirs();
				}
				dirExists[idx] = true;
			}
			writers[idx] = new Writer(dir + pid);
		}

		// 写入数据
		// wrap.clear();
		writers[idx].write(dmsg);

	}

	@Override
	public void flush() {
		for (Writer w : writers) {
			if (w != null) {
				w.close();
			}
		}
	}

	private final class Writer {

		private final byte[] buf = new byte[Global.CACHE_SIZE];
		private short pos = 0;
		
		private final RandomAccessFile file;
		
		public Writer(final String path) {
			// 打开新的文件
			try {
				file = new RandomAccessFile(path, "rw");
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException();
			}
		}

		final void write(final ProducerMessage dmsg) {

			pos = dmsg.wrap(buf,pos);
			
			if (pos > Global.CMPRESS_SIZE) {

				deflater.reset();
				deflater.setInput(buf, 0, pos);
				deflater.finish();

				short clen = (short)deflater.deflate(cmp, 2, pos);
				// short
				cmp[0] = (byte) ((clen >> 8) & 0xFF);
				cmp[1] = (byte) (clen & 0xFF);

				try {
					file.write(cmp, 0, clen + 2);
				} catch (IOException e) {
					e.printStackTrace();
				}

				pos = 0;
			}

		}

		final void close() {

			try {
				
				if (pos > 0) {
					
					deflater.reset();
					deflater.setInput(buf, 0, pos);
					deflater.finish();

					short clen = (short)deflater.deflate(cmp, 2, pos);
					// short
					cmp[0] = (byte) ((clen >> 8) & 0xFF);
					cmp[1] = (byte) (clen & 0xFF);

				
					file.write(cmp, 0, clen + 2);
				}

				file.writeShort(0);
				// 关闭文件
				file.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}
	
	private final class ProducerMessage implements BytesMessage {

		private final DefaultKeyValue headers = new DefaultKeyValue(2);
		private final DefaultKeyValue pros = new DefaultKeyValue(4);
		private byte[] body;

		String tag;

		public ProducerMessage(byte[] body) {
			this.body = body;
		}

		@Override
		public byte[] getBody() {
			return body;
		}

		@Override
		public BytesMessage setBody(byte[] body) {
			this.body = body;
			return this;
		}

		@Override
		public KeyValue headers() {
			return headers;
		}

		@Override
		public KeyValue properties() {
			return pros;
		}

		@Override
		public Message putHeaders(String key, int value) {
			headers.put(key, value);
			return this;
		}

		@Override
		public Message putHeaders(String key, long value) {
			headers.put(key, value);
			return this;
		}

		@Override
		public Message putHeaders(String key, double value) {
			headers.put(key, value);
			return this;
		}

		@Override
		public Message putHeaders(String key, String value) {
			headers.put(key, value);
			return this;
		}

		@Override
		public Message putProperties(String key, int value) {
			pros.put(key, value);
			return this;
		}

		@Override
		public Message putProperties(String key, long value) {
			pros.put(key, value);
			return this;
		}

		@Override
		public Message putProperties(String key, double value) {
			pros.put(key, value);
			return this;
		}

		@Override
		public Message putProperties(String key, String value) {
			pros.put(key, value);
			return this;
		}

		/**
		 * 存储格式<br>
		 * 
		 * 1 heads<br>
		 * 2 pros<br>
		 * 3 body<br>
		 * 
		 * 具体如下：<br>
		 * <br>
		 * headsKeyNum<br>
		 * keyBytesSize | keyBytes | valueBytesSize | valueBytes <br>
		 * ...<br>
		 * <br>
		 * prosKeyNum<br>
		 * keyBytesSize | keyBytes | valueBytesSize | valueBytes <br>
		 * ...<br>
		 * <br>
		 * bodySize | bodyBytes <br>
		 * 
		 */
		final short wrap(final byte[] buf, short pos) {

			// 存长度
			DefaultKeyValue kv;
			byte[] bytes;
			short len;
			byte i;

			// headers
			kv = headers;
			buf[pos++] = kv.num;
			for (i = 0; i < kv.num; ++i) {
				// 写入 key 2,10
				bytes = kv.keys[i].getBytes();
				len = (short) bytes.length;
				buf[pos++] = (byte) len; // 10
				System.arraycopy(bytes, 0, buf, pos, len);
				pos += len;

				// 写入 value 4,1003
				bytes = kv.vals[i];
				len = (short) bytes.length;
				buf[pos++] = (byte) (len >> 8 & 0xff);
				buf[pos++] = (byte) (len & 0xff);
				System.arraycopy(bytes, 0, buf, pos, len);
				pos += len;
			}

			// pros
			kv = pros;
			buf[pos++] = kv.num;
			for (i = 0; i < kv.num; ++i) {
				// 写入 key 2,10
				bytes = kv.keys[i].getBytes();
				len = (short) bytes.length;
				buf[pos++] = (byte) len; // 10
				System.arraycopy(bytes, 0, buf, pos, len);
				pos += len;

				// 写入 value 4,1003
				bytes = kv.vals[i];
				len = (short) bytes.length;
				buf[pos++] = (byte) (len >> 8 & 0xff);
				buf[pos++] = (byte) (len & 0xff);
				System.arraycopy(bytes, 0, buf, pos, len);
				pos += len;
			}

			// 写入body
			// wrap.putShort((short) body.length);
			len = (short) body.length;
			buf[pos++] = (byte) (len >> 8 & 0xff);
			buf[pos++] = (byte) (len & 0xff);
			// wrap.put(body);
			System.arraycopy(body, 0, buf, pos, len);
			pos += len;

			return pos;
		}

	}
	
	///////////////////////////

	
	@Override
	public void send(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public Promise<Void> sendAsync(Message message) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public Promise<Void> sendAsync(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void sendOneway(Message message) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void sendOneway(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public BatchToPartition createBatchToPartition(String partitionName) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}
}
