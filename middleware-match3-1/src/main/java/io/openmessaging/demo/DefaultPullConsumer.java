package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public final class DefaultPullConsumer implements PullConsumer {

	private final KeyValue properties;

	/** 读的queue名称 */
	private String queue;

	/** 所有的槽位 */
	private String[] tags;
	private int size;

	/** 起始位置 */
	private int cur = 0;

	public DefaultPullConsumer(final KeyValue properties) {
		this.properties = properties;
		active(this, properties);
	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	private final byte[] cmp = new byte[Global.CACHE_SIZE];
	
	private final byte[] buf = new byte[Global.CACHE_SIZE];
	private int bp;
	private int bn;

	private final Inflater inflater = new Inflater();

	/** 文件映射 */
	private FileChannel[] chls;
	private MappedByteBuffer mBuf;
	private int cIdx;

	private final boolean load() {
		short len;
		while ((len = mBuf.getShort()) == 0) { // 要换文件了
			if (++cIdx == chls.length) {
				return false;
			}
			try {
				mBuf = chls[cIdx].map(MapMode.READ_ONLY, 0, chls[cIdx].size());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		mBuf.get(cmp, 0, len);
		inflater.reset();
		inflater.setInput(cmp, 0, len);

		// buf.clear();
		bp = 0;
		try {
			bn = inflater.inflate(buf);
		} catch (DataFormatException e) {
			e.printStackTrace();
		}
		return true;
	}

	private String headKey;
	private String tag;
	private byte[] tagBytes;

	private final void createFile() {

		// buf.clear();
		bp = 0;
		bn = 0;

		tag = tags[cur];
		tagBytes = tag.getBytes();
		if (tag.charAt(0) == 'T') {
			headKey = MessageHeader.TOPIC;
		} else {
			headKey = MessageHeader.QUEUE;
		}

		chls = tag2chl.get(tag);

		cIdx = 0;
		try {
			mBuf = chls[cIdx].map(MapMode.READ_ONLY, 0, chls[cIdx].size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public final Message poll() {
		if ((bp < bn) || load()) {
			return new ConsumerMessage();
		} else {
			if (++cur < size) {
				createFile();
				return poll();
			} else {
				return null;
			}
		}
	}

	@Override
	public Message poll(KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void ack(String messageId) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void ack(String messageId, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void attachQueue(final String queueName, final Collection<String> topics) {

		// 存储到TreeSet中（进行了排序）
		queue = queueName;
		final TreeSet<String> bucketsSet = new TreeSet<>();

		for (String topic : topics) {
			if (tag2chl.containsKey(topic)) {
				bucketsSet.add(topic);
			}
		}

		// 转存到数组中
		size = bucketsSet.size();
		if (tag2chl.containsKey(queue)) {
			size += 1;
		}

		tags = new String[size];
		int idx = 0;
		for (String bucket : bucketsSet) {
			tags[idx] = bucket;
			idx++;
		}

		if (idx < size) {
			tags[idx] = queue;
		}

		if (tags.length > 0) {
			createFile();
		}

	}

	/// >> 读类
	private static boolean isActived;
	private static String STORE_PATH;

	private static final Map<String, FileChannel[]> tag2chl = new HashMap<>(100);

	@SuppressWarnings("resource")
	public static synchronized void active(final DefaultPullConsumer consumer, final KeyValue properties) {
		if (isActived) {
			return;
		}

		STORE_PATH = properties.getString("STORE_PATH") + File.separator;
		File[] bucketFiles = new File(STORE_PATH).listFiles();

		for (File bucketFile : bucketFiles) {
			File[] dataFiles = bucketFile.listFiles();
			FileChannel[] channels = new FileChannel[dataFiles.length];

			for (int i = 0; i < channels.length; ++i) {
				try {
					channels[i] = new RandomAccessFile(dataFiles[i], "r").getChannel();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
			tag2chl.put(bucketFile.getName(), channels);
		}
		isActived = true;

		return;
	}

	/// >>

	private byte n; private byte i; private byte[] k; private byte[] v;

	private final class ConsumerMessage implements BytesMessage {

		private final DefaultKeyValue headers;
		private final DefaultKeyValue properties;
		private byte[] body;

		public ConsumerMessage() {
			
			// 设置 headers 2
			// 添加键值对
			headers = new DefaultKeyValue(2);
			headers.put(headKey, tagBytes);
			// n = buf.get();
			n = buf[bp++];
			for (i = 0; i < n; ++i) {
				k = new byte[buf[bp++]];
				// buf.get(k);
				System.arraycopy(buf, bp, k, 0, k.length);
				bp += k.length;

				// v = new byte[buf.getShort()];
				v = new byte[((buf[bp] & 0xff) << 8) | ((buf[bp + 1] & 0xff))];
				bp += 2;
				// buf.get(v);
				System.arraycopy(buf, bp, v, 0, v.length);
				bp += v.length;

				headers.put(new String(k), v);
			}

			// 设置 properties 4
			n = buf[bp++];
			properties = new DefaultKeyValue(n);
			// 添加键值对
			for (i = 0; i < n; ++i) {
				k = new byte[buf[bp++]];
				// buf.get(k);
				System.arraycopy(buf, bp, k, 0, k.length);
				bp += k.length;

				// v = new byte[buf.getShort()];
				v = new byte[((buf[bp] & 0xff) << 8) | ((buf[bp + 1] & 0xff))];
				bp += 2;
				// buf.get(v);
				System.arraycopy(buf, bp, v, 0, v.length);
				bp += v.length;

				properties.put(new String(k), v);
			}

			// 设置body
			// body = new byte[buf.getShort()];
			body = new byte[((buf[bp] & 0xff) << 8) | ((buf[bp + 1] & 0xff))];
			bp += 2;
			// buf.get(body);
			System.arraycopy(buf, bp, body, 0, body.length);
			bp += body.length;
		}

		@Override
		public byte[] getBody() {
			return body;
		}

		@Override
		public final BytesMessage setBody(final byte[] body) {
			this.body = body;
			return this;
		}

		@Override
		public final KeyValue headers() {
			return headers;
		}

		@Override
		public final KeyValue properties() {
			return properties;
		}

		@Override
		public final Message putHeaders(final String k, final int v) {
			headers.put(k, v);
			return this;
		}

		@Override
		public final Message putHeaders(final String k, final long v) {
			headers.put(k, v);
			return this;
		}

		@Override
		public final Message putHeaders(final String k, final double v) {
			headers.put(k, v);
			return this;
		}

		@Override
		public final Message putHeaders(final String k, final String v) {
			headers.put(k, v);
			return this;
		}

		@Override
		public final Message putProperties(final String k, final int v) {
			properties.put(k, v);
			return this;
		}

		@Override
		public final Message putProperties(final String k, final long v) {
			properties.put(k, v);
			return this;
		}

		@Override
		public final Message putProperties(final String k, final double v) {
			properties.put(k, v);
			return this;
		}

		@Override
		public final Message putProperties(final String k, final String v) {
			properties.put(k, v);
			return this;
		}
	}

}
