package com.alibaba.middleware.race.sync;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class QingServer {

	private ServerSocket serverSocket;

	public QingServer(int port) throws IOException {
		this.serverSocket = new ServerSocket(port);
	}

	public void service() throws IOException {
		while (true) {
			Socket socket = this.serverSocket.accept();
			new Thread(new QingHandle(socket)).start();
		}
	}

	static class QingHandle implements Runnable {

		private Socket socket;

		public QingHandle(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run() {
			
			// 初始化变量 
			try {
			
				final BufferedOutputStream out = new BufferedOutputStream(this.socket.getOutputStream());
				
				long item = 0; int pk = 0;
				
				final long[] result = Server.readTask.get();
		
				//for (pk = Server.start + 1; pk < Server.end; ++pk) {
					for (pk = 1000001; pk < 8000000; ++pk) {

					// 没有记录
					if (result[pk] == 0) {
						continue;
					}

					// 主键 int
					out.write((byte) ((pk >> 24) & 0xFF));
					out.write((byte) ((pk >> 16) & 0xFF));
					out.write((byte) ((pk >>  8) & 0xFF));
					out.write((byte) ( pk & 0xFF));
					
					// 值 value
					item = result[pk];
					out.write((byte) ((item >> 56) & 0xFF));
					out.write((byte) ((item >> 48) & 0xFF));
					out.write((byte) ((item >> 40) & 0xFF));
					out.write((byte) ((item >> 32) & 0xFF));
					out.write((byte) ((item >> 24) & 0xFF));
					out.write((byte) ((item >> 16) & 0xFF));
					out.write((byte) ((item >>  8) & 0xFF));
					out.write((byte) ( item & 0xFF));
				
				}

				out.flush();
				out.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (socket != null) {
					try {
						socket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
