package small;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MapMemeryBuffer {  
  
    public static void main(String[] args) throws Exception {  
       long ts = System.currentTimeMillis();
       ByteBuffer byteBuffer = ByteBuffer.allocateDirect(16 *  1024 * 1024);
       for(int i = 1;i <= 10;i ++){
    	   System.out.println("deal "+i);
    	   RandomAccessFile mFile = new RandomAccessFile("data/" + i + ".txt", "r");
           FileChannel channel = mFile.getChannel();
           while(true){
        	   byteBuffer.clear();
               int len = channel.read(byteBuffer);
               if(len <= 0){
            	   break;
               }
               byteBuffer.flip();
           }
           mFile.close();
       }
       System.out.println(System.currentTimeMillis() - ts);
    }  
}  