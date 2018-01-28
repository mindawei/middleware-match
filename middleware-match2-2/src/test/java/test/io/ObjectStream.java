package test.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;


public class ObjectStream {
	
	public static void write(String filename,Map<String, String> kvMap){
        try {
            boolean isexist=false;
            File file=new File(filename);
            if(file.exists())
                isexist=true;
            
            FileOutputStream fo = new FileOutputStream (filename,true);
            ObjectOutputStream os = new ObjectOutputStream(fo);
            
            long pos=0;
            if(isexist)
            {
                pos=fo.getChannel().position()-4;
                fo.getChannel().truncate(pos);
            }
            
            // 可以写入多次
            os.writeObject(kvMap); 
            os.close();  
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
    }

	
	
	public static Map<String, String> read(String fileName,String key,String value) {	
		FileInputStream fis = null;   
        ObjectInputStream ois = null;   
        try {   
            fis = new FileInputStream(fileName);   
            ois = new ObjectInputStream(fis);  
           
            while(fis.available()>0){
            	
            	Object object = ois.readObject();
            	
				@SuppressWarnings("unchecked")
				Map<String, String> map = (Map<String, String>)object;
            	if(value.equals(map.get(key)))
            		return map;
            }
           
        } catch (Exception e) {   
            e.printStackTrace();   
        } finally {   
            if (fis != null) {   
                try {   
                    fis.close();   
                } catch (IOException e1) {   
                    e1.printStackTrace();   
                }   
            }   
            if (ois != null) {   
                try {   
                    ois.close();   
                } catch (IOException e2) {   
                    e2.printStackTrace();   
                }   
            }   
        }   
        return null;  
	}
	
	public static void main(String[] args) {
		String key = "id";
		Map<String,String> kvMap1 = new HashMap<String,String>();
		kvMap1.put(key, "1");
		
		Map<String,String> kvMap2 = new HashMap<String,String>();
		kvMap2.put(key, "2");
		
		String fileName = "objectsData";
		//write(fileName,kvMap1);
		//write(fileName,kvMap2);
		System.out.println(read(fileName, key, "2"));
		
	}

}
