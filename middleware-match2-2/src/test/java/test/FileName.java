package test;

public class FileName {
	public static void main(String[] args) {
//		String goodid = "good_70765d51";
//		System.out.println(goodid.hashCode());
		
		long createtime = 1463116105L;
//		1463116105
//		1463011200
//		1463097600
		long daySeconds = 86400L; //24L * 3600L;
		
		// 1463068800
		System.out.println(daySeconds);
		long dayCreatetime = (createtime / daySeconds) * daySeconds;
		
		System.out.println(dayCreatetime / daySeconds );
		
		System.out.println( 1463089833L / daySeconds );
		
		System.out.println((1463068800L+86400L) / daySeconds);
		
		
		System.out.println(16933*daySeconds);
		System.out.println(16934*daySeconds);
		
		
	}
}
