package io.openmessaging.test2;

public class Tester {

	public static void main(String[] args) throws Exception {
		System.out.println("ProducerTester begin！");
		ProducerTester.run();
		System.out.println("ProducerTester end！");
		
		Thread.sleep(2000L);
		
		System.out.println("ConsumerTester begin！");
		ConsumerTester.run();
		System.out.println("ConsumerTester end！");
	
		
	}
}
