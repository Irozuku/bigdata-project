package org.mdp.kafka.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.mdp.kafka.def.KafkaConstants;
import org.mdp.kafka.sim.ECommerceStream;

public class ECommerceSimulator {
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		if(args.length!=2){
			System.err.println("Usage: [ecommerce_file_gzipped] [speed_up (int)]");
			return;
		}
		
		BufferedReader eventsECommerce = new BufferedReader(new InputStreamReader(new GZIPInputStream (new FileInputStream(args[0]))));

		List<String> eventTopics = new ArrayList<>(Arrays.asList("view", "cart", "remove_from_cart", "purchase"));
		
		int speedUp = Integer.parseInt(args[1]);
		
		Producer<String, String> eventProducer = new KafkaProducer<String, String>(KafkaConstants.PROPS);
		ECommerceStream eCommerceStream = new ECommerceStream(eventsECommerce, eventProducer, eventTopics, speedUp);
		
		Thread eCommerceThread = new Thread(eCommerceStream);
		
		eCommerceThread.start();
		
		try{
			eCommerceThread.join();
		} catch(InterruptedException e){
			System.err.println("Interrupted!");
		}
		
		eventProducer.close();
	}
}
