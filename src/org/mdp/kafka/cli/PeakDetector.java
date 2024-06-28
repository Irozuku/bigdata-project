package org.mdp.kafka.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mdp.kafka.def.KafkaConstants;

public class PeakDetector {
	public static int FIFO_SIZE;
	public static int EVENT_START_TIME_INTERVAL;
	public static int EVEN_END_TIME_INTERVAL;
	public static String start;
	public static String finish;
	public static String category = null;
	public static List<List<String>> peakEvents = new ArrayList<>();
	
	public static final List<String> CATEGORIES = new ArrayList<>(Arrays.asList("accessories", 
			"apparel", 
			"appliances", 
			"auto",
			"computers",
			"construction",
			"country_yard",
			"electronics",
			"furniture",
			"kids",
			"medicine",
			"unknown",
			"sport",
			"stationery"));;
			
			
	public static void main(String[] args) throws FileNotFoundException, IOException{
		if(args.length!=3 && args.length!=4){
			System.err.println("Usage [eventType] [numEvents] [timePeak] [category*]");
			return;
		}
		
		// Print the list of pairs after ctrl+c (START_EVENT_TIME, FINISH_EVENT_TIME)
	    Runtime.getRuntime().addShutdownHook(new Thread() {
	        public void run() {
	          System.out.println(peakEvents);
	        }
	      });
		
		try {
			FIFO_SIZE = Integer.valueOf(args[1]);
			EVENT_START_TIME_INTERVAL = Integer.valueOf(args[2]) * 1000;
			EVEN_END_TIME_INTERVAL = 2 * EVENT_START_TIME_INTERVAL;
			if (args.length == 4) {
				if (!CATEGORIES.contains(args[3])) {
					System.err.println("Not a valid category");
					System.err.println(CATEGORIES.toString());
		            return;
				}
				category = args[3];
				
			}
			
		} catch (NumberFormatException e) {
            System.err.println("Invalid integer input for numEvents/timePeak");
            return;
        } 

		Properties props = KafkaConstants.PROPS;

		// randomise consumer ID so messages cannot be read by another consumer
		//   (or at least it's more likely that a meteor wipes out life on Earth)
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		consumer.subscribe(Arrays.asList(args[0]));

		// initialize a FIFO queue
		LinkedList<ConsumerRecord<String, String>> fifo = new LinkedList<ConsumerRecord<String, String>>();

		// initialize some state variables
		boolean inEvent = false;
		int events = 0;
		
		try{
			while (true) {
				// every ten milliseconds get all records in a batch
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
				
				// for all records in the batch
				for (ConsumerRecord<String, String> record : records) {
					if (category != null) {
						String[] row = record.value().split(",");
						if (!category.equals(row[4])) {
							continue;
						}
					}
					
					fifo.add(record);

					// check if we have FIFO_SIZE elements in the FIFO queue
					if(fifo.size()>=FIFO_SIZE) {
						// remove first element of queue
						ConsumerRecord<String, String> oldest = fifo.removeFirst();
						
						// diference of timestamps
						long gap = record.timestamp() - oldest.timestamp();
						
						// event condition
						if(gap <= EVENT_START_TIME_INTERVAL && !inEvent) {
							inEvent = true;
							events++;
							System.out.println("START event-id:" + events + ": start:"+oldest.timestamp() + " value:"+oldest.value() + " rate:" + FIFO_SIZE + " records in " + gap + " ms");
							start = oldest.value().split(",")[0];
						}
						else if(gap >= EVEN_END_TIME_INTERVAL && inEvent) {
							inEvent = false;
							System.out.println("END event:" + events + ": finish:" + record.timestamp() + " value:" + record.value() + " rate:" + FIFO_SIZE + " records in " + gap + " ms");
							finish = record.value().split(",")[0];
							peakEvents.add(new ArrayList<>(Arrays.asList(start, finish)));
						}	
					}
				}
			}
		} finally {
			consumer.close();
		}
	}
}
