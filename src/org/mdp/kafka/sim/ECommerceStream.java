package org.mdp.kafka.sim;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ECommerceStream implements Runnable {
	// cannot be static since not synchronised
	public final SimpleDateFormat ECOMMERCE_DATE = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
	
	BufferedReader br;
	long startSim = 0;
	long startData = 0;
	long lastData = 0;
	int speedup;
	Producer<String, String> producer;
	List<String> topics;
	
	public ECommerceStream(BufferedReader br, Producer<String, String> producer, List<String> topics, int speedup){
		this(br,System.currentTimeMillis(),producer,topics,speedup);
	}
	
	public ECommerceStream(BufferedReader br, long startSim, Producer<String, String> producer, List<String> topics, int speedup){
		this.br = br;
		this.startSim = startSim;
		this.producer = producer;
		this.speedup = speedup;
		this.topics = topics;
		this.ECOMMERCE_DATE.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	@Override
	public void run() {
		String line;
		long wait = 0;
		try {
			// get rid of first line (column name)
			br.readLine();
			while((line = br.readLine())!=null) {
				String[] tabs = line.split(",");
				try {
					long timeData = getUnixTime(tabs[0]);
					if(startData == 0) // first element read
						startData = timeData;
					
					wait = calculateWait(timeData);
					
					// replace the category with the prefix (example : electronics.audio.headphone -> electronics)
					tabs[4] = tabs[4].split("\\.")[0];
					
					// replace every "nan" or empty space in category row to 'unknown'
			        if (tabs[4].equals("nan") || tabs[4].isEmpty()) {
			        	tabs[4] = "unknown";
			        }
			        
			        // replace line read with previous modifications
			        String result = Arrays.toString(tabs).replaceAll(",\\s*", ",");
			        result = result.replaceAll("[\\[\\]]","");
			        // this happens if the last column is empty (user-session)
			        if (!result.equals(line)) {
				        result = result + ",";
			        }
			        line = result;
					
					// we assing a unique id considering event_time + product_id + user_id
					String idStr = tabs[0] + tabs[2] + tabs[7];
					
					if(wait>0){
						Thread.sleep(wait);
					}
					if (topics.contains(tabs[1])) {
						producer.send(new ProducerRecord<String,String>(tabs[1], 0, timeData, idStr, line));
					}
				} catch(ParseException | NumberFormatException pe){
					System.err.println("Cannot parse date "+tabs[0]);
				}
				
				if (Thread.interrupted()) {
				    throw new InterruptedException();
				}
			}
		} catch(IOException ioe) {
			System.err.println(ioe.getMessage());
		} catch(InterruptedException ie) {
			System.err.println("Interrupted "+ie.getMessage());
		}
		
		System.err.println("Finished! Messages were "+wait+" ms from target speed-up times.");
	}
	
	private long calculateWait(long time) {
		long current = System.currentTimeMillis();
		
		// how long we have waited since start
		long delaySim = current - startSim;
		if(delaySim<0) {
			// the first element ...
			// wait until startSim
			return delaySim*-1;
		}
		
		// calculate how long we should wait since start
		long delayData = time - startData;
		long shouldDelay = delayData / speedup;
		
		// if we've already waited long enough
		if(delaySim>=shouldDelay) return 0;
		// otherwise return wait time
		else return shouldDelay - delaySim;
	}

	// example 2019-11-01 10:22:04 UTC
	public long getUnixTime(String dateTime) throws ParseException{
		Date d = ECOMMERCE_DATE.parse(dateTime);
		return d.getTime();
	}
}
