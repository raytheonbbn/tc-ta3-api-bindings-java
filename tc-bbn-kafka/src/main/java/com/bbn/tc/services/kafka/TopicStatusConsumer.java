/*
 * Copyright (c) 2020 Raytheon BBN Technologies Corp.
 * See LICENSE.txt for details.
 */

package com.bbn.tc.services.kafka;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

/**
 * Simple consumer that consumes the last CDM record on each topic and extracts the timestamp
 */

public class TopicStatusConsumer extends NewCDMConsumer {
	
    // Move shared stuff to NewCDMConsumer
    
	private static final Logger logger = Logger.getLogger(TopicStatusConsumer.class);
       
    protected static int period_sec = 300; // 5 minutes
    protected static String outputFile = "/tmp/TopicStatus.txt";
    SimpleDateFormat sdf = new SimpleDateFormat("dd-M-yyyy hh:mm:ss");
       
    public TopicStatusConsumer(String kafkaServer, String groupId,
			String topics, int duration, String consumerSchemaFilename,
			String producerSchemaFilename, int period) {
    	
    	super(kafkaServer, groupId, topics, duration, false, true,
    			consumerSchemaFilename, producerSchemaFilename, "latest", "-1", false, -1, -1, null);
    	TopicStatusConsumer.defaultDuration = duration;
    	TopicStatusConsumer.period_sec = period;
    }
    
    public TopicStatusConsumer() {
        this(kafkaServer, consumerGroupID, topicStr, defaultDuration,
             consumerSchemaFilename, producerSchemaFilename, period_sec);
    }
    
    @Override
    public void run() {
    	logger.info("Started TopicStatusConsumer");

    	ConsumerRecords<String, GenericContainer> records = null;

    	long initDuration = duration;
    	long endTime = (duration <= 0) ? Long.MAX_VALUE : System.currentTimeMillis() + initDuration * 1000;

    	logger.info("Stopping at "+endTime);
    	String lineSep = System.getProperty("line.separator");

    	// long startTime = System.currentTimeMillis();

    	Map<TopicPartition, Long> lastOffset = new HashMap<TopicPartition, Long>();
    	Map<TopicPartition, Long> lastTime = new HashMap<TopicPartition, Long>();

    	try{
    		while (!shutdown.get() && System.currentTimeMillis() <= endTime) {

    			// Set all offsets to the latest
    			Set<TopicPartition> consumer_assignment = consumer.assignment();
    			if (logger.isDebugEnabled()) {
    				for (TopicPartition tPart : consumer_assignment) {
    					logger.debug("Assigned topic: "+tPart);
    				}
    			}

    			long nowTime = System.currentTimeMillis();
    			String dateStr = sdf.format(new Date(nowTime));

                consumer.seekToEnd(consumer_assignment);
    			for (TopicPartition tPart : consumer_assignment) {
    				long partitionOffset = consumer.position(tPart);
    				logger.debug("Last offset for "+tPart+" is "+partitionOffset);
    				Long prevOffset = lastOffset.get(tPart);
    				
					if (prevOffset != null && prevOffset.longValue() == partitionOffset) {
						// Same record
						logger.debug("Same record as previously: "+prevOffset);
					} else if (partitionOffset > 0) {
						lastOffset.put(tPart, partitionOffset);
						lastTime.put(tPart, nowTime);
					}
					
    				if (partitionOffset > 0) {
    					partitionOffset = partitionOffset - 1;
    					consumer.seek(tPart, partitionOffset);
    					logger.debug("Setting offset for "+tPart+" to "+partitionOffset);
    				}
    			}

    			records = consumer.poll(500);

    			if (records.count() == 0) {
    				logger.warn("No records available on any topic!"); 
    			} else {
    				// Don't really need to consume here since we have the offset already
    				//  leaving this in incase we want to do something more later
    				for (ConsumerRecord<String, GenericContainer> record : records) {

    					String topic = record.topic();
    					int partition = record.partition();

    					TopicPartition tPart = new TopicPartition(topic, partition);
    					logger.debug(tPart+": Offset: "+record.offset()+" Time: "+dateStr);
    				}
    			}

    			File oFile = new File(outputFile);
    			BufferedWriter bwrite = null;
    			try {
    				bwrite = new BufferedWriter(new FileWriter(oFile));
    			} catch (IOException ex) {
    				ex.printStackTrace();
    				bwrite = null;
    			}

    			logger.info("Topic Status for "+dateStr);
    			for (TopicPartition tPart : consumer_assignment) {
    				Long offset = lastOffset.get(tPart);
    				Long time = lastTime.get(tPart);
    				String timeStr = "NULL";
    				if (time != null) {
    					timeStr = sdf.format(new Date(time));
    				}
    				StringBuffer sb = new StringBuffer();
    				sb.append(tPart).append(" offset: ").append(offset).append(" time: ");
    				sb.append(timeStr).append(" status: ");
    				if (offset == null || time == null) {
    					sb.append("NO RECORDS");
    				} else if (time != nowTime) {
    					sb.append("OLD");
    				} else {
    					sb.append("LIVE");
    				}

    				logger.info(sb.toString());

    				if (bwrite != null) {
    					sb.append(lineSep);
    					bwrite.write(sb.toString());
    				}
    			}    	
    			if (bwrite != null) {
    				bwrite.close();
    			}
    			logger.debug("Sleeping for "+period_sec+" seconds");
    			Thread.sleep(period_sec * 1000);    			
    		}

    		logger.info("Duration "+duration+" (ms) elapsed. Exiting");
    		closeConsumer();
    		logger.info("Done.");

    	} catch(Exception e){
    		logger.error("Error while consuming", e);
    		e.printStackTrace();
    	}
    }
    
    public static boolean parseAdditionalArgs(String[] args) {
        int index = 0;
        while(index < args.length){
            String option = args[index].trim();
            if(option.equals("-p")){
                index++;
                String periodStr = args[index];
                try {
                    period_sec = Integer.parseInt(periodStr);
                } catch (NumberFormatException ex) {
                    System.err.println("Bad period parameter, expecting an int (seconds)");
                    return false;
                } 
            } else if(option.equals("-of")){
                index++;
                outputFile = args[index];
            }
            index++;
        } 
        return true;
    }
    
    public static String usage() {
        StringBuffer sb = new StringBuffer(NewCDMConsumer.usage());
        sb.append("     -p    period (seconds), how often to check the last published record \n");
        sb.append("     -of   String filename, output file to write to\n");
        return sb.toString();
    }

    public static void main(String [] args){
        consumerGroupID = "TopicStatusConsumer";
    	if(!NewCDMConsumer.parseArgs(args, false)) {
    		logger.error(usage());
    		System.exit(1);
    	}
    	
    	parseAdditionalArgs(args);

    	TopicStatusConsumer tconsumer = new TopicStatusConsumer();
    	//start the consumer
    	tconsumer.start();
    }
    
    public String getConfig() {  
        char separator = '=';
        char fseparator = ',';
        StringBuffer cfg = new StringBuffer(super.getConfig());
        cfg.append("period").append(separator).append(period_sec).append(fseparator);
        cfg.append("outputFile").append(separator).append(outputFile).append(fseparator);
        return cfg.toString();
    }




}
