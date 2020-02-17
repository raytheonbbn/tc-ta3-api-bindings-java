package com.bbn.tc.services.kafka.checker;

import org.apache.log4j.Logger;

import com.bbn.tc.schema.avro.cdm20.NetFlowObject;
import com.bbn.tc.schema.avro.cdm20.TCCDMDatum;

public class NetFlowRecordChecker extends CDMChecker {
    private static final Logger logger = Logger.getLogger(NetFlowRecordChecker.class);
    
    int netFlowsFound = 0;
    int validNetFlowsFound = 0;
    
    public NetFlowRecordChecker() {
        netFlowsFound = 0;
        validNetFlowsFound = 0;
    }
    
    @Override
    public String description() {
        return "Valitate NetFlow records";
    }

    @Override
    public void processRecord(String key, TCCDMDatum datum) throws Exception {  
        Object record = datum.getDatum();
        if (record instanceof NetFlowObject) {
            netFlowsFound++;
            NetFlowObject nfo = (NetFlowObject)record;
            boolean valid = true;

            CharSequence localAddr = nfo.getLocalAddress();
            if (localAddr.length() <= 1) {
                logger.warn("NetFlow localAddress length < 1: "+localAddr);
                valid = false;
            }
            Integer localPort = nfo.getLocalPort();
            if (localPort == null) {
                logger.warn("No local port");
                valid = false;
            } else if (localPort < 0 || localPort > 65535) {
                logger.warn("Invalid local port number: "+localPort);
                valid = false;
            } 
            
            CharSequence remoteAddr = nfo.getRemoteAddress();
            if (remoteAddr.length() <= 1) {
                logger.warn("NetFlow remoteAddress length < 1: "+remoteAddr);
                valid = false;
            }
            Integer remotePort = nfo.getRemotePort();
            if (remotePort == null) {
                logger.warn("No remote port");
                valid = false;
            } else if (remotePort < 0 || remotePort > 65535) {
                logger.warn("Invalid local port number: "+remotePort);
                valid = false;
            } 
            
            if (valid) {
                validNetFlowsFound++;
            } else {
		logger.debug("Record: "+jsonSerializer.serializeToJson(datum, true));
	    }
      
        }
        
    }
    
    @Override
    public void close() {
        if (netFlowsFound == 0) {
            logger.warn("NO NetFlow records were found!");
        } else {
            logger.info(netFlowsFound+" NetFlow records were found");
        }
        
        if (validNetFlowsFound == netFlowsFound) {
            logger.info("ALL NetFlow records found were valid");
        } else if (validNetFlowsFound > 0) {
            logger.warn(validNetFlowsFound+" out of "+netFlowsFound+" NetFlow records had fully valid parameters");
        } else {
            logger.warn("NO NetFlow records had full valid parameters!");
        }
    }
}
