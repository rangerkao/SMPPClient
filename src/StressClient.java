
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.sql.*;
import java.util.*;
import java.text.*;

import org.apache.log4j.PropertyConfigurator;
import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.BindType;
import org.jsmpp.bean.GeneralDataCoding;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.bean.SMSCDeliveryReceipt;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.bean.OptionalParameter;
import org.jsmpp.bean.OptionalParameter.Tag;
import org.jsmpp.util.AbsoluteTimeFormatter;
import org.jsmpp.util.TimeFormatter;
import org.jsmpp.session.QuerySmResult;
import org.jsmpp.bean.MessageState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jsmpp.bean.MessageClass;
import org.jsmpp.bean.Alphabet;

public class StressClient implements Runnable {
    private static final String DEFAULT_PASSWORD = "test17";
    private static final String DEFAULT_SYSID = "17life";
    private static final Logger logger = LoggerFactory.getLogger(StressClient.class);
    private static final String DEFAULT_LOG4J_PATH = "client-log4j.properties";
    private static final String DEFAULT_HOST = "10.42.1.163";
    private static final Integer DEFAULT_PORT = 2775;
    private static final Long DEFAULT_TRANSACTIONTIMER = 2000L;
    private static final Integer DEFAULT_BULK_SIZE = 100000;
    private static final Integer DEFAULT_PROCESSOR_DEGREE = 3;
    private static final Integer DEFAULT_MAX_OUTSTANDING = 10;
    
    private static Integer THREADCOUNT=100;
    private AtomicInteger requestCounter = new AtomicInteger();
    private AtomicInteger totalRequestCounter = new AtomicInteger();
    private AtomicInteger responseCounter = new AtomicInteger();
    private AtomicInteger totalResponseCounter = new AtomicInteger();
    private AtomicLong maxDelay = new AtomicLong();
    private ExecutorService execService;
    private String host;
    private int port;
    private int bulkSize;
    private SMPPSession smppSession = new SMPPSession();
    private AtomicBoolean exit = new AtomicBoolean();
    private int id;
    private static String systemId;
    private static String password;
    private static String sourceAddr;
    private static String destinationAddr;
		private static String FailLimit;
		private static String RetryLimit;
		private static String SendPeriod,AlertMailTo;
		private static int iSendPeriod;
		private static int iFailLimit;
    private static TimeFormatter timeFormatter = new AbsoluteTimeFormatter();
    
    private static  int reconnectLimit = 20;
    private static  int reconnectCount = 1;
    
		private static byte curSeq=0;
		class msgStatus{
			public String sndFrom="";
			public String sndTo="";
			public String MsgBody="";
			public String MsgID="";
			public int MsgSeq=0;
			public String rspID="";
			public int status;
			public int tries;
			public int fails=0;
			public Date sendDate=null;
			public longmsgStatus[] rspIDs;
		}
		class longmsgStatus{
			public String MsgID="";
			public int MsgSeq=0;
			public int part;
			public String rspID="";
			public int status;
			public boolean inserted=false;
		}
		
		private ArrayList<msgStatus> qryMsg= new ArrayList<msgStatus>();
		private ArrayList<msgStatus> sndMsg= new ArrayList<msgStatus>();
		
    public StressClient(int id, String host, int port, int bulkSize,
            String systemId, String password, String sourceAddr,
            String destinationAddr, long transactionTimer,
            int pduProcessorDegree, int maxOutstanding) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.bulkSize = bulkSize;
        this.systemId = systemId;
        this.password = password;
        this.sourceAddr = sourceAddr;
        this.destinationAddr = destinationAddr;
        smppSession.setPduProcessorDegree(pduProcessorDegree);
        smppSession.setTransactionTimer(transactionTimer);
        execService = Executors.newFixedThreadPool(maxOutstanding);
    }
		synchronized byte getLongSeq(){
			curSeq++;
			return curSeq;
		}
    private void shutdown() {
        execService.shutdown();
        exit.set(true);
    }
    
    public void run() {
        try {
            smppSession.connectAndBind(host, port, BindType.BIND_TRX, systemId,
                    password, "cln", TypeOfNumber.UNKNOWN,
                    NumberingPlanIndicator.UNKNOWN, null);
            logger.info("Bound to " + host + ":" + port);
        } catch (IOException e) {
            logger.error("Failed initialize connection or bind", e);
            return;
        }
        //new TrafficWatcherThread().start();
				new QueryResultThread().start();
				
				Connection conn = null;
				PreparedStatement ps=null;
				Statement  csUpdate=null;
				ResultSet rs = null;
				
				//20141030 if THREADCOUNT is less than 10
				int sNum=THREADCOUNT-10;
				if(sNum<0)
					sNum=0;
				
				int zk=0;
				while (!exit.get()) {
					zk++;
					/*if(zk==5)
						smppSession.close();*/
					String str = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
					//20141103 add status 95 (sended result fail)
					String sqlUpdate="update msgitem set status=98 where (schedule<='"+str+"' or schedule='0') and (status=0 or status=99 or status=95) and tries<"+RetryLimit;
					//String sql="select m.userid,i.msgid,seq,phoneno,msgbody,tries from messages m, msgitem i where m.msgid=i.msgid and (schedule<='"+str+"' or schedule='0') and (status=0 or status=99) and tries<"+RetryLimit;
					
					//20141030 added! select with limited number
					
					String sql="select m.userid,i.msgid,seq,phoneno,msgbody,tries from messages m, msgitem i where m.msgid=i.msgid and status=98  limit "+(THREADCOUNT-requestCounter.get());
					System.out.println(sql);
					bulkSize=0;
					try{
						sndMsg.clear();
						conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
						csUpdate = conn.createStatement();
						csUpdate.executeUpdate(sqlUpdate);
						ps = conn.prepareStatement(sql);
						rs = ps.executeQuery();
						while (rs.next()){
							msgStatus mm=new msgStatus();
							mm.sndFrom=rs.getString("userid");
							mm.sndTo=rs.getString("phoneno");
							mm.MsgBody=rs.getString("msgbody");
							mm.MsgID=rs.getString("msgid");
							mm.MsgSeq=rs.getInt("seq");
							mm.rspID="";
							mm.status=97;
							mm.tries=rs.getInt("tries");
							synchronized(sndMsg) {
								sndMsg.add(mm);
							}
						}
					}catch (Exception e){
							e.printStackTrace();
							sendmail(errorAddInfo()+"DB exception:"+"Cannot connect to Database. Exceprion Message:"+e.getMessage());
					}finally{
						try{
							rs.close();
							ps.close();
							csUpdate.close();
							conn.close();
						}catch(Exception e){
						}
					}
					bulkSize=sndMsg.size();
					logger.info("Starting send " + bulkSize + " message...");
					for (int i = 0; i < bulkSize && !exit.get(); i++) {
						msgStatus tmm=null;
						synchronized(sndMsg) {
							tmm=sndMsg.get(i);
						}
						if (tmm!=null){
							execService.execute(newSendTask(tmm));
							synchronized(qryMsg) {
								qryMsg.add(tmm);
							}
						}
						
						while(requestCounter.get()>sNum){
						/*while (requestCounter.get()>=THREADCOUNT){*/
							logger.info("thread count limit reached.....");
							try {
									Thread.sleep(1000);
							} catch (InterruptedException e) {
							}
						}
					}
					logger.info("Starting send " + bulkSize + " message done...");
					try {
							Thread.sleep(iSendPeriod);
					} catch (InterruptedException e) {
					}
        }
        logger.info("Send Thread Finished");
        smppSession.unbindAndClose();
    }
     /*Error Mag Add information of Time,Ip Address,Error Occur Reason*/
    
    static String errorAddInfo(){
    	String ip ="";
    	
    	try {
			ip=InetAddress.getLocalHost()+"";
		} catch (UnknownHostException e) {
			ip="unknow";
			e.printStackTrace();
		}
		return "Time:"+new Date()+",IP:"+ip+"," ;
    }
    
    
		/*
		UCS2 is what you need to do.. thats natively supported by GSM and will render cyrillic variants correctly on russian GSM phones.
Encode in big endian ordering and omit the byte order marker (BOM) at the start... google on your given APIs/libraries of choice in terms of how to convert to BE-encoded UCS2 without BOM.
Also, set data_coding field to UCS2 value.. 0x08 and sm_length to the physical number of octets.. not to the number of unicode characters.
'ABC' in UCS2 is 00 41 00 42 00 43 .. encoded in SMPP as 6 octets, 2 per character
		*/
    int sendLong(byte[] msgBytesO,msgStatus msg) throws Exception{
			int remain=0;
			byte[] udh = new byte[6];
			udh[0]=5; //same for any msg
			udh[1]=0; //same for any msg
			udh[2]=3; //same for any msg
			udh[3]=getLongSeq(); //CSMS reference number, must be same for all the SMS parts in the CSMS
			System.out.println("cut BOM");
			byte[] msgBytes=new byte[msgBytesO.length-2];
			System.arraycopy(msgBytesO, 2, msgBytes , 0, msgBytes.length);
			System.out.println("cut BOM done");
			int parts=msgBytes.length/134;
			if (msgBytes.length%134>0){
				parts++;
				remain=1;
			}
			String [] rspIDs=new String[parts];
			msg.rspIDs=new longmsgStatus[parts];
			udh[4]=(byte)parts; //total number of parts
			

			//Need to set UDHI
			ESMClass esm = new ESMClass(64);

			//first part
			byte[] msgPart = new byte[140];
			int i;
			org.jsmpp.bean.TypeOfNumber ton=TypeOfNumber.ALPHANUMERIC;
			try {
					long c = Long.parseLong(msg.sndFrom);
					ton = TypeOfNumber.INTERNATIONAL;
			}
			catch(NumberFormatException e) {
					System.out.println("not send from number:"+msg.sndFrom+":");
			}
			for (i=0;i<parts-remain;i++){
				udh[5]=(byte)(i+1); //this part's number in the sequence
				System.arraycopy(udh, 0, msgPart , 0, 6);
				System.arraycopy(msgBytes, i*134, msgPart , 6, 134);
				System.out.println("sending parts:"+msgPart[0]+","+msgPart[1]+","+msgPart[2]+","+msgPart[3]+","+msgPart[4]+","+msgPart[5]);
				//Call session.submitShortMessage() method
				//passing msgPart array for shortMessage 
				//argument and esm for ESMClass argument
				rspIDs[i]=smppSession.submitShortMessage("", ton , NumberingPlanIndicator.UNKNOWN, msg.sndFrom, TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, msg.sndTo, esm           , (byte)0, (byte)0,  "", null, new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT), (byte)0, new GeneralDataCoding(8), (byte)0, msgPart);
				System.out.println("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i]);
				logger.info("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i] );
				
				//20141106
				longmsgStatus l =new longmsgStatus();
				l.MsgID=msg.MsgID;
				l.MsgSeq=msg.MsgSeq;
				l.part=i+1;
				l.status=97;
				l.rspID=rspIDs[i];
				msg.rspIDs[i]=l;
				
			}
			if (remain==1){
				udh[5]=(byte)(i+1);
				byte[] msgPartRemain = new byte[(msgBytes.length%134)+6];
				System.arraycopy(udh, 0, msgPartRemain , 0, 6);
				System.arraycopy(msgBytes, i*134, msgPartRemain , 6, (msgBytes.length%134));
				System.out.println("sending parts:"+msgPartRemain[0]+","+msgPartRemain[1]+","+msgPartRemain[2]+","+msgPartRemain[3]+","+msgPartRemain[4]+","+msgPartRemain[5]);
				rspIDs[i]=smppSession.submitShortMessage("", ton , NumberingPlanIndicator.UNKNOWN, msg.sndFrom, TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, msg.sndTo, esm, (byte)0, (byte)0,  "", null, new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT), (byte)0, new GeneralDataCoding(8), (byte)0, msgPartRemain);
				System.out.println("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i]);
				logger.info("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i] );
				
				//20141106
				longmsgStatus l =new longmsgStatus();
				l.MsgID=msg.MsgID;
				l.MsgSeq=msg.MsgSeq;
				l.part=i+1;
				l.status=97;
				l.rspID=rspIDs[i];
				msg.rspIDs[i]=l;
				
			}
			//Thread.sleep(2000);
			//for (int j=0;j<i;j++){
			//	System.out.println("querying :"+j+",id:"+rspIDs[j]);
			//	QuerySmResult q4=smppSession.queryShortMessage(rspIDs[j],TypeOfNumber.ALPHANUMERIC , NumberingPlanIndicator.UNKNOWN, msg.sndFrom);
			//	if (q4.getMessageState()==MessageState.DELIVERED){
			//		return 2;
			//	}
			//}
			
			//20141106
			//return 2;
			return 97;
		}
    int sendAsciiLong(byte[] msgBytes,msgStatus msg) throws Exception{
			int remain=0;
			byte[] udh = new byte[6];
			udh[0]=5; //same for any msg
			udh[1]=0; //same for any msg
			udh[2]=3; //same for any msg
			udh[3]=getLongSeq(); //CSMS reference number, must be same for all the SMS parts in the CSMS
			int parts=msgBytes.length/153;
			if (msgBytes.length%153>0){
				parts++;
				remain=1;
			}
			String [] rspIDs=new String[parts];
			msg.rspIDs=new longmsgStatus[parts];
			udh[4]=(byte)parts; //total number of parts
			

			//Need to set UDHI
			ESMClass esm = new ESMClass(64);

			//first part
			byte[] msgPart = new byte[160];
			int i;
			org.jsmpp.bean.TypeOfNumber ton=TypeOfNumber.ALPHANUMERIC;
			try {
					long c = Long.parseLong(msg.sndFrom);
					ton = TypeOfNumber.INTERNATIONAL;
			}
			catch(NumberFormatException e) {
					System.out.println("not send from number:"+msg.sndFrom+":"+e.getMessage());
			}
			for (i=0;i<parts-remain;i++){
				udh[5]=(byte)(i+1); //this part's number in the sequence
				System.arraycopy(udh, 0, msgPart , 0, 6);
				System.arraycopy(msgBytes, i*154, msgPart , 6, 154);
				System.out.println("sending parts:"+msgPart[0]+","+msgPart[1]+","+msgPart[2]+","+msgPart[3]+","+msgPart[4]+","+msgPart[5]);
				//Call session.submitShortMessage() method
				//passing msgPart array for shortMessage 
				//argument and esm for ESMClass argument
				rspIDs[i]=smppSession.submitShortMessage("", ton , NumberingPlanIndicator.UNKNOWN, msg.sndFrom, TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, msg.sndTo, esm, (byte)0, (byte)0,  "", null, new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT), (byte)0, new GeneralDataCoding(false, true, MessageClass.CLASS1, Alphabet.ALPHA_DEFAULT), (byte)0, msgPart);
				System.out.println("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i]);
				logger.info("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i] );
				
				//20141106
				longmsgStatus l =new longmsgStatus();
				l.MsgID=msg.MsgID;
				l.MsgSeq=msg.MsgSeq;
				l.part=i+1;
				l.status=97;
				l.rspID=rspIDs[i];
				msg.rspIDs[i]=l;
			}
			if (remain==1){
				udh[5]=(byte)(i+1);
				byte[] msgPartRemain = new byte[(msgBytes.length%154)+6];
				System.arraycopy(udh, 0, msgPartRemain , 0, 6);
				System.arraycopy(msgBytes, i*154, msgPartRemain , 6, (msgBytes.length%154));
				System.out.println("sending parts:"+msgPartRemain[0]+","+msgPartRemain[1]+","+msgPartRemain[2]+","+msgPartRemain[3]+","+msgPartRemain[4]+","+msgPartRemain[5]);
				rspIDs[i]=smppSession.submitShortMessage("", ton , NumberingPlanIndicator.UNKNOWN, msg.sndFrom, TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, msg.sndTo, esm, (byte)0, (byte)0,  "", null, new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT), (byte)0, new GeneralDataCoding(false, true, MessageClass.CLASS1, Alphabet.ALPHA_DEFAULT), (byte)0, msgPartRemain);
				System.out.println("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i]);
				logger.info("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i] );
				
				//20141106
				longmsgStatus l =new longmsgStatus();
				l.MsgID=msg.MsgID;
				l.MsgSeq=msg.MsgSeq;
				l.part=i+1;
				l.status=97;
				l.rspID=rspIDs[i];
				msg.rspIDs[i]=l;
			}
			//Thread.sleep(2000);
			//for (int j=0;j<i;j++){
			//	System.out.println("querying :"+j+",id:"+rspIDs[j]);
			//	QuerySmResult q4=smppSession.queryShortMessage(rspIDs[j],TypeOfNumber.ALPHANUMERIC , NumberingPlanIndicator.UNKNOWN, msg.sndFrom);
			//	if (q4.getMessageState()==MessageState.DELIVERED){
			//		return 2;
			//	}
			//}
			
			//20141106
			//return 2;
			return 97;
		}
		void sendmail(String msg){
			String [] cmd=new String[3];
			cmd[0]="/bin/bash";
			cmd[1]="-c";
			cmd[2]= "/bin/echo \""+msg+"\" | /bin/mail -s \"SMPP System alert\" "+AlertMailTo ;

			try{
				Process p = Runtime.getRuntime().exec (cmd);
				p.waitFor();
				System.out.println("send mail cmd:"+cmd);
			}catch (Exception e){
				System.out.println("send mail fail:"+msg);
			}
		}
		
		void reconnect(String msgID){
			//try connect again
			synchronized(smppSession) {
				reconnectCount=1;
				while(!smppSession.getSessionState().isBound()||smppSession.getSessionState()==SessionState.CLOSED){
					try {
						Thread.sleep(1*1000);
						smppSession =new SMPPSession();
			            smppSession.connectAndBind(host, port, BindType.BIND_TRX, systemId,
			                    password, "cln", TypeOfNumber.UNKNOWN,
			                    NumberingPlanIndicator.UNKNOWN, null);
			            logger.info(reconnectCount+"th Retry Bound to " + host + ":" + port);
			            System.out.println(reconnectCount+"th Retry Bound to " + host + ":" + port);
			        } catch (IOException   e) {
			            logger.error(reconnectCount+"th retry Bound is Failed reinitialize connection or bind", e);
			            System.out.println(reconnectCount+"th retry Bound is Failed reinitialize connection or bind");
			        } catch (InterruptedException e) {
			        	logger.error(reconnectCount+"th retry Bound is Failed reinitialize connection or bind", e);
			            System.out.println(reconnectCount+"th retry Bound is Failed reinitialize connection or bind");
					}
					if(reconnectCount<=reconnectLimit){
						reconnectCount++;
					}else{
						break;
					}
				}
				if(reconnectCount<reconnectLimit){
					logger.debug("smppSession state is Bounded!");
				}else{
					if(msgID!=null){
						logger.error(errorAddInfo()+"The "+msgID+" Msg cannot send!");
						sendmail(errorAddInfo()+"The "+msgID+" Msg cannot send!");
					}
				}
			}
			
		}
		void parseSMPPResponse(final msgStatus msg){
			
		}
		
    private Runnable newSendTask(final msgStatus msg) {
        return new Runnable() {
            public void run() {

				String message = msg.MsgBody;
				String rspMsgID = "";
				org.jsmpp.bean.TypeOfNumber ton = TypeOfNumber.ALPHANUMERIC;
				try {
					long c = Long.parseLong(msg.sndFrom);
					ton = TypeOfNumber.INTERNATIONAL;
				} catch (NumberFormatException e) {
					System.out.println("not send from number:" + msg.sndFrom
							+ ":");
				}
				
					try {
	                	if(msg.MsgBody==null)
	                		throw new Exception("MsgBodyisNull!");	
	                	
						requestCounter.incrementAndGet();
						System.out.println("The "+requestCounter.get()+"th msg Start send! ");
						long startTime = System.currentTimeMillis();
						if( msg.MsgBody.getBytes().length == msg.MsgBody.length()){
							byte [] b=msg.MsgBody.getBytes("iso8859-1");
							if (b.length>160){
								System.out.println("submitted long ASCII:"+msg.sndTo+",length:"+b.length);
								rspMsgID="--";
								msg.rspID=rspMsgID;
								msg.status=sendAsciiLong(b,msg);
							}else{
								rspMsgID=smppSession.submitShortMessage("", ton , NumberingPlanIndicator.UNKNOWN, msg.sndFrom, TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, msg.sndTo, new ESMClass(), (byte)0, (byte)0,  "", null, new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT), (byte)0, new GeneralDataCoding(false, true, MessageClass.CLASS1, Alphabet.ALPHA_DEFAULT), (byte)0, msg.MsgBody.getBytes("iso8859-1"));
								System.out.println("submitted:"+msg.sndTo+",rspid:"+rspMsgID);		
								logger.info("The SM "+msg.MsgID+" has rspIDs "+rspMsgID);
								msg.rspID=rspMsgID;
								msg.status=97;
							}
						}else{
							byte [] b=msg.MsgBody.getBytes("UTF16");
							if (msg.MsgBody.length()>70){
								System.out.println("submitted long UTF16:"+msg.sndTo+",length:"+b.length);
								rspMsgID="--";
								msg.rspID=rspMsgID;
								msg.status=sendLong(b,msg);
							}else{
								byte[] c=new byte[b.length-2];
								System.arraycopy(b, 2, c , 0, c.length);
								rspMsgID=smppSession.submitShortMessage("", ton , NumberingPlanIndicator.UNKNOWN, msg.sndFrom, TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, msg.sndTo, new ESMClass(), (byte)0, (byte)0,  "", null, new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT), (byte)0, new GeneralDataCoding(8), (byte)0, c);
								System.out.println("submitted:"+msg.sndTo+",rspid:"+rspMsgID);
								logger.info("The SM "+msg.MsgID+" has rspIDs "+rspMsgID);
								msg.rspID=rspMsgID;
								msg.status=97;
							}
						}
						long delay = System.currentTimeMillis() - startTime;
						responseCounter.incrementAndGet();
						if (maxDelay.get() < delay) {
								maxDelay.set(delay);
						}
						msg.sendDate=new Date();
						//new code for parsing response
						parseSMPPResponse(msg);
										
	                } catch (PDUException e) {
	                    logger.error("Failed submit short message PDUException '" + message + "'", e);
											sendmail(errorAddInfo()+msg.MsgID+" send to gateway exception:"+"Because incorrect format of PDU passed as a parameter or received from SMSC. Exception Message: "+e.getMessage());
											reconnect(msg.MsgID);
											msg.status=96;
	                    //shutdown();
	                } catch (ResponseTimeoutException e) {
	                    logger.error("Failed submit short message  ResponseTimeoutException'" + message + "'", e);
											sendmail(errorAddInfo()+msg.MsgID+" send to gateway exception:"+"Because response is not received in timeout from SMSC. Exception Message: "+e.getMessage());
											reconnect(msg.MsgID);
											msg.status=96;
	                    //shutdown();
	                } catch (InvalidResponseException e) {
	                    logger.error("Failed submit short message InvalidResponseException'" + message + "'", e);
											sendmail(errorAddInfo()+msg.MsgID+" send to gateway exception:"+"Because receive unexpected response from SMSC. Exception Message: "+e.getMessage());
	                    //shutdown();
	                } catch (NegativeResponseException e) {
	                    logger.error("Failed submit short message NegativeResponseException'" + message + "'", e);
											sendmail(errorAddInfo()+msg.MsgID+" send to gateway exception:"+"Because  receive an negative response from SMSC. Exception Message: "+e.getMessage());
	                    //shutdown();
	                } catch (IOException e) {
	                    logger.error("Failed submit short message IOException '" + message + "'", e);
											sendmail(errorAddInfo()+msg.MsgID+" send to gateway exception:"+"Because IO Problem. Exception Message: "+e.getMessage());
											reconnect(msg.MsgID);
											msg.status=96;
	                    //shutdown();
	                } catch (Exception e){
										logger.error("Failed submit short message Exception'" + message + "'", e);
										sendmail(errorAddInfo()+msg.MsgID+" send to gateway exception:"+e.getMessage());
										reconnect(msg.MsgID);
										msg.status=96;
					}finally{
						System.out.println("The "+requestCounter.get()+"th msg sending end ! ");
							requestCounter.decrementAndGet();
					}
				}
        };
    }
    
    private class TrafficWatcherThread extends Thread {
        @Override
        public void run() {
            logger.info("Starting traffic watcher...");
            while (!exit.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                int requestPerSecond = requestCounter.getAndSet(0);
                int responsePerSecond = responseCounter.getAndSet(0);
                long maxDelayPerSecond = maxDelay.getAndSet(0);
                totalRequestCounter.addAndGet(requestPerSecond);
                int total = totalResponseCounter.addAndGet(responsePerSecond);
                logger.info("Request/Response per second : " + requestPerSecond + "/" + responsePerSecond + " of " + total + " maxDelay=" + maxDelayPerSecond);
								System.out.println("Request/Response per second : " + requestPerSecond + "/" + responsePerSecond + " of " + total + " maxDelay=" + maxDelayPerSecond);
            }
        }
    }

    private class QueryResultThread extends Thread {
        @Override
        public void run() {
            logger.info("Starting QueryResult watcher...");
						Connection conn = null;
						PreparedStatement ps=null;
						String str = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
						String sql="update msgitem set tries=?, status=?,donetime=?,rspid=? where msgid=? and seq=? ";
						String sql2="update longmsgitem set status=?,donetime=?,rspid=? where msgid=? and seq=? and part=? ";
						String sql3="insert into longmsgitem (msgid,seq,part,status,donetime,rspid) Values(?,?,?,?,?,?) ";
						bulkSize=0;
						try{
							conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
							ps = conn.prepareStatement(sql);
						}catch (SQLException e){
							logger.info("QueryResultThread get Connection Exception..."+e.getMessage());
							try{
								ps.close();
								conn.close();
							}catch(SQLException e1){
							}
							return;
						}
			while (!exit.get()) {
				try {
					Thread.sleep(8000);
				} catch (InterruptedException e) {
				}

				for (int j = 0; j < qryMsg.size(); j++) {
					msgStatus id = null;
					synchronized (qryMsg) {
						id = qryMsg.get(j);
					}
					if (id != null) {
						str = new SimpleDateFormat("yyyyMMddHHmmss")
								.format(new java.util.Date());
						if (id.rspID != null && !id.rspID.equals("")&& !id.rspID.equals("--")) {
							// status is not DELIVERED
							if (id.status != 2) {
								try {
									id.fails++;
									org.jsmpp.bean.TypeOfNumber ton = TypeOfNumber.ALPHANUMERIC;
									try {
										long c = Long.parseLong(id.sndFrom);
										ton = TypeOfNumber.INTERNATIONAL;
									} catch (NumberFormatException e) {
										System.out
												.println("not send from number:"
														+ id.sndFrom + ":");
									}
									QuerySmResult q4 = smppSession
											.queryShortMessage(
													id.rspID,
													ton,
													NumberingPlanIndicator.UNKNOWN,
													id.sndFrom);
									id.status = q4.getMessageState().value();
									System.out.println("query:" + id.rspID
											+ ",status:" + id.status);
									if (id.status == 0) {
										id.status = 97;
									}
									// if (q4.getMessageState() ==
									// MessageState.DELIVERED) {
									// 20141103 2 to 9 is final status
									if (id.status == 2 || id.status == 3
											|| id.status == 4 || id.status == 5
											|| id.status == 6 || id.status == 7
											|| id.status == 8 || id.status == 9) {
										str = "20" + q4.getFinalDate();
										str = str
												.substring(0, str.length() - 4);
										System.out
												.println("remove from check list:final date:"
														+ str);
										synchronized (qryMsg) {
											qryMsg.remove(id);
											j--;
										}
										// yyyy MM dd HH mm ss
										// 2014 05 20 14 24 04 532+
										// assume : YYMMDDhhmmss
									}
								} catch (NegativeResponseException e3) {
									if (e3.getMessage().indexOf("67") == -1) {
										logger.info("queryShortMessage "
												+ id.MsgID + "," + id.MsgSeq
												+ " Exception..."
												+ e3.getMessage());
										System.out.println("queryShortMessage "
												+ id.MsgID + "," + id.MsgSeq
												+ " Exception..."
												+ e3.getMessage());
										sendmail("queryShortMessage exception:"
												+ e3.getMessage());
									}
								} catch (Exception e2) {
									logger.info("queryShortMessage " + id.MsgID
											+ "," + id.MsgSeq + " Exception..."
											+ e2.getMessage());
									System.out
											.println("queryShortMessage "
													+ id.MsgID + ","
													+ id.MsgSeq
													+ " Exception..."
													+ e2.getMessage());
									sendmail("queryShortMessage exception:"
											+ e2.getMessage());

									reconnect(null);
								}
							} else {
								synchronized (qryMsg) {
									qryMsg.remove(id);
									j--;
								}
							}
						} else if (id.rspID != null && id.rspID.equals("--")) {
							// 20141106 proccess Long SMS
							System.out.println("checking Long SMS rspids...");
							// only enroute need be checked
							if (id.status != 2) {
								id.fails++;

								org.jsmpp.bean.TypeOfNumber ton = TypeOfNumber.ALPHANUMERIC;
								try {
									long c = Long.parseLong(id.sndFrom);
									ton = TypeOfNumber.INTERNATIONAL;
								} catch (NumberFormatException e) {
									System.out.println("not send from number:"
											+ id.sndFrom + ":");
								}

								boolean drlivery = true;
								boolean rejected = false;
								boolean requeryStatus = false;
								for (longmsgStatus l : id.rspIDs) {
									try {
										QuerySmResult q4 = smppSession.queryShortMessage(l.rspID,ton,NumberingPlanIndicator.UNKNOWN,id.sndFrom);
										l.status = q4.getMessageState().value();
										System.out.println("part=" + l.part+",query:"+ l.rspID + ",status:"+ l.status);
										if (l.status == 0) {
											l.status = 97;
										}
										// 20141103 only check 0,1.
										if (l.status == 2 || l.status == 3 || l.status == 4|| l.status == 5|| l.status == 6 || l.status == 7|| l.status == 8|| l.status == 9) {
											System.out.println("part "+l.part+" is  final status!");
											if (q4.getMessageState() != MessageState.DELIVERED) {
												drlivery = false;
												// Give main status of sms,order by Delivered,Rejected,Undeliverable
												if (q4.getMessageState() == MessageState.REJECTED) {
													rejected = true;
												}
											}

											str = "20" + q4.getFinalDate();
											str = str.substring(0,str.length() - 4);

										} else {
											// contain enroute status
											drlivery = false;
											requeryStatus = true;
											System.out.println("part "+l.part+" is not final status!");
										}
									} catch (NegativeResponseException e3) {
										if (e3.getMessage().indexOf("67") == -1) {
											logger.info("queryShortMessage "+ l.MsgID+ ","+ l.MsgSeq+ ","+ l.part+ " Exception..."+ e3.getMessage());
											System.out.println("queryShortMessage "+ l.MsgID+ ","+ l.MsgSeq+ ","+ l.part+ " Exception..."+ e3.getMessage());
											sendmail("queryShortMessage exception:"
													+ e3.getMessage());
										}
									} catch (Exception e2) {
										logger.info("queryShortMessage "+ l.MsgID + "," + l.MsgSeq+ "," + l.part+ " Exception..."+ e2.getMessage());
										System.out.println("queryShortMessage "+ l.MsgID+ ","+ l.MsgSeq+ ","+ l.part+ " Exception..."+ e2.getMessage());
										sendmail("queryShortMessage exception:"
												+ e2.getMessage());

										reconnect(null);
									}

									// insert or update Data
									try {
										System.out.println("insert or update SMS sm="+ l.MsgID+ ",seq="+ l.MsgSeq+ ",part="+ l.part+ ",status="+ l.status);
										PreparedStatement pst;
										if (l.inserted) {
											pst = conn.prepareStatement(sql2);
											pst.setInt(1, l.status);
											pst.setString(2, str);
											pst.setString(3, l.rspID);
											pst.setString(4, l.MsgID);
											pst.setInt(5, l.MsgSeq);
											pst.setInt(6, l.part);
										} else {
											pst = conn.prepareStatement(sql3);
											pst.setString(1, l.MsgID);
											pst.setInt(2, l.MsgSeq);
											pst.setInt(3, l.part);
											pst.setInt(4, l.status);
											pst.setString(5, str);
											pst.setString(6, l.rspID);
											l.inserted = true;
										}
										pst.executeUpdate();
										pst.close();
									} catch (SQLException e) {
										logger.info("QueryResultThread Exception..."
												+ e.getMessage());
										e.printStackTrace();
										try {
											ps.close();
											conn.close();
										} catch (SQLException e1) {
										}
										try {
											conn = DriverManager
													.getConnection(
															"jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8",
															"smpper", "SmIpp3r");
											ps = conn.prepareStatement(sql);
										} catch (SQLException e1) {
										}
									}
								}

								// need recheck status,set main status 1 enroute
								if (requeryStatus) {
									System.out.println("need recheck!");
									id.status = 1;

								} else {
									System.out.println("need not recheck!");
									synchronized (qryMsg) {
										qryMsg.remove(id);
										j--;
									}
									if (drlivery) {
										id.status = 2;
									} else if (rejected) {
										id.status = 8;
									} else {
										id.status = 5;
									}
								}
							} else {
								synchronized (qryMsg) {
									qryMsg.remove(id);
									j--;
								}
								// when main sms is removed, update part sms
								// status
								for (longmsgStatus l : id.rspIDs) {
									System.out
											.println("insert or update SMS sm="+ l.MsgID + ",seq="+ l.MsgSeq + ",part="+ l.part + ",status="+ l.status);
									PreparedStatement pst;
									try {
										pst = conn.prepareStatement(sql2);
										pst.setInt(1, l.status);
										pst.setString(2, str);
										pst.setString(3, l.rspID);
										pst.setString(4, l.MsgID);
										pst.setInt(5, l.MsgSeq);
										pst.setInt(6, l.part);
										pst.executeUpdate();
										pst.close();
									} catch (SQLException e) {
										logger.info("QueryResultThread Exception..."
												+ e.getMessage());
										e.printStackTrace();
										try {
											ps.close();
											conn.close();
										} catch (SQLException e1) {
										}
										try {
											conn = DriverManager
													.getConnection(
															"jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8",
															"smpper", "SmIpp3r");
											ps = conn.prepareStatement(sql);
										} catch (SQLException e1) {
										}
									}

								}
							}

							/*
							 * id.status = 2; synchronized (qryMsg) {
							 * qryMsg.remove(id); j--; }
							 */
							// TODO

						} else {
							logger.info("No response ID " + id.MsgID + ","+ id.MsgSeq + "...");
							System.out.println("No response ID " + id.MsgID+ "," + id.MsgSeq + "...");
							synchronized (qryMsg) {
								qryMsg.remove(id);
								j--;
							}
						}
						try {
							if (id.rspID != null && !id.rspID.equals("")) {
								if (id.fails < iFailLimit + 1) {
									logger.info("done " + id.MsgID + ","
											+ id.status + "...");
									ps.setInt(1, id.tries);
									ps.setInt(2, id.status);
								} else {
									ps.setInt(1, id.tries + 1);
									ps.setInt(2, 0);
									logger.info("try query fails times "
											+ id.MsgID + "," + id.MsgSeq
											+ "...");
									synchronized (qryMsg) {
										qryMsg.remove(id);
										j--;
									}
								}
								// 20141103 if sms has been sended over two day,
								// no continue trace .
								if (id.sendDate.before(new Date(new Date()
										.getTime() - 1000 * 60 * 60 * 24 * 1))) {
									System.out.println(id.MsgID + ","
											+ id.MsgSeq
											+ " is over one day ...");
									id.status = 96;
									ps.setInt(1, id.tries);
									ps.setInt(2, id.status);
									synchronized (qryMsg) {
										qryMsg.remove(id);
										j--;
									}
								}
							} else {
								ps.setInt(1, id.tries + 1);
								ps.setInt(2, id.status);
							}
							ps.setString(5, id.MsgID);
							ps.setString(3, str);
							ps.setString(4, id.rspID);
							ps.setInt(6, id.MsgSeq);

							System.out.println("update:" + id.MsgID
									+ ",status:" + id.status + " with time:"
									+ str);
							ps.executeUpdate();
						} catch (SQLException e) {
							logger.info("QueryResultThread Exception..."
									+ e.getMessage());
							e.printStackTrace();
							try {
								ps.close();
								conn.close();
							} catch (SQLException e1) {
							}
							try {
								conn = DriverManager
										.getConnection(
												"jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8",
												"smpper", "SmIpp3r");
								ps = conn.prepareStatement(sql);
							} catch (SQLException e1) {
							}
						}
					}
				}
			}
			try {
				ps.close();
				conn.close();
			} catch (SQLException e) {
			}
		}
    }
		
    public static void main(String[] args) throws Exception{
        String host = DEFAULT_HOST;
        String systemId = DEFAULT_SYSID;
        String password = DEFAULT_PASSWORD;
        String sourceAddr = "";
        String destinationAddr = "";
        if (args.length > 0) {
            try {
            	
                THREADCOUNT = Integer.parseInt(args[0]);
                //THREADCOUNT = 3;
                throw new NumberFormatException();
            }
            catch(NumberFormatException e) {
                System.out.println("Must enter integer as first argument.");
            }
				}
        Class.forName("org.postgresql.Driver");
				DriverManager.setLoginTimeout(10);
        int port;
        port = DEFAULT_PORT;
        
        long transactionTimer;
        transactionTimer = DEFAULT_TRANSACTIONTIMER;
        
        int bulkSize;
        bulkSize = DEFAULT_BULK_SIZE;
        
        int processorDegree;
        processorDegree = DEFAULT_PROCESSOR_DEGREE;
        
        int maxOutstanding;
        maxOutstanding = DEFAULT_MAX_OUTSTANDING;
        
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream("StressClient.properties");
        prop.load(fis);
        fis.close();
				RetryLimit=prop.getProperty("RetryLimit");
				FailLimit=prop.getProperty("FailLimit");
				iFailLimit=Integer.parseInt(FailLimit);
				SendPeriod=prop.getProperty("SendPeriod");
				iSendPeriod=Integer.parseInt(SendPeriod);
				AlertMailTo=prop.getProperty("AlertMailTo");
        String log4jPath =DEFAULT_LOG4J_PATH;
        PropertyConfigurator.configure(log4jPath);
        logger.info("Target server {}:{}", host, port);
        logger.info("System ID: {}", systemId);
        logger.info("Password: {}", password);
        logger.info("Transaction timer: {}", transactionTimer);
        logger.info("Bulk size: {}", bulkSize);
        logger.info("Max outstanding: {}", maxOutstanding);
        logger.info("Processor degree: {}", processorDegree);
				logger.info("Fail Limit: {}", FailLimit);
				logger.info("clear Period: {}", iSendPeriod);
        logger.info("AlertMailTo: {}", AlertMailTo);
        StressClient stressClient = new StressClient(0, host, port, bulkSize,
                systemId, password, sourceAddr, destinationAddr,
                transactionTimer, processorDegree, maxOutstanding);
        stressClient.run();
    }
}
