
/**
 * Program History
 * 
 * 20140904 程式接手
 * 20141103 完成斷線重連
 * 20141103 新增select上限限制，維持發送儲列個數
 * 20141103 新增兩天後不查(96逾時)功能
 * 20141103 新增發送失敗(95)狀態，並可重新select進行發送
 * 20141103 修改最終狀態定義，只有0、1為需查詢狀態
 * 20141106 完成長簡訊查詢功能，並記錄於table
 * 20141112 修改7不是最終狀態
 * 20141223 新增10分鐘回覆ACK功能
 * 20150128 將QUERY改為1441次（每分鐘一次）
 * 20150213 將QUERY 改為289次（每五分鐘次）
 * 20150210 新增查詢過去未完成功能
 * 20150311 修改警示簡訊集合後每10分鐘發送
 * 20150316 查詢過去功能，新增97作為需查詢狀態
 * 
 */
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

import org.apache.log4j.Logger;
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
import org.jsmpp.session.QuerySmResult;
import org.jsmpp.bean.MessageClass;
import org.jsmpp.bean.Alphabet;

public class StressClient implements Runnable {
    private static final String DEFAULT_PASSWORD = "test17";
    private static final String DEFAULT_SYSID = "17life";
    private static Logger logger;
    private static final String DEFAULT_LOG4J_PATH = "client-log4j.properties";
    private static  String DEFAULT_HOST = "10.42.1.163";
    private static final Integer DEFAULT_PORT = 2775;
    private static final Long DEFAULT_TRANSACTIONTIMER = 2000L;
    private static final Integer DEFAULT_BULK_SIZE = 100000;
    private static final Integer DEFAULT_PROCESSOR_DEGREE = 3;
    private static final Integer DEFAULT_MAX_OUTSTANDING = 10;
    
    private static Integer THREADCOUNT=100;
    private AtomicInteger requestCounter = new AtomicInteger();
  //not use at 20150820 mark
    /*private AtomicInteger totalRequestCounter = new AtomicInteger();
    private AtomicInteger totalResponseCounter = new AtomicInteger();*/
    private AtomicInteger responseCounter = new AtomicInteger();
    private AtomicLong maxDelay = new AtomicLong();
    private ExecutorService execService;
    private ExecutorService execQueryService;
    private String host;
    @SuppressWarnings("unused")
	private int id;
    private int port;
    private int bulkSize;
    private SMPPSession smppSession = new SMPPSession();
    private AtomicBoolean exit = new AtomicBoolean();
    private static String systemId;
    private static String password;
    private static String sourceAddr;
    private static String destinationAddr;
	private static String Host_IP;
	private static String FailLimit;
	private static String RetryLimit;
	private static String SendPeriod,AlertMailTo,QuertPeriod,QuertPastPeriod;
	private static int iQuertPastPeriod;
	private static int iQuertPeriod;
	private static int iSendPeriod;
	private static int iFailLimit;
	//not use at 20150820 mark
    /*private static TimeFormatter timeFormatter = new AbsoluteTimeFormatter();*/
    
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    
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
			public List<longmsgStatus> rspIDs =new ArrayList<longmsgStatus>();
			public boolean isres=false;
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
		private static ArrayList<String> ErrorLog= new ArrayList<String>();
		
		//20150210
		private ArrayList<msgStatus> qryMsgPast= new ArrayList<msgStatus>();
		
    public StressClient(int id, String host, int port, int bulkSize,
            String systemId, String password, String sourceAddr,
            String destinationAddr, long transactionTimer,
            int pduProcessorDegree, int maxOutstanding) {
    	this.id = id;
        this.host = host;
        this.port = port;
        this.bulkSize = bulkSize;
      //not use at 20150820 mark
        this.systemId = systemId;
        this.password = password;
        this.sourceAddr = sourceAddr;
        this.destinationAddr = destinationAddr;
        smppSession.setPduProcessorDegree(pduProcessorDegree);
        smppSession.setTransactionTimer(transactionTimer);
        execService = Executors.newFixedThreadPool(maxOutstanding);
        execQueryService = Executors.newFixedThreadPool(maxOutstanding);
    }
		synchronized byte getLongSeq(){
			curSeq++;
			return curSeq;
		}
	//not use at 20150820 mark
    /*private void shutdown() {
        execService.shutdown();
        exit.set(true);
    }*/
    //20150316 modify add select status 97
    public void setQueryPast(){
    	logger.info("setQueryPast...");
    	Connection conn = null;
    	PreparedStatement pst = null;
		Statement st=null;
		ResultSet rs = null;
		ResultSet rs2 = null;
		//short SMS
		String sql = 
				"select b.userid,a.phoneno,a.msgbody,a.msgid,a.seq,a.rspID,a.status,a.sendtime "
				+ "from msgitem a,messages b "
				+ "where a.msgid=b.msgid and (status=1 or status=97) and rspid!='--' ";
		
		//long SMS
		String sql2 = 
				"select b.userid,a.phoneno,a.msgbody,a.msgid,a.seq,a.rspID,a.status,a.sendtime "
				+ "from msgitem a,messages b "
				+ "where a.msgid=b.msgid and (status=1 or status=97) and rspid='--' ";
		
		String sql3 =
				"select msgid,seq,part,status,rspid "
				+ "from longmsgitem "
				+ "where msgid=? and seq=?";
		
		try {
			conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
			
			st = conn.createStatement();
			
			rs=st.executeQuery(sql);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			
			while(rs.next()){
				msgStatus mm=new msgStatus();
				mm.sndFrom=rs.getString("userid");
				mm.sndTo=rs.getString("phoneno");
				mm.MsgBody=rs.getString("msgbody");
				mm.MsgID=rs.getString("msgid");
				mm.MsgSeq=rs.getInt("seq");
				mm.rspID=rs.getString("rspID");
				mm.status=rs.getInt("status");
				mm.sendDate=(rs.getString("sendtime")==null ?new Date():sdf.parse(rs.getString("sendtime")));
					
				if(resUserID.contains(mm.sndFrom)){
					mm.isres=true;
				}
				
				synchronized(qryMsgPast) {
					qryMsgPast.add(mm);
				}
			}
			
			rs=st.executeQuery(sql2);
			pst=conn.prepareStatement(sql3);
			while(rs.next()){
				msgStatus mm=new msgStatus();
				mm.sndFrom=rs.getString("userid");
				mm.sndTo=rs.getString("phoneno");
				mm.MsgBody=rs.getString("msgbody");
				mm.MsgID=rs.getString("msgid");
				mm.MsgSeq=rs.getInt("seq");
				mm.rspID=rs.getString("rspID");
				mm.status=rs.getInt("status");
				mm.sendDate=(rs.getString("sendtime")==null ?new Date():sdf.parse(rs.getString("sendtime")));
				logger.info(mm.MsgID+","+mm.MsgSeq+","+mm.status);
				pst.setString(1, mm.MsgID);
				pst.setInt(2, mm.MsgSeq);
				
				rs2 = pst.executeQuery();
				
				
				while(rs2.next()){
					longmsgStatus ls = new longmsgStatus();
					ls.MsgID = mm.MsgID;
					ls.MsgSeq = mm.MsgSeq;
					ls.part = rs2.getInt("part");
					ls.rspID = rs2.getString("rspid");
					ls.status =rs2.getInt("status");
					mm.rspIDs.add(ls);
					
				}
				
				
				if(resUserID.contains(mm.sndFrom)){
					mm.isres=true;
				}
				
				synchronized(qryMsgPast) {
					qryMsgPast.add(mm);
				}
			}
			
			

		} catch (SQLException e) {
			logger.error("setPastqueryError SQLException",e);
			//sendmail(errorAddInfo()+"set Pastquery got SQLException"+"Error Msg"+e.getMessage());
			synchronized(getErrorLog()){
				getErrorLog().add(errorAddInfo()+"set Pastquery got SQLException"+"Error Msg"+e.getMessage());
			}


		} catch (ParseException e) {
			logger.error("setPastqueryError ParseException",e);
			synchronized(getErrorLog()){
				getErrorLog().add(errorAddInfo()+"set Pastquery got ParseException"+"Error Msg"+e.getMessage());
			}
		}finally{
			
			try {
				if(conn!=null){
					conn.close();
				}
				if(st!=null){
					st.close();
				}
				if(pst!=null){
					pst.close();
				}
				if(rs!=null){
					rs.close();
				}
			} catch (SQLException e) {
				logger.error("setPastqueryError at close connection get exception",e);
				synchronized(getErrorLog()){
					getErrorLog().add(errorAddInfo()+"at close connection got error. "+"Error Msg"+e.getMessage());
				}
			}
		}
		
		logger.info("setQueryPast   end...");
		
		
    }
    
    public void run() {
    	
    	//20150820 add listener
    	smppSession.setMessageReceiverListener(new Listener());
    	
        try {
            smppSession.connectAndBind(host, port, BindType.BIND_TRX, systemId,password, "cln", TypeOfNumber.UNKNOWN,NumberingPlanIndicator.UNKNOWN, null);
            logger.info("Bound to " + host + ":" + port);
        } catch (IOException e) {
            logger.error("Failed initialize connection or bind", e);
            return;
        }
        //new TrafficWatcherThread().start();
        Thread Qt=new QueryResultThread();
        
        Qt.start();
        //20150211 add
        Thread Qpt=new QueryPastThread();
        setQueryPast();
        Qpt.start();
        
        //20150312 add
        Thread ELG=new ErrorMailControl();
        ELG.start();
  
        
 
		//new QueryResultThread().start();
		
		Connection conn = null;
		Statement st=null;
		Statement csUpdate=null;
		Statement csGroup=null;
		ResultSet rs = null;
		ResultSet rs2 = null;

		while (!exit.get()) {
			//20141030 if THREADCOUNT is less than 10
			int sNum=THREADCOUNT-10;
			if(sNum<0)
				sNum=0;
			
			//int zk=0;
			
			
			//zk++;
			/*if(zk==5)
				smppSession.close();*/
			//20150518 mod not to ss
			String str = new SimpleDateFormat("yyyyMMddHHmm").format(new java.util.Date());
			//20141103 add status 95 (sended result fail)
			//String sqlUpdate="update msgitem set status=98 where (schedule<='"+str+"' or schedule='0') and (status=0 or status=99 or status=95) and tries<"+RetryLimit;
			//20150129 modified
			String sqlUpdate="update msgitem set status=98 where (schedule<='"+str+"' or schedule='0') and (status=0 or status=99 or status=95) ";
			
			//String sql="select m.userid,i.msgid,seq,phoneno,msgbody,tries from messages m, msgitem i where m.msgid=i.msgid and (schedule<='"+str+"' or schedule='0') and (status=0 or status=99) and tries<"+RetryLimit;
			
			//20141030 added! select with limited number
			//20150318 modified order by tries , send from less tries
			//20150518 mod
			/*String sql="select m.userid,i.msgid,seq,phoneno,msgbody,tries from messages m, msgitem i where m.msgid=i.msgid  and status=98 and tries<"+RetryLimit+" order by tries limit "+(THREADCOUNT-requestCounter.get());*/

			
			String sqlGroup=""
					+ "select c.userid,c.schedule,count(1) from ( select a.msgid,a.seq,b.userid,(case when a.schedule='0' then 0 else 1 end ) schedule  "
					+ "from msgitem a,messages b  where a.msgid=b.msgid and a.status=98 and tries<"+RetryLimit+" ) c group by c.userid,c.schedule order by c.userid,c.schedule ";

			
			List<Map<String,String>> grouplist = new ArrayList<Map<String,String>>();
			
			
			try {
				conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
				
				grouplist.clear();
				sndMsg.clear();
				bulkSize=0;
				
				//update needed to 98
				csUpdate = conn.createStatement();
				csUpdate.executeUpdate(sqlUpdate);
				//logger.info(sqlUpdate);
				//get group
				csGroup = conn.createStatement();
				//logger.info(sqlGroup);
				rs = csGroup.executeQuery(sqlGroup);
				


				while(rs.next()){
					Map<String,String> map = new HashMap<String,String>();
					String userid = rs.getString("userid");
					String schedule = rs.getString("schedule");
					//logger.info("userid="+userid+",schedule="+schedule);
					map.put("userid", userid);
					map.put("schedule", schedule);
					grouplist.add(map);
				}
				
				//select
				
				for(Map<String,String> map:grouplist){
					String userid = map.get("userid");
					String schedule = map.get("schedule");
					logger.info("userid="+userid+",schedule="+schedule);
					st = conn.createStatement();
					String sql="select m.userid,i.msgid,seq,phoneno,msgbody,tries "
							+ "from messages m, msgitem i "
							+ "where m.msgid=i.msgid  and status=98 and tries<"+RetryLimit+" "
							+ "and m.userid='"+userid+"' and "+("0".equals(schedule)?"i.schedule='0' ":"i.schedule!='0' ")
							+ "order by tries limit "+(THREADCOUNT-requestCounter.get());
					
					//logger.info(sql);
					rs2 = st.executeQuery(sql);
					
					while (rs2.next()){
						msgStatus mm=new msgStatus();
						mm.sndFrom=rs2.getString("userid");
						mm.sndTo=rs2.getString("phoneno");
						mm.MsgBody=rs2.getString("msgbody");
						mm.MsgID=rs2.getString("msgid");
						mm.MsgSeq=rs2.getInt("seq");
						mm.rspID="";
						mm.status=97;
						mm.tries=rs2.getInt("tries");
							
						if(resUserID.contains(mm.sndFrom)){
							mm.isres=true;
						}
						
						synchronized(sndMsg) {
							sndMsg.add(mm);
						}
					}
					
					bulkSize=sndMsg.size();
					
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
							while (requestCounter.get()>=THREADCOUNT){
								logger.info("thread count limit reached.....");
								try {
										Thread.sleep(1000);
								} catch (InterruptedException e) {
								}
							}
						}
					}
					try {
						Thread.sleep(iSendPeriod);
					} catch (InterruptedException e) {
					}
					logger.info("Sended " + bulkSize + " message...");
				}
				if(bulkSize==0){
					try {
						Thread.sleep(iSendPeriod);
					} catch (InterruptedException e) {
					}
					logger.info("Sended " + bulkSize + " message...");
				}
			} catch (SQLException e1) {
				logger.error("At run get exception",e1);
				synchronized(getErrorLog()){
					getErrorLog().add(errorAddInfo()+"At run get exception"+e1.getMessage());
				}
			}finally{
				try{
					if(rs!=null)
						rs.close();
					if(rs2!=null)
						rs2.close();
					if(st!=null)
						st.close();
					if(csUpdate!=null)
						csUpdate.close();
					if(csGroup!=null)
						csGroup.close();
					if(conn!=null)
						conn.close();
				}catch(Exception e){
					logger.error("run at close Connection got Exception..."+e.getMessage());
					//sendmail(errorAddInfo()+"Because QueryResultThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
					synchronized(getErrorLog()){
						getErrorLog().add(errorAddInfo()+"run at close Connection got Exception. "+"Error Msg"+e.getMessage());
					}
				}
			}
			
			/*System.out.println(sql);
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
						
					if(resUserID.contains(mm.sndFrom)){
						mm.isres=true;
					}
					
					synchronized(sndMsg) {
						sndMsg.add(mm);
					}
					
					
				}
			}catch (Exception e){
				logger.error(errorAddInfo()+"DB exception:"+"Cannot connect to Database. Exceprion ",e);

				//sendmail(errorAddInfo()+"DB exception:"+"Cannot connect to Database. Exceprion Message:"+e.getMessage());
				synchronized(ErrorLog){
					ErrorLog.add(errorAddInfo()+"DB exception:"+"Cannot connect to Database. Exceprion Message:"+e.getMessage());
				}
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
				while (requestCounter.get()>=THREADCOUNT){
					logger.info("thread count limit reached.....");
					try {
							Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				}
			}
			//logger.info("Starting send " + bulkSize + " message done...");
			try {
					Thread.sleep(iSendPeriod);
			} catch (InterruptedException e) {
			}*/
        }
        logger.info("Send Thread Finished");
        smppSession.unbindAndClose();
    }
     /*Error Mag Add information of Time,Ip Address,Error Occur Reason*/
    
    static String errorAddInfo(){
    	String ip ="";
    	
    	try {
			ip=InetAddress.getLocalHost().getHostAddress()+"";
		} catch (UnknownHostException e) {
			ip="unknow";
			e.printStackTrace();
		}
		return "Time:"+new Date()+",IP:"+ip+"." ;
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
			udh[4]=(byte)parts; //total number of parts
			

			//Need to set UDHI
			ESMClass esm = new ESMClass(64);

			//first part
			byte[] msgPart = new byte[140];
			int i;
			org.jsmpp.bean.TypeOfNumber ton=TypeOfNumber.ALPHANUMERIC;
			try {
					@SuppressWarnings("unused")
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
				rspIDs[i]=submitSMS(ton,msg.sndFrom, msg.sndTo, esm,new GeneralDataCoding(8),msgPart);
				System.out.println("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i]);
				logger.info("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i] );
				
				//20141106
				longmsgStatus l =new longmsgStatus();
				l.MsgID=msg.MsgID;
				l.MsgSeq=msg.MsgSeq;
				l.part=i+1;
				l.status=97;
				l.rspID=rspIDs[i];
				msg.rspIDs.add(l);				
			}
			if (remain==1){
				udh[5]=(byte)(i+1);
				byte[] msgPartRemain = new byte[(msgBytes.length%134)+6];
				System.arraycopy(udh, 0, msgPartRemain , 0, 6);
				System.arraycopy(msgBytes, i*134, msgPartRemain , 6, (msgBytes.length%134));
				System.out.println("sending parts:"+msgPartRemain[0]+","+msgPartRemain[1]+","+msgPartRemain[2]+","+msgPartRemain[3]+","+msgPartRemain[4]+","+msgPartRemain[5]);
				rspIDs[i]=submitSMS(ton,msg.sndFrom,msg.sndTo, esm,new GeneralDataCoding(8),msgPartRemain);
				System.out.println("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i]);
				logger.info("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i] );
				
				//20141106
				longmsgStatus l =new longmsgStatus();
				l.MsgID=msg.MsgID;
				l.MsgSeq=msg.MsgSeq;
				l.part=i+1;
				l.status=97;
				l.rspID=rspIDs[i];
				msg.rspIDs.add(l);
				
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
			udh[4]=(byte)parts; //total number of parts
			

			//Need to set UDHI
			ESMClass esm = new ESMClass(64);

			//first part
			byte[] msgPart = new byte[160];
			int i;
			org.jsmpp.bean.TypeOfNumber ton=TypeOfNumber.ALPHANUMERIC;
			try {
					@SuppressWarnings("unused")
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
				rspIDs[i]=submitSMS(ton,msg.sndFrom,msg.sndTo, esm,new GeneralDataCoding(false, true, MessageClass.CLASS1, Alphabet.ALPHA_DEFAULT),msgPart);
				System.out.println("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i]);
				logger.info("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i] );
				
				//20141106
				longmsgStatus l =new longmsgStatus();
				l.MsgID=msg.MsgID;
				l.MsgSeq=msg.MsgSeq;
				l.part=i+1;
				l.status=97;
				l.rspID=rspIDs[i];
				msg.rspIDs.add(l);
			}
			if (remain==1){
				udh[5]=(byte)(i+1);
				byte[] msgPartRemain = new byte[(msgBytes.length%154)+6];
				System.arraycopy(udh, 0, msgPartRemain , 0, 6);
				System.arraycopy(msgBytes, i*154, msgPartRemain , 6, (msgBytes.length%154));
				System.out.println("sending parts:"+msgPartRemain[0]+","+msgPartRemain[1]+","+msgPartRemain[2]+","+msgPartRemain[3]+","+msgPartRemain[4]+","+msgPartRemain[5]);
				rspIDs[i]=submitSMS(ton,msg.sndFrom,msg.sndTo,esm,new GeneralDataCoding(false, true, MessageClass.CLASS1, Alphabet.ALPHA_DEFAULT),msgPartRemain);
				System.out.println("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i]);
				logger.info("The SM "+msg.MsgID+" "+(i+1)+"th part has rspIDs "+rspIDs[i] );
				
				//20141106
				longmsgStatus l =new longmsgStatus();
				l.MsgID=msg.MsgID;
				l.MsgSeq=msg.MsgSeq;
				l.part=i+1;
				l.status=97;
				l.rspID=rspIDs[i];
				msg.rspIDs.add(l);
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
    
    	String submitSMS(TypeOfNumber sourceAddrTon,String sourceAddr,String destinationAddr,ESMClass esmClass,
    			GeneralDataCoding dataCoding,byte[] shortMessage) throws PDUException, ResponseTimeoutException, InvalidResponseException, NegativeResponseException, IOException{
    											
    		return smppSession.submitShortMessage(
				//	serviceType	, sourceAddrTon	, sourceAddrNpi					, sourceAddr, 
    				""			, sourceAddrTon , NumberingPlanIndicator.UNKNOWN, sourceAddr, 
				//	destAddrTon					, destAddrNpi					, destinationAddr, esmClass, protocolId, 
    				TypeOfNumber.INTERNATIONAL	, NumberingPlanIndicator.UNKNOWN, destinationAddr, esmClass, (byte)0, 
    			//	priorityFlag, scheduleDeliveryTime	, validityPeriod, 
    				(byte)0		,  ""					, null			, 
    			//	registeredDelivery									, replaceIfPresentFlag	, 
    				new RegisteredDelivery(SMSCDeliveryReceipt.SUCCESS_FAILURE)	, (byte)0				, 
    			//	dataCoding, smDefaultMsgId	, shortMessage, optionalParameters)
    				dataCoding,(byte)0			, shortMessage);
    	}
		void sendmail(String msg){
			String ip ="";
			try {
				//ip = InetAddress.getLocalHost().getHostAddress();
				ip=Host_IP;
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			
			msg=msg+" from location "+ip;			
			
			String [] cmd=new String[3];
			cmd[0]="/bin/bash";
			cmd[1]="-c";
			cmd[2]= "/bin/echo \""+msg+"\" | /bin/mail -s \"SMPP System alert\" -r STRESS_CLIENT_ALERT_MAIL "+AlertMailTo+"." ; ;

			try{
				Process p = Runtime.getRuntime().exec (cmd);
				p.waitFor();
				System.out.println("send mail cmd:"+cmd);
			}catch (Exception e){
				System.out.println("send mail fail:"+msg);
			}
		}
		
		void reconnect(String msgID){
			logger.info("reconnect...");
			//try connect again
			synchronized(smppSession) {
				reconnectCount=1;
				while(!smppSession.getSessionState().isBound()||smppSession.getSessionState()==SessionState.CLOSED){
					try {
						smppSession.unbindAndClose();
					} catch (Exception e1) {
						logger.error("unbindAndClose error ", e1);
					}
					
					try {
						Thread.sleep(1*1000);
						smppSession =new SMPPSession();
						smppSession.setMessageReceiverListener(new Listener());
			            smppSession.connectAndBind(host, port, BindType.BIND_TRX, systemId,
			                    password, "cln", TypeOfNumber.UNKNOWN,
			                    NumberingPlanIndicator.UNKNOWN, null);
			            logger.info(reconnectCount+"th Retry Bound to " + host + ":" + port);
			        } catch (IOException   e) {
			            logger.error(reconnectCount+"th retry Bound is Failed reinitialize connection or bind", e);
			        } catch (InterruptedException e) {
			        	logger.error(reconnectCount+"th retry Bound is Failed reinitialize connection or bind", e);
					}
					if(reconnectCount<reconnectLimit){
						reconnectCount++;
					}else{
						break;
					}
				}
				if(reconnectCount<reconnectLimit){
					logger.debug("smppSession state is Bounded!");
				}else{
					if(msgID!=null){
						logger.error(errorAddInfo()+" The "+msgID+" Msg cannot send!");
						//sendmail(errorAddInfo()+" The "+msgID+" Msg cannot send!");
						synchronized(getErrorLog()){
							getErrorLog().add(errorAddInfo()+" The "+msgID+" Msg cannot send!");
						}
					}
				}
			}
			
		}
		void parseSMPPResponse(final msgStatus msg){
			
		}
		
    private Runnable newSendTask(final msgStatus msg) {
        return new Runnable() {
            public void run() {
            	msg.sendDate=new Date();

				//String message = msg.MsgBody;
				String rspMsgID = "";
				org.jsmpp.bean.TypeOfNumber ton = TypeOfNumber.ALPHANUMERIC;
				try {
					@SuppressWarnings("unused")
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
						//System.out.println("The "+requestCounter.get()+"th msg Start send! ");
						long startTime = System.currentTimeMillis();
						if( msg.MsgBody.getBytes().length == msg.MsgBody.length()){
							byte [] b=msg.MsgBody.getBytes("iso8859-1");
							if (b.length>160){
								
								sendAsciiLong(b,msg);
								logger.info("submitted long ASCII:"+msg.sndTo+",length:"+b.length+",seq:"+msg.MsgSeq);
								rspMsgID="--";
								msg.rspID=rspMsgID;
								msg.status=97;
										
							}else{
								rspMsgID=submitSMS(ton,msg.sndFrom,msg.sndTo,new ESMClass(),new GeneralDataCoding(false, true, MessageClass.CLASS1, Alphabet.ALPHA_DEFAULT),msg.MsgBody.getBytes("iso8859-1"));
								logger.info("submitted:"+msg.sndTo+",rspid:"+rspMsgID+",seq:"+msg.MsgSeq);		
								msg.rspID=rspMsgID;
								msg.status=97;
							}
						}else{
							byte [] b=msg.MsgBody.getBytes("UTF16");
							if (msg.MsgBody.length()>70){
								sendLong(b,msg);
								logger.info("submitted long UTF16:"+msg.sndTo+",length:"+b.length+",seq:"+msg.MsgSeq);
								rspMsgID="--";
								msg.rspID=rspMsgID;
								msg.status=97;	
								
							}else{
								byte[] c=new byte[b.length-2];
								System.arraycopy(b, 2, c , 0, c.length);
								rspMsgID=submitSMS(ton,msg.sndFrom,msg.sndTo,new ESMClass(),new GeneralDataCoding(8),c);
								logger.info("submitted:"+msg.sndTo+",rspid:"+rspMsgID+",seq:"+msg.MsgSeq);
								msg.rspID=rspMsgID;
								msg.status=97;
							}
						}
						long delay = System.currentTimeMillis() - startTime;
						responseCounter.incrementAndGet();
						if (maxDelay.get() < delay) {
								maxDelay.set(delay);
						}
						
						
				
	                } catch (PDUException e) {
	                    logger.error("Caused by PDUException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+")", e);
	                    //sendmail(errorAddInfo()+"Caused by PDUException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
	                    synchronized(getErrorLog()){
							getErrorLog().add(errorAddInfo()+"Caused by PDUException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
						}					
	                    reconnect(msg.MsgID);
	                    msg.status=95;
	                    //shutdown();
	                } catch (ResponseTimeoutException e) {
	                	logger.error("Caused by ResponseTimeoutException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+")", e);
	                    //sendmail(errorAddInfo()+"Caused by ResponseTimeoutException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
	                    synchronized(getErrorLog()){
							getErrorLog().add(errorAddInfo()+"Caused by ResponseTimeoutException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
	                    }							
	                    reconnect(msg.MsgID);
						msg.status=95;
	                    //shutdown();
	                } catch (InvalidResponseException e) {
	                	logger.error("Caused by InvalidResponseException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+")", e);
	                    //sendmail(errorAddInfo()+"Caused by InvalidResponseException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
	                    synchronized(getErrorLog()){
							getErrorLog().add(errorAddInfo()+"Caused by InvalidResponseException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
						}							
	                    msg.status=95;			
						//shutdown();
	                } catch (NegativeResponseException e) {
	                	logger.error("Caused by NegativeResponseException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+")", e);
	                    //sendmail(errorAddInfo()+"Caused by NegativeResponseException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
	                    synchronized(getErrorLog()){
							getErrorLog().add(errorAddInfo()+"Caused by NegativeResponseException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
						}							
	                    msg.status=95;
	                    
	                    if(e.getMessage().toLowerCase().indexOf("0b") != -1){
	                    	msg.status=94;
	                    }
	                    
						//shutdown();
	                } catch (IOException e) {
	                	logger.error("Caused by IOException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+")", e);
	                    //sendmail(errorAddInfo()+"Caused by IOException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
	                    synchronized(getErrorLog()){
							getErrorLog().add(errorAddInfo()+"Caused by IOException , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
						}							
	                    reconnect(msg.MsgID);
						msg.status=95;
	                    //shutdown();
	                } catch (Exception e){
	                	logger.error("Caused by Exception , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+")", e);
	                    //sendmail(errorAddInfo()+"Caused by Exception , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
	                    synchronized(getErrorLog()){
							getErrorLog().add(errorAddInfo()+"Caused by Exception , Failed submit short message(User:"+msg.sndFrom+",msgid:"+msg.MsgID+",Seq:"+msg.MsgSeq+",number:"+msg.sndTo+") Exception Message: "+e.getMessage());
						}							
	                    reconnect(msg.MsgID);
						msg.status=95;
					}finally{
						//logger.info("The "+requestCounter.get()+"th msg sending end ! ");
							requestCounter.decrementAndGet();	
					}
					
					if(msg.status==95 ){
						msg.isres=false;
						msg.tries=msg.tries+1;
						synchronized (qryMsg) {
							qryMsg.remove(msg);
						}
					}
					
					
					//20141222 add
					//20141223 add
					Connection conn = null;
					PreparedStatement ps = null;
					PreparedStatement ps2 = null;
					PreparedStatement ps3 = null;
					try {
						
						conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
						
						//add on 20150108
						String sql3="update msgitem set tries=?, status=?,rspid=?,sendtime=? where msgid=? and seq=? ";
						logger.info("update msgid="+msg.MsgID+",seq="+msg.MsgSeq+",status="+msg.status+",sendtime="+msg.sendDate.toString());
						ps3=conn.prepareStatement(sql3);
						ps3.setInt(1, msg.tries);
						ps3.setInt(2, msg.status);
						ps3.setString(3, msg.rspID);
						//20150513 add
						ps3.setString(4, sdf.format(msg.sendDate));
						ps3.setString(5, msg.MsgID);
						ps3.setInt(6, msg.MsgSeq);
						ps3.executeUpdate();			
						
						
						//20150513 add
						if(95!=msg.status){
							//If is long SMS message 
							if("--".equals(msg.rspID)){
								String sql2="insert into longmsgitem (msgid,seq,part,rspid) Values(?,?,?,?) ";
								ps2 = conn.prepareStatement(sql2);
								for(int i=0;i<msg.rspIDs.size();i++){
									longmsgStatus l= msg.rspIDs.get(i);
									ps2 = conn.prepareStatement(sql2);
									ps2.setString(1, l.MsgID);
									ps2.setInt(2, l.MsgSeq);
									ps2.setInt(3, l.part);
									ps2.setString(4, l.rspID);
									ps2.executeUpdate();
								}
							}

							if(msg.isres){
								String sql ="insert into responselog (msgid,phoneno,sendtime,createtime) values(?,?,?,now()) ";
								//20150715 add
								if(94==msg.status)
									sql="insert into responselog (msgid,phoneno,sendtime,createtime,status) values(?,?,?,now(),'F') ";
								
								ps = conn.prepareStatement(sql);
								logger.info("Excute insert new Date to responselog!"+msg.MsgID+","+msg.sndTo);
								ps.setString(1, msg.MsgID);
								ps.setString(2, msg.sndTo);
								ps.setString(3, sdf.format(new Date(msg.sendDate.getTime()+600000)));
								ps.executeUpdate();	
							}
						}
					} catch (SQLException e) {
						logger.error("submitted got SQLException..."+e.getMessage());
						//sendmail(errorAddInfo()+"Because QueryResultThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
						synchronized(getErrorLog()){
							getErrorLog().add(errorAddInfo()+"submitted got SQLException. "+"Error Msg"+e.getMessage());
						}
					}finally{
						try {
							if(conn!=null)conn.close();
							if(ps!=null) ps.close();
							if(ps2!=null)ps2.close();
							if(ps3!=null)ps3.close();
						} catch (SQLException e) {
							logger.error("submitted at close Connection got Exception..."+e.getMessage());
							//sendmail(errorAddInfo()+"Because QueryResultThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
							synchronized(getErrorLog()){
								getErrorLog().add(errorAddInfo()+"submitted at close Connection got Exception. "+"Error Msg"+e.getMessage());
							}
						}
					}

					
				}
        };
    }
    //not used at 20150820 mark
   /* private class TrafficWatcherThread extends Thread {
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
            }
        }
    }*/
  
    
    static Set<Integer> finalStatus = new HashSet<Integer>();

    static void setfinalStatus(){
		finalStatus.add(2);
		finalStatus.add(3);
		finalStatus.add(4);
		finalStatus.add(5);
		finalStatus.add(8);
		finalStatus.add(9);
    }
    
    
    private class QueryResultThread extends Thread {
        @Override
        public void run() {
            logger.info("Starting QueryResult watcher...");
			Connection conn = null;
			PreparedStatement ps=null;
			PreparedStatement ps2=null;
			PreparedStatement pst=null;
			String str = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
			String sql="update msgitem set tries=?, status=?,donetime=?,rspid=? where msgid=? and seq=? ";
			String sql2="update longmsgitem set status=?,donetime=?,rspid=? where msgid=? and seq=? and part=? ";
			
			//20141222 add
			String sql4 = "update responselog set acktime=?,status=?,updatetime=now() where msgid=? and phoneno=? ";
			
			
			bulkSize=0;
			/*try{
				conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
				ps = conn.prepareStatement(sql);
				ps2 = conn.prepareStatement(sql4);
				pst = conn.prepareStatement(sql2);
			}catch (SQLException e){
				logger.error("QueryResultThread get Connection Exception..."+e.getMessage());
				//sendmail(errorAddInfo()+"Because QueryResultThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
				synchronized(getErrorLog()){
					getErrorLog().add(errorAddInfo()+"Because QueryResultThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
				}
				try{
					if(ps!=null)
						ps.close();
					if(ps2!=null)
						ps2.close();
					if(pst!=null)
						pst.close();
					conn.close();
				}catch(SQLException e1){
				}
				return;
			}*/
						
			
			while (!exit.get()) {
				try {
					//20150129 modified 15000 to 60000
					Thread.sleep(iQuertPeriod);
				} catch (InterruptedException e) {
				}
				
				
				
				try {
					conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
					ps = conn.prepareStatement(sql);
					ps2 = conn.prepareStatement(sql4);
					pst = conn.prepareStatement(sql2);
					
					
					logger.info("Query Start...");
					for (int j = 0; j < qryMsg.size(); j++) {

						msgStatus id = null;
						synchronized (qryMsg) {
							id = qryMsg.get(j);
						}
						if (id != null) {
							str = new SimpleDateFormat("yyyyMMddHHmmss")
									.format(new java.util.Date());
							
							//20141223 add process about status 95 send exception fail
							if (id.rspID != null && !id.rspID.equals("")&& !id.rspID.equals("--")) {
								// status is not DELIVERED
								if (id.status != 2) {
									try {
										//search times add
										id.fails++;
										org.jsmpp.bean.TypeOfNumber ton = TypeOfNumber.ALPHANUMERIC;
										try {
											@SuppressWarnings("unused")
											long c = Long.parseLong(id.sndFrom);
											ton = TypeOfNumber.INTERNATIONAL;
										} catch (NumberFormatException e) {
											logger.error("not send from number:" + id.sndFrom + ":");
										}
										//Query sms status from SMPP
										QuerySmResult q4 = smppSession.queryShortMessage( id.rspID,ton,NumberingPlanIndicator.UNKNOWN,id.sndFrom);
										id.status = q4.getMessageState().value();
										//logger.info("query:" + id.rspID + ",status:" + id.status);
										
										//if query result equal to zero,set status to 97(on route)
										if (id.status == 0) {
											id.status = 97;
										}
										// if (q4.getMessageState() ==
										// MessageState.DELIVERED) {
										// 20141103 2 to 9 is final status
										// 20141112 7 is not final status,assume smpp center have not process yet
										if (finalStatus.contains(id.status)) {
											str = "20" + q4.getFinalDate();
											str = str.substring(0, str.length() - 4);
											logger.info("remove from check list:final date:"	+ str);
											
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
						                    logger.error("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+")", e3);
											//sendmail("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e3.getMessage());
											synchronized(getErrorLog()){
												getErrorLog().add("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e3.getMessage());
											}
										}
									} catch (Exception e2) {
					                    logger.error("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+")", e2);
										//sendmail("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:" + e2.getMessage());
										synchronized(getErrorLog()){
											getErrorLog().add("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:" + e2.getMessage());
										}
										reconnect(id.MsgID);
									}
								} else {
									synchronized (qryMsg) {
										qryMsg.remove(id);
										j--;
									}
								}
							} else if (id.rspID != null && id.rspID.equals("--")) {
								// 20141106 proccess Long SMS
								//logger.info("checking Long SMS rspids...");
								// only enroute need be checked
								if (id.status != 2) {
									id.fails++;

									//proccess senfrom
									org.jsmpp.bean.TypeOfNumber ton = TypeOfNumber.ALPHANUMERIC;
									try {
										@SuppressWarnings("unused")
										long c = Long.parseLong(id.sndFrom);
										ton = TypeOfNumber.INTERNATIONAL;
									} catch (NumberFormatException e) {
										logger.error("not send from number:" + id.sndFrom + ":");
									}

									//query status
									boolean reCheck=false;
									boolean haveUndilivered=false;
									boolean reject=false;
									//logger.info("for long SMS rspIDs length is "+id.rspIDs.length);

									for(int i=0;i<id.rspIDs.size();i++){
										longmsgStatus l= id.rspIDs.get(i);
										try {
											if(l.rspID!=null && !"".equalsIgnoreCase(l.rspID)){
												QuerySmResult q4 = smppSession.queryShortMessage(l.rspID,ton,NumberingPlanIndicator.UNKNOWN,id.sndFrom);
												l.status = q4.getMessageState().value();
												//logger.info("part=" + l.part+",query:"+ l.rspID + ",status:"+ l.status);
												
												//20141222 add
												if(finalStatus.contains(l.status)){
													str = "20" + q4.getFinalDate();
													str = str.substring(0, str.length() - 4);
												}

												if (l.status == 0) {
													l.status = 97;
												}
												if(l.status!=2){
													haveUndilivered=true;
												}
												if(l.status==0 || l.status==1 || l.status==7 || l.status==97){
													reCheck=true;
												}
												if(l.status==8){
													reject=true;
												}		
											}else{
												logger.info("No response ID " + l.MsgID + ","+ l.MsgSeq + l.part+"...");
												synchronized (qryMsg) {
													qryMsg.remove(id);
													j--;
												}
											}
											
										} catch (NegativeResponseException e3) {
											if (e3.getMessage().indexOf("67") == -1) {
							                    logger.error("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+")", e3);
												//sendmail("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e3.getMessage());
												synchronized(getErrorLog()){
													getErrorLog().add("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e3.getMessage());
												}
											}
											reCheck=true;
										} catch (Exception e2) {
						                    logger.error("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+")", e2);
											//sendmail("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:" + e2.getMessage());
											synchronized(getErrorLog()){
												getErrorLog().add("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:" + e2.getMessage());
											}
											reconnect(id.MsgID);
											reCheck=true;
										}
									}
										
									

									
									if(reCheck){
										logger.info("need recheck!");
										id.status = 1;
									}else{
										logger.info("need not recheck!");
										synchronized (qryMsg) {
											qryMsg.remove(id);
											j--;
										}
										
										if (!haveUndilivered) {
											id.status = 2;
										} else if (reject) {
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
								}

							} else {
								logger.info("No response ID " + id.MsgID + ","+ id.MsgSeq + "...");
								synchronized (qryMsg) {
									qryMsg.remove(id);
									j--;
								}
							}
							try {
								//if rspip is exists
								if (id.rspID != null && !id.rspID.equals("")) {
									if (id.fails < iFailLimit + 1) {
										logger.info("msgId="+id.MsgID + ",status=" + id.status+ ",msgseq=" + id.MsgSeq + ",fails="+id.fails+"...");
										ps.setInt(1, id.tries);
										ps.setInt(2, id.status);
										//20150312 change when no rspid,remove msg from query at after sending
									} /*else {
										ps.setInt(1, id.tries + 1);
										//20150129 modifed status 0 to 3(overdue)
										ps.setInt(2, 3);
										logger.info("try query fails times " + id.MsgID + "," + id.MsgSeq + "...");
										synchronized (qryMsg) {
											qryMsg.remove(id);
											j--;
										}
									}*/
									// 20141103 if sms has been sended over one day,
									// no continue trace .
									if (!finalStatus.contains(id.status) && id.sendDate!=null && id.sendDate.before(new Date(new Date().getTime() - (1000 * 60 * 60 * 24 * 1 + 1000 * 60 * 1)))) {
										logger.info(id.MsgID + "," + id.MsgSeq + " is over one day ...");
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
									
									logger.error("msgid : "+id.MsgID+" haven't rspid!");
								}
								ps.setString(5, id.MsgID);
								ps.setString(3, str);
								ps.setString(4, id.rspID);
								ps.setInt(6, id.MsgSeq);

								logger.info("update:" + id.MsgID + ",status:" + id.status + " with time:" + str);
								ps.executeUpdate();

								//20141222 add update data to response Log table in ten minute
								if(id.isres && id.sendDate!=null &&new Date().before(new Date(id.sendDate.getTime()+600000))){
									
									ps2.setString(1, str);
									if(id.status==0 || id.status==1 || id.status==97){
										ps2.setString(2, "R");
									}else if(id.status==2){
										ps2.setString(2, "O");
									}else{
										ps2.setString(2, "F");
									}
									
									ps2.setString(3, id.MsgID);
									ps2.setString(4, id.sndTo);
									
									ps2.executeUpdate();
								}
								
								//if id id long SMSmessage
								if(id.rspIDs!=null && id.rspIDs.size()>0){
									for (longmsgStatus l : id.rspIDs) {
										pst.setInt(1, l.status);
										pst.setString(2, str);
										pst.setString(3, l.rspID);
										pst.setString(4, l.MsgID);
										pst.setInt(5, l.MsgSeq);
										pst.setInt(6, l.part);
										pst.executeUpdate();
									}						
								}
		
								
							} catch (SQLException e) {
								logger.error("QueryResultThread got SQLException...",e);
								//sendmail("QueryResultThread got SQLException...<br> Exception message:"+e.getMessage());
								synchronized(getErrorLog()){
									getErrorLog().add("QueryResultThread got SQLException...<br> Exception message:"+e.getMessage());
								}
								try {
									if(ps!=null)
										ps.close();
									if(ps2!=null)
										ps2.close();
									if(pst!=null)
										pst.close();
									if(conn!=null)
										conn.close();
								} catch (SQLException e1) {
									logger.error("QueryResultThread gst close Connection got Exception..."+e.getMessage());
									//sendmail(errorAddInfo()+"Because QueryResultThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
									synchronized(getErrorLog()){
										getErrorLog().add(errorAddInfo()+"QueryResultThread gst close Connection got Exception. "+"Error Msg"+e.getMessage());
									}
								}
								try {
									conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper", "SmIpp3r");
									ps = conn.prepareStatement(sql);
									ps2 = conn.prepareStatement(sql4);
									pst = conn.prepareStatement(sql2);
								} catch (SQLException e1) {
								}
							}
						}
					}
				} catch (SQLException e) {
					logger.error("QueryResultThread get Connection Exception..."+e.getMessage());
					//sendmail(errorAddInfo()+"Because QueryResultThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
					synchronized(getErrorLog()){
						getErrorLog().add(errorAddInfo()+"Because QueryResultThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
					}
				}finally{
					try {
						if(ps!=null)
							ps.close();
						if(ps2!=null)
							ps2.close();
						if(pst!=null)
							pst.close();
						if(conn!=null)
							conn.close();
					} catch (SQLException e) {
						logger.error("QueryResultThread gst close Connection got Exception..."+e.getMessage());
						//sendmail(errorAddInfo()+"Because QueryResultThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
						synchronized(getErrorLog()){
							getErrorLog().add(errorAddInfo()+"QueryResultThread gst close Connection got Exception. "+"Error Msg"+e.getMessage());
						}
					}
				}
			}
		}
    }
		
    static List<String> resUserID = new ArrayList<String>();
    public static void main(String[] args) throws Exception{
        
    	
    	
        String systemId = DEFAULT_SYSID;
        String password = DEFAULT_PASSWORD;
        String sourceAddr = "";
        String destinationAddr = "";
        
        String host = DEFAULT_HOST;
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
        
        String log4jPath =DEFAULT_LOG4J_PATH;
        PropertyConfigurator.configure(log4jPath);
        logger =Logger.getLogger(StressClient.class);
        logger.info("Target server "+ host+" "+port);
        logger.info("System ID: "+ systemId);
        logger.info("Password: "+ password);
        logger.info("Transaction timer: "+ transactionTimer);
        logger.info("Bulk size: "+ bulkSize);
        logger.info("Max outstanding: "+ maxOutstanding);
        logger.info("Processor degree: "+ processorDegree);
		logger.info("Fail Limit: "+ FailLimit);
		logger.info("send Period: "+ iSendPeriod);
		logger.info("query Period: "+ iQuertPeriod);
		logger.info("query Past Period: "+ iQuertPastPeriod);
        logger.info("AlertMailTo: "+ AlertMailTo);
        
        if (args.length > 0) {
            try {
            	
                THREADCOUNT = Integer.parseInt(args[0]);
                //THREADCOUNT = 3;
                throw new NumberFormatException();
            }
            catch(NumberFormatException e) {
                logger.error("Must enter integer as first argument.");
            }
				}
        Class.forName("org.postgresql.Driver");
				DriverManager.setLoginTimeout(10);
 
        
        FileInputStream fis = new FileInputStream("StressClient.properties");
        Properties prop = new Properties();
        prop.load(fis);
        fis.close();
        		Host_IP=prop.getProperty("Host_IP");
				RetryLimit=prop.getProperty("RetryLimit");
				FailLimit=prop.getProperty("FailLimit");
				iFailLimit=Integer.parseInt(FailLimit);
				SendPeriod=prop.getProperty("SendPeriod");
				iSendPeriod=Integer.parseInt(SendPeriod);
				QuertPeriod=prop.getProperty("QueryPeriod");
				iQuertPeriod=Integer.parseInt(QuertPeriod);
				QuertPastPeriod=prop.getProperty("QueryPastPeriod");
				iQuertPastPeriod=Integer.parseInt(QuertPastPeriod);
				AlertMailTo=prop.getProperty("AlertMailTo");
				if(prop.getProperty("DEFAULT_HOST")!=null)
				DEFAULT_HOST=prop.getProperty("DEFAULT_HOST");
				
		

        StressClient stressClient = new StressClient(0, host, port, bulkSize,
                systemId, password, sourceAddr, destinationAddr,
                transactionTimer, processorDegree, maxOutstanding);
        
        //20150129 add
        setfinalStatus();
        
        //20141222 add check need for response msg info
        String sql = "select USERID from smppuser where isres=1";
        Connection conn = null;
        conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper", "SmIpp3r");
		if(conn!=null) logger.info("success");
        Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(sql);
		logger.info("Query need send respose user id!:"+sql);
		
		while(rs.next()){
			resUserID.add(rs.getString("USERID"));
		}
		st.close();
		rs.close();
		conn.close();

		
		
        stressClient.run();
    }
    
    //20150312 add
    private class ErrorMailControl extends Thread {
        @Override
        public void run() {
            logger.info("Starting ErrorMailControl Thread...");

			while (true) {
				try {
					//waiting 10 minute for every round
					Thread.sleep(600000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if(getErrorLog().size()>0){
					String msg = "";
					synchronized(getErrorLog()){
						for(String s:getErrorLog()){
							msg+=s+"\n\n";
						}
						getErrorLog().clear();
					}
					sendmail(msg);
				}
			}
		}
    }
    
    //20150211 add
    private class QueryPastThread extends Thread {
        @Override
        public void run() {
            logger.info("Starting QueryPast watcher..."+qryMsgPast.size());
			
						
			
			while (qryMsgPast.size()>0) {
				try {
					//20150129 modified 15000 to 60000
					Thread.sleep(iQuertPastPeriod);
				} catch (InterruptedException e) {
				}

				logger.info("Query Past Start...");
				for (int j = 0; j < qryMsgPast.size(); j++) {
					msgStatus id =null;
					synchronized (qryMsgPast) {
						id = qryMsgPast.get(j);
					}
					execQueryService.execute(newQueryTask(id));
				}
			}
		}
    }
    
    private Runnable newQueryTask(final msgStatus id) {
        return new Runnable() {
            public void run() {
            	
            	Connection conn = null;
    			PreparedStatement ps=null;
    			PreparedStatement ps2=null;
    			PreparedStatement pst=null;
    			String str = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
    			String sql="update msgitem set tries=?, status=?,donetime=?,rspid=? where msgid=? and seq=? ";
    			String sql2="update longmsgitem set status=?,donetime=?,rspid=? where msgid=? and seq=? and part=? ";
    			
    			//20141222 add
    			String sql4 = "update responselog set acktime=?,status=?,updatetime=now() where msgid=? and phoneno=? ";
    			
    			
    			bulkSize=0;
    			/*try{
    				conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
    				ps = conn.prepareStatement(sql);
    				ps2 = conn.prepareStatement(sql4);
    				pst = conn.prepareStatement(sql2);
    			}catch (SQLException e){
    				logger.error("QueryPastThread get Connection Exception..."+e.getMessage());
    				try{
    					ps.close();
    					ps2.close();
    					pst.close();
    					conn.close();
    				}catch(SQLException e1){
    				}
    				
    				//sendmail(errorAddInfo()+"Because QueryPastThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
    				synchronized(getErrorLog()){
						getErrorLog().add(errorAddInfo()+"Because QueryPastThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
					}
    				return;
    			}*/
    			
    			
    			try{
    				conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
    				ps = conn.prepareStatement(sql);
    				ps2 = conn.prepareStatement(sql4);
    				pst = conn.prepareStatement(sql2);
    				
    				if (id != null) {

    					str = new SimpleDateFormat("yyyyMMddHHmmss")
    							.format(new java.util.Date());
    					
    					//20141223 add process about status 95 send exception fail
    					if (id.rspID != null && !id.rspID.equals("")&& !id.rspID.equals("--")) {
    						// status is not DELIVERED
    						if (id.status != 2) {
    							try {
    								//search times add
    								id.fails++;
    								org.jsmpp.bean.TypeOfNumber ton = TypeOfNumber.ALPHANUMERIC;
    								try {
    									Long.parseLong(id.sndFrom);
    									ton = TypeOfNumber.INTERNATIONAL;
    								} catch (NumberFormatException e) {
    									//logger.error("not send from number:" + id.sndFrom + ":");
    								}
    								//Query sms status from SMPP
    								QuerySmResult q4 = smppSession.queryShortMessage( id.rspID,ton,NumberingPlanIndicator.UNKNOWN,id.sndFrom);
    								id.status = q4.getMessageState().value();
    								//logger.info("past : " + id.rspID + ",status:" + id.status);
    								
    								//if query result equal to zero,set status to 97(on route)
    								if (id.status == 0) {
    									id.status = 97;
    								}
    								// if (q4.getMessageState() ==
    								// MessageState.DELIVERED) {
    								// 20141103 2 to 9 is final status
    								// 20141112 7 is not final status,assume smpp center have not process yet
    								if (finalStatus.contains(id.status)) {
    									str = "20" + q4.getFinalDate();
    									str = str.substring(0, str.length() - 4);
    									logger.info("Past : remove from check list:final date:"	+ str);
    									
    									synchronized (qryMsgPast) {
    										qryMsgPast.remove(id);
    									}
    									// yyyy MM dd HH mm ss
    									// 2014 05 20 14 24 04 532+
    									// assume : YYMMDDhhmmss
    								}
    							} catch (NegativeResponseException e3) {
    								if (e3.getMessage().indexOf("67") == -1) {
    				                    logger.error("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+")", e3);
    									//sendmail("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e3.getMessage());
    									synchronized(getErrorLog()){
    										getErrorLog().add("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e3.getMessage());
    									}
    								}
    							} catch (Exception e2) {
    			                    logger.error("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+")", e2);
    								//sendmail("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e2.getMessage());
    								synchronized(getErrorLog()){
    									getErrorLog().add("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e2.getMessage());
    								}
    								reconnect(id.MsgID);
    							}
    						} else {
    							synchronized (qryMsgPast) {
    								qryMsgPast.remove(id);
    							}
    						}
    					} else if (id.rspID != null && id.rspID.equals("--")) {
    						// 20141106 proccess Long SMS
    						//logger.info("checking Long SMS rspids...");
    						// only enroute need be checked
    						if (id.status != 2) {
    							id.fails++;

    							//proccess senfrom
    							org.jsmpp.bean.TypeOfNumber ton = TypeOfNumber.ALPHANUMERIC;
    							try {
    								Long.parseLong(id.sndFrom);
    								ton = TypeOfNumber.INTERNATIONAL;
    							} catch (NumberFormatException e) {
    								//logger.error("Past : not send from number:" + id.sndFrom + ":");
    							}

    							//query status
    							boolean reCheck=false;
    							boolean haveUndilivered=false;
    							boolean reject=false;
    							//logger.info("for long SMS rspIDs length is "+id.rspIDs.length);

    							for(int i=0;i<id.rspIDs.size();i++){
    								longmsgStatus l= id.rspIDs.get(i);
    								try {
    									if(l.rspID!=null && !"".equalsIgnoreCase(l.rspID)){
    										QuerySmResult q4 = smppSession.queryShortMessage(l.rspID,ton,NumberingPlanIndicator.UNKNOWN,id.sndFrom);
    										l.status = q4.getMessageState().value();
    										//logger.info("past : part=" + l.part+",query:"+ l.rspID + ",status:"+ l.status);
    										
    										//20141222 add
    										if(finalStatus.contains(l.status)){
    											str = "20" + q4.getFinalDate();
    											str = str.substring(0, str.length() - 4);
    										}

    										if (l.status == 0) {
    											l.status = 97;
    										}
    										if(l.status!=2){
    											haveUndilivered=true;
    										}
    										if(l.status==0 || l.status==1 || l.status==7 || l.status==97){
    											reCheck=true;
    										}
    										if(l.status==8){
    											reject=true;
    										}		
    									}else{
    										logger.info("Past : No response ID " + l.MsgID + ","+ l.MsgSeq + l.part+"...");
    										synchronized (qryMsgPast) {
    											qryMsgPast.remove(id);
    										}
    									}
    									
    								} catch (NegativeResponseException e3) {
    									if (e3.getMessage().indexOf("67") == -1) {
    					                    logger.error("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+")", e3);
    										//sendmail("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e3.getMessage());
    										synchronized(getErrorLog()){
    											getErrorLog().add("Caused by NegativeResponseException , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e3.getMessage());
    										}
    									}
    									reCheck=true;
    								} catch (Exception e2) {
    				                    logger.error("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+")", e2);
    									//sendmail("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e2.getMessage());
    									synchronized(getErrorLog()){
    										getErrorLog().add("Caused by Exception , Failed Query status of message(User:"+id.sndFrom+",msgid:"+id.MsgID+",Seq:"+id.MsgSeq+",number:"+id.sndTo+"). exception:"+ e2.getMessage());
    									}
    									reconnect(id.MsgID);
    									reCheck=true;
    								}
    							}
    								
    							

    							
    							if(reCheck){
    								logger.info("Past : need recheck!");
    								id.status = 1;
    							}else{
    								logger.info("Past : need not recheck!");
    								synchronized (qryMsgPast) {
    									qryMsgPast.remove(id);
    								}
    								
    								if (!haveUndilivered) {
    									id.status = 2;
    									synchronized (qryMsgPast) {
    										qryMsgPast.remove(id);
    									}
    								} else if (reject) {
    									id.status = 8;
    								} else {
    									id.status = 5;
    								}
    							}

    						} else {
    							synchronized (qryMsgPast) {
    								qryMsgPast.remove(id);
    							}
    						}
    					} else {
    						logger.info("Past : No response ID " + id.MsgID + ","+ id.MsgSeq + "...");
    						synchronized (qryMsgPast) {
    							qryMsgPast.remove(id);
    						}
    					}
    					try {
    						//if rspip is exists
    						if (id.rspID != null && !id.rspID.equals("")) {
    							if (id.fails < iFailLimit + 1) {
    								//logger.info("Past : msgId="+id.MsgID + ",status=" + id.status+ ",msgseq" + id.MsgSeq + "fails="+id.fails+"...");
    								ps.setInt(1, id.tries);
    								ps.setInt(2, id.status);
    							} else {
    								ps.setInt(1, id.tries + 1);
    								//20150129 modifed status 0 to 3(overdue)
    								ps.setInt(2, 3);
    								logger.info("Past : try query fails times " + id.MsgID + "," + id.MsgSeq + "...");
    								synchronized (qryMsgPast) {
    									qryMsgPast.remove(id);
    								}
    							}
    							// 20141103 if sms has been sended over one day,
    							// no continue trace .
    							if (!finalStatus.contains(id.status) && id.sendDate!=null && id.sendDate.before(new Date(new Date().getTime() - 1000 * 60 * 60 * 24 * 1))) {
    								logger.info("Past : "+id.MsgID + "," + id.MsgSeq + " is over one day ...");
    								id.status = 96;
    								ps.setInt(1, id.tries);
    								ps.setInt(2, id.status);
    								synchronized (qryMsgPast) {
    									qryMsgPast.remove(id);
    								}
    							}
    						} else {
    							ps.setInt(1, id.tries + 1);
    							ps.setInt(2, id.status);
    							
    							logger.error("Past : msgid : "+id.MsgID+" haven't rspid!");
    						}
    						ps.setString(5, id.MsgID);
    						ps.setString(3, str);
    						ps.setString(4, id.rspID);
    						ps.setInt(6, id.MsgSeq);

    						//logger.info("Past : update:" + id.MsgID + ",status:" + id.status + " with time:" + str);
    						ps.executeUpdate();

    						//20141222 add update data to response Log table in ten minute
    						if(id.isres && id.sendDate!=null &&new Date().before(new Date(id.sendDate.getTime()+600000))){
    							
    							ps2.setString(1, str);
    							if(id.status==0 || id.status==1 || id.status==97){
    								ps2.setString(2, "R");
    							}else if(id.status==2){
    								ps2.setString(2, "O");
    							}else{
    								ps2.setString(2, "F");
    							}
    							
    							ps2.setString(3, id.MsgID);
    							ps2.setString(4, id.sndTo);
    							
    							ps2.executeUpdate();
    						}
    						
    						//if id id long SMSmessage
    						if(id.rspIDs!=null && id.rspIDs.size()>0){
    							for (longmsgStatus l : id.rspIDs) {
    								pst.setInt(1, l.status);
    								pst.setString(2, str);
    								pst.setString(3, l.rspID);
    								pst.setString(4, l.MsgID);
    								pst.setInt(5, l.MsgSeq);
    								pst.setInt(6, l.part);
    								pst.executeUpdate();
    							}						
    						}

    						
    					} catch (SQLException e) {
    						logger.error("QueryResultThread got SQLException...",e);
    						//sendmail("QueryResultThread got SQLException...<br> Exception message:"+e.getMessage());
    						synchronized(getErrorLog()){
    							getErrorLog().add("QueryResultThread got SQLException...<br> Exception message:"+e.getMessage());
    						}	
    					}
    				}
    			}catch (SQLException e){
    				logger.error("QueryPastThread get Connection Exception..."+e.getMessage());
    				synchronized(getErrorLog()){
						getErrorLog().add(errorAddInfo()+"Because QueryPastThread can't connect to DB,return. "+"Error Msg"+e.getMessage());
					}
    			}finally{
    				try{
    					if(ps!=null)
    						ps.close();
    					if(ps2!=null)
    						ps2.close();
    					if(pst!=null)
    						pst.close();
    					if(conn!=null)
    						conn.close();
    				}catch(SQLException e){
    					logger.error("QueryResultThread ParseException",e);
    					synchronized(getErrorLog()){
    						getErrorLog().add(errorAddInfo()+"at close connection got error. "+"Error Msg"+e.getMessage());
    					}
    				}
    			}
            }
        };
	}
	public static ArrayList<String> getErrorLog() {
		return ErrorLog;
	}
	public static void setErrorLog(ArrayList<String> errorLog) {
		ErrorLog = errorLog;
	}
}
