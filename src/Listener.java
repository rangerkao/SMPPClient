import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jsmpp.bean.AlertNotification;
import org.jsmpp.bean.DataSm;
import org.jsmpp.bean.DeliverSm;
import org.jsmpp.bean.DeliveryReceipt;
import org.jsmpp.bean.MessageType;
import org.jsmpp.extra.ProcessRequestException;
import org.jsmpp.session.DataSmResult;
import org.jsmpp.session.MessageReceiverListener;
import org.jsmpp.session.Session;
import org.jsmpp.util.InvalidDeliveryReceiptException;


public class Listener implements MessageReceiverListener {
	
	Logger ListenerLogger;
	
	public Listener(){
		loadProperties();
	}
	
	public void onAcceptDeliverSm(DeliverSm deliverSm)	throws ProcessRequestException {
		
        if (MessageType.SMSC_DEL_RECEIPT.containedIn(deliverSm.getEsmClass())) {
            // this message is delivery receipt
            try {
                DeliveryReceipt delReceipt = deliverSm.getShortMessageAsDeliveryReceipt();
                
                // lets cover the id to hex string format
                /*long id = Long.parseLong(delReceipt.getId()) & 0xffffffff;
                String messageId = Long.toString(id, 16).toUpperCase();*/
                
                /*
                 * you can update the status of your submitted message on the
                 * database based on messageId
                 */
                
                //ListenerLogger.info("Receiving delivery receipt for message '" + messageId + " ' from " + deliverSm.getSourceAddr() + " to " + deliverSm.getDestAddress() + " : " + delReceipt);
                
                String id=delReceipt.getId();
                String submitDate=new SimpleDateFormat("yyyyMMddHHmmss").format(delReceipt.getSubmitDate());
                String doneDate=new SimpleDateFormat("yyyyMMddHHmmss").format(delReceipt.getDoneDate());
                String status=delReceipt.getFinalStatus().toString();
                int statusValue=delReceipt.getFinalStatus().value()+1;
                
                //************new
                ListenerLogger.info(	"\n"
                			+ 	"Id\t:\t"+id+"\n"
                			+ 	"Submitted\t:\t"+delReceipt.getSubmitted()+"\n"
            				+ 	"SubmitDate\t:\t"+submitDate+"\n"
    						+ 	"Delivered\t:\t"+delReceipt.getDelivered()+"\n"
							+ 	"DoneDate\t:\t"+doneDate+"\n"
							+ 	"FinalStatus\t:\t"+status+"\n"
							+ 	"FinalStatus value\t:\t"+statusValue+"\n"
							+ 	"Text\t:\t"+delReceipt.getText()+"\n"
							+ 	"Error\t:\t"+delReceipt.getError()+"\n");         

                processDB(id,statusValue,doneDate);
               
            } catch (InvalidDeliveryReceiptException e) {
            	ListenerLogger.info("Failed getting delivery receipt",e);    
            	synchronized(StressClient.getErrorLog()){
            		StressClient.getErrorLog().add(StressClient.errorAddInfo()+" Failed getting delivery receipt "+"Error Msg"+e.getMessage());
				}
            }        
        } else {
            // this message is regular short message
            
            /*
             * you can save the incoming message to database.
             */
            
        	ListenerLogger.info("Receiving not SMSC_DEL_RECEIPT message : " + new String(deliverSm.getShortMessage()));
        	synchronized(StressClient.getErrorLog()){
        		StressClient.getErrorLog().add(StressClient.errorAddInfo()+" Receiving not SMSC_DEL_RECEIPT message : " + new String(deliverSm.getShortMessage()));
			}
        }
    }
    
    public void onAcceptAlertNotification(AlertNotification alertNotification) {
    }
    
    public DataSmResult onAcceptDataSm(DataSm dataSm, Session source)
            throws ProcessRequestException {
        return null;
    }
    
    private void loadProperties(){
		//Properties props =getProperties();
		//PropertyConfigurator.configure(props);
		ListenerLogger =Logger.getLogger(Listener.class);
		ListenerLogger.info("Logger Load Success!");
	}
    
    private Properties getProperties(){
		Properties result=new Properties();
		
		//result.setProperty("log4j.rootCategory", "INFO, FileOutput");
		
		result.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
		result.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
		result.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d [%5p] (%F:%L) - %m%n");
		
		result.setProperty("log4j.appender.Listener", "org.apache.log4j.DailyRollingFileAppender");
		result.setProperty("log4j.appender.Listener.layout", "org.apache.log4j.PatternLayout");
		result.setProperty("log4j.appender.Listener.layout.ConversionPattern", "%d [%5p] (%F:%L) - %m%n");
		result.setProperty("log4j.appender.Listener.DatePattern", "'.'yyyyMMdd");
		result.setProperty("log4j.appender.Listener.File", "Listener.log");

		return result;
	}
    
    private void processDB(String id,int statusValue,String doneDate){
    	Connection conn =null;        	
        Statement st = null;
        ResultSet rs = null;
		try {
			conn= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
			st = conn.createStatement();
			
			String sql = "select count(1) ab from msgitem where rspid = '"+id+"'";
			
			rs = st.executeQuery(sql);
			ListenerLogger.info("Execute select:"+sql);	
			
			String count = null;
			
			while(rs.next()){
				count = rs.getString("ab");
			}
			
			String msgid = null;
			String phoneno = null;
			Integer seq = null;
			String isres =null;
			
			if(!"0".equals(count)){
				//短簡訊處理
				sql = "select msgid,seq from msgitem where rspid ='"+id+"' ";
				rs = st.executeQuery(sql);
				ListenerLogger.info("Execute select:"+sql);	

				while(rs.next()){
					msgid = rs.getString("msgid");
					seq = rs.getInt("seq");
				}
				
				sql = "update msgitem set status='"+statusValue+"' "
						+ ",donetime='"+doneDate+"' "
						+ "where rspid='"+id+"' ";
				st.executeUpdate(sql);
				ListenerLogger.info("Execute update:"+sql);	
			}else{
				//長簡訊處理
				sql = "select msgid,seq from longmsgitem where rspid ='"+id+"' ";
				rs = st.executeQuery(sql);
				ListenerLogger.info("Execute select:"+sql);	

				while(rs.next()){
					msgid = rs.getString("msgid");
					seq = rs.getInt("seq");
				}
		
				sql = "update longmsgitem set status="+statusValue+" "
						+",donetime='"+doneDate+"' "
						+ "where rspid='"+id+"' ";
				st.executeUpdate(sql);
				ListenerLogger.info("Execute update:"+sql);	
				
				
				
				//處理主簡訊資料
				boolean success = true;
				boolean reject = false;
				boolean routing = false;
				
				sql = "select status from longmsgitem where msgid='"+msgid+"' and seq="+seq+" ";
				rs = st.executeQuery(sql);
				ListenerLogger.info("Execute select:"+sql);	
				
				while(rs.next()){
					if(rs.getString("status")==null || 
							"97".equals(rs.getString("status"))|| 
							"7".equals(rs.getString("status"))|| 
							"1".equals(rs.getString("status"))|| 
							"0".equals(rs.getString("status"))){
						routing = true;
					}else if(!"2".equals(rs.getString("status"))){
						success = false;
						
						if("8".equals(rs.getString("status")))
							reject=true;
					}
				}
				
				if(routing){
					statusValue = 1;
				}else if(!success){
					if(reject)
						statusValue = 8;
					else
						statusValue = 5;
				}else{
					statusValue = 2;
				}

				sql = "update msgitem set status="+statusValue+" "
						+",donetime='"+doneDate+"' "
						+"where msgid='"+msgid+"' and seq="+seq+" ";
				st.executeUpdate(sql);
				ListenerLogger.info("Execute update:"+sql);	

			}
			
			//Response Process
			sql = "select c.isres,b.msgid,b.phoneno,b.sendtime "
					+ "from smppuser c,messages a,msgitem b "
					+ "where c.userid=a.userid and a.msgid=b.msgid and b.msgid='"+msgid+"' and b.seq="+seq+" "
					+ "and  sendtime >= to_char(now()- interval '10 minute','yyyyMMddhh24miss') ";
			rs = st.executeQuery(sql);
			ListenerLogger.info("Execute select:"+sql);	
			
			while(rs.next()){
				isres = rs.getString("isres");
				phoneno = rs.getString("phoneno");
			}
			if("1".equals(isres)){
				String statusS = null;
				if(statusValue==0 || statusValue==1 || statusValue==97){
					statusS = "R";
				}else if(statusValue==2){
					statusS = "O";
				}else{
					statusS = "F";
				}
				sql = "update responselog set acktime='"+doneDate+"',status='"+statusS+"' where msgid='"+msgid+"' and phoneno='"+phoneno+"'";
				st.executeUpdate(sql);
				ListenerLogger.info("Execute select:"+sql);	
			}
			
			
		} catch (SQLException e) {
			ListenerLogger.info("At listener processDB got SQLException : " ,e);
        	synchronized(StressClient.getErrorLog()){
        		StressClient.getErrorLog().add(StressClient.errorAddInfo()+" At listener processDB got SQLException :  " + e.getMessage());
			}
		}finally{
			
			try {
				if(conn!=null){
					conn.close();
				}
				if(st!=null){
					st.close();
				}
				if(rs!=null){
					rs.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }
    
    
    
}
