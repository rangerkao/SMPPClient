
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResponseBatch {
	
	private static final Logger logger = LoggerFactory.getLogger(ResponseBatch.class);

	private static int period_Time = 30;
	private static int post_period_Time = 1;
	private static final String DEFAULT_LOG4J_PATH = "client-log4j.properties";
	private static String AlertMailTo;
	private static String resURL;
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Class.forName("org.postgresql.Driver");
		DriverManager.setLoginTimeout(10);
		Properties prop = new Properties();
        FileInputStream fis;
        
		fis = new FileInputStream("StressClient.properties");
		prop.load(fis);
		fis.close();
		period_Time = Integer.parseInt(prop.getProperty("Batch_period_Time"));
		AlertMailTo = prop.getProperty("AlertMailTo");
		resURL = prop.getProperty("response_URL");
		String log4jPath =DEFAULT_LOG4J_PATH;
		PropertyConfigurator.configure(log4jPath);
		
		logger.info("Load Propties success :"+
					"\nperiod_Time="+period_Time+
					"\nAlertMailTo="+AlertMailTo+
					"\nResponseURL="+resURL);


		ResponseBatch re=new ResponseBatch();
		re.regularProccess();
	}
	
	private void regularProccess(){
		
		
		long s1=0;
		long s2=System.currentTimeMillis();
		while(true){
			s2=System.currentTimeMillis();
			if((s2-s1)>period_Time*1000){
				s1=System.currentTimeMillis();
				proccess();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	
	class res{
		String msgid;
		String phoneno;
		String status;
		String acktime;
		
		String sendResult;
	}
	
	
	private void proccess(){
		Connection conn = null;
		Statement st = null;
		Statement st2 = null;
		ResultSet rs = null;
		
		List<res> resList = new ArrayList<res>();
		
		logger.info("Program run..."+new Date());
		try {
			conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
			st = conn.createStatement();
			
			String sql="select  msgid,phoneno,status,acktime from responselog where sendresult is null and sendtime<'"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())+"'";

			logger.debug("Query response msg item :"+sql);
			rs = st.executeQuery(sql);

			while(rs.next()){
				res r = new res();
				r.msgid = rs.getString("msgid");
				r.phoneno = rs.getString("phoneno");
				r.status = rs.getString("status");
				r.acktime = rs.getString("acktime");
				resList.add(r);
			}		
			
			String url=null;
			String param=null;
			String charset=null;
			for(res r : resList){
				
				//url="http://www.17life.com/Service/S2TCallback.ashx";
				url = resURL;
				param="msgID="+r.msgid.replace("=", "%3D")+"&phone="+r.phoneno+"&status="+r.status+"&acktime="+r.acktime;
				charset=null;
				
				try {
					r.sendResult=String.valueOf(HttpPost(url,param,charset));
					if(!"200".equals(r.sendResult))
						throw new IOException("Post not success Error");
				} catch (IOException e) {
					//XXX
					logger.error("post "+url+"?"+param+" error at "+new Date(),e);
					sendmail("post "+url+"?"+param+" error at "+new Date()+" . ErrorMessage:"+e.getMessage());
					if(r.sendResult==null)
						r.sendResult="Java error";
				}
				sql="update responselog set sendresult='"+r.sendResult+"' where msgid = '"+r.msgid+"' and phoneno = '"+r.phoneno+"'";
				st.execute(sql);
				try {
					Thread.sleep(post_period_Time * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		} catch (SQLException e1) {
			e1.printStackTrace();
		}finally{
			try {
				rs.close();
				st.close();
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	public int HttpPost(String url,String param,String charset) throws IOException{
		URL obj = new URL(url);
		
		if(charset!=null && !"".equals(charset))
			param=URLEncoder.encode(param, charset);
		
		
		HttpURLConnection con =  (HttpURLConnection) obj.openConnection();
 
		//add reuqest header
		/*con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", USER_AGENT);
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");*/
 
		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(param);
		wr.flush();
		wr.close();
 
		int responseCode = con.getResponseCode();
		logger.info("\nSending 'POST' request to URL : " + url);
		logger.info("Post parameters : " + new String(param.getBytes("ISO8859-1")));
		logger.info("Response Code : " + responseCode);
 
		/*BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();*/
 
		//print result
		return responseCode;
	}
	
	
	void sendmail(String msg){
		synchronized(StressClient.getErrorLog()){
			StressClient.getErrorLog().add(msg);
		}
	}
	
	/*void sendmail(String msg){
		String ip ="";
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		msg=msg+" from location "+ip;			
		
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
	}*/

}
