package com.iglomo;

import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import java.security.*;
import java.util.*;
import java.text.*;
import java.io.*;
import java.sql.*;

public class SmppServiceImplementation implements SmppServiceInterface{
	String rMsg="";
	String sresult="";
	DBConnection c=DBConnection.getInstance();
	Connection conn=c.getConnection();
	private String getOrderNo(String uid) {
			String str = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
			Random rnd=new Random();
			long r=rnd.nextLong();
			String h=uid.substring(0,2);
			return h+"="+r+str;
	}
	/*
	public static void main(String[] args) throws Exception {
		SmppServiceImplementation smpp=new SmppServiceImplementation();
		FileInputStream fis=new FileInputStream(args[0]);
		byte [] b=new byte[fis.available()];
		fis.read(b);
		System.out.println(smpp.sendSMPP(new String(b,"UTF-8")));
	}
	*/
	public String sendSMPP(String s) {
	  Element element1;
	  NodeList fstNm;
		String SCHEDULE="",MULTIPLE="",MSG="";
		Calendar now = Calendar.getInstance();
		int year = now.get(Calendar.YEAR);
		int month = now.get(Calendar.MONTH)+1; // Note: zero based!
		int day = now.get(Calendar.DAY_OF_MONTH);
		int hour = now.get(Calendar.HOUR_OF_DAY);
		int minute = now.get(Calendar.MINUTE);
		String [] errmsg={"","","","排程日期格式錯誤","排程日期早於今天","訊息長度過長","空白的電話號碼"};
		ArrayList<String> PHONE = new ArrayList<String>();
		String susername;
		String spassword;
		String sorgcode;
		String sremark;
		String msgid="";
		String rResultPart1="<?xml version=\"1.0\" encoding=\"UTF-8\"?>"+
                        "<SMSRESPONSE>"+
                        "<USERNAME>甲先生</USERNAME>"+
                        "<ORGCODE>代發組織分類</ORGCODE>";
		String rResultPart2="	<DATA>";
		String rResultPart3="	</DATA>"+
												"</SMSRESPONSE>";
	  byte [] b=null;
		int sequenceNo=1;
		int r=-1;
	  try{

				b=s.getBytes("utf-8");
		    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		    Document doc = dbf.newDocumentBuilder().parse(new ByteArrayInputStream(b));
		    doc.getDocumentElement().normalize();

		    NodeList USERNAME = doc.getElementsByTagName("USERNAME");
		    element1 = (Element) USERNAME.item(0);
		    fstNm = element1.getChildNodes();
		    susername=((Node) fstNm.item(0)).getNodeValue();
		    NodeList PASSWORD = doc.getElementsByTagName("PASSWORD");
		    element1 = (Element) PASSWORD.item(0);
		    fstNm = element1.getChildNodes();
		    spassword=((Node) fstNm.item(0)).getNodeValue();
		    NodeList ORGCODE = doc.getElementsByTagName("ORGCODE");
		    element1 = (Element) ORGCODE.item(0);
		    fstNm = element1.getChildNodes();
		    sorgcode=((Node) fstNm.item(0)).getNodeValue();
				NodeList REMARK = doc.getElementsByTagName("REMARK");
		    element1 = (Element) REMARK.item(0);
		    fstNm = element1.getChildNodes();
		    sremark=((Node) fstNm.item(0)).getNodeValue();
				
				if ((r=doLogin(susername,spassword))==1){
					msgid=getOrderNo(susername);
					rResultPart1="<?xml version=\"1.0\" encoding=\"UTF-8\"?>"+
											 "<SMSRESPONSE>"+
											 "<USERNAME>"+susername+"</USERNAME>"+
											 "<ORGCODE>"+sorgcode+"</ORGCODE>";
					rResultPart1+="	<UID>"+msgid+"</UID>"+rResultPart2;
					
					NodeList DATA = doc.getElementsByTagName("ITEM");
					if(DATA!=null){
						for (int i = 0; i < DATA.getLength(); i++) {
							Node ITEM = DATA.item(i);
							PHONE.clear();
							if(ITEM.getNodeType()==Node.ELEMENT_NODE) {
							 for(Node node=ITEM.getFirstChild();node!=null;node=node.getNextSibling()) {
									r=0;
									if(node.getNodeType()==Node.ELEMENT_NODE) {
										if(node.getNodeName().equals("SCHEDULE")) {
											SCHEDULE=node.getFirstChild().getNodeValue();
											if (!SCHEDULE.equals("0")){
												String [] datepart=SCHEDULE.split(" ");
												if (datepart.length<2){
													r=-3;
													break;
												}
												String [] d=datepart[0].split("/");
												String [] h=datepart[1].split(":");
												if (d.length<3){
													r=-3;//日期格式錯誤
													break;
												}
												if (h.length<3){
													r=-3;//日期格式錯誤
													break;
												}
												try{
													if (year>Integer.parseInt(d[0])){
														r=-4;//早於發送日期
													}
													if (month>Integer.parseInt(d[1])){
														r=-4;//早於發送日期
													}
													if (day>Integer.parseInt(d[2])){
														r=-4;//早於發送日期
													}
													if (hour>Integer.parseInt(h[0])){
														r=-4;//早於發送日期
													}
													if (minute>Integer.parseInt(h[1])){
														r=-4;//早於發送日期
													}
												}catch(Exception e){
													r=-4;//早於發送日期
													break;
												}
												SCHEDULE=d[0]+d[1]+d[2]+h[0]+h[1];
											}
											System.out.println(SCHEDULE);
										}else
										if(node.getNodeName().equals("MULTIPLE")) {
											MULTIPLE=node.getFirstChild().getNodeValue();
											System.out.println(MULTIPLE);
										}else
										if(node.getNodeName().equals("MSG")) {
											MSG=node.getFirstChild().getNodeValue();
											if( MSG.matches("[a-zA-Z0-9|\\.]*") ){
												// 只有英文數字的處理
												byte [] bmsg=MSG.getBytes("iso8859-1");
												System.out.println("English only :"+bmsg.length);
												if (bmsg.length>765){
													r=-5;
												}
											}else{
												// 有其他自元的處理
												if (MSG.length()>335){
													r=-5;
												}
											}
											System.out.println(MSG);
										}else
										if(node.getNodeName().equals("PHONE")) {
											String phoneno=node.getFirstChild().getNodeValue();
											if (phoneno.trim().length()==0){
												r=-6;
											}else{
												PHONE.add(phoneno);
											}
										}
									}
									System.out.println("out r="+r);
									if (r<0){
										break;
									}
								}
								String rr="0";
								if (r<0){
									rr=errmsg[-r];
								}else{
									for (int j=0;j<PHONE.size();j++){
										System.out.println(PHONE.get(j));
										r=insertRequest(msgid,sequenceNo,SCHEDULE,PHONE.get(j),MSG);
										sequenceNo++;
									}
								}
								rResultPart1+="<ITEM><RESPONSE>"+rr+"</RESPONSE></ITEM>";
							}
						}
						sresult=rResultPart1+rResultPart3;
						r=insertRequestHead(susername,msgid,sorgcode,sequenceNo,sremark);
						if (r==0){
							if (r==-1){
								notice();
							}
							sresult="<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
											+"<SMSRESPONSE>"
											+"<USERNAME>"+susername+"</USERNAME>"
											+"<ORGCODE>"+sorgcode+"</ORGCODE>"
											+"<ERROR>"+rMsg+"</ERROR>"
											+"</SMSRESPONSE>";
						}
					}
				}else{
					if (r==-1){
						notice();
					}
					sresult="<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
									+"<SMSRESPONSE>"
									+"<USERNAME>"+susername+"</USERNAME>"
									+"<ORGCODE>"+sorgcode+"</ORGCODE>"
									+"<ERROR>"+rMsg+"</ERROR>"
									+"</SMSRESPONSE>";
				}
		}catch (Exception ee){
	    StringWriter sw = new StringWriter();
	    PrintWriter pw = new PrintWriter(sw);
	    ee.printStackTrace(pw);
			rMsg=sw.toString();
			System.out.println(rMsg);
		}
		return sresult;
	}
	private void notice(){
		Runtime rt = Runtime.getRuntime();
		String rtMsg="";
		try{
			Properties prop = new Properties();
			prop.load(new FileInputStream("/home/smpper/smppconfig.properties"));
			Process proc = rt.exec("mail -s \""+prop.getProperty("noticemailTitle")+"\" "+prop.getProperty("noticemailAddr")+" < \""+rMsg+"\"");
			proc.waitFor();
		}catch(InterruptedException e){
			rtMsg="notice InterruptedException "+e.getMessage();
			System.out.println(rtMsg);
		}catch (IOException ioe){
			rtMsg="notice IOException "+ioe.getMessage();
			System.out.println(rtMsg);
		}
	}
	private int doLogin(String u,String p){
		String sql="select * from smppuser where userid=? and passwd=md5(?)";
		PreparedStatement ps=null;
		ResultSet rs = null;
		int r=0;
		if (conn==null){
			rMsg="do Login Error in get Connection:";
			return 0;
		}
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1,u);
			ps.setString(2,p);
			rs = ps.executeQuery();
			if (rs.next()){
				r=1;
			}else{
				rMsg="使用者名稱或密碼錯誤";
				System.out.println(rMsg);
			}
		} catch (SQLException sqle) {
			rMsg="login Exception:"+sqle.getMessage();
			System.out.println(rMsg);
			r=-1;
		}finally{
			try{
				rs.close();
				ps.close();
			}catch(Exception e){
			}
			return r;
		}
	}
	private int insertRequestHead(String uid, String msgid,String orgcode,int counts,String remark) {
		String sql="insert into messages (userid,msgid,createtime,orgcode,itemcount,remark) values(?,?,?,?,?,?)";
		PreparedStatement ps=null;
		int result=0;
		if (conn==null){
			rMsg="Error in get Connection of IRH:";
			System.out.println(rMsg);
			return 0;
		}
		try{
			String str = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
			ps = conn.prepareStatement(sql);
			ps.setString(1,uid);
			ps.setString(2,msgid);
			ps.setString(3,str);
			ps.setString(4,orgcode);
			ps.setInt(5,counts);
			ps.setString(6,remark);
			result = ps.executeUpdate();
		}catch(Exception e){
			rMsg="IRH Exception:"+e.getMessage();
			System.out.println(rMsg);
			result=-1;
		}finally{
			try{
				ps.close();
			}catch(Exception e){
			}
			return result;
		}
	}
 	private int insertRequest(String msgid,int seq, String s,String phone,String msg) {
      String sql="insert into msgitem (msgid,seq,schedule,phoneno,msgbody,tries,status) values(?,?,?,?,?,0,0)";
      PreparedStatement ps=null;
      int result=0;
			if (conn==null){
				rMsg="Error in get Connection in IR:";
				System.out.println(rMsg);
				return 0;
			}
      try{
        ps = conn.prepareStatement(sql);
				ps.setString(1,msgid);
				ps.setInt(2,seq);
				ps.setString(3,s);
				ps.setString(4,phone);
				ps.setString(5,msg);
        result = ps.executeUpdate();
      }catch(Exception e){
        rMsg="IR Exception:"+e.getMessage();
				System.out.println(rMsg);
				result=-1;
      }finally{
        try{
          ps.close();
        }catch(Exception e){
        }
        return result;
      }
  }
}