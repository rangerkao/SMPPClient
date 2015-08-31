import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;

import org.jsmpp.session.ServerResponseHandler;
import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.Alphabet;
import org.jsmpp.bean.BindType;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.GeneralDataCoding;
import org.jsmpp.bean.MessageClass;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.OptionalParameter;
import org.jsmpp.bean.OptionalParameters;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.bean.SMSCDeliveryReceipt;
import org.jsmpp.bean.OptionalParameter.Tag;
import org.jsmpp.bean.SubmitSm;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ProcessRequestException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.session.BindParameter;
import org.jsmpp.session.QuerySmResult;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.session.ServerResponseDeliveryAdapter;


public class hello {

	public static void main(String[] args){
		// TODO Auto-generated method stub
		
		System.out.println("hello!");
		
		String myLongMessage = "測試簡訊4_台新銀行理財商品處國際市場資訊: 美股連三漲，"
				+ "GE宣布將回購股票並退出風險較高的金融業務激勵大盤，道瓊漲0.6%、標普漲0.5%至2102點。"
				+ "道瓊歐洲600漲0.9%續創新高。俄股跌0.4%、巴西股漲0.8%。美國油氣鑽井平台數連降18週，"
				+ "創1986年來最大降幅，布蘭特原油漲2.7%至58.1美元、西德州漲1.9%至51.8美元。南非幣貶0.5%至12。"
				+ "隨上證昨漲1.9%至4034點，香港國企股近期動能亦增強，昨漲1.7%，港交所CEO李小加稱，"
				+ "未來滬港通投資額度會至少增20%~30%。惠譽(FITCH)對韓國及巴西債信展望一升一降，"
				+ "瑞銀分析師表示希臘近期違約機率已升高至50%以上。 美元兌離岸人民幣 (-0.0001=6.2178) "
				+ "美元兌人民幣 (0.0028=6.2087) 歐元兌美元 (-0.0055=1.0604) 美元兌日圓 (-0.3600=120.22) "
				+ "美元兌南非幣 (0.0554=11.9962) 美元兌新台幣 (0.0980=31.226) 美元對新幣 (-0.0018=1.3664) "
				+ "澳幣兌美元 (-0.0010=0.7682) 黃金(美元/盎司) (1.1%=1207.6) 西德州中級原油(美元/桶) (2.3%=57.9) "
				+ "布蘭特油價(美元/桶) (1.7%=51.6) 美國10年期公債殖利率 (-1.23bps=1.947%) :"
				+ "本訊息僅供內部教育訓練使用，請勿外流";
		
		System.out.println(myLongMessage.length());


		SMPPSession smppSession = new SMPPSession();
		byte[] msg = new byte[0];
		try {
			OptionalParameter messagePayloadParameter = new OptionalParameter.OctetString(Tag.MESSAGE_PAYLOAD, new String(myLongMessage.getBytes("BIG5"),"iso8859-1"));
			
			smppSession.connectAndBind("10.42.1.163", 2775, new BindParameter(BindType.BIND_TRX, "17life", "test17", "cln", TypeOfNumber.UNKNOWN, NumberingPlanIndicator.UNKNOWN, null));
			/*System.out.println(smppSession.submitShortMessage("", TypeOfNumber.ALPHANUMERIC, NumberingPlanIndicator.UNKNOWN, "SmppTest", 
					TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, "886989235253", 
					new ESMClass(), (byte)0, (byte)0, "", null, new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT), 
					(byte)0, new GeneralDataCoding(false, true, MessageClass.CLASS1, Alphabet.ALPHA_DEFAULT),(byte)0, 
					msg, messagePayloadParameter));*/
			
			String rspID = "14e70b619440a2a01a100c1fcd7ff97f";
			String sndFrom = "85269171717";
			
			org.jsmpp.bean.TypeOfNumber ton = TypeOfNumber.ALPHANUMERIC;
			try {
				Long.parseLong(sndFrom);
				ton = TypeOfNumber.INTERNATIONAL;
			} catch (NumberFormatException e) {
				//logger.error("not send from number:" + id.sndFrom + ":");
			}
			
			QuerySmResult q4 = smppSession.queryShortMessage( rspID,ton,NumberingPlanIndicator.UNKNOWN,sndFrom);
			System.out.println(q4.getMessageState().value());
			String str = "20"+q4.getFinalDate();
			System.out.println(str.substring(0, str.length() - 4));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PDUException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ResponseTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NegativeResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			smppSession.unbindAndClose();
		}

		
		
	}

}
