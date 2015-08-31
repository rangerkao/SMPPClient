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
		
		String myLongMessage = "����²�T4_�x�s�Ȧ�z�]�ӫ~�B��ڥ�����T: ���ѳs�T���A"
				+ "GE�ť��N�^�ʪѲ��ðh�X���I���������ķ~�ȿE�y�j�L�A�Dã��0.6%�B�д���0.5%��2102�I�C"
				+ "�Dã�ڬw600��0.9%��зs���C�X�Ѷ^0.4%�B�ڦ�Ѻ�0.8%�C����o���p�����x�Ƴs��18�g�A"
				+ "��1986�~�ӳ̤j���T�A�����S��o��2.7%��58.1�����B��w�{��1.9%��51.8�����C�n�D���S0.5%��12�C"
				+ "�H�W�ҬQ��1.9%��4034�I�A�������Ѫ���ʯ��W�j�A�Q��1.7%�A����CEO���p�[�١A"
				+ "���Ӻ���q����B�׷|�ܤּW20%~30%�C�f�A(FITCH)������Τڦ�ūH�i��@�ɤ@���A"
				+ "��Ȥ��R�v��ܧ�þ����H�����v�w�ɰ���50%�H�W�C �����I�����H���� (-0.0001=6.2178) "
				+ "�����I�H���� (0.0028=6.2087) �ڤ��I���� (-0.0055=1.0604) �����I��� (-0.3600=120.22) "
				+ "�����I�n�D�� (0.0554=11.9962) �����I�s�x�� (0.0980=31.226) ������s�� (-0.0018=1.3664) "
				+ "�D���I���� (-0.0010=0.7682) ����(����/�s�q) (1.1%=1207.6) ��w�{���ŭ�o(����/��) (2.3%=57.9) "
				+ "�����S�o��(����/��) (1.7%=51.6) ����10�~�����ŴާQ�v (-1.23bps=1.947%) :"
				+ "���T���ȨѤ����Ш|�V�m�ϥΡA�Фť~�y";
		
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
