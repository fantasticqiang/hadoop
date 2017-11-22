import java.io.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.interfaces.RSAPublicKey;

public class PayTest {

    private static Certificate certificate = null;
    private static RSAPublicKey publicKey = null;
    public static void main(String[] args) throws IOException {

        // 网上支付平台证书
        InputStream inStream = null;
        try {
            inStream = new FileInputStream("D:\\server_cert.cer");
            CertificateFactory certFactory = CertificateFactory
                    .getInstance("X.509");
            certificate = certFactory.generateCertificate(inStream);
            publicKey = (RSAPublicKey) certificate.getPublicKey();
        } catch (Exception e) {
            System.out.println("初始化网上支付平台证书出错：");
            e.printStackTrace();
        } finally {
            if (inStream != null) try {inStream.close();} catch (Exception e) {}
        }

        getpub();

    }

    public static void getpub() throws IOException {
        InputStream is = new FileInputStream("D:\\a.cer");
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len = -1;
        while((len = is.read(buffer)) != -1){
            os.write(buffer,0,len);
        }
        String result = new String(os.toByteArray());
        System.out.println(result);
    }
}
