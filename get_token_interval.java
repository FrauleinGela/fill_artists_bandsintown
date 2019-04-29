import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 *
 * @author ngela
 */
public class GettingToken extends TimerTask{
   
    public static final String client_id = "";
    public static final String client_secret = "";
   private String token;
//this task will repeat after 55 minutes
    public GettingToken(){
        
    }
    public String getToken(){
        return this.token;
    }

     //method to convert String to json
    private static String readAllToJson(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

   public void gettingTokens() throws Exception {
       System.out.println("I  AM HERE====> MARICAS");
        HttpURLConnection connection = (HttpURLConnection) new URL("https://accounts.spotify.com/api/token?grant_type=client_credentials").openConnection();
        String encodeString = client_id + ":" + client_secret;
        String base64login = new String(Base64.encodeBase64(encodeString.getBytes()));
        connection.setRequestProperty("Authorization", "Basic " + base64login);
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
        wr.flush();
        wr.close();
        int responseCode = connection.getResponseCode();

        switch (responseCode) {

            case 200: {
                //ask for token to spotify api 
                try {
                    InputStream is = connection.getInputStream();
                    BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
                    String jsonText = readAllToJson(rd);
                    JSONObject json = new JSONObject(jsonText);
                    System.out.println("json " + json);
                    is.close();
                    rd.close();
                    if(!json.isNull("refresh_token")){
                       this.token=json.get("refresh_token").toString();
                        System.out.println("refresh token");
                    }else {
                       this.token= json.get("access_token").toString();
                        System.out.println("access token " + token);
                    }
                } catch (IOException e) {
                    System.out.println("error here and sleeping to try again " + e.toString());
                    Thread.sleep(10000);
                    e.printStackTrace();
                }

            }
           
        }
    }
   //run call to my method getting token gettingtoken update the token.. 
    @Override
    public void run() {
        try {
            gettingTokens();
        } catch (Exception ex) {
            Logger.getLogger(GettingToken.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
