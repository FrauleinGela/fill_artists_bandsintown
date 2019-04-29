import java.sql.*;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import static java.lang.Integer.parseInt;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
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

import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author ngela
 */
public class ParseToXml {

    //method to convert String to json
    private static String readAllToJson(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    private static void executeBatch(PreparedStatement stm_artist_new, PreparedStatement stm_types_new,
            PreparedStatement stm_artist_types, PreparedStatement stm_picture_new, PreparedStatement stm_types,
            int total_counter, Map<String, Integer> types_spotifyname_idartist) 
            throws SQLException {
        System.out.println("executing batches");
        stm_artist_new.executeBatch();
        stm_artist_new.close();
        System.out.println("finished artists ");
        stm_picture_new.executeBatch();
        stm_picture_new.close();
        System.out.println("finished pictures");
            //now we check types from our DB

        Map<String, Integer> array_types = new HashMap<String, Integer>();
        //result of types from database
        ResultSet rs_types = stm_types.executeQuery();
        int last_type_id = 0;
        while (rs_types.next()) {
            if (!array_types.containsKey(rs_types.getString(2))) {
                array_types.put(rs_types.getString(2), rs_types.getInt(1));
            }
        }

        //check what should be the next id for types
        for (Map.Entry<String, Integer> entry : array_types.entrySet()) {
            if (entry.getValue() > last_type_id) {
                last_type_id = entry.getValue();
            }
        }

             //we check if artist map that contains name of type already exists in map, if it's not
        //we added with the counter id.
        for (Map.Entry<String, Integer> entry : types_spotifyname_idartist.entrySet()) {
            if (array_types.containsKey(entry.getKey())) {
                //we only updates the artistypes with id type and id artist
                stm_artist_types.setInt(1, array_types.get(entry.getKey()));
                stm_artist_types.setLong(2, entry.getValue());
                stm_artist_types.addBatch();
            } else {
                //add a new type and add artisttypes with the new id of type and id of artist from map
                last_type_id = last_type_id + 1;
                stm_types_new.setInt(1, last_type_id);
                stm_types_new.setString(2, entry.getKey());
                stm_types_new.addBatch();
                stm_artist_types.setInt(1, last_type_id);
                stm_artist_types.setLong(2, entry.getValue());
                stm_artist_types.addBatch();
            }

        }

        stm_types_new.executeBatch();
        System.out.println("finished types ");
        stm_artist_types.executeBatch();
        System.out.println("finished artist types");
        stm_types_new.close();
        stm_artist_types.close();
        
        System.out.println("counter total " + total_counter);
    }

    //method to connect with Url return null if status 400 and if its
    //200 it returns InputStream
    
    public static InputStream connectionFromUrl(String url, String token, int numberRow,
           PreparedStatement stm_artist_new,PreparedStatement stm_types_new,
           PreparedStatement stm_artist_types,PreparedStatement stm_picture_new,
           PreparedStatement stm_types, int total_counter,Timer timer,
           Map<String, Integer> types_spotifyname_idartist)
           throws MalformedURLException, Exception {
         InputStream value = null;
         try {
             HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection(); 
            connection.setConnectTimeout(60000);
        boolean passed = false;
        if (token != null) {
            connection.setDoOutput(true);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", "Bearer " + token);

        }
        int responseCode = connection.getResponseCode();
    
        while (!passed) {
            switch (responseCode) {
                case 404: {
                    //it didnt find result
                    value = null;
                    passed = true;
                    break;
                }
                case 200: {
                    //here calling to parseXml
                    try {
                        InputStream is = new URL(url).openStream();
                        passed = true;
                        value = is;
                      

                    } catch (IOException e) {

                        Thread.sleep((long) 10000);
                        passed = false;
                    }
                    break;
                }
                case 429: {
                    double sleepFloatingPoint = Double.valueOf(connection.getHeaderField("Retry-After"));
                    double sleepMillis = 1000 * sleepFloatingPoint;
                    Thread.sleep((long) sleepMillis);
                    passed = false;
                }
                case 400: {

                     Thread.sleep(3000);   
                    passed = false;
                }
                default: {
                    //it didnt find result
                    passed = true;
                    value = null;
                    break;
                }
            } 
        }
        }catch (SocketTimeoutException ste) {
                System.out.println("TIME OUT last artist name with no added"+numberRow);
                executeBatch(stm_artist_new,stm_types_new,
           stm_artist_types,stm_picture_new,stm_types,total_counter,types_spotifyname_idartist);
                timer.cancel();
                timer.purge();
            }
        return value;
    }

    
    private static Connection getConnection() throws URISyntaxException, SQLException {
    URI dbUri = new URI("postgres://kczoqrhvhxehgc:plX_CG2927Sl9DLVpkammtd64m@ec2-54-204-39-67.compute-1.amazonaws.com:5432/desnbd9a8re9ir");
    String username = dbUri.getUserInfo().split(":")[0];
    String password = dbUri.getUserInfo().split(":")[1];
    String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + ':' + dbUri.getPort() + dbUri.getPath()+"?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";

    return DriverManager.getConnection(dbUrl, username, password);
}

    public static void main(String[] args) throws IOException, ParseException, SQLException, InterruptedException, Exception {
        
        /*
        while counter%1000 == 1
print coounter/1000
if counter == 7000
        */
        System.out.println("offset "+args[0]+ " and limit "+args[1]);
        int total_counter = 0;
        Connection conn_concertmate;
        Connection conn_musicBrainz;
        PreparedStatement stm_pictures;
        PreparedStatement stm_artists;
        PreparedStatement stm_types;
        PreparedStatement stm_types_new;
        PreparedStatement stm_artist_types;
        PreparedStatement stm_picture_new;
        PreparedStatement stm_artist_new;
        PreparedStatement stm_artists_musicbrainz;
        int startRow = Integer.parseInt(args[0]);
        int limit =Integer.parseInt(args[1]);
        conn_concertmate = getConnection();//got from getConnection method where we have our url Database from Heroku
        //conn_concertmate = DriverManager
       //         .getConnection("jdbc:postgresql://localhost/concertmate",
       //                "concertmate-api", "lain1991");
        conn_musicBrainz=DriverManager.getConnection("jdbc:postgresql://concertmate-artistlist.ctziwerwf4ta.eu-west-1.rds.amazonaws.com:5432/artists",
                 "mariconuser", "concertmate");
        String query_picture_artist = "insert into pictures(artist_id,image_ext_url,profile,created_at,updated_at) VALUES(?,?,?,'2015-12-02 16:35:18','2015-12-02 16:35:18')";
        String query_types = "select id,name_type from types;";
        String query_new_type = "insert into types (id,name_type,created_at,updated_at) VALUES(?,?,'2012-02-27 16:35:18','2012-02-27 16:35:18')";
        String query_artist_type = "insert into artisttypes(type_id,artist_id,created_at,updated_at) VALUES(?,?,'2015-12-02 16:35:18','2015-12-02 16:35:18')";
        String query_artists = "select id,artistname from artists";
        String query_artist_musicbrainz="SELECT ar.id, ar.name FROM artist as ar "
                + " where ar.type IN (1,2) order by ar.id"+" LIMIT "+limit+" OFFSET "+startRow;
        String query_artists_table="INSERT INTO artists(id,artistname,from_user,created_at,updated_at)"
                + " values(?,?,false,'2012-02-27 16:35:18','2012-02-27 16:35:18');";
        if (conn_concertmate != null && conn_musicBrainz!=null) {
            System.out.println("connected");
             stm_artists_musicbrainz= conn_musicBrainz.prepareStatement(query_artist_musicbrainz);
             stm_artist_new=conn_concertmate.prepareStatement(query_artists_table);
            //stm_artists = conn_concertmate.prepareStatement(query_artists);
            stm_types = conn_concertmate.prepareStatement(query_types);
            stm_types_new = conn_concertmate.prepareStatement(query_new_type);
            stm_picture_new = conn_concertmate.prepareStatement(query_picture_artist);
            stm_artist_types = conn_concertmate.prepareStatement(query_artist_type);
            
            
            ResultSet rs =stm_artists_musicbrainz.executeQuery();
            String artistname;
            Long id;
            JSONArray json_images;

            Map<String,Integer> types_spotifyname_idartist = new HashMap<String,Integer>();
            List<String> json_types = new ArrayList();
            

        //here we call our first token , directly in run()
       
       
        //Creating timer which executes method to get refresh_token after 55 minutes
        Timer timer = new Timer();
            GettingToken token1=new GettingToken();
            //my error is here.b.efore i did like this. inbu 55 minutes call this task token1 again.. token1 supossed is callling to run..
        timer.scheduleAtFixedRate(token1, 10, 3300000);

            int counter_id_artist=0;

            while (rs.next()) {
                String jsonText;
                JSONObject json;
               artistname = rs.getString(2);
               System.out.println(artistname + "column "+rs.getRow());

               // artistname = "drake";
                String new_nameartist = artistname.replaceAll("\\?", "");
               new_nameartist= new_nameartist.replaceAll("\\s", "%20");
                // the method wrehadJsonFromUrl is here
                String url = "https://api.spotify.com/v1/search?q="+new_nameartist+"&type=artist";
                //calling token from spotify api 
                InputStream conn = connectionFromUrl(url,token1.getToken(),rs.getRow(),stm_artist_new,stm_types_new,
                        stm_artist_types,stm_picture_new,stm_types,rs.getRow(),timer,types_spotifyname_idartist);
               
                BufferedReader rd;
                
                if (conn != null) {
                    
                    rd = new BufferedReader(new InputStreamReader(conn, Charset.forName("UTF-8")));
                     
                    jsonText = readAllToJson(rd);
                    
                    json = new JSONObject(jsonText);
                    conn.close();
                    rd.close();
                    JSONObject json_artists = json.getJSONObject("artists");//object               
                    JSONArray items_json = json_artists.getJSONArray("items");//array into of artists
                    if (items_json.length() != 0) {
                

                        for (int i = 0; i < items_json.length(); i++) {
                            JSONObject item = items_json.getJSONObject(i);
                            if (i == 0 && item.get("name").toString().equalsIgnoreCase(artistname)) {
                                counter_id_artist = counter_id_artist + 1;
                                stm_artist_new.setString(2, artistname);
                                stm_artist_new.setLong(1,rs.getLong(1));
                                stm_artist_new.addBatch();
                                total_counter = total_counter + 1;
                                //array of gnres
                                JSONArray genres_array = item.getJSONArray("genres");
                                if (genres_array.length() != 0) {
                                    //we check all genres and save in a map <type name, id artist>
                                    for (int j = 0; j < genres_array.length(); j++) {
                                        String type = genres_array.getString(j);
                                        types_spotifyname_idartist.put(type, rs.getInt(1));
                                    }
                                }
                                //array of images and looking for the first image
                                JSONArray images_array = item.getJSONArray("images");
                                if (images_array.length() != 0) {
                                    //adding image as profile picture
                                    JSONObject images = images_array.getJSONObject(i);
                                    stm_picture_new.setLong(1,rs.getLong(1));
                                    stm_picture_new.setString(2, images.get("url").toString());
                                    stm_picture_new.setBoolean(3, true);
                                    stm_picture_new.addBatch();

                                    //here add one more image from bandsintown as no profile picture
                                    String url_to_bandsintown = "http://api.bandsintown.com/artists/"+new_nameartist+".json?api_version=2.0&app_id=CONCERTMATE";
                                    InputStream conn_band = connectionFromUrl(url_to_bandsintown, null,rs.getRow(),
                                            stm_artist_new,stm_types_new,
                        stm_artist_types,stm_picture_new,stm_types,rs.getRow(),timer,types_spotifyname_idartist);

                                    BufferedReader rd_band;
                                    String jsonText_band;
                                    if (conn_band != null) {
                                        rd_band = new BufferedReader(new InputStreamReader(conn_band, Charset.forName("UTF-8")));
                                        jsonText_band = readAllToJson(rd_band);

                                        conn_band.close();
                                        rd_band.close();
                                        json = new JSONObject(jsonText_band);
                                        if (json.get("image_url") != null) {
                                            stm_picture_new.setLong(1,rs.getLong(1));
                                            stm_picture_new.setString(2, json.get("image_url").toString());
                                            stm_picture_new.setBoolean(3, false);
                                            stm_picture_new.addBatch();
                                        }
                                    }

                                }
                            }

                        }
                               
                    } else {
                        //if artist is not found in spotify , it looks for a image in bandsintown api

                        String url_to_bandsintown = "http://api.bandsintown.com/artists/" + new_nameartist + ".json?api_version=2.0&app_id=CONCERTMATE";
                       
                        InputStream conn_band = connectionFromUrl(url_to_bandsintown, null,rs.getRow(),
                                                                 stm_artist_new,stm_types_new,
                                                                 stm_artist_types,stm_picture_new,stm_types,rs.getRow(),
                                                                 timer,types_spotifyname_idartist);
                        if (conn_band != null) {

                        total_counter=total_counter+1;
                        counter_id_artist= counter_id_artist+1;
                             stm_artist_new.setString(2, artistname);
                              stm_artist_new.setLong(1,rs.getLong(1));
                              stm_artist_new.addBatch();
                            rd = new BufferedReader(new InputStreamReader(conn_band, Charset.forName("UTF-8")));
                            jsonText = readAllToJson(rd);
                            conn_band.close();
                            rd.close();
                            json = new JSONObject(jsonText);
                            if (json.get("image_url") != null) {
                                stm_picture_new.setLong(1,rs.getLong(1));
                                stm_picture_new.setString(2, json.get("image_url").toString());
                                stm_picture_new.setBoolean(3, true);
                                stm_picture_new.addBatch();
                            }
                        }
                    }

                } else {
                        //if artist is not found in spotify , it looks for a image in bandsintown api
                        String url_to_bandsintown = "http://api.bandsintown.com/artists/" + new_nameartist + ".json?api_version=2.0&app_id=YOUR_APP_ID";
                       
                        InputStream conn_band = connectionFromUrl(url_to_bandsintown,
                                null,rs.getRow(),stm_artist_new,stm_types_new,
                                 stm_artist_types,stm_picture_new,stm_types,rs.getRow(),timer,
                                 types_spotifyname_idartist);
                        if (conn_band != null) {
                   
                            counter_id_artist= counter_id_artist+1;
                            stm_artist_new.setString(2, artistname);
                              stm_artist_new.setLong(1,rs.getLong(1));
                              stm_artist_new.addBatch();
                            total_counter=total_counter+1;
                            rd = new BufferedReader(new InputStreamReader(conn_band, Charset.forName("UTF-8")));
                            jsonText = readAllToJson(rd);
                            conn_band.close();
                             rd.close();
                            json = new JSONObject(jsonText);
                            if (json.get("image_url") != null) {
                                stm_picture_new.setLong(1,rs.getLong(1));
                                stm_picture_new.setString(2, json.get("image_url").toString());
                                stm_picture_new.setBoolean(3, true);
                                stm_picture_new.addBatch();
                            }
                            
                        }
                    }
            }
                timer.cancel();
                timer.purge();
               executeBatch(stm_artist_new,stm_types_new,
           stm_artist_types,stm_picture_new,stm_types,total_counter,types_spotifyname_idartist);
          
        }
    }
}
