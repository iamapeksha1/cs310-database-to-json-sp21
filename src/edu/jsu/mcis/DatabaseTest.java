package edu.jsu.mcis;

import java.sql.*;
import java.util.ArrayList;
import org.json.simple.*;


public class DatabaseTest {
    
   public static void main(String[] args) {
        
        JSONArray array = getJSONData();
        System.out.println("\nDATABASE TO JSON PARSER");
        System.out.println("=========================");
        System.out.println(array);
        System.out.println();
        
    }

    public static JSONArray getJSONData() {
                
        Connection connection = null;
        PreparedStatement pstSelect = null, pstUpdate = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        JSONArray records = new JSONArray();
        
        String query, value;
        
        ArrayList<String> key = new ArrayList<>();
        
        boolean hasresults;
        int resultCount, columnCount, updateCount = 0;
        
        try {
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test?autoReconnect=true&useSSL=false");
            String username = "root";
            String password = "001215473";

            /* Load the MySQL JDBC Driver */
            
            Class.forName("com.mysql.cj.jdbc.Driver");
            
            /* Open Connection */

            connection = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (connection.isValid(0)) {
                
                /* Prepare Select Query */
                
                query = "SELECT * FROM people";
                pstSelect = connection.prepareStatement(query);
                
                /* Execute Select Query */
                hasresults = pstSelect.execute();       
                
                /* Get Results*/
                
                while ( hasresults || pstSelect.getUpdateCount() != -1 ) {

                    if ( hasresults ) {
                        
                        /* Get ResultSet Metadata */
                        
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();
                        
                        /* Get Column Names; Append them in an ArraList "key" */
                        
                        for (int i = 2; i <= columnCount; i++) {
                            key.add(metadata.getColumnLabel(i));
                        }
                        
                        /* Get Data, Put the data in JSONObject */
                        
                        while(resultset.next()) {
                            
                            /* Begin Next ResultSet Row; Loop Through ResultSet 
                            Columns; Append to jsonObject */
                            
                            JSONObject object = new JSONObject();

                            for (int i = 2; i <= columnCount; i++) {
                                
                                JSONObject jsonObject = new JSONObject();
                                value = resultset.getString(i);
                                
                                if (resultset.wasNull()) {
                                    jsonObject.put(key.get(i-2), "NULL");
                                    jsonObject.toJSONString();
                                }

                                else {
                                    jsonObject.put(key.get(i-2), value);
                                    jsonObject.toString();
                                }
                                
                                object.putAll(jsonObject);

                            }
                            records.add(object);

                        }
                        
                    }

                    else {

                        resultCount = pstSelect.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }

                    }
                    
                    /* Check for More Data */

                    hasresults = pstSelect.getMoreResults();

                }
                
            }
            
            /* Close Database Connection */
            
            connection.close();
            
        }
        
        catch (Exception e) {
            System.err.println(e.toString());
        }
        
        /* Close Other Database Objects */
        
        finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
            
            if (pstUpdate != null) { try { pstUpdate.close(); pstUpdate = null; } catch (Exception e) {} }
            
        }
        
        return records;
            
    }
        
}   