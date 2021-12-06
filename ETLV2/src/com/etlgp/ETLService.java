package com.etlgp;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;
import java.io.*;
import java.net.*;
import java.nio.charset.*;
import org.json.*;




public class ETLService {
	
	
	public static class lectura {
		 
        private String estacion;
        private String time;   
        private String caudal;
        private String conductividad;
        private String nivel;
        private String ph;
        private String temperatura;
 
        private lectura (String _Estacion, String _Time, String _Caudal, String _Conductividad, String _Nivel, String _PH, String _Temperatura ) {
           
        	estacion = _Estacion;
            time = _Time;
            caudal = _Caudal;
            conductividad = _Conductividad; 
            nivel = _Nivel;
            ph = _PH;
            temperatura = _Temperatura;
     
        }
        
    }
	
	public static class ultima_lectura {
		 
        private String estacion;
        private String time;   
       
 
        private ultima_lectura (String _Estacion, String _Time  ) {
           
        	estacion = _Estacion;
            time = _Time;

     
        }
        
    }
	
	public static void main(String[] args) throws IOException, JSONException {
		// TODO Auto-generated method stub	
	

		    url_update();
  
		
		
	}
	
	
	private static void url_update() throws IOException, JSONException
	{

		 try {
			    System.out.println("Connect to URL ...");
			    String url ="http://localhost/projects/PDCMLCC_WEB/PDCMLCC/controller/ultimas_lecturas.php";
			    JSONObject json = readJsonFromUrl(url);
			    String op1=((String)json.get("estatus"));
			    System.out.println("URL Response :"+op1);
			    obtener_ultimas_lecturas();
		    } catch (Exception e) {
		       
		    	System.out.println("I cant resolve the URL..");
		    }
	}
		

	
	public static void obtener_ultimas_lecturas() throws IOException, JSONException
	{
		/**Obtenemos Hora de ultima lectura en servidor GP **/
		
		List<ultima_lectura> ultimas_lecturas = new ArrayList<ultima_lectura>();
		String URL  ="jdbc:mysql://127.0.0.1:3306/gpconsul_pdc?autoReconnect=true&useSSL=false";
		String user ="root";
    	String pwd  ="";
    	
    	 try ( Connection connection = DriverManager.getConnection(URL,user,pwd);)
	     {
	 
			 CallableStatement statement = connection.prepareCall("{call ultimas_lecturas()}");
			 boolean hasResults = statement.execute();
			 if (hasResults) {
				  ResultSet rs = statement.getResultSet();
				  while (rs.next()) {
				     
					  ultimas_lecturas.add(new ultima_lectura(rs.getString (2), rs.getString (3)));
				       
				  }
				}
			 statement.close();				
		     System.out.println("Ready..last lectors is in the chache..");
		    
	 
	        } catch (SQLException ex) {
	        	System.out.println("Error handle Database GP");
	      }
    	 obtener_lecturas_remotas(ultimas_lecturas);   	
    	   	   	
    	
		
	}
	public static void obtener_lecturas_remotas(List<ultima_lectura> ultimas_lecturas)
	{
		List<lectura> lecturas = new ArrayList<lectura>();    
		
		/***********************************/
			String date_init = get_date_start() ;		
			String date_finish =  get_date_finish();	
			System.out.println("Actual datetime:"+date_finish);
		/***********************************/	
	    	String URL="jdbc:sqlserver://sqlsvrccazint01.database.windows.net:1433;";
	    	String database="database=MLCCSMADATA;";
	    	String user="user=SMA_rd_GPConsultores@sqlsvrccazint01;";
	    	String pwd ="password=czp5es?G_Y;";
	    	String SQL;
	    	String conexionUrl = URL+database+user+pwd;	   	
    	    	
    	try(Connection connection = DriverManager.getConnection(conexionUrl);)
		{
    		Statement s = connection.createStatement();	
    		for (int i = 0; i <= ultimas_lecturas.size()-1; i++) {
    			
    			 SQL ="select  IDEstacion, TIMESTAMP,Caudal,Conductividad,Nivel,PH,Temperatura from dbo.PozosSMA_GPConsultores  where TIMESTAMP BETWEEN '"+ultimas_lecturas.get(i).time+"' AND '"+date_finish+"' AND IDEstacion = '"+ultimas_lecturas.get(i).estacion+"';";
    			 System.out.println("Searching entries for "+ultimas_lecturas.get(i).estacion);
    			 ResultSet rs = s.executeQuery (SQL);
    			 while (rs.next())
    			 {
    	     
    			        lecturas.add(new lectura(rs.getString (1), rs.getString (2), rs.getString (3), rs.getString (4), rs.getString (5), rs.getString (6), rs.getString (7)));
  	  		      
    			 }
    			
    		}
    		connection.close();	
		   	System.out.println("Connection has been succesfull with MLCC Server..");
			System.out.println("Connection Closed ..");
		} 
		catch (SQLException e) 
		{
		  System.out.println("Error connection Extract server");
		}
    	
    	System.out.println("Total entries encountered :"+lecturas.size());
    	show_data_server(lecturas); //Show data encountered
    	transaction_server(lecturas); //Process data encountered 	   	
    	
    			
	}
	public static void get_actual_hour(){
		 
		  LocalDate today = LocalDate.now();
		  System.out.println(today);
	}
	public static void get_actual_time(){
		
		 Calendar calendar = Calendar.getInstance();		 
		 Calendar now = Calendar.getInstance();		 
		 int hours = calendar.get(Calendar.HOUR_OF_DAY);
		 int minutes = calendar.get(Calendar.MINUTE);
		 int seconds = calendar.get(Calendar.SECOND);		 
		 System.out.println(now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE));
		 System.out.println(hours+":"+minutes+":"+seconds);
		    
	}
	public static String sentence_sql(String data){
		
		String email = "erivas@gpconsultores.cl";
		return email;			
		
	}
	public static String get_date_finish(){
		
	
		  Calendar fecha = Calendar.getInstance();
		  String actual_date = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", fecha);
		  return actual_date;
    	
		
	}
    public static String  get_date_start(){
		
	
    	  Calendar fecha = Calendar.getInstance();
    	  fecha.add(Calendar.HOUR, -6);    	  
		  String actual_date = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", fecha);
		  return actual_date;   	 
    	
    	
		
	}
    public static void connection_server(String dateStart,String dateSend )
    {   
    	List<lectura> lecturas = new ArrayList<lectura>();    	
    	String URL="jdbc:sqlserver://sqlsvrccazint01.database.windows.net:1433;";
    	String database="database=MLCCSMADATA;";
    	String user="user=SMA_rd_GPConsultores@sqlsvrccazint01;";
    	String pwd ="password=czp5es?G_Y;";
    	String SQL ="select  IDEstacion, TIMESTAMP,Caudal,Conductividad,Nivel,PH,Temperatura from dbo.PozosSMA_GPConsultores  where TIMESTAMP BETWEEN '"+dateStart+"' AND '"+dateSend+"'";
    	String conexionUrl = URL+database+user+pwd;	
    	System.out.println(SQL);		
    	
		try(Connection connection = DriverManager.getConnection(conexionUrl);)
		{
			
		    Statement s = connection.createStatement();		  
		    ResultSet rs = s.executeQuery (SQL);
			System.out.println("Getting data...");
		    while (rs.next())
		    {
		      
		        lecturas.add(new lectura(rs.getString (1), rs.getString (2), rs.getString (3), rs.getString (4), rs.getString (5), rs.getString (6), rs.getString (7)));
		       		      
		    }
		    connection.close();		    
			System.out.println("Connection has been succesfull with  this server..");
			System.out.println("Connection Closed ..");
		} 
		catch (SQLException e) 
		{
		  System.out.println("Error Connection Extract Server");
		}
		System.out.println("extrac_lectors=>"+(lecturas.size()));
		
		
		//show_data_server(lecturas);
		//load_remote_server(lecturas);
		transaction_server(lecturas);
		
	
		
    }
    public static void transaction_server(List<lectura> lecturas)
    {
    	int dataSize = lecturas.size();
    	if(dataSize!=0)
    	{
    		System.out.println(dataSize+" Sending to remote server");
    		show_data_server(lecturas);
    		load_remote_server(lecturas);
    	}
    	else
    	{
    		System.out.println("List of lectors is empty");
    		
    	}
    }
    public static void load_remote_server(List<lectura> lecturas)
    {
    	
    	/*String URL  ="jdbc:mysql://gpcumplimiento.cl:3306/gpcumpli_enlinea?autoReconnect=true&useSSL=false";
    	String user ="gpcumpli_admin";
    	String pwd  ="30cuY2[OAgAr";*/
    	
    	String URL  ="jdbc:mysql://127.0.0.1:3306/gpconsul_pdc?autoReconnect=true&useSSL=false";
		String user ="root";
    	String pwd  ="";
    	
    	 try ( Connection connection = DriverManager.getConnection(URL,user,pwd);)
    	     {
		 
				 CallableStatement statement = connection.prepareCall("{call db_insert(?, ?, ?, ?, ?, ? ,?)}");
				 for (int i = 0; i <= lecturas.size()-1; i++)
				 {
					 System.out.println("processing..."+i+" of "+(lecturas.size()-1));
								
					    statement.setString(1, lecturas.get(i).estacion);
			            statement.setString(2, lecturas.get(i).time);
			            statement.setString(3, lecturas.get(i).ph);
			            statement.setString(4, convert_ce(lecturas.get(i).conductividad));
			            statement.setString(5, lecturas.get(i).temperatura);
			            statement.setString(6, lecturas.get(i).caudal);
			            statement.setString(7, lecturas.get(i).nivel);		 
			            statement.execute();          
			         					 
				 }
			     statement.close();		           
			     System.out.println("Stored procedure called successfully!");
		 
		        } catch (SQLException ex) {
		            ex.printStackTrace();
		        }
    	
    	 
    }
    public static String convert_ce(String conductividad)
    {   
        if(conductividad!=null){
        	
        	float f=Float.parseFloat(conductividad)*1000;    
        	return Float.toString(f);
        }
        else {
        	return null;
        }
    	
    }
    
    public static void show_data_server(List<lectura> lecturas){
    	
    	for (int i = 0; i <= lecturas.size()-1; i++)
		{
    		System.out.println("estacion: " + lecturas.get(i).estacion +" fecha:"+lecturas.get(i).time+ " caudal: " +lecturas.get(i).caudal+ " "+"conductividad:"+convert_ce(lecturas.get(i).conductividad)+" "+"nivel :"+lecturas.get(i).nivel+ "ph : "+lecturas.get(i).ph);
        }
    	    	
    }
    
    
    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
          sb.append((char) cp);
        }
        return sb.toString();
      }

      public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {

          InputStream is = new URL(url).openStream();
        try {
          BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
          String jsonText = readAll(rd);
          JSONObject json = new JSONObject(jsonText);
          return json;
        } finally {
          is.close();
        }
      }
    
   
    
  
    

}
