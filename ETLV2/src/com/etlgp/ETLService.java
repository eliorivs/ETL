package com.etlgp;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.io.*;
import java.net.*;
import java.nio.charset.*;
import org.json.*;

import java.util.logging.*;



class MyFormatter extends Formatter {
    // Create a DateFormat to format the logger timestamp.
    private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");

    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        builder.append(df.format(new Date(record.getMillis()))).append(" - ");
        builder.append("[").append(record.getSourceClassName()).append(".");
        builder.append(record.getSourceMethodName()).append("] - ");
        builder.append("[").append(record.getLevel()).append("] - ");
        builder.append(formatMessage(record));
       /* builder.append("\n");*/
        builder.append(System.lineSeparator());
        return builder.toString();
    }

    public String getHead(Handler h) {
        return super.getHead(h);
    }

    public String getTail(Handler h) {
        return super.getTail(h);
    }
}

public class ETLService {
	
	private final static Logger LOGGER = Logger.getLogger("com.etlgp");
	
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
			
		    configure_logs();	
		    url_update();
  		
		
	}
	 private static void configure_logs() throws SecurityException, IOException {
		
		 boolean append = false;
		 Logger logger = Logger.getLogger("etl_log"); 
		 logger.setUseParentHandlers(append); 
		 String pathLog = "C:/Datos/MyLog.log";
		 FileHandler fhandler = new FileHandler(pathLog,true);  
		 
		 try {     

			    logger.addHandler(fhandler);		
			    MyFormatter formatter = new MyFormatter();
			    fhandler.setFormatter(formatter);  

			} catch (SecurityException e){  
			    e.printStackTrace();  
			    logger.info("error generating LOGS");
			} 			 
	 }	 
	 
	
	private static void url_update() throws IOException, JSONException
	{ 
		Logger logger = Logger.getLogger("etl_log");
		 
		 logger.info("Starting ...");
		 try {
			    System.out.println("Connecting to Web Service...");
			    logger.info("Connecting to web service...");
			    String url ="http://localhost/projects/PDCMLCC_WEB/PDCMLCC/controller/ultimas_lecturas.php";
			    JSONObject json = readJsonFromUrl(url);
			    String op1=((String)json.get("estatus"));
			    logger.info("Reply from Web Service : "+op1);
			    System.out.println("Web Service Status : "+op1);
			    obtener_ultimas_lecturas();
		    } catch (Exception e){
		        
		    	System.out.println("I cant resolve the URL..");
		    	logger.severe("impossible to resolve url "+e);
		    }
	}
		

	
	public static void obtener_ultimas_lecturas() throws IOException, JSONException
	{
		Logger logger = Logger.getLogger("etl_log");	
		
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
		     System.out.println("Ready.. last lectors are in the dynamic memory..");
		     logger.info("last readings obtained from gp database");
	 
	        } catch (SQLException ex) {
	        	System.out.println("Error handle gp database");
	        	 logger.info("Error handle gp database");
	      }
    	 obtener_lecturas_remotas(ultimas_lecturas);  
    	   	   	
		
	}
	public static void obtener_lecturas_remotas(List<ultima_lectura> ultimas_lecturas)
	{   
		Logger logger = Logger.getLogger("etl_log");	
		List<lectura> lecturas = new ArrayList<lectura>();
		int encountered = 0;
		
		/***********************************/
			String date_init = get_date_start() ;		
			String date_finish =  get_date_finish();	
			System.out.println("Actual Datetime : "+date_finish);
		/***********************************/	
	    	String URL="jdbc:sqlserver://sqlsvrccazint01.database.windows.net:1433;";
	    	String database="database=MLCCSMADATA;";
	    	String user="user=SMA_rd_GPConsultores@sqlsvrccazint01;";
	    	String pwd ="password=czp5es?G_Y;";
	    	String SQL;
	    	String conexionUrl = URL+database+user+pwd;	   	
    	    	
    	try(Connection connection = DriverManager.getConnection(conexionUrl);)
		{
    		logger.info("connection established with mlcc server");
    		Statement s = connection.createStatement();	
    		for (int i = 0; i <= ultimas_lecturas.size()-1; i++) {
    			
    			 encountered =0;
    			//SQL ="select  IDEstacion, TIMESTAMP,Caudal,Conductividad,Nivel,PH,Temperatura from dbo.PozosSMA_GPConsultores  where TIMESTAMP  BETWEEN '"+ultimas_lecturas.get(i).time+"' AND '"+date_finish+"' AND IDEstacion = '"+ultimas_lecturas.get(i).estacion+"';";
    			 SQL ="select  IDEstacion, TIMESTAMP,Caudal,Conductividad,Nivel,PH,Temperatura from dbo.PozosSMA_GPConsultores  where TIMESTAMP > '"+ultimas_lecturas.get(i).time+"' AND  TIMESTAMP <  '"+date_finish+"' AND IDEstacion = '"+ultimas_lecturas.get(i).estacion+"';"; 
    			// System.out.println(SQL);
    			// System.out.println("Searching entries for "+ultimas_lecturas.get(i).estacion+" "+percentage(i,ultimas_lecturas.size()));
    			 ResultSet rs = s.executeQuery (SQL);
    			 while (rs.next())
    			 {
    	     
    			        lecturas.add(new lectura(rs.getString (1), rs.getString (2), rs.getString (3), rs.getString (4), rs.getString (5), rs.getString (6), rs.getString (7)));
  	  		            encountered ++;
    			 }
    			 System.out.println("Searching entries for "+ultimas_lecturas.get(i).estacion+" >"+ultimas_lecturas.get(i).time+" Process => "+percentage(i,ultimas_lecturas.size()-1)+ " Result => "+ encountered );
     			     			
    		}
    		connection.close();	
    		logger.info("connection terminated with mlcc server");
		   	System.out.println("Connection has been succesfull with MLCC Server..");
			System.out.println("Connection Closed ..");
		} 
		catch (SQLException e) 
		{
		  System.out.println("Error connection Extract server");
		  logger.info("Error trying to extract readings");
		}
    	logger.info("total entries encountered in mlcc database : "+lecturas.size());
    	System.out.println("Total entries encountered : "+lecturas.size());    
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
	public static String percentage(int contador , int lecturas){
	    float p = contador*100/lecturas;
		return p+" %";	
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
		
		transaction_server(lecturas);	
	
		
    }
    public static void transaction_server(List<lectura> lecturas)
    {
    	
    	Logger logger = Logger.getLogger("etl_log");	
    	int dataSize = lecturas.size();
    	if(dataSize!=0)
    	{   
    		logger.info(dataSize+" entries have been sent to  gpserver");
    		System.out.println(dataSize+" Sending to remote server");
    		show_data_server(lecturas);
    		load_remote_server(lecturas);
    	}
    	else
    	{
    		System.out.println("List of lectors is empty");
    		logger.info("No entries found");
    		
    	}
    	logger.info("Bye.. see you soon.. ");
    }
    public static void load_remote_server(List<lectura> lecturas)
    {
    	Logger logger = Logger.getLogger("etl_log");	
    	/*String URL  ="jdbc:mysql://gpcumplimiento.cl:3306/gpcumpli_enlinea?autoReconnect=true&useSSL=false";
    	String user ="gpcumpli_admin";
    	String pwd  ="30cuY2[OAgAr";*/
    	
    	String URL  ="jdbc:mysql://127.0.0.1:3306/gpconsul_pdc?autoReconnect=true&useSSL=false";
		String user ="root";
    	String pwd  ="";
    	int errores =0;
    	 try ( Connection connection = DriverManager.getConnection(URL,user,pwd);){
    		 logger.info("transforming & sending data encountered to gpserver...");
				 CallableStatement statement = connection.prepareCall("{call db_insert(?, ?, ?, ?, ?, ? ,?, ?)}");
				 
				 for (int i = 0; i <= lecturas.size()-1; i++){
					 
					 System.out.println("processing... "+i+" of "+(lecturas.size()-1)+" tasks "+ percentage(i,lecturas.size()-1));								
					 statement.setString(1,  lecturas.get(i).estacion);
					 statement.setString(2,  get_date_finish());
			         statement.setString(3,  lecturas.get(i).time);
			         statement.setString(4,  convert_2f(lecturas.get(i).ph));
			         statement.setString(5,  convert_ce(lecturas.get(i).conductividad));
	                 statement.setString(6,  convert_2f(lecturas.get(i).temperatura));
		             statement.setString(7,  convert_2f(lecturas.get(i).caudal));
		             statement.setString(8,  convert_2f(lecturas.get(i).nivel));		 
		            
		             try{
		            	   statement.execute(); 
		             }
		             catch (SQLException e){  
		                	 errores++;
		            		 logger.severe("error :"+e);
		             }            
		             	             
		            			         					 
				 }
			     statement.close();	
			     logger.info("connection with gp database is closed.. ");
			     System.out.println("Stored procedure called successfully!");
			     if(errores!=0){
			    	  
			    	 logger.severe("data wasnt sent.. you have "+errores+" errors");
			    	
			     }
			     else{
			    	 
			    	 logger.info("data was sent successfully..");
			     }
			     logger.info("Bye ;) ");		 
		 
		        } catch (SQLException ex) {
		            ex.printStackTrace();
		            logger.info("error connecting to gp database ");
		        }
    	
    	 
    }
    public static String convert_ce(String conductividad)
    {   
        if(conductividad!=null){
        	
        	float f=Float.parseFloat(conductividad)*1000;    
        	return String.format("%.0f", f);
        }
        else{
        	
        	return null;
        }
    	
    }
    public static String convert_2f(String dato) {
    	
    	  if(dato!=null){
    		  
    		  float f=Float.parseFloat(dato)*1;    
          	  return String.format("%.2f", f);        	
          
          }
          else{
          	
        	  return null;
          }
    	
    }
    
    
    public static void show_data_server(List<lectura> lecturas){
    	
    	for (int i = 0; i <= lecturas.size()-1; i++){
    		
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
