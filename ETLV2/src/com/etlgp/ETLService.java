package com.etlgp;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;



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
		

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String date_finish, date_init;					
		System.out.println("ETL - Service");
		date_init = get_date_start() ;		
		date_finish =  get_date_finish();		
		System.out.println("start search:"+date_init);
		System.out.println("end search:"+date_finish);
		connection_server(date_init, date_finish);
	
		
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
		  System.out.println("Error Connection Extrac Server");
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
    	
    	String URL  ="jdbc:mysql://gpcumplimiento.cl:3306/gpcumpli_enlinea?autoReconnect=true&useSSL=falsejdbc:mysql://gpcumplimiento.cl:3306/gpcumpli_enlinea?autoReconnect=true&useSSL=false";
    	String user ="gpcumpli_admin";
    	String pwd  ="30cuY2[OAgAr";
    	
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
    
  
    

}
