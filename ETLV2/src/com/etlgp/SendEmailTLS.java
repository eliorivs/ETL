package com.etlgp;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class SendEmailTLS {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		    final String username = "erivas@gpconsultores.cl";
	        final String password = "gp292021";

	        Properties prop = new Properties();
			prop.put("mail.smtp.host", "smtp.gmail.com");
	        prop.put("mail.smtp.port", "587");
	        prop.put("mail.smtp.auth", "true");
	        prop.put("mail.smtp.starttls.enable", "true"); //TLS
	        
	        Session session = Session.getInstance(prop,
	                new javax.mail.Authenticator() {
	                    protected PasswordAuthentication getPasswordAuthentication() {
	                        return new PasswordAuthentication(username, password);
	                    }
	                });
 
	        try {

	            Message message = new MimeMessage(session);
	            message.setFrom(new InternetAddress("gp@gpconsultores.cl"));
	            message.setRecipients(
	                    Message.RecipientType.TO,
	                    InternetAddress.parse("erivas@gpconsultores.cl, ing.eliorivas@gmail.com")
	            );
	            message.setSubject("Testing Gmail TLS");
	            message.setText("Testing send mails from java,"
	                    + "\n\n Please do not spam my email!");

	            Transport.send(message);

	            System.out.println("Done");

	        } catch (MessagingException e) {
	            e.printStackTrace();
	        }

	}

}
