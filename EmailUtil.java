package com.secoo.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.internet.MimeUtility;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.*;  
import org.apache.hadoop.fs.FileSystem;  
import java.io.*;  
import java.net.URI;  
import java.net.URISyntaxException;  
import java.util.ArrayList;  
import java.util.List; 
//import org.apache.hadoop.io.IOUtils;

/**
 * 功能：用于实现邮件获取，获取其发送日期，附件，内容，并将附件存储到hdfs
 * 
 * @author Secoo
 *
 */
public class EmailUtil {

	Logger logger = Logger.getLogger(EmailUtil.class);
	private Properties props = new Properties();

	public void doMail(final String user, final String password, String host, String auth, String protocol)
			throws Exception {
		props.put("mail.pop3.host", host);
		props.put("mail.pop3.auth", auth);
		props.put("mail.transport.protocol", protocol);

		Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(user, password);
			}
		});

		logger.info("UserName:[" + user + "] Host:[" + host + "] Authentication:[" + auth + "] Protocol:[" + protocol
				+ "] ");
		Store store = session.getStore("pop3s");
		store.connect(host, user, password);
		Folder folder = store.getFolder("INBOX");
		folder.open(Folder.READ_ONLY);
		Message[] messages = folder.getMessages();
		logger.info("The count of the Email is :" + messages.length);
		// int count = 1;
		for (Message message : messages) {
			// System.out.println("-------");
			// System.out.println("Email No."+ count++);
			// System.out.println("Subject: "+ message.getSubject());
			// System.out.println("From: "+ message.getFrom()[0]);
			// System.out.println("Text: "+ message.getContent().toString());
			// System.out.println("Sent: "+ message.getSentDate());

			if (message.getSubject() != null) {
				Object o = message.getContent();
				if (o instanceof Multipart) {
					Multipart multipart = (Multipart) o;
					// System.out.println("Multipart-->");
					// System.out.println(multipart.getContentType());
					String multipart_content = multipart.getContentType();
					if (multipart_content != null && multipart_content.contains("multipart/mixed")) // 判断是否带附件
					{
						logger.info(message.getSubject());
						logger.info("带附件的.获取附件");
						reMultipart(multipart);
					}
				}
			}
		}
	}

	/**
	 * @param multipart
	 * @throws Exception
	 */
	private void reMultipart(Multipart multipart) throws Exception {

		for (int j = 0, n = multipart.getCount(); j < n; j++) {

			Part part = multipart.getBodyPart(j);

			if (part.getContent() instanceof Multipart) {
				Multipart p = (Multipart) part.getContent();

				reMultipart(p);
			} else {
				rePart(part);
			}
		}
	}

	private void rePart(Part part)
			throws MessagingException, UnsupportedEncodingException, IOException, FileNotFoundException {
		if (part.getDisposition() != null) {
			logger.info("========================================================");
			String strFileNmae = MimeUtility.decodeText(part.getFileName());
			logger.info("发现附件: " + MimeUtility.decodeText(part.getFileName()));
			logger.info("内容类型: " + MimeUtility.decodeText(part.getContentType()));
			logger.info("附件内容: " + part.getContent());
			
			
			
			InputStream in =part.getInputStream();// 附件输入流
			
			
			byte[] byteArrayContent = IOUtils.toByteArray(in);
			String xxx = new String(byteArrayContent);
			System.out.println("附件内容："+xxx);
			
			createFile("hdfs://center1.secoo-inc.com:8020/mail/files/aaa", byteArrayContent);
			List<String> list = IOUtils.readLines(in, "utf-8");

			for (int i = 0; i < list.size(); i++) {
				logger.info("line " + (i) + list.get(i));
			}

			// to hdfs

//			java.io.FileOutputStream out = new FileOutputStream(strFileNmae);
//			int data;
//			while ((data = in.read()) != -1) {
//				out.write(data);
//			}
//			in.close();
//			out.close();
		} else {
			if (part.getContentType().startsWith("text/plain")) {
				logger.info("文本内容: " + part.getContent());

			} else {
				// System.out.println("HTML内容：" + part.getContent());
			}
		}
	}
	
	
	//conf.set("fs.defaultFS", "hdfs://ssmaster:9000/")
    public void createFile(String dst, byte[] contents)  
            throws IOException {  
    	
    	//org.apache.hadoop.io.IOUtils.
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://center1.secoo-inc.com:8020/");
//        conf.set("hadoop.user.name", "hdfs");
        Path dstPath = new Path(dst);  
        FileSystem fs = dstPath.getFileSystem(conf);  
  
        FSDataOutputStream outputStream = fs.create(dstPath);  
        outputStream.write(contents,0,contents.length); 
        
        outputStream.close();  
        System.out.println("create file " + dst + " success!");  
        fs.close();  
    }  

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:\\hadoop2.7.1");
		System.setProperty("user.name", "hdfs");
		System.out.println(System.getProperties());
		EmailUtil mail = new EmailUtil();
		try {
			mail.doMail("1384166@qq.com", "lomaexvxgnmncaad", "pop.qq.com", "true", "pop3");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
