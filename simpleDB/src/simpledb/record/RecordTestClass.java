package simpledb.record;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;

public class RecordTestClass {
	/*
	– Random randomNumberGenerator
	– RandomStr randomStringGenerator (nuova classe per generare stringhe casuali)
	– Il metodo main lancia i test
	• Init new database
	• Schema definition
	• Table info
	• Insert
	• Read all records
	• Delete half
	• Read and calculate
	• Insert again
	• Read all records
	– Metodi insert, read, delete
	– Metodi per le statistiche
	*/

	public static void main(String[] args) {
		Connection conn = null;
		try {
			// ?
			SimpleDB.init("studentdb");
			
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			stmt.executeUpdate(s);
			System.out.println("Table STUDENT created.");

			s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
			
			int numberStudent = 10000;
			s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
			for (int i=1; i<=numberStudent; i++)
				stmt.executeUpdate(s + createRandomStudent(i));
			System.out.println("STUDENT records inserted.");

		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static String createRandomStudent(int i) {
//		"(1, 'joe', 10, 2004)"
		String s = "(" + i + ", " + generateName() + ", " + generateNumber() + ", " + generateYear() + ")";		
		return s;
	}
	
	private static String generateName() {
		char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
		StringBuilder sb = new StringBuilder();
		sb.append("'");
		Random random = new Random();
		for (int i = 0; i < 8; i++) {
		    char c = chars[random.nextInt(chars.length)];
		    sb.append(c);
		}
		sb.append("'");
		String output = sb.toString();
		return output;
	}
	
	private static int generateNumber() {
		Random r = new Random();
		int low = 1;
		int high = 30;
		return r.nextInt(high-low) + low;
	}
	
	private static int generateYear() {
		Random r = new Random();
		int low = 1980;
		int high = 2000;
		return r.nextInt(high-low) + low;
	}

}
