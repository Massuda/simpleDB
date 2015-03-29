package simpledb.record;

import java.util.*;

import simpledb.server.SimpleDB;
import simpledb.stats.BasicFileStats;
import simpledb.stats.BasicRecordStats;
import simpledb.tx.Transaction;
public class RecordTestClass {

	private static int STUDENT_QUANTITY=10000;
	private static int STUDENT_QUANTITY_SECOND_INSERT=7000;
	private static int STRING_LENGTH=25;
	private static String TABLE_FIELD_MATRICOLA="matricola";
	private static String TABLE_FIELD_NOME= "nome";
	private static String TABLE_FIELD_NUMEROCASUALE= "numcasuale";
	private static long READ_BLOCK=0;
	private static long WRITTEN_BLOCK=0;
	private static Random randomNumber = new Random();
	private static StringRandom randomString= new StringRandom();
	
	

public static void main(String [] args) {
	
	SimpleDB.init("studentDB");
	Schema schema= new Schema();
	schema.addIntField(TABLE_FIELD_MATRICOLA);
	schema.addStringField(TABLE_FIELD_NOME, STRING_LENGTH);
	schema.addIntField(TABLE_FIELD_NUMEROCASUALE);
	TableInfo infoTable= new TableInfo("students",schema);
	SimpleDB.fileMgr().resetMapStats();
	
	insertStudents(infoTable,STUDENT_QUANTITY);
	SimpleDB.fileMgr().resetMapStats();
	
	readStudents(infoTable);
	SimpleDB.fileMgr().resetMapStats();
	
	deleteStudents(infoTable,STUDENT_QUANTITY/2);
	SimpleDB.fileMgr().resetMapStats();
	
	readStudentsCalculate(infoTable);
	SimpleDB.fileMgr().resetMapStats();
	
	insertStudents(infoTable,STUDENT_QUANTITY_SECOND_INSERT);
	SimpleDB.fileMgr().resetMapStats();
	
	readStudents(infoTable);
	SimpleDB.fileMgr().resetMapStats();
	
	System.out.println("\n");
	System.out.println("Totale accessi al disco: "+ (READ_BLOCK+WRITTEN_BLOCK));
	

}

private static void insertStudents(TableInfo table, int qty) {
	Transaction tr= new Transaction();
	RecordFile record= new RecordFile(table,tr);
	
	for(int i=0; i<qty; i++){
		record.insert();
		record.setInt(TABLE_FIELD_MATRICOLA, i);
		record.setString(TABLE_FIELD_NOME, randomString.get(STRING_LENGTH));
		record.setInt(TABLE_FIELD_NUMEROCASUALE,randomNumber.nextInt(qty));
	}
	printRecordStats(record);
	printBlockStats(record.getFilename());
	record.close();
	tr.commit();
}

private static void readStudents(TableInfo table) {
	Transaction tr = new Transaction();
	RecordFile record= new RecordFile(table,tr);
	record.next();
	do {
		record.getInt(TABLE_FIELD_MATRICOLA);
		record.getString(TABLE_FIELD_NOME);
		record.getInt(TABLE_FIELD_NUMEROCASUALE);
	} while (record.next());
	
	printRecordStats(record);
	printBlockStats(record.getFilename());
	record.close();
	tr.commit();

}
private static void deleteStudents(TableInfo table, int x) {
Transaction tx= new Transaction();
RecordFile rfTable= new RecordFile(table,tx);
rfTable.next();
  do {
	  if(rfTable.getInt(TABLE_FIELD_MATRICOLA)>x) {
		  rfTable.delete();
	  }
  }
  while (rfTable.next());

		printRecordStats(rfTable);
		printBlockStats(rfTable.getFilename());
		rfTable.close();
		tx.commit();

  

}

private static void readStudentsCalculate(TableInfo table){
	Transaction tx= new Transaction();
	RecordFile rfTable= new RecordFile(table,tx);
	long sum=0;
	int students=0;
	rfTable.next();
	do {
		sum+=rfTable.getInt(TABLE_FIELD_NUMEROCASUALE);
		students+=1;
	} while(rfTable.next());
	System.out.println("Media: "+sum/students);
	printRecordStats(rfTable);
	printBlockStats(rfTable.getFilename());
	rfTable.close();
	tx.commit();
	}
	
private static void printRecordStats(RecordFile rfTable){
	Map<RID,BasicRecordStats> statsMap= rfTable.getStatsRecord();
	long totalRecordRead=0;
	long totalRecordWritten=0;
	System.out.println("\n");
	for(RID recordIDStats: statsMap.keySet()){
		BasicRecordStats currStats=statsMap.get(recordIDStats);
		totalRecordRead+=currStats.getReadFieldsRecord();
		totalRecordWritten+= currStats.getWrittenFieldsRecord();
	}
	System.out.println("Read record "+totalRecordRead);
	System.out.println("Written record "+totalRecordWritten);
	}


private static void printBlockStats(String fileName) {
	if(SimpleDB.fileMgr().getMapStats().containsKey(fileName)) {
		printBlockStats(fileName,SimpleDB.fileMgr().getMapStats().get(fileName));
	}
}
private static void printBlockStats(String fileName,BasicFileStats fileStats){
	System.out.println("Read record: "+fileStats.getBlockRead());
	System.out.println("Written record: "+fileStats.getBlockWritten());
	READ_BLOCK+= fileStats.getBlockRead();
	WRITTEN_BLOCK+= fileStats.getBlockWritten();
}
}
class StringRandom {
	private final char[] c = alphanumeric();
	private final Random rand;
	public StringRandom(){
		this(null);
	}
	public StringRandom(Random rand){
		this.rand=(rand!=null) ?  rand : new Random();
	}
	public String get(int len){
		StringBuffer out = new StringBuffer();
		while(out.length() < len){
			int idx=Math.abs((rand.nextInt() % c.length));
			out.append(c[idx]);
		}
		return out.toString();
	}
	private char[] alphanumeric(){
		StringBuffer buf=new StringBuffer(128);
		for(int i=65;  i<=90; i++){
			buf.append((char) i);
		}
		for(int i=97;  i<=122; i++){
			buf.append((char) i);
		}
		return buf.toString().toCharArray();
	}
}
