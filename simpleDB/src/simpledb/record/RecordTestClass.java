package simpledb.record;

import java.util.*;
import simpledb.server.SimpleDB;
import simpledb.stats.BasicFileStats;
import simpledb.stats.BasicRecordStats;
import simpledb.tx.Transaction;
public class RecordTestClass {

	private static String MATRICOLA="matricola";
	private static String NOME= "nome";
	private static String NUMERO_RANDOM= "randomNumber";
	private static long READ_BLOCK=0;
	private static long WRITTEN_BLOCK=0;
	private static Random randomNumber = new Random();
	private static RandomStr randomString= new RandomStr();


	public static void main(String [] args) {

		SimpleDB.init("studentDB");
		Schema schemaTable= new Schema();
		schemaTable.addIntField(MATRICOLA);
		schemaTable.addStringField(NOME, 20);
		schemaTable.addIntField(NUMERO_RANDOM);
		TableInfo tableInfo= new TableInfo("studenti",schemaTable);
		SimpleDB.fileMgr().resetMapStats();

		insertStudents(tableInfo,10000);
		SimpleDB.fileMgr().resetMapStats();
		readStudents(tableInfo);
		SimpleDB.fileMgr().resetMapStats();
		deleteStudents(tableInfo,10000/2);
		SimpleDB.fileMgr().resetMapStats();
		readStudentsCalculate(tableInfo);
		SimpleDB.fileMgr().resetMapStats();
		insertStudents(tableInfo,7000);
		SimpleDB.fileMgr().resetMapStats();
		readStudents(tableInfo);
		SimpleDB.fileMgr().resetMapStats();

		System.out.println("\n");
		System.out.println("Accessi totali al disco: "+ (READ_BLOCK+WRITTEN_BLOCK));
	}

	private static void insertStudents(TableInfo table, int qty) {
		Transaction tx= new Transaction();
		RecordFile rfTable= new RecordFile(table,tx);

		for(int i=0; i<qty; i++){
			rfTable.insert();
			rfTable.setInt(MATRICOLA, i);
			rfTable.setString(NOME, randomString.get(20));
			rfTable.setInt(NUMERO_RANDOM,randomNumber.nextInt(qty));

		}
		printRecordStats(rfTable);
		printBlockStats(rfTable.getFilename());
		rfTable.close();
		tx.commit();
	}

	private static void readStudents(TableInfo table) {
		Transaction tx = new Transaction();
		RecordFile rfTable= new RecordFile(table,tx);
		rfTable.next();

		do {
			rfTable.getInt(MATRICOLA);
			rfTable.getString(NOME);
			rfTable.getInt(NUMERO_RANDOM);
		} while (rfTable.next());

		printRecordStats(rfTable);
		printBlockStats(rfTable.getFilename());
		rfTable.close();
		tx.commit();

	}
	private static void deleteStudents(TableInfo table, int studentQuality) {
		Transaction tx= new Transaction();
		RecordFile rfTable= new RecordFile(table,tx);
		rfTable.next();
		do {
			if(rfTable.getInt(MATRICOLA)>studentQuality) {
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
		long sumCasuale=0;
		int studentsCount=0;
		rfTable.next();
		do {
			sumCasuale+=rfTable.getInt(NUMERO_RANDOM);
			studentsCount+=1;
		} while(rfTable.next());
		System.out.println("AVG: "+sumCasuale/studentsCount);
		printRecordStats(rfTable);
		printBlockStats(rfTable.getFilename());
		rfTable.close();
		tx.commit();
	}

	private static void printRecordStats(RecordFile rfTable){
		Map<RID,BasicRecordStats> statsMap= rfTable.getStatsRecord();
		long totalRecordRead=0;
		long totalRecordWritten=0;
		long totalRecordFieldRead=0;
		long totalRecordFieldWritten=0;
		System.out.println("\n");
		for(RID recordIDStats: statsMap.keySet()){
			BasicRecordStats currStats=statsMap.get(recordIDStats);
			totalRecordRead+=currStats.getReadFieldsRecord();
			totalRecordWritten+= currStats.getWrittenFieldsRecord();
		}
		System.out.println("Record read "+totalRecordRead);
		System.out.println("Record written "+totalRecordWritten);
	}

	private static void printAllBlockStats(){
		Map<String,BasicFileStats> statsMap=
				SimpleDB.fileMgr().getMapStats();
		long totalBlockRead=0;
		long totalBlockWritten=0;
		System.out.println("\n");
		//System.out.println("FILENAME\t\tBLK R\tBLK W");
		for(String fileName: statsMap.keySet()) {
			BasicFileStats bTemp=statsMap.get(fileName);
			totalBlockRead+=bTemp.getBlockRead();
			totalBlockWritten+=bTemp.getBlockWritten();
			printBlockStats(fileName,bTemp);
			System.out.println("+\n");
			System.out.println("total\t\t "+totalBlockRead+ "\t"+totalBlockWritten);



		}


	}
	private static void printBlockStats(String fileName) {
		if(SimpleDB.fileMgr().getMapStats().containsKey(fileName)) {
			printBlockStats(fileName,SimpleDB.fileMgr().getMapStats().get(fileName));
		}
	}
	private static void printBlockStats(String fileName,BasicFileStats fileStats){
		System.out.println("Blocks read: "+fileStats.getBlockRead());
		System.out.println("Blocks written: "+fileStats.getBlockWritten());
		READ_BLOCK+= fileStats.getBlockRead();
		WRITTEN_BLOCK+= fileStats.getBlockWritten();
	}
}
class RandomStr {
	private final char[] alphanumeric = alphanumeric();
	private final Random rand;
	public RandomStr(){
		this(null);
	}
	public RandomStr(Random rand){
		this.rand=(rand!=null) ?  rand : new Random();
	}
	public String get(int len){
		StringBuffer out = new StringBuffer();
		while(out.length() < len){
			int idx=Math.abs((rand.nextInt() % alphanumeric.length));
			out.append(alphanumeric[idx]);
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
