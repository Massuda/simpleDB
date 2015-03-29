package simpledb.stats;

public class BasicRecordStats {
	private int readRecord;
	private int writtenRecord;
	private int readFieldsRecord;
	private int writtenFieldsRecord;
	
	public void readRecordIncrementer(){
		this.readRecord++;
	}
	
	public void writtenRecordIncrementer(){
		this.writtenRecord++;
	}
	
	public void readFieldsRecordIncrementer(){
		this.readFieldsRecord++;
	}
	
	public void writtenFieldsRecordIncrementer(){
		this.writtenFieldsRecord++;
	}
	
	public int getReadRecord() {
		return readRecord;
	}
	public void setReadRecord(int readRecord) {
		this.readRecord = readRecord;
	}
	public int getWrittenRecord() {
		return writtenRecord;
	}
	public void setWrittenRecord(int writtenRecord) {
		this.writtenRecord = writtenRecord;
	}
	public int getReadFieldsRecord() {
		return readFieldsRecord;
	}
	public void setReadFieldsRecord(int readFieldsRecord) {
		this.readFieldsRecord = readFieldsRecord;
	}
	public int getWrittenFieldsRecord() {
		return writtenFieldsRecord;
	}
	public void setWrittenFieldsRecord(int writtenFieldsRecord) {
		this.writtenFieldsRecord = writtenFieldsRecord;
	}
	
	
}
