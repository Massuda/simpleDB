package simpledb.stats;

public class BasicFileStats {
	private int blockRead;
	private int blockWritten;
	
	public void blockReadIncrementer(){
		this.blockRead++;
	}
	
	public void blockWrittenIncrementer(){
		this.blockWritten++;
	}

	public int getBlockRead() {
		return blockRead;
	}
	
	public void setBlockRead(int blockRead) {
		this.blockRead = blockRead;
	}
	
	public int getBlockWritten() {
		return blockWritten;
	}
	
	public void setBlockWritten(int blockWritten) {
		this.blockWritten = blockWritten;
	}
	

}
