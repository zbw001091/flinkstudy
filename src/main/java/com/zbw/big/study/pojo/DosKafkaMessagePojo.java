package com.zbw.big.study.pojo;

public class DosKafkaMessagePojo {

	public DosKafkaMessagePojo() {
	}

	private String table;
	private String op_type;
	private TmMkOpportunity tmmkopportunity;
	
	public String getOpType() {
		return op_type;
	}
	public void setOpType(String op_type) {
		this.op_type = op_type;
	}
	public TmMkOpportunity getTmmkopportunity() {
		return tmmkopportunity;
	}
	public void setTmmkopportunity(TmMkOpportunity tmmkopportunity) {
		this.tmmkopportunity = tmmkopportunity;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
}
