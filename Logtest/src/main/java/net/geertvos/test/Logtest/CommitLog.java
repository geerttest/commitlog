package net.geertvos.test.Logtest;

import java.util.List;


public class CommitLog {

	private CassandraCommitLogDao dao;
	private int partition;
	
	public CommitLog(int partition, CassandraCommitLogDao dao) {
		this.dao = dao;
		this.partition = partition;
	}
	
	public void commit(Message message) {
		dao.write(partition, message);
	}
	
	public void ack(Message message) {
		dao.delete(partition, message.getId());
	}
	
	public List<Message> getUnacked() {
		return dao.getPartition(partition);
	}
}
