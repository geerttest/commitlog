package net.geertvos.test.Logtest;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Hello world!
 * 
 */
public class App {
	public static void main(String[] args) throws InterruptedException {
		Thread.sleep(30000);
		Cluster myCluster = HFactory.getOrCreateCluster("Geert Cluster", "localhost:9160");
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("MyKeyspace", "ColumnFamilyName", ComparatorType.BYTESTYPE);

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("MyKeyspace", ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(cfDef));
		
		KeyspaceDefinition keyspaceDef = myCluster.describeKeyspace("MyKeyspace");
		if(keyspaceDef == null) {
			myCluster.addKeyspace(newKeyspace, true);
		}
		
		Keyspace ksp = HFactory.createKeyspace("MyKeyspace", myCluster);
		CassandraCommitLogDao dao = new CassandraCommitLogDao(ksp, "ColumnFamilyName");
		CommitLog log = new CommitLog(1, dao);

		long start = System.currentTimeMillis();
		int x = 10000;
		for(int i=0;i<x;i++) {
			Message message = new Message("Hello there",UUID.randomUUID());
			log.commit(message);
		}
		long total = System.currentTimeMillis() - start;
		System.out.println("It took "+total+"ms to write "+x+" messages");
		long start2 = System.currentTimeMillis();
		List<Message> unacked = log.getUnacked();
		long total2 = System.currentTimeMillis() - start2;
		System.out.println("It took "+total2+"ms to read "+x+" messages");
		long start3 = System.currentTimeMillis();
		for(Message m : unacked){
			log.ack(m);
		}
		long total3 = System.currentTimeMillis() - start3;
		System.out.println("It took "+total3+"ms to ack "+x+" messages");
	}
}
