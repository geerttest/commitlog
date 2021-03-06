package net.geertvos.test.Logtest;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.model.HSlicePredicate;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class CassandraCommitLogDao {

	private ColumnFamilyTemplate<Integer, UUID> template;
	private Kryo kryo;
	private Output out = new Output(1,Integer.MAX_VALUE);

	public CassandraCommitLogDao(Keyspace ksp, String columnFamily) {
		kryo = new Kryo();
		kryo.register(Message.class, 100);
		kryo.addDefaultSerializer(UUID.class, new KryoUUIDSerializer());
		template = new ThriftColumnFamilyTemplate<Integer, UUID>(ksp,
				columnFamily,
                                                               IntegerSerializer.get(),
                                                               UUIDSerializer.get());

	}

	public void write(int partition, Message message) {
		ColumnFamilyUpdater<Integer, UUID> updater = template.createUpdater(partition);
		updater.setByteArray(message.getId(), serialize(message));
		template.update(updater);
	}
	
	public Message read(int partition, UUID id) {
		 ColumnFamilyResult<Integer, UUID> res = template.queryColumns(partition);
		 return deserialize(res.getByteArray(id));
	}
	
	public void delete(int partition, UUID id) {
		template.deleteColumn(partition, id);
	}
	
	private byte[] serialize(Message m) {
		out.clear();
		kryo.writeObject(out, m);
		return out.getBuffer();
	}
	
	private Message deserialize(byte[] data) {
		Input in = new Input(data);
		return kryo.readObject(in, Message.class);
	}

	public List<Message> getPartition(int partition) {
		HSlicePredicate<UUID> predicate = new HSlicePredicate<UUID>(UUIDSerializer.get());
		predicate.setRange(null, null, false, Integer.MAX_VALUE);
		ColumnFamilyResult<Integer, UUID> res = template.queryColumns(partition,predicate);
		ArrayList<Message> messages = new ArrayList<Message>();
		for(UUID column : res.getColumnNames()) {
			Message m = deserialize(res.getByteArray(column));
			messages.add(m);
		}
		return messages;
	}
}
