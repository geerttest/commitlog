package net.geertvos.test.Logtest;

import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoUUIDSerializer extends Serializer<UUID> {

	@Override
	public UUID read(Kryo arg0, Input arg1, Class arg2) {
		long leastSigBits = arg1.readLong();
		long mostSigBits =  arg1.readLong();
		UUID uuid = new UUID(mostSigBits, leastSigBits);
		return uuid;
	}

	@Override
	public void write(Kryo arg0, Output arg1, UUID arg2) {
		arg1.writeLong(arg2.getLeastSignificantBits());
		arg1.writeLong(arg2.getMostSignificantBits());
	}
	
}
