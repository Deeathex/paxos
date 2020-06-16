package consensus.utils;

import consensus.Paxos;

public class ValueUtils {
    public static Paxos.Value getUndefinedValue() {
        return Paxos.Value.newBuilder()
                .setDefined(false)
                .build();
    }

    public static Paxos.Value makeCopy(Paxos.Value value) {
        return Paxos.Value.newBuilder()
                .setDefined(value.getDefined())
                .setV(value.getV())
                .build();
    }
}
