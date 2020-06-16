package consensus.abstractions;

import consensus.Paxos;

public interface Abstraction {
    boolean handle(Paxos.Message message);
}
