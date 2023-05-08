package com.hazelcast.map.impl.operation;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanPublisher;

public class PublishWanEventOperation extends Operation implements PartitionAwareOperation {
    private final String wanReplicationName;
    private final String wanPublisherId;
    private final WanEvent wanEvent;

    public PublishWanEventOperation(String wanReplicationName, String wanPublisherId, int partitionId, WanEvent wanEvent) {
        this.wanReplicationName = wanReplicationName;
        this.wanPublisherId = wanPublisherId;
        this.wanEvent = wanEvent;
        this.setPartitionId(partitionId);
    }

    @Override
    public void run() throws Exception {
        WanPublisher publisher = this.getNodeEngine().getWanReplicationService().getPublisherOrFail(this.wanReplicationName, this.wanPublisherId);
        publisher.publishReplicationEvent(wanEvent);
    }

    @Override
    public Object getResponse() {
        return super.getResponse();
    }
}
