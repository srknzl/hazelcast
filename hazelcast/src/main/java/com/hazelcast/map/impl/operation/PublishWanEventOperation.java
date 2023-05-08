package com.hazelcast.map.impl.operation;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanPublisher;

public class PublishWanEventOperation extends Operation implements PartitionAwareOperation {
    private String wanReplicationName;
    private String wanPublisherId;
    private WanEvent wanEvent;

    public PublishWanEventOperation() {
    }

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
}
