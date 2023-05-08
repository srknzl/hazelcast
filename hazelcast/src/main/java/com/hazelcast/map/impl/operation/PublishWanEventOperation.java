package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanPublisher;


public class PublishWanEventOperation extends Operation implements PartitionAwareOperation, IdentifiedDataSerializable {
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

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_WAN_PUBLISH_EVENT;
    }
}
