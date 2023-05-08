package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.io.IOException;


public class PublishWanEventOperation extends MapOperation implements PartitionAwareOperation, IdentifiedDataSerializable {
    private String wanReplicationName;
    private String wanPublisherId;
    private InternalWanEvent wanEvent;

    public PublishWanEventOperation() {
    }

    public PublishWanEventOperation(String wanReplicationName, String wanPublisherId, int partitionId, InternalWanEvent wanEvent) {
        this.wanReplicationName = wanReplicationName;
        this.wanPublisherId = wanPublisherId;
        this.wanEvent = wanEvent;
        this.setPartitionId(partitionId);
    }

    @Override
    protected void runInternal() {
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

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(wanEvent);
        out.writeString(wanReplicationName);
        out.writeString(wanPublisherId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        wanEvent = in.readObject();
        wanReplicationName = in.readString();
        wanPublisherId = in.readString();
    }
}
