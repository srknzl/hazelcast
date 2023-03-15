/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Returns information about the queried Raft group.
 * <p>
 * This operation is committed to the Metadata group.
 */
public class GetRaftGroupOp extends MetadataRaftGroupOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    // Please note that targetGroupId is the Raft group that is being queried and groupId argument is the Metadata Raft group
    private CPGroupId targetGroupId;

    public GetRaftGroupOp() {
    }

    public GetRaftGroupOp(CPGroupId targetGroupId) {
        this.targetGroupId = targetGroupId;
    }

    @Override
    public Object run(MetadataRaftGroupManager metadataGroupManager, long commitIndex) {
        metadataGroupManager.checkMetadataGroupInitSuccessful();
        return metadataGroupManager.getGroup(targetGroupId);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.GET_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(targetGroupId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        targetGroupId = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", targetGroupId=").append(targetGroupId);
    }
}
