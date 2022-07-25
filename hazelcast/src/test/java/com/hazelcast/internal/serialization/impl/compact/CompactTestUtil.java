/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import example.serialization.EmployeeDTO;
import example.serialization.ExternalizableEmployeeDTO;
import example.serialization.FixedFieldsDTO;
import example.serialization.InnerDTO;
import example.serialization.MainDTO;
import example.serialization.NamedDTO;
import example.serialization.VarFieldsDTO;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class CompactTestUtil {

    private CompactTestUtil() {

    }

    @Nonnull
    static GenericRecord createCompactGenericRecord(MainDTO mainDTO) {
        InnerDTO inner = mainDTO.p;
        GenericRecord[] namedRecords = new GenericRecord[inner.nn.length];
        int i = 0;
        for (NamedDTO named : inner.nn) {
            GenericRecord namedRecord = GenericRecordBuilder.compact("named")
                    .setString("name", named.name)
                    .setInt32("myint", named.myint).build();
            namedRecords[i++] = namedRecord;
        }

        GenericRecord innerRecord = GenericRecordBuilder.compact("inner")
                .setArrayOfInt8("b", inner.bytes)
                .setArrayOfInt16("s", inner.shorts)
                .setArrayOfInt32("i", inner.ints)
                .setArrayOfInt64("l", inner.longs)
                .setArrayOfFloat32("f", inner.floats)
                .setArrayOfFloat64("d", inner.doubles)
                .setArrayOfString("strings", inner.strings)
                .setArrayOfGenericRecord("nn", namedRecords)
                .setArrayOfDecimal("bigDecimals", inner.bigDecimals)
                .setArrayOfTime("localTimes", inner.localTimes)
                .setArrayOfDate("localDates", inner.localDates)
                .setArrayOfTimestamp("localDateTimes", inner.localDateTimes)
                .setArrayOfTimestampWithTimezone("offsetDateTimes", inner.offsetDateTimes)
                .build();

        return GenericRecordBuilder.compact("main")
                .setInt8("b", mainDTO.b)
                .setBoolean("bool", mainDTO.bool)
                .setInt16("s", mainDTO.s)
                .setInt32("i", mainDTO.i)
                .setInt64("l", mainDTO.l)
                .setFloat32("f", mainDTO.f)
                .setFloat64("d", mainDTO.d)
                .setString("str", mainDTO.str)
                .setDecimal("bigDecimal", mainDTO.bigDecimal)
                .setGenericRecord("p", innerRecord)
                .setTime("localTime", mainDTO.localTime)
                .setDate("localDate", mainDTO.localDate)
                .setTimestamp("localDateTime", mainDTO.localDateTime)
                .setTimestampWithTimezone("offsetDateTime", mainDTO.offsetDateTime)
                .setNullableInt8("nullable_b", mainDTO.b)
                .setNullableBoolean("nullable_bool", mainDTO.bool)
                .setNullableInt16("nullable_s", mainDTO.s)
                .setNullableInt32("nullable_i", mainDTO.i)
                .setNullableInt64("nullable_l", mainDTO.l)
                .setNullableFloat32("nullable_f", mainDTO.f)
                .setNullableFloat64("nullable_d", mainDTO.d)
                .build();
    }

    @Nonnull
    private static InnerDTO createInnerDTO() {
        NamedDTO[] nn = new NamedDTO[2];
        nn[0] = new NamedDTO("name", 123);
        nn[1] = new NamedDTO("name", 123);
        return new InnerDTO(new boolean[]{true, false}, new byte[]{0, 1, 2}, new char[]{'0', 'a', 'b'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321},
                new String[]{"test", null}, nn,
                new BigDecimal[]{new BigDecimal("12345"), new BigDecimal("123456")},
                new LocalTime[]{LocalTime.now(), null, LocalTime.now()},
                new LocalDate[]{LocalDate.now(), null, LocalDate.now()},
                new LocalDateTime[]{LocalDateTime.now(), null},
                new OffsetDateTime[]{OffsetDateTime.now()},
                new Boolean[]{true, false, null},
                new Byte[]{0, 1, 2, null}, new Character[]{'i', null, '9'},
                new Short[]{3, 4, 5, null}, new Integer[]{9, 8, 7, 6, null}, new Long[]{0L, 1L, 5L, 7L, 9L, 11L},
                new Float[]{0.6543f, -3.56f, 45.67f}, new Double[]{456.456, 789.789, 321.321});
    }

    @Nonnull
    static MainDTO createMainDTO() {
        InnerDTO inner = createInnerDTO();
        return new MainDTO((byte) 113, true, '\u1256', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main object created for testing!", inner,
                new BigDecimal("12312313"), LocalTime.now(), LocalDate.now(), LocalDateTime.now(), OffsetDateTime.now(),
                (byte) 113, true, '\u4567', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d);
    }

    @Nonnull
    static VarFieldsDTO createVarFieldsDTO() {
        InnerDTO inner = createInnerDTO();
        return new VarFieldsDTO(new boolean[]{true, false}, new byte[]{0, 1, 2},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, "test",
                new String[]{"test", null}, new BigDecimal("12345"),
                new BigDecimal[]{new BigDecimal("12345.123123123"), null},
                null, new LocalTime[]{LocalTime.now(), null, LocalTime.now()},
                LocalDate.now(),  new LocalDate[]{LocalDate.now(), null, LocalDate.now()},
                LocalDateTime.now(), new LocalDateTime[]{LocalDateTime.now(), null},
                OffsetDateTime.now(), null, inner, new InnerDTO[]{createInnerDTO(), null},
                null, new Boolean[]{true, false, null}, (byte) 123, new Byte[]{0, 1, 2, null},
                (short) 1232, new Short[]{3, 4, 5, null}, null, new Integer[]{9, 8, 7, 6, null},
                12323121331L, new Long[]{0L, 1L, 5L, 7L, 9L, 12323121331L}, 0.6543f,
                new Float[]{0.6543f, -3.56f, 45.67f}, 456.4123156,
                new Double[]{456.4123156, 789.789, 321.321});
    }

    @Nonnull
    static FixedFieldsDTO createFixedFieldsDTO() {
        return new FixedFieldsDTO((byte) 1, true, (short) 1231, 123123123, 123123123123L, 12312.123f,
                1111.1111111123123);
    }

    public static InternalSerializationService createSerializationService(SchemaService schemaService) {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
    }

    public static SchemaService createInMemorySchemaService() {
        return new SchemaService() {
            private final Map<Long, Schema> schemas = new ConcurrentHashMap<>();

            @Override
            public Schema get(long schemaId) {
                return schemas.get(schemaId);
            }

            @Override
            public void put(Schema schema) {
                long schemaId = schema.getSchemaId();
                Schema existingSchema = schemas.putIfAbsent(schemaId, schema);
                if (existingSchema != null && !schema.equals(existingSchema)) {
                    throw new IllegalStateException("Schema with schemaId " + schemaId + " already exists. "
                            + "existing schema " + existingSchema
                            + "new schema " + schema);
                }
            }

            @Override
            public void putLocal(Schema schema) {
                put(schema);
            }
        };
    }

    public static void verifyReflectiveSerializerIsUsed(SerializationConfig serializationConfig) {
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(CompactTestUtil.createInMemorySchemaService())
                .setConfig(serializationConfig)
                .build();

        ExternalizableEmployeeDTO object = new ExternalizableEmployeeDTO();
        Data data = serializationService.toData(object);
        assertFalse(object.usedExternalizableSerialization());

        ExternalizableEmployeeDTO deserializedObject = serializationService.toObject(data);
        assertFalse(deserializedObject.usedExternalizableSerialization());
    }

    public static void verifyExplicitSerializerIsUsed(SerializationConfig serializationConfig) {
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(CompactTestUtil.createInMemorySchemaService())
                .setConfig(serializationConfig)
                .build();

        EmployeeDTO object = new EmployeeDTO(1, 1);
        Data data = serializationService.toData(object);

        EmployeeDTO deserializedObject = serializationService.toObject(data);
        assertEquals(object, deserializedObject);
    }
}
