package io.trino.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.relation.Extension;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TableExtensionDetailConverter {
    private TableExtensionDetailConverter() {}

    public static Extension.ExtensionTableDetail getExtensionTableDetail(Any any) throws InvalidProtocolBufferException {
        // Empty wrapper to store the protobuf
        return new Extension.ExtensionTableDetail() {
            @Override
            public NamedStruct deriveSchema() {
                return NamedStruct.of(Collections.emptyList(), Type.Struct.builder().nullable(true).build());
            }

            @Override
            public Any toProto() {
                return any;
            }
        };
//        if (Objects.equals(any.getTypeUrl(), "type.googleapis.com/%s".formatted(Extensions.EventPlatformExtensionTable.getDescriptor().getFullName()))) {
//            return new EventPlatformExtensionTable(any.unpack(Extensions.EventPlatformExtensionTable.class));
//        }
//        throw new IllegalArgumentException("Unknown extension table: %s".formatted(any.getTypeUrl()));
    }

//    public record EventPlatformExtensionTable(Extensions.EventPlatformExtensionTable extensionTable) implements Extension.ExtensionTableDetail {
//        @Override
//        public Any toProto() {
//            return Any.pack(this.extensionTable);
//        }
//
//        @Override
//        public NamedStruct deriveSchema() {
//            return NamedStruct.of(
//                    List.of("timestamp"),
//                    Type.Struct.builder()
//                            .addFields(Type.I64.builder().nullable(false).build())
//                            .nullable(false)
//                            .build()
//            );
//        }
//    }
}
