package io.trino.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Extension;
import io.substrait.relation.ProtoRelConverter;

import java.io.IOException;

public class CustomProtoPlanConverter extends ProtoPlanConverter {
    public CustomProtoPlanConverter() throws IOException {}

    @Override
    protected ProtoRelConverter getProtoRelConverter(ExtensionLookup functionLookup) {
        return new CustomProtoRelConverter(functionLookup, this.extensionCollection);
    }

    private static class CustomProtoRelConverter extends ProtoRelConverter {
        public CustomProtoRelConverter(ExtensionLookup lookup) throws IOException {
            super(lookup);
        }

        public CustomProtoRelConverter(ExtensionLookup lookup, SimpleExtension.ExtensionCollection extensions) {
            super(lookup, extensions);
        }

        @Override
        protected Extension.ExtensionTableDetail detailFromExtensionTable(Any any) {
            try {
                return TableExtensionDetailConverter.getExtensionTableDetail(any);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
