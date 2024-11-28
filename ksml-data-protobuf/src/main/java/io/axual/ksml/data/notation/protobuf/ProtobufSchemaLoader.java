package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */


import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.apicurio.registry.serde.protobuf.ProtobufSchemaParser;
import io.apicurio.registry.utils.IoUtil;
import io.apicurio.registry.utils.protobuf.schema.DynamicSchema;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.loader.SchemaLoader;
import io.axual.ksml.data.schema.DataSchema;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

@Slf4j
public class ProtobufSchemaLoader extends SchemaLoader {
    private static final ProtobufSchemaMapper MAPPER = new ProtobufSchemaMapper();
    private static final Location DEFAULT_LOCATION = Location.get("");

    public ProtobufSchemaLoader(String schemaDirectory) {
        super("PROTOBUF", schemaDirectory, ".proto");
    }

    @Override
    protected DataSchema parseSchema(String name, String schema) {
        final var proto = new ProtobufSchemaParser<>().parseSchema(schema.getBytes(), Collections.emptyMap());
        return MAPPER.toDataSchema(name, proto);



//        final var proto = FileDescriptorUtils.toFileDescriptorProto(schema, name, Optional.empty(),new HashMap<>());
//        final var s = new ProtobufSchema(proto.,null);
//
//        try {
//            final var proto = DynamicSchema.parseFrom(schema.getBytes());
//            return MAPPER.toDataSchema(name, proto);
//        } catch (Descriptors.DescriptorValidationException | IOException e) {
//            throw new SchemaException("Could not parse PROTOBUF schema '" + name + "': " + e.getMessage(), e);
//        }
    }

//    private ProtoFileElement parseProto(String name, String schema) {
//        try {
//            return ProtoParser.Companion.parse(DEFAULT_LOCATION, schema);
//        } catch (Exception e) {
//            // If the file did not contain a text schema, then see if we can decode it as binary
//            try {
//                return convertToFileElement(DescriptorProtos.FileDescriptorProto.parseFrom(schema.getBytes()));
//            } catch (Exception e2) {
//                throw new SchemaException("Could not parse PROTOBUF schema '" + name + "': " + e.getMessage(), e2);
//            }
//        }
//    }

//    private ProtoFileElement convertToFileElement(DescriptorProtos.FileDescriptorProto fileDescriptor) {
//        final var imports = new ArrayList<String>();
//        final var publicImports = new ArrayList<String>();
//        final var types = new ArrayList<TypeElement>();
//        fileDescriptor.getMessageTypeList().forEach(m -> types.add(convertToMessage(fileDescriptor, m)));
//        fileDescriptor.getEnumTypeList().forEach(e -> types.add(convertToEnum(e)));
//        return new ProtoFileElement(DEFAULT_LOCATION,
//                fileDescriptor.getPackage(),
//                fileDescriptor.getSyntax(),
//                imports,
//                publicImports,
//                types,
//                services.build(),
//                extendElements.build(),
//                options.build()
//        );
//    }
//
//    private MessageElement convertToMessage(DescriptorProtos.FileDescriptorProto fileDescriptor, DescriptorProtos.DescriptorProto messageDescriptor) {
//        final var fields = new ArrayList<FieldElement>();
//        final var nested = new ArrayList<TypeElement>();
//        final var reserved = new ArrayList<ReservedElement>();
//        final var extensions = new ArrayList<ExtensionsElement>();
//        final var oneOfs = new HashMap<String, List<FieldElement>>();
//        final var oneOfOptions = new HashMap<String, String>();
//
//        return new MessageElement(DEFAULT_LOCATION,
//                descriptor.getName(),
//                "",
//                nested,
//                options,
//                reserved,
//                fields,
//                oneofs.stream()
//                        .map(e -> toOneof(e.getKey(), e.getValue(), oneofsOptions.get(e.getKey())))
//                        .filter(e -> !e.getFields().isEmpty())
//                        .collect(Collectors.toList()),
//                extensions,
//                Collections.emptyList(),
//                extendElements
//        );
//    }
//
//    private TypeElement convertToEnum(DescriptorProtos.EnumDescriptorProto enumDescriptor) {
//        final var constants = new ArrayList<EnumConstantElement>();
//        for (final var element:enumDescriptor.getValueList()) {
//            final var options = new ArrayList<OptionElement>();
//            if (element.getOptions().hasDeprecated())
//                options.add(new OptionElement("deprecated", OptionElement.Kind.BOOLEAN, element.getOptions().getDeprecated(), false));
//            if (element.getOptions().hasDebugRedact())
//                options.add(new OptionElement("debug_redact", OptionElement.Kind.BOOLEAN, element.getOptions().getDeprecated(), false));
//            if (element.getOptions().hasFeatures())
//                options.add(convertToFeaturesOption(element.getOptions().getFeatures()));
//            //TODO: meta features
//            options.addAll(convertToCustomOptions(element.getOptions()));
//            constants.add(new EnumConstantElement(DEFAULT_LOCATION, element.getName(), element.getNumber(), "", options));
//        }
//
//        final var reserveds = new ArrayList<ReservedElement>();
//        for (final var range: enumDescriptor.getReservedRangeList()) {
//            reserveds.add(convertToReserved(range));
//        }
//        for (final var name: enumDescriptor.getReservedNameList()) {
//            reserveds.add(new ReservedElement(DEFAULT_LOCATION, "", Collections.singletonList(name)));
//        }
//
//        final var options = new ArrayList<OptionElement>();
//        if (enumDescriptor.getOptions().hasAllowAlias())
//            options.add(new OptionElement(ALLOW_ALIAS, OptionElement.Kind.BOOLEAN, enumDescriptor.getOptions().getAllowAlias(), false));
//        if (enumDescriptor.getOptions().hasDeprecated())
//            options.add(new OptionElement(DEPRECATED, OptionElement.Kind.BOOLEAN, enumDescriptor.getOptions().getDeprecated(), false));
//        if (enumDescriptor.getOptions().hasFeatures())
//          options.add(convertToFeaturesOption(enumDescriptor.getOptions().getFeatures()));
//        // TODO: meta features
//        options.addAll(convertToCustomOptions(enumDescriptor.getOptions()));
//
//        return new EnumElement(DEFAULT_LOCATION, enumDescriptor.getName(), "", options, constants, reserveds);
//    }
}
