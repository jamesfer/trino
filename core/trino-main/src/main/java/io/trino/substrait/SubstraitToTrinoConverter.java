/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.substrait;

import com.google.inject.Inject;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import extensions.tables.Extensions;
import io.airlift.log.Logger;
import io.substrait.expression.ExpressionVisitor;
import io.substrait.expression.FieldReference;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.Cross;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.Expand;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ExtensionTable;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Join;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.RelVisitor;
import io.substrait.relation.Set;
import io.substrait.relation.Sort;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeVisitor;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableFunctionRegistry;
import io.trino.security.AccessControl;
import io.trino.security.InjectedConnectorAccessControl;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.transaction.TransactionManager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SubstraitToTrinoConverter
{
    private static final Logger log = Logger.get(SubstraitToTrinoConverter.class);
    private final AccessControl accessControl;
    private final TableFunctionRegistry tableFunctionRegistry;
    private final TransactionManager transactionManager;
    private final Metadata metadata;

    @Inject
    public SubstraitToTrinoConverter(
            AccessControl accessControl,
            TableFunctionRegistry tableFunctionRegistry,
            TransactionManager transactionManager,
            Metadata metadata)
    {
        this.accessControl = accessControl;
        this.tableFunctionRegistry = tableFunctionRegistry;
        this.transactionManager = transactionManager;
        this.metadata = metadata;
    }

    public Plan convert(Session session, io.substrait.plan.Plan substraitPlan)
    {
        log.info("Converting substrait plan");
        final var roots = substraitPlan.getRoots();
        if (roots.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one root, but got %d".formatted(roots.size()));
        }

        final var tree = roots.getFirst();
        final var rel = tree.getInput();
        final var planNode = rel.accept(new RelConverter(
                this.accessControl,
                this.tableFunctionRegistry,
                this.transactionManager,
                this.metadata,
                session
        ));

        // Trino plans require an output node
        final var outputNode = new OutputNode(
                new PlanNodeId("HardCodedOutput"),
                planNode,
                planNode.getOutputSymbols().stream().map(Symbol::name).toList(),
                planNode.getOutputSymbols());
        return new Plan(outputNode, StatsAndCosts.empty());
    }

    private static class ExpressionConverter implements ExpressionVisitor<Expression, RuntimeException>
    {
        @Override
        public Expression visit(io.substrait.expression.Expression.BoolLiteral boolLiteral) throws RuntimeException
        {
            return new Constant(BooleanType.BOOLEAN, boolLiteral.value());
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.I8Literal i8Literal) throws RuntimeException
        {
            return new Constant(TinyintType.TINYINT, (long) i8Literal.value());
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.I16Literal i16Literal) throws RuntimeException
        {
            return new Constant(SmallintType.SMALLINT, (long) i16Literal.value());
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.I32Literal i32Literal) throws RuntimeException
        {
            return new Constant(IntegerType.INTEGER, (long) i32Literal.value());
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.I64Literal i64Literal) throws RuntimeException
        {
            return new Constant(BigintType.BIGINT, i64Literal.value());
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.NullLiteral nullLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.FP32Literal fp32Literal) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.FP64Literal fp64Literal) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.StrLiteral strLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.BinaryLiteral binaryLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.TimeLiteral timeLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.DateLiteral dateLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.TimestampLiteral timestampLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.TimestampTZLiteral timestampTZLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.PrecisionTimestampLiteral precisionTimestampLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.PrecisionTimestampTZLiteral precisionTimestampTZLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.IntervalYearLiteral intervalYearLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.IntervalDayLiteral intervalDayLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.IntervalCompoundLiteral intervalCompoundLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.UUIDLiteral uuidLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.FixedCharLiteral fixedCharLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.VarCharLiteral varCharLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.FixedBinaryLiteral fixedBinaryLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.DecimalLiteral decimalLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.MapLiteral mapLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.EmptyMapLiteral emptyMapLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.ListLiteral listLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.EmptyListLiteral emptyListLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.StructLiteral structLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.UserDefinedLiteral userDefinedLiteral) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.Switch aSwitch) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.IfThen ifThen) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.ScalarFunctionInvocation scalarFunctionInvocation) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.WindowFunctionInvocation windowFunctionInvocation) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.Cast cast) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.SingleOrList singleOrList) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.MultiOrList multiOrList) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(FieldReference fieldReference) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.SetPredicate setPredicate) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.ScalarSubquery scalarSubquery) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Expression visit(io.substrait.expression.Expression.InPredicate inPredicate) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    private record SymbolConverter(Iterator<String> names) implements TypeVisitor<Symbol, RuntimeException>
    {
        private Symbol makeSymbol(io.trino.spi.type.Type type) {
                assert names.hasNext() : "Not enough names provided";
                return new Symbol(type, names.next());
            }

            @Override
            public Symbol visit(Type.Bool bool) throws RuntimeException
        {
                return makeSymbol(BooleanType.BOOLEAN);
            }

            @Override
            public Symbol visit(Type.I8 i8) throws RuntimeException
        {
                return makeSymbol(TinyintType.TINYINT);
            }

            @Override
            public Symbol visit(Type.I16 i16) throws RuntimeException
        {
                return makeSymbol(SmallintType.SMALLINT);
            }

            @Override
            public Symbol visit(Type.I32 i32) throws RuntimeException
        {
                return makeSymbol(IntegerType.INTEGER);
            }

            @Override
            public Symbol visit(Type.I64 i64) throws RuntimeException
        {
                return makeSymbol(BigintType.BIGINT);
            }

            @Override
            public Symbol visit(Type.FP32 fp32) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.FP64 fp64) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.Str str) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.Binary binary) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.Date date) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.Time time) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.TimestampTZ timestampTZ) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.Timestamp timestamp) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.PrecisionTimestamp precisionTimestamp) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.PrecisionTimestampTZ precisionTimestampTZ) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.IntervalYear intervalYear) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.IntervalDay intervalDay) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.IntervalCompound intervalCompound) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.UUID uuid) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.FixedChar fixedChar) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.VarChar varChar) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.FixedBinary fixedBinary) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.Decimal decimal) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.Struct struct) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.ListType listType) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.Map map) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Symbol visit(Type.UserDefined userDefined) throws RuntimeException
        {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }

    private static class RelConverter implements RelVisitor<PlanNode, RuntimeException>
    {
        private final AccessControl accessControl;
        private final TableFunctionRegistry tableFunctionRegistry;
        private final TransactionManager transactionManager;
        private final Metadata metadata;
        private final Session session;

        public RelConverter(
                AccessControl accessControl,
                TableFunctionRegistry tableFunctionRegistry,
                TransactionManager transactionManager,
                Metadata metadata,
                Session session)
        {
            this.accessControl = accessControl;
            this.tableFunctionRegistry = tableFunctionRegistry;
            this.transactionManager = transactionManager;
            this.metadata = metadata;
            this.session = session;
        }

        private Row convertStructToRow(io.substrait.expression.Expression.StructLiteral struct)
        {
            final var visitor = new ExpressionConverter();
            final var constants = struct.fields().stream()
                    .map(field -> {
                        final var expression = field.accept(visitor);
                        if (!(expression instanceof Constant constant)) {
                            throw new IllegalArgumentException("Trino rows can only accept constant expressions. The converted expression was %s".formatted(expression));
                        }
                        return (Expression) constant;
                    })
                    .toList();
            return new Row(constants);
        }

        private List<Symbol> convertNamedStructToSymbols(NamedStruct namedStruct)
        {
            final var namesIterator = namedStruct.names().iterator();
            final var symbolConverter = new SymbolConverter(namesIterator);
            return namedStruct.struct().fields().stream()
                    .map(field -> field.accept(symbolConverter))
                    .toList();
        }

        @Override
        public PlanNode visit(VirtualTableScan virtualTableScan) throws RuntimeException
        {
            if (virtualTableScan.getFilter().isPresent()) {
                throw new IllegalArgumentException("Filter is not supported on virtual table scan nodes");
            }

            final var rows = virtualTableScan.getRows()
                    .stream()
                    .map(struct -> (Expression) convertStructToRow(struct))
                    .toList();

            final var symbols = convertNamedStructToSymbols(virtualTableScan.getInitialSchema());

            return new ValuesNode(
                    new PlanNodeId("VirtualTableScan"),
                    symbols,
                    rows.size(),
                    Optional.of(rows));
        }

        @Override
        public PlanNode visit(Project project) throws RuntimeException
        {
            final var input = project.getInput().accept(this);

            final var expressionVisitor = new ExpressionConverter();
            final var counter = new AtomicInteger(1);
            final var assignmentMap = project.getExpressions().stream()
                    .map(expression -> expression.accept(expressionVisitor))
                    .map(expression -> Map.entry(
                            new Symbol(expression.type(), "expr_%d".formatted(counter.getAndIncrement())),
                            expression))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return new ProjectNode(
                    new PlanNodeId("Project"),
                    input,
                    new Assignments(assignmentMap)
            );
        }

        @Override
        public PlanNode visit(ExtensionTable extensionTable) throws RuntimeException
        {
            log.info("Visiting extension table");

            final var protoPrefix = "type.googleapis.com";
            final var detailProto = extensionTable.getDetail().toProto();
            if (Objects.equals(detailProto.getTypeUrl(), "%s/%s".formatted(protoPrefix, Extensions.TableFunctionCall.getDescriptor().getFullName()))) {
                Extensions.TableFunctionCall tableFunctionCall;
                try {
                    tableFunctionCall = detailProto.unpack(Extensions.TableFunctionCall.class);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }

                final var maybeCatalog = this.metadata.getCatalogHandle(this.session, tableFunctionCall.getCatalogName());
                if (maybeCatalog.isEmpty()) {
                    throw new IllegalArgumentException("Catalog %s not found".formatted(tableFunctionCall.getCatalogName()));
                }
                final var catalogHandle = maybeCatalog.get();

                // Find transaction
                final var maybeTransactionId = this.session.getTransactionId();
                if (maybeTransactionId.isEmpty()) {
                    throw new IllegalArgumentException("Transaction ID not found");
                }
                final var transactionId = maybeTransactionId.get();
                final var transaction = this.transactionManager.getTransactionInfo(transactionId);
                final var connectorTransaction = this.transactionManager.getConnectorTransaction(transactionId, catalogHandle);

                // Look up the table function call
                final var schemaFunctionName = new SchemaFunctionName(tableFunctionCall.getSchemaName(), tableFunctionCall.getFunctionName());
                final var maybeConnectorTableFunction = this.tableFunctionRegistry.resolve(catalogHandle, schemaFunctionName);
                if (maybeConnectorTableFunction.isEmpty()) {
                    throw new IllegalArgumentException("Table function %s not found".formatted(schemaFunctionName));
                }
                final var connectorTableFunction = maybeConnectorTableFunction.get();

                // Parse the arguments
                final var arguments = tableFunctionCall.getArgumentsList().stream()
                        .map(x -> {
                            final var name = x.getName();
                            final var protoValue = x.getValue();
                            Argument argument;
                            if (protoValue.hasInt64Value()) {
                                argument = new ScalarArgument(BigintType.BIGINT, protoValue.getInt64Value());
                            } else if (protoValue.hasStringValue()) {
                                argument = new ScalarArgument(VarcharType.VARCHAR, protoValue.getStringValue());
                            } else {
                                throw new RuntimeException("Unsupported argument type");
                            }

                            return Map.entry(name, argument);
                        })
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                // Run table function analysis
                final var tableFunctionAnalysis = connectorTableFunction.analyze(
                        this.session.toConnectorSession(catalogHandle),
                        connectorTransaction,
                        arguments,
                        new InjectedConnectorAccessControl(accessControl, session.toSecurityContext(), catalogHandle.getCatalogName().toString()));

                // Determine the function return type
                final var returnTypeSpecification = connectorTableFunction.getReturnTypeSpecification();
                Descriptor descriptor;
                if (returnTypeSpecification instanceof ReturnTypeSpecification.DescribedTable describedTable) {
                    descriptor = describedTable.getDescriptor();
                } else if (returnTypeSpecification instanceof ReturnTypeSpecification.GenericTable) {
                    final var maybeDescriptor = tableFunctionAnalysis.getReturnedType();
                    if (maybeDescriptor.isEmpty()) {
                        throw new IllegalArgumentException("Table function %s did not return a descriptor".formatted(schemaFunctionName));
                    }
                    descriptor = maybeDescriptor.get();
                } else {
                    throw new IllegalArgumentException("Unsupported return type specification.");
                }

                final var symbols = descriptor.getFields().stream()
                        .map(field -> new Symbol(field.getType().get(), field.getName().get()))
                        .toList();

                return new TableFunctionNode(
                        new PlanNodeId("TableFunction"),
                        tableFunctionCall.getFunctionName(),
                        catalogHandle,
                        arguments,
                        symbols,
                        List.of(),
                        List.of(),
                        List.of(),
                        new TableFunctionHandle(
                                catalogHandle,
                                tableFunctionAnalysis.getHandle(),
                                connectorTransaction));
            }

            throw new UnsupportedOperationException("Unsupported extension table details proto: %s".formatted(detailProto.getTypeUrl()));
        }


        @Override
        public PlanNode visit(NamedScan namedScan) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(Aggregate aggregate) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(EmptyScan emptyScan) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(Fetch fetch) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(Filter filter) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(Join join) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(Set set) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(LocalFiles localFiles) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(Expand expand) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(Sort sort) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(Cross cross) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(ExtensionLeaf extensionLeaf) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(ExtensionSingle extensionSingle) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(ExtensionMulti extensionMulti) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(HashJoin hashJoin) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(MergeJoin mergeJoin) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(NestedLoopJoin nestedLoopJoin) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public PlanNode visit(ConsistentPartitionWindow consistentPartitionWindow) throws RuntimeException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
