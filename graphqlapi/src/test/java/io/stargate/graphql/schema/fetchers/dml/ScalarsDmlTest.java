package io.stargate.graphql.schema.fetchers.dml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import graphql.ExecutionResult;
import io.stargate.db.datastore.ArrayListBackedRow;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.DmlTestBase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ScalarsDmlTest extends DmlTestBase {
  public static final Table table = buildTable();
  public static final Keyspace keyspace =
      ImmutableKeyspace.builder().name("scalars").addTables(table).build();

  private static Table buildTable() {
    ImmutableTable.Builder tableBuilder =
        ImmutableTable.builder()
            .keyspace("scalars_ks")
            .name("scalars")
            .addColumns(
                ImmutableColumn.builder()
                    .keyspace("scalars")
                    .table("scalars_table")
                    .name("id")
                    .type(Column.Type.Int)
                    .kind(Column.Kind.PartitionKey)
                    .build());

    tableBuilder.addColumns(
        Arrays.stream(Column.Type.values())
            .filter(t -> !t.isCollection() && !t.isTuple() && !t.isUserDefined())
            .map(ScalarsDmlTest::getColumn)
            .toArray(ImmutableColumn[]::new));

    return tableBuilder.build();
  }

  private static ImmutableColumn getColumn(Column.Type type) {
    return ImmutableColumn.builder()
        .keyspace("scalars_ks")
        .table("scalars")
        .name(getName(type))
        .type(type)
        .kind(Column.Kind.Regular)
        .build();
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldSupportLiteralsForScalars(
      Column.Type type, Object value, boolean asJsonString, String expectedLiteral) {

    String mutation = "mutation { insertScalars(value: { %s:%s, id:1 }) { applied } }";

    String graphQLValue = value.toString();
    if (asJsonString) {
      graphQLValue = String.format("\"%s\"", value.toString());
    }

    String name = getName(type);
    String expectedCQL =
        String.format(
            "INSERT INTO scalars_ks.scalars (id,%s) VALUES (1,%s)",
            name, expectedLiteral != null ? expectedLiteral : value.toString());

    assertSuccess(String.format(mutation, name, graphQLValue), expectedCQL);
  }

  @Test
  public void shouldSupportDeserializingScalars() {
    Row row = createRowForSingleValue("bigintvalue", 1L);
    when(resultSet.currentPageRows()).thenReturn(Collections.singletonList(row));
    ExecutionResult result = executeGraphQl("query { scalars { values { bigintvalue } } }");
    assertThat(result.getErrors()).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("getIncorrectValues")
  public void incorrectLiteralsForScalarsShouldResultInError(
      Column.Type type, Object value, boolean asJsonString) {

    String mutation = "mutation { insertScalars(value: { %s:%s, id:1 }) { applied } }";

    String graphQLValue = value.toString();
    if (asJsonString) {
      graphQLValue = String.format("\"%s\"", value.toString());
    }

    assertError(String.format(mutation, getName(type), graphQLValue), "Validation error");
  }

  private static Stream<Arguments> getValues() {
    return Stream.of(
        arguments(Column.Type.Ascii, "abc", true, "'abc'"),
        arguments(Column.Type.Ascii, "", true,  "''"),
        arguments(Column.Type.Bigint, -1, false, null),
        arguments(Column.Type.Bigint, 1, false, null),
        arguments(Column.Type.Bigint, -2147483648, false, null),
        arguments(Column.Type.Bigint, 2147483647, false, null),
        arguments(Column.Type.Bigint, 2147483648023L, false, null),
        arguments(Column.Type.Bigint, "9223372036854775807", true, null),
        arguments(Column.Type.Bigint, "1", true, null),
        arguments(Column.Type.Blob, "AQID//7gEiMB", true, "0x010203fffee0122301"),
        arguments(Column.Type.Blob, "/w==", true, "0xff")
    );
  }

  private static Stream<Arguments> getIncorrectValues() {
    return Stream.of(
        arguments(Column.Type.Bigint, "1.2", false, null),
        arguments(Column.Type.Bigint, "ABC", false, null));
  }

  /** Gets the column name for a scalar type. */
  private static String getName(Column.Type type) {
    return type.name().toLowerCase() + "value";
  }

  @Override
  public Keyspace getKeyspace() {
    return keyspace;
  }

  private static Row createRow(List<Column> columns, Map<String, Object> data) {
    List<ByteBuffer> values = new ArrayList<>(columns.size());
    for (Column column : columns) {
      Object v = data.get(column.name());
      values.add(v == null ? null : column.type().codec().encode(v, ProtocolVersion.DEFAULT));
    }
    return new ArrayListBackedRow(columns, values, ProtocolVersion.DEFAULT);
  }

  private static Row createRowForSingleValue(String columnName, Object value) {
    Map<String, Object> values = new HashMap<>();
    values.put(columnName, value);
    return createRow(Collections.singletonList(table.column(columnName)), values);
  }
}
