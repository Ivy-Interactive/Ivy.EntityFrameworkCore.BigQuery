using Ivy.EntityFrameworkCore.BigQuery.TestModels.Array;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

public abstract class ArrayQueryTest<TFixture> : QueryTestBase<TFixture>
    where TFixture : ArrayQueryFixture, new()
{
    protected ArrayQueryTest(TFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    #region Roundtrip

    [ConditionalFact]
    public virtual void Roundtrip()
    {
        using var ctx = Fixture.CreateContext();
        var entity = ctx.ArrayEntities.Single(e => e.Id == 1);

        Assert.Equal([1, 2, 3], entity.IntArray);
        Assert.Equal([1, 2, 3], entity.IntList);
        Assert.Equal([10L, 20L, 30L], entity.LongArray);
        Assert.Equal(["apple", "banana", "cherry"], entity.StringArray);
        Assert.Equal(["apple", "banana", "cherry"], entity.StringList);
        Assert.Equal([1.1, 2.2, 3.3], entity.DoubleArray);
        Assert.Equal([1.1, 2.2, 3.3], entity.DoubleList);
        Assert.Equal([true, false, true], entity.BoolArray);
        Assert.Equal([1, 2, 3], entity.ByteArray);
    }

    #endregion

    #region Indexers

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Index_with_constant(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray[0] == 1));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Index_with_parameter(bool async)
    {
        var index = 0;

        return AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray[index] == 1));
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task String_array_index_with_constant(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.StringArray[0] == "apple"));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Index_in_projection(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Select(e => new { e.Id, FirstInt = e.IntArray[0], SecondString = e.StringArray[1] }));

#pragma warning disable CS0472 // Comparing value type to null
    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Non_nullable_value_array_index_compare_to_null(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray[1] == null),
            assertEmpty: true);

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Non_nullable_reference_array_index_compare_to_null(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.StringArray[1] == null),
            assertEmpty: true);
#pragma warning restore CS0472

    #endregion Indexers

    #region ElementAt

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task ElementAt_with_constant(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray.ElementAt(0) == 1));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task ElementAt_with_parameter(bool async)
    {
        var index = 1;

        return AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray.ElementAt(index) == 2));
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task ElementAt_in_projection(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Select(e => new { e.Id, Second = e.StringArray.ElementAt(1) }));

    #endregion ElementAt

    #region Length/Count

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_Length(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray.Length == 3));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_Length_in_projection(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Select(e => new { e.Id, Count = e.IntArray.Length }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task List_Count(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntList.Count == 3));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Enumerable_Count(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray.Count() == 3));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task String_array_Length(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.StringArray.Length >= 3));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_Length_on_EF_Property(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => EF.Property<int[]>(e, nameof(ArrayEntity.IntArray)).Length == 3));

    #endregion Length/Count

    #region SequenceEqual

    // TODO: BigQuery does not support direct array equality comparison with `=` operator.
    // See docs/ArraySequenceEqual.md for potential workarounds using UNNEST or TO_JSON_STRING.
    // These tests are skipped until a workaround is implemented.
    [ConditionalTheory(Skip = "BigQuery does not support array equality comparison - see docs/ArraySequenceEqual.md")]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task SequenceEqual_with_parameter(bool async)
    {
        var arr = new[] { 1, 2, 3 };

        return AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray.SequenceEqual(arr)));
    }

    [ConditionalTheory(Skip = "BigQuery does not support array equality comparison - see docs/ArraySequenceEqual.md")]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task SequenceEqual_with_different_parameter(bool async)
    {
        var arr = new[] { 4, 5, 6, 7 };

        return AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray.SequenceEqual(arr)));
    }

    #endregion SequenceEqual

    #region Containment

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_column_Contains_literal_item(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray.Contains(1)));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_column_Contains_parameter_item(bool async)
    {
        var p = 1;

        return AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray.Contains(p)));
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_column_Contains_column_item(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray.Contains(e.Id)));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task String_array_Contains_literal(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.StringArray.Contains("apple")));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_constant_Contains_column(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => new[] { 1, 2 }.Contains(e.Id)));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_param_Contains_column(bool async)
    {
        var arr = new[] { 1, 2 };

        return AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => arr.Contains(e.Id)));
    }

    #endregion Containment

    #region Any/All

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Any_no_predicate(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray.Any()));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Any_with_predicate(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray.Any(i => i > 5)));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task All_with_predicate(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => e.IntArray.All(i => i > 0)));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Any_Contains_on_constant_array(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => new[] { 1, 2 }.Any(p => e.IntArray.Contains(p))));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task All_Contains(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().Where(e => new[] { 1, 2 }.All(p => e.IntArray.Contains(p))));

    #endregion Any/All

    #region SelectMany

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task SelectMany_array_column(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>().SelectMany(e => e.IntArray));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task SelectMany_with_projection(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .SelectMany(e => e.IntArray, (e, i) => new { e.Id, Value = i }));

    #endregion SelectMany

    #region First/FirstOrDefault

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_First(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray.First() == 1));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_FirstOrDefault(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray.FirstOrDefault() == 1));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_First_in_projection(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Select(e => new { e.Id, First = e.StringArray.First() }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task List_First(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntList.First() == 1));

    #endregion First/FirstOrDefault

    #region Concat/Reverse

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_Concat_with_constant(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Select(e => new { e.Id, Combined = e.IntArray.Concat(new[] { 99, 100 }).ToArray() }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_Concat_two_columns(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray != null && e.IntList != null)
                .Select(e => new { e.Id, Combined = e.IntArray.Concat(e.IntList).ToArray() }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_Reverse(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Select(e => new { e.Id, Reversed = e.IntArray.Reverse().ToArray() }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_Concat_then_Reverse(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Select(e => new { e.Id, Result = e.IntArray.Concat(new[] { 99 }).Reverse().ToArray() }));

    #endregion Concat/Reverse

    #region Complex Queries

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Multiple_array_operations_in_projection(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Select(e => new
                {
                    e.Id,
                    Length = e.IntArray.Length,
                    First = e.IntArray.First(),
                    Second = e.IntArray.ElementAt(1),
                    StringFirst = e.StringArray.First()
                }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_operations_with_predicates(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray.Length > 2 && e.IntArray.First() < 5));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_indexing_with_arithmetic(bool async)
    {
        var offset = 1;

        return AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray[0 + offset] == 2));
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Different_array_types_in_same_query(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .Where(e => e.IntArray.Length == 3 && e.StringArray.First() == "apple"));

    #endregion Complex Queries

    #region Ordering

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task OrderBy_array_length(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .OrderBy(e => e.IntArray.Length)
                .ThenBy(e => e.Id),
            assertOrder: true);

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task OrderBy_array_first_element(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayEntity>()
                .OrderBy(e => e.IntArray.First())
                .ThenBy(e => e.Id),
            assertOrder: true);

    #endregion Ordering

    #region Subqueries

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_in_scalar_subquery(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<ArrayContainerEntity>()
                .Where(c => c.ArrayEntities.OrderBy(e => e.Id).First().IntArray.Length > 0));

    #endregion Subqueries
}
