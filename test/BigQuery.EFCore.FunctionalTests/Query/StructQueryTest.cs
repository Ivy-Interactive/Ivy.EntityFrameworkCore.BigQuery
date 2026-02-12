using Ivy.EntityFrameworkCore.BigQuery.TestModels.Struct;
using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query;

/// <summary>
/// Base test class for STRUCT type queries.
/// Tests cover: STRUCT field access in filters, projections, ordering, and full STRUCT materialization.
/// </summary>
public abstract class StructQueryTest<TFixture> : QueryTestBase<TFixture>
    where TFixture : StructQueryFixture, new()
{
    protected StructQueryTest(TFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    #region Simple STRUCT Field Access

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Project_struct_field(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null)
                .Select(e => new { e.Name, City = e.HomeAddress!.City }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Project_multiple_struct_fields(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null)
                .Select(e => new
                {
                    e.Id,
                    Street = e.HomeAddress!.Street,
                    City = e.HomeAddress.City,
                    ZipCode = e.HomeAddress.ZipCode
                }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Project_entity_id_with_struct_filter(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null && e.HomeAddress.City == "Seattle")
                .Select(e => e.Id));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Filter_by_struct_field_with_parameter(bool async)
    {
        var city = "Portland";
        return AssertQuery(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null && e.HomeAddress.City == city)
                .Select(e => e.Id));
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task OrderBy_struct_field_project_id(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null)
                .OrderBy(e => e.HomeAddress!.City)
                .ThenBy(e => e.Id)
                .Select(e => new { e.Id, City = e.HomeAddress!.City }),
            assertOrder: true);

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task String_Contains_on_struct_field(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null && e.HomeAddress.Street.Contains("Main"))
                .Select(e => e.Id));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task String_StartsWith_on_struct_field(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null && e.HomeAddress.ZipCode != null && e.HomeAddress.ZipCode.StartsWith("98"))
                .Select(e => e.Id));

    #endregion

    #region Nested STRUCT Access

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Project_nested_struct_fields(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<CustomerEntity>()
                .Where(e => e.Contact != null)
                .Select(e => new
                {
                    e.CustomerName,
                    Email = e.Contact!.Email
                }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Filter_by_nested_struct_field(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<CustomerEntity>()
                .Where(e => e.Contact != null &&
                           e.Contact.MailingAddress != null &&
                           e.Contact.MailingAddress.City == "San Francisco")
                .Select(e => e.Id));

    // Skipped: BigQuery NULL semantics for nested STRUCT fields differ from LINQ-to-objects
    // [ConditionalTheory]
    // [MemberData(nameof(IsAsyncData))]
    // public virtual Task Filter_by_nested_struct_nullable_field(bool async)
    //     => AssertQuery(
    //         async,
    //         ss => ss.Set<CustomerEntity>()
    //             .Where(e => e.Contact != null && e.Contact.Phone == null)
    //             .Select(e => e.Id));

    #endregion

    #region Full STRUCT Materialization

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Project_entire_struct_column(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null)
                .Select(e => new { e.Name, e.HomeAddress }),
            elementSorter: e => e.Name,
            elementAsserter: (e, a) =>
            {
                Assert.Equal(e.Name, a.Name);
                Assert.NotNull(a.HomeAddress);
                Assert.Equal(e.HomeAddress!.Street, a.HomeAddress!.Street);
                Assert.Equal(e.HomeAddress.City, a.HomeAddress.City);
                Assert.Equal(e.HomeAddress.ZipCode, a.HomeAddress.ZipCode);
            });

    #endregion

    #region Aggregate Queries

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Count_with_struct_field_filter(bool async)
        => AssertCount(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null && e.HomeAddress.City == "Seattle"));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Distinct_on_struct_field(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<PersonEntity>()
                .Where(e => e.HomeAddress != null)
                .Select(e => e.HomeAddress!.City)
                .Distinct());

    #endregion

    #region ARRAY<STRUCT> Tests

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_of_structs_roundtrip(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<OrderEntity>()
                .Select(e => new { e.Id, e.OrderNumber, ItemCount = e.Items.Count }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Array_of_structs_first_element_field(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<OrderEntity>()
                .Select(e => new { e.OrderNumber, FirstProduct = e.Items[0].ProductName }));

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual Task Filter_by_array_of_structs_element_field(bool async)
        => AssertQuery(
            async,
            ss => ss.Set<OrderEntity>()
                .Where(e => e.Items[0].Quantity > 3)
                .Select(e => e.Id));

    #endregion
}
