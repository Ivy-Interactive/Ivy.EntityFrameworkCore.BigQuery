using Ivy.EntityFrameworkCore.BigQuery.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.JsonQuery;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.Update;

namespace Ivy.EntityFrameworkCore.BigQuery.Update;

public class JsonUpdateBigQueryTest : JsonUpdateTestBase<JsonUpdateBigQueryTest.JsonUpdateBigQueryFixture>
{
    public JsonUpdateBigQueryTest(JsonUpdateBigQueryFixture fixture)
        : base(fixture)
    {
        ClearLog();
    }

    // BigQuery doesn't support GetDbTransaction(), so we need to reseed data between tests.
    // The fixture seeds JsonEntitiesBasic, JsonEntitiesInheritance, and JsonEntitiesConverters.
    // JsonEntityAllTypes is NOT seeded due to EF Core's ModificationCommand.WriteJson issues during INSERT.

    // Track the current transaction to only reseed once per test (useTransaction is called multiple times)
    private IDbContextTransaction? _currentTransaction;

    private void UseTransactionReseed(DatabaseFacade facade, IDbContextTransaction transaction)
    {
        // Only reseed on first call for this transaction (before testOperation)
        // Don't reseed on subsequent calls (before nestedTestOperation) to preserve test changes
        if (transaction == _currentTransaction)
        {
            return;
        }
        _currentTransaction = transaction;

        // BigQuery doesn't support transaction rollback, so we need to clean
        // and reseed data before each test to ensure consistent test state.
        using var context = CreateContext();
        context.JsonEntitiesBasic.ExecuteDelete();
        context.JsonEntitiesInheritance.ExecuteDelete();
        context.JsonEntitiesConverters.ExecuteDelete();
        // Re-seed data
        var jsonEntitiesBasic = JsonQueryData.CreateJsonEntitiesBasic();
        var jsonEntitiesInheritance = JsonQueryData.CreateJsonEntitiesInheritance();
        var jsonEntitiesConverters = JsonQueryData.CreateJsonEntitiesConverters();
        context.JsonEntitiesBasic.AddRange(jsonEntitiesBasic);
        context.JsonEntitiesInheritance.AddRange(jsonEntitiesInheritance);
        context.JsonEntitiesConverters.AddRange(jsonEntitiesConverters);
        context.SaveChanges();
    }

    #region Tests using JsonEntitiesBasic - ENABLED

    public override Task Add_entity_with_json()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var newEntity = new JsonEntityBasic
                {
                    Id = 2,
                    Name = "NewEntity",
                    OwnedCollectionRoot = [],
                    OwnedReferenceRoot = new JsonOwnedRoot
                    {
                        Name = "RootName",
                        Number = 42,
                        OwnedCollectionBranch = [],
                        OwnedReferenceBranch = new JsonOwnedBranch
                        {
                            Id = 7,
                            Date = new DateTime(2010, 10, 10),
                            Enum = JsonEnum.Three,
                            Fraction = 42.42m,
                            OwnedCollectionLeaf =
                            [
                                new JsonOwnedLeaf { SomethingSomething = "ss1" },
                                new JsonOwnedLeaf { SomethingSomething = "ss2" }
                            ],
                            OwnedReferenceLeaf = new JsonOwnedLeaf { SomethingSomething = "ss3" }
                        }
                    },
                };

                context.Set<JsonEntityBasic>().Add(newEntity);
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                Assert.Equal(2, query.Count);

                var newEntity = query.Single(e => e.Id == 2);
                Assert.Equal("NewEntity", newEntity.Name);
                Assert.Empty(newEntity.OwnedCollectionRoot);
                Assert.Equal("RootName", newEntity.OwnedReferenceRoot.Name);
                Assert.Equal(42, newEntity.OwnedReferenceRoot.Number);
                Assert.Empty(newEntity.OwnedReferenceRoot.OwnedCollectionBranch);
                Assert.Equal(new DateTime(2010, 10, 10), newEntity.OwnedReferenceRoot.OwnedReferenceBranch.Date);
                Assert.Equal(JsonEnum.Three, newEntity.OwnedReferenceRoot.OwnedReferenceBranch.Enum);
                Assert.Equal(42.42m, newEntity.OwnedReferenceRoot.OwnedReferenceBranch.Fraction);
                Assert.Equal(7, newEntity.OwnedReferenceRoot.OwnedReferenceBranch.Id);

                Assert.Equal("ss3", newEntity.OwnedReferenceRoot.OwnedReferenceBranch.OwnedReferenceLeaf.SomethingSomething);

                var collectionLeaf = newEntity.OwnedReferenceRoot.OwnedReferenceBranch.OwnedCollectionLeaf;
                Assert.Equal(2, collectionLeaf.Count);
                Assert.Equal("ss1", collectionLeaf[0].SomethingSomething);
                Assert.Equal("ss2", collectionLeaf[1].SomethingSomething);
            });

    public override Task Add_entity_with_json_null_navigations()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var newEntity = new JsonEntityBasic
                {
                    Id = 2,
                    Name = "NewEntity",
                    OwnedCollectionRoot = null,
                    OwnedReferenceRoot = new JsonOwnedRoot
                    {
                        Name = "RootName",
                        Number = 42,
                        OwnedReferenceBranch = new JsonOwnedBranch
                        {
                            Id = 7,
                            Date = new DateTime(2010, 10, 10),
                            Enum = JsonEnum.Three,
                            Fraction = 42.42m,
                            OwnedCollectionLeaf =
                            [
                                new JsonOwnedLeaf { SomethingSomething = "ss1" },
                                new JsonOwnedLeaf { SomethingSomething = "ss2" }
                            ],
                            OwnedReferenceLeaf = null,
                        }
                    },
                };

                context.Set<JsonEntityBasic>().Add(newEntity);
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                Assert.Equal(2, query.Count);

                var newEntity = query.Single(e => e.Id == 2);
                Assert.Equal("NewEntity", newEntity.Name);
                Assert.Null(newEntity.OwnedCollectionRoot);
                Assert.Equal("RootName", newEntity.OwnedReferenceRoot.Name);
                Assert.Equal(42, newEntity.OwnedReferenceRoot.Number);
                Assert.Null(newEntity.OwnedReferenceRoot.OwnedCollectionBranch);
                Assert.Equal(new DateTime(2010, 10, 10), newEntity.OwnedReferenceRoot.OwnedReferenceBranch.Date);
                Assert.Equal(JsonEnum.Three, newEntity.OwnedReferenceRoot.OwnedReferenceBranch.Enum);
                Assert.Equal(42.42m, newEntity.OwnedReferenceRoot.OwnedReferenceBranch.Fraction);
                Assert.Equal(7, newEntity.OwnedReferenceRoot.OwnedReferenceBranch.Id);

                Assert.Null(newEntity.OwnedReferenceRoot.OwnedReferenceBranch.OwnedReferenceLeaf);

                var collectionLeaf = newEntity.OwnedReferenceRoot.OwnedReferenceBranch.OwnedCollectionLeaf;
                Assert.Equal(2, collectionLeaf.Count);
                Assert.Equal("ss1", collectionLeaf[0].SomethingSomething);
                Assert.Equal("ss2", collectionLeaf[1].SomethingSomething);
            });

    public override Task Add_json_reference_root()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot = null;
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();

                Assert.Null(entity.OwnedReferenceRoot);
                entity.OwnedReferenceRoot = new JsonOwnedRoot
                {
                    Name = "RootName",
                    Number = 42,
                    OwnedCollectionBranch = [],
                    OwnedReferenceBranch = new JsonOwnedBranch
                    {
                        Id = 7,
                        Date = new DateTime(2010, 10, 10),
                        Enum = JsonEnum.Three,
                        Fraction = 42.42m,
                        OwnedCollectionLeaf =
                        [
                            new JsonOwnedLeaf { SomethingSomething = "ss1" },
                            new JsonOwnedLeaf { SomethingSomething = "ss2" }
                        ],
                        OwnedReferenceLeaf = new JsonOwnedLeaf { SomethingSomething = "ss3" }
                    }
                };
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var updatedEntity = await context.JsonEntitiesBasic.SingleAsync();
                var updatedReference = updatedEntity.OwnedReferenceRoot;
                Assert.Equal("RootName", updatedReference.Name);
                Assert.Equal(42, updatedReference.Number);
                Assert.Empty(updatedReference.OwnedCollectionBranch);
                Assert.Equal(new DateTime(2010, 10, 10), updatedReference.OwnedReferenceBranch.Date);
                Assert.Equal(JsonEnum.Three, updatedReference.OwnedReferenceBranch.Enum);
                Assert.Equal(42.42m, updatedReference.OwnedReferenceBranch.Fraction);
                Assert.Equal(7, updatedReference.OwnedReferenceBranch.Id);
                Assert.Equal("ss3", updatedReference.OwnedReferenceBranch.OwnedReferenceLeaf.SomethingSomething);
                var collectionLeaf = updatedReference.OwnedReferenceBranch.OwnedCollectionLeaf;
                Assert.Equal(2, collectionLeaf.Count);
                Assert.Equal("ss1", collectionLeaf[0].SomethingSomething);
                Assert.Equal("ss2", collectionLeaf[1].SomethingSomething);
            });

    public override Task Add_json_reference_leaf()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot.OwnedCollectionBranch[0].OwnedReferenceLeaf = null;
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();

                Assert.Null(entity.OwnedReferenceRoot.OwnedCollectionBranch[0].OwnedReferenceLeaf);
                var newLeaf = new JsonOwnedLeaf { SomethingSomething = "ss3" };
                entity.OwnedReferenceRoot.OwnedCollectionBranch[0].OwnedReferenceLeaf = newLeaf;

                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var updatedEntity = await context.JsonEntitiesBasic.SingleAsync();
                var updatedReference = updatedEntity.OwnedReferenceRoot.OwnedCollectionBranch[0].OwnedReferenceLeaf;
                Assert.Equal("ss3", updatedReference.SomethingSomething);
            });

    public override Task Add_element_to_json_collection_root()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();

                var newRoot = new JsonOwnedRoot
                {
                    Name = "new Name",
                    Number = 142,
                    OwnedCollectionBranch = [],
                    OwnedReferenceBranch = new JsonOwnedBranch
                    {
                        Id = 7,
                        Date = new DateTime(2010, 10, 10),
                        Enum = JsonEnum.Three,
                        Fraction = 42.42m,
                        OwnedCollectionLeaf =
                        [
                            new JsonOwnedLeaf { SomethingSomething = "ss1" },
                            new JsonOwnedLeaf { SomethingSomething = "ss2" }
                        ],
                        OwnedReferenceLeaf = new JsonOwnedLeaf { SomethingSomething = "ss3" }
                    }
                };

                entity.OwnedCollectionRoot.Add(newRoot);
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var updatedEntity = await context.JsonEntitiesBasic.SingleAsync();
                var updatedCollection = updatedEntity.OwnedCollectionRoot;
                Assert.Equal(3, updatedCollection.Count);
                Assert.Equal("new Name", updatedCollection[2].Name);
                Assert.Equal(142, updatedCollection[2].Number);
                Assert.Empty(updatedCollection[2].OwnedCollectionBranch);
                Assert.Equal(new DateTime(2010, 10, 10), updatedCollection[2].OwnedReferenceBranch.Date);
                Assert.Equal(JsonEnum.Three, updatedCollection[2].OwnedReferenceBranch.Enum);
                Assert.Equal(7, updatedCollection[2].OwnedReferenceBranch.Id);
                Assert.Equal(42.42m, updatedCollection[2].OwnedReferenceBranch.Fraction);
                Assert.Equal("ss3", updatedCollection[2].OwnedReferenceBranch.OwnedReferenceLeaf.SomethingSomething);
                var collectionLeaf = updatedCollection[2].OwnedReferenceBranch.OwnedCollectionLeaf;
                Assert.Equal(2, collectionLeaf.Count);
                Assert.Equal("ss1", collectionLeaf[0].SomethingSomething);
                Assert.Equal("ss2", collectionLeaf[1].SomethingSomething);
            });

    public override Task Add_element_to_json_collection_root_null_navigations()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();

                var newRoot = new JsonOwnedRoot
                {
                    Name = "new Name",
                    Number = 142,
                    OwnedCollectionBranch = null,
                    OwnedReferenceBranch = new JsonOwnedBranch
                    {
                        Id = 7,
                        Date = new DateTime(2010, 10, 10),
                        Enum = JsonEnum.Three,
                        Fraction = 42.42m,
                        OwnedReferenceLeaf = null
                    }
                };

                entity.OwnedCollectionRoot.Add(newRoot);
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var updatedEntity = await context.JsonEntitiesBasic.SingleAsync();
                var updatedCollection = updatedEntity.OwnedCollectionRoot;
                Assert.Equal(3, updatedCollection.Count);
                Assert.Equal("new Name", updatedCollection[2].Name);
                Assert.Equal(142, updatedCollection[2].Number);
                Assert.Null(updatedCollection[2].OwnedCollectionBranch);
                Assert.Equal(new DateTime(2010, 10, 10), updatedCollection[2].OwnedReferenceBranch.Date);
                Assert.Equal(JsonEnum.Three, updatedCollection[2].OwnedReferenceBranch.Enum);
                Assert.Equal(7, updatedCollection[2].OwnedReferenceBranch.Id);
                Assert.Equal(42.42m, updatedCollection[2].OwnedReferenceBranch.Fraction);
                Assert.Null(updatedCollection[2].OwnedReferenceBranch.OwnedReferenceLeaf);
                Assert.Null(updatedCollection[2].OwnedReferenceBranch.OwnedCollectionLeaf);
            });

    public override Task Add_element_to_json_collection_branch()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                var newBranch = new JsonOwnedBranch
                {
                    Id = 77,
                    Date = new DateTime(2010, 10, 10),
                    Enum = JsonEnum.Three,
                    Fraction = 42.42m,
                    OwnedCollectionLeaf =
                    [
                        new JsonOwnedLeaf { SomethingSomething = "ss1" },
                        new JsonOwnedLeaf { SomethingSomething = "ss2" }
                    ],
                    OwnedReferenceLeaf = new JsonOwnedLeaf { SomethingSomething = "ss3" }
                };

                entity.OwnedReferenceRoot.OwnedCollectionBranch.Add(newBranch);
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var updatedEntity = await context.JsonEntitiesBasic.SingleAsync();
                var updatedCollection = updatedEntity.OwnedReferenceRoot.OwnedCollectionBranch;
                Assert.Equal(3, updatedCollection.Count);
                Assert.Equal(new DateTime(2010, 10, 10), updatedCollection[2].Date);
                Assert.Equal(JsonEnum.Three, updatedCollection[2].Enum);
                Assert.Equal(42.42m, updatedCollection[2].Fraction);
                Assert.Equal(77, updatedCollection[2].Id);
                Assert.Equal("ss3", updatedCollection[2].OwnedReferenceLeaf.SomethingSomething);
                var collectionLeaf = updatedCollection[2].OwnedCollectionLeaf;
                Assert.Equal(2, collectionLeaf.Count);
                Assert.Equal("ss1", collectionLeaf[0].SomethingSomething);
                Assert.Equal("ss2", collectionLeaf[1].SomethingSomething);
            });

    public override Task Add_element_to_json_collection_leaf()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                var newLeaf = new JsonOwnedLeaf { SomethingSomething = "ss1" };
                entity.OwnedReferenceRoot.OwnedReferenceBranch.OwnedCollectionLeaf.Add(newLeaf);
                ClearLog();
                await context.SaveChangesAsync();

                // Do SaveChanges again, see #28813
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var updatedEntity = await context.JsonEntitiesBasic.SingleAsync();
                var updatedCollection = updatedEntity.OwnedReferenceRoot.OwnedReferenceBranch.OwnedCollectionLeaf;
                Assert.Equal(3, updatedCollection.Count);
                Assert.Equal("ss1", updatedCollection[2].SomethingSomething);
            });

    public override Task Add_element_to_json_collection_on_derived()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesInheritance.OfType<JsonEntityInheritanceDerived>().ToListAsync();
                var entity = query.Single();

                var newBranch = new JsonOwnedBranch
                {
                    Id = 77,
                    Date = new DateTime(2010, 10, 10),
                    Enum = JsonEnum.Three,
                    Fraction = 42.42m,
                    OwnedCollectionLeaf =
                    [
                        new JsonOwnedLeaf { SomethingSomething = "ss1" },
                        new JsonOwnedLeaf { SomethingSomething = "ss2" }
                    ],
                    OwnedReferenceLeaf = new JsonOwnedLeaf { SomethingSomething = "ss3" }
                };

                entity.CollectionOnDerived.Add(newBranch);
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.JsonEntitiesInheritance.OfType<JsonEntityInheritanceDerived>().SingleAsync();
                var updatedCollection = result.CollectionOnDerived;

                Assert.Equal(new DateTime(2010, 10, 10), updatedCollection[2].Date);
                Assert.Equal(JsonEnum.Three, updatedCollection[2].Enum);
                Assert.Equal(42.42m, updatedCollection[2].Fraction);
                Assert.Equal(77, updatedCollection[2].Id);
                Assert.Equal("ss3", updatedCollection[2].OwnedReferenceLeaf.SomethingSomething);
                var collectionLeaf = updatedCollection[2].OwnedCollectionLeaf;
                Assert.Equal(2, collectionLeaf.Count);
                Assert.Equal("ss1", collectionLeaf[0].SomethingSomething);
                Assert.Equal("ss2", collectionLeaf[1].SomethingSomething);
            });

    public override Task Delete_entity_with_json()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();

                context.Set<JsonEntityBasic>().Remove(entity);
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().CountAsync();

                Assert.Equal(0, result);
            });

    public override Task Delete_json_reference_root()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot = null;
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var updatedEntity = await context.JsonEntitiesBasic.SingleAsync();
                Assert.Null(updatedEntity.OwnedReferenceRoot);
            });

    public override Task Delete_json_reference_leaf()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot.OwnedReferenceBranch.OwnedReferenceLeaf = null;
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var updatedEntity = await context.JsonEntitiesBasic.SingleAsync();
                Assert.Null(updatedEntity.OwnedReferenceRoot.OwnedReferenceBranch.OwnedReferenceLeaf);
            });

    public override Task Delete_json_collection_root()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedCollectionRoot = null;
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Null(result.OwnedCollectionRoot);
            });

    public override Task Delete_json_collection_branch()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot.OwnedCollectionBranch = null;
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Null(result.OwnedReferenceRoot.OwnedCollectionBranch);
            });

    public override Task Edit_element_in_json_collection_root1()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedCollectionRoot[0].Name = "Modified";
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                var resultCollection = result.OwnedCollectionRoot;
                Assert.Equal(2, resultCollection.Count);
                Assert.Equal("Modified", resultCollection[0].Name);
            });

    public override Task Edit_element_in_json_collection_root2()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedCollectionRoot[1].Name = "Modified";
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                var resultCollection = result.OwnedCollectionRoot;
                Assert.Equal(2, resultCollection.Count);
                Assert.Equal("Modified", resultCollection[1].Name);
            });

    public override Task Edit_element_in_json_collection_branch()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedCollectionRoot[0].OwnedCollectionBranch[0].Date = new DateTime(2111, 11, 11);
                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Equal(new DateTime(2111, 11, 11), result.OwnedCollectionRoot[0].OwnedCollectionBranch[0].Date);
            });

    public override Task Edit_element_in_json_multiple_levels_partial_update()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot.OwnedReferenceBranch.Date = new DateTime(2111, 11, 11);
                entity.OwnedReferenceRoot.Name = "edit";
                entity.OwnedCollectionRoot[0].OwnedCollectionBranch[1].OwnedCollectionLeaf[0].SomethingSomething = "yet another change";
                entity.OwnedCollectionRoot[0].OwnedCollectionBranch[1].OwnedCollectionLeaf[1].SomethingSomething = "and another";
                entity.OwnedCollectionRoot[0].OwnedCollectionBranch[0].OwnedCollectionLeaf[0].SomethingSomething = "...and another";

                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Equal(new DateTime(2111, 11, 11), result.OwnedReferenceRoot.OwnedReferenceBranch.Date);
                Assert.Equal("edit", result.OwnedReferenceRoot.Name);
                Assert.Equal(
                    "yet another change", result.OwnedCollectionRoot[0].OwnedCollectionBranch[1].OwnedCollectionLeaf[0].SomethingSomething);
                Assert.Equal(
                    "and another", result.OwnedCollectionRoot[0].OwnedCollectionBranch[1].OwnedCollectionLeaf[1].SomethingSomething);
                Assert.Equal(
                    "...and another", result.OwnedCollectionRoot[0].OwnedCollectionBranch[0].OwnedCollectionLeaf[0].SomethingSomething);
            });

    public override Task Edit_element_in_json_branch_collection_and_add_element_to_the_same_collection()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot.OwnedCollectionBranch[0].Fraction = 4321.3m;
                entity.OwnedReferenceRoot.OwnedCollectionBranch.Add(
                    new JsonOwnedBranch
                    {
                        Id = 77,
                        Date = new DateTime(2222, 11, 11),
                        Enum = JsonEnum.Three,
                        Fraction = 45.32m,
                        OwnedReferenceLeaf = new JsonOwnedLeaf { SomethingSomething = "cc" },
                    });

                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Equal(4321.3m, result.OwnedReferenceRoot.OwnedCollectionBranch[0].Fraction);

                Assert.Equal(new DateTime(2222, 11, 11), result.OwnedReferenceRoot.OwnedCollectionBranch[2].Date);
                Assert.Equal(JsonEnum.Three, result.OwnedReferenceRoot.OwnedCollectionBranch[2].Enum);
                Assert.Equal(45.32m, result.OwnedReferenceRoot.OwnedCollectionBranch[2].Fraction);
                Assert.Equal(77, result.OwnedReferenceRoot.OwnedCollectionBranch[2].Id);
                Assert.Equal("cc", result.OwnedReferenceRoot.OwnedCollectionBranch[2].OwnedReferenceLeaf.SomethingSomething);
            });

    public override Task Edit_two_elements_in_the_same_json_collection()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot.OwnedCollectionBranch[0].OwnedCollectionLeaf[0].SomethingSomething = "edit1";
                entity.OwnedReferenceRoot.OwnedCollectionBranch[0].OwnedCollectionLeaf[1].SomethingSomething = "edit2";

                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Equal("edit1", result.OwnedReferenceRoot.OwnedCollectionBranch[0].OwnedCollectionLeaf[0].SomethingSomething);
                Assert.Equal("edit2", result.OwnedReferenceRoot.OwnedCollectionBranch[0].OwnedCollectionLeaf[1].SomethingSomething);
            });

    public override Task Edit_two_elements_in_the_same_json_collection_at_the_root()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedCollectionRoot[0].Name = "edit1";
                entity.OwnedCollectionRoot[1].Name = "edit2";

                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Equal("edit1", result.OwnedCollectionRoot[0].Name);
                Assert.Equal("edit2", result.OwnedCollectionRoot[1].Name);
            });

    public override Task Edit_collection_element_and_reference_at_once()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot.OwnedCollectionBranch[1].OwnedCollectionLeaf[0].SomethingSomething = "edit1";
                entity.OwnedReferenceRoot.OwnedCollectionBranch[1].OwnedReferenceLeaf.SomethingSomething = "edit2";

                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Equal("edit1", result.OwnedReferenceRoot.OwnedCollectionBranch[1].OwnedCollectionLeaf[0].SomethingSomething);
                Assert.Equal("edit2", result.OwnedReferenceRoot.OwnedCollectionBranch[1].OwnedReferenceLeaf.SomethingSomething);
            });

    public override Task Edit_single_enum_property()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot.OwnedReferenceBranch.Enum = JsonEnum.Two;
                entity.OwnedCollectionRoot[1].OwnedCollectionBranch[1].Enum = JsonEnum.Two;

                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Equal(JsonEnum.Two, result.OwnedReferenceRoot.OwnedReferenceBranch.Enum);
                Assert.Equal(JsonEnum.Two, result.OwnedCollectionRoot[1].OwnedCollectionBranch[1].Enum);
            });

    public override Task Edit_single_numeric_property()
        => TestHelpers.ExecuteWithStrategyInTransactionAsync(
            CreateContext,
            UseTransactionReseed,
            async context =>
            {
                var query = await context.JsonEntitiesBasic.ToListAsync();
                var entity = query.Single();
                entity.OwnedReferenceRoot.Number = 999;
                entity.OwnedCollectionRoot[1].Number = 1024;

                ClearLog();
                await context.SaveChangesAsync();
            },
            async context =>
            {
                var result = await context.Set<JsonEntityBasic>().SingleAsync();
                Assert.Equal(999, result.OwnedReferenceRoot.Number);
                Assert.Equal(1024, result.OwnedCollectionRoot[1].Number);
            });

    #endregion

    #region Skipped tests - JsonEntityAllTypes (not seeded)

    // These tests use JsonEntityAllTypes which is not seeded due to EF Core WriteJson issues
    private const string SkipAllTypesReason = "JsonEntityAllTypes is not seeded due to EF Core WriteJson issues";

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_bool()
        => base.Edit_single_property_bool();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_byte()
        => base.Edit_single_property_byte();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_char()
        => base.Edit_single_property_char();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_datetime()
        => base.Edit_single_property_datetime();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_datetimeoffset()
        => base.Edit_single_property_datetimeoffset();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_decimal()
        => base.Edit_single_property_decimal();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_double()
        => base.Edit_single_property_double();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_guid()
        => base.Edit_single_property_guid();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_int16()
        => base.Edit_single_property_int16();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_int32()
        => base.Edit_single_property_int32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_int64()
        => base.Edit_single_property_int64();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_signed_byte()
        => base.Edit_single_property_signed_byte();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_single()
        => base.Edit_single_property_single();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_timespan()
        => base.Edit_single_property_timespan();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_dateonly()
        => base.Edit_single_property_dateonly();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_timeonly()
        => base.Edit_single_property_timeonly();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_uint16()
        => base.Edit_single_property_uint16();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_uint32()
        => base.Edit_single_property_uint32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_uint64()
        => base.Edit_single_property_uint64();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_nullable_int32()
        => base.Edit_single_property_nullable_int32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_nullable_int32_set_to_null()
        => base.Edit_single_property_nullable_int32_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_enum()
        => base.Edit_single_property_enum();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_enum_with_int_converter()
        => base.Edit_single_property_enum_with_int_converter();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_nullable_enum()
        => base.Edit_single_property_nullable_enum();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_nullable_enum_set_to_null()
        => base.Edit_single_property_nullable_enum_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_nullable_enum_with_int_converter()
        => base.Edit_single_property_nullable_enum_with_int_converter();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_nullable_enum_with_int_converter_set_to_null()
        => base.Edit_single_property_nullable_enum_with_int_converter_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_nullable_enum_with_converter_that_handles_nulls()
        => base.Edit_single_property_nullable_enum_with_converter_that_handles_nulls();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_nullable_enum_with_converter_that_handles_nulls_set_to_null()
        => base.Edit_single_property_nullable_enum_with_converter_that_handles_nulls_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_bool()
        => base.Edit_single_property_collection_of_bool();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_byte()
        => base.Edit_single_property_collection_of_byte();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_char()
        => base.Edit_single_property_collection_of_char();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_datetime()
        => base.Edit_single_property_collection_of_datetime();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_datetimeoffset()
        => base.Edit_single_property_collection_of_datetimeoffset();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_decimal()
        => base.Edit_single_property_collection_of_decimal();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_double()
        => base.Edit_single_property_collection_of_double();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_guid()
        => base.Edit_single_property_collection_of_guid();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_int16()
        => base.Edit_single_property_collection_of_int16();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_int32()
        => base.Edit_single_property_collection_of_int32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_int64()
        => base.Edit_single_property_collection_of_int64();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_signed_byte()
        => base.Edit_single_property_collection_of_signed_byte();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_single()
        => base.Edit_single_property_collection_of_single();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_timespan()
        => base.Edit_single_property_collection_of_timespan();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_dateonly()
        => base.Edit_single_property_collection_of_dateonly();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_timeonly()
        => base.Edit_single_property_collection_of_timeonly();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_uint16()
        => base.Edit_single_property_collection_of_uint16();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_uint32()
        => base.Edit_single_property_collection_of_uint32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_uint64()
        => base.Edit_single_property_collection_of_uint64();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_nullable_int32()
        => base.Edit_single_property_collection_of_nullable_int32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_nullable_int32_set_to_null()
        => base.Edit_single_property_collection_of_nullable_int32_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_enum()
        => base.Edit_single_property_collection_of_enum();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_enum_with_int_converter()
        => base.Edit_single_property_collection_of_enum_with_int_converter();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_nullable_enum()
        => base.Edit_single_property_collection_of_nullable_enum();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_nullable_enum_set_to_null()
        => base.Edit_single_property_collection_of_nullable_enum_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_nullable_enum_with_int_converter()
        => base.Edit_single_property_collection_of_nullable_enum_with_int_converter();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_nullable_enum_with_int_converter_set_to_null()
        => base.Edit_single_property_collection_of_nullable_enum_with_int_converter_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_nullable_enum_with_converter_that_handles_nulls()
        => base.Edit_single_property_collection_of_nullable_enum_with_converter_that_handles_nulls();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_nullable_enum_with_converter_that_handles_nulls_set_to_null()
        => base.Edit_single_property_collection_of_nullable_enum_with_converter_that_handles_nulls_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_two_properties_on_same_entity_updates_the_entire_entity()
        => base.Edit_two_properties_on_same_entity_updates_the_entire_entity();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_a_scalar_property_and_reference_navigation_on_the_same_entity()
        => base.Edit_a_scalar_property_and_reference_navigation_on_the_same_entity();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_a_scalar_property_and_collection_navigation_on_the_same_entity()
        => base.Edit_a_scalar_property_and_collection_navigation_on_the_same_entity();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_a_scalar_property_and_another_property_behind_reference_navigation_on_the_same_entity()
        => base.Edit_a_scalar_property_and_another_property_behind_reference_navigation_on_the_same_entity();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_numeric()
        => base.Edit_single_property_collection_of_numeric();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_string()
        => base.Edit_single_property_collection_of_string();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_bool()
        => base.Edit_single_property_relational_collection_of_bool();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_byte()
        => base.Edit_single_property_relational_collection_of_byte();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_char()
        => base.Edit_single_property_relational_collection_of_char();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_datetime()
        => base.Edit_single_property_relational_collection_of_datetime();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_datetimeoffset()
        => base.Edit_single_property_relational_collection_of_datetimeoffset();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_decimal()
        => base.Edit_single_property_relational_collection_of_decimal();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_double()
        => base.Edit_single_property_relational_collection_of_double();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_guid()
        => base.Edit_single_property_relational_collection_of_guid();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_int16()
        => base.Edit_single_property_relational_collection_of_int16();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_int32()
        => base.Edit_single_property_relational_collection_of_int32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_int64()
        => base.Edit_single_property_relational_collection_of_int64();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_signed_byte()
        => base.Edit_single_property_relational_collection_of_signed_byte();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_single()
        => base.Edit_single_property_relational_collection_of_single();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_timespan()
        => base.Edit_single_property_relational_collection_of_timespan();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_uint16()
        => base.Edit_single_property_relational_collection_of_uint16();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_uint32()
        => base.Edit_single_property_relational_collection_of_uint32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_uint64()
        => base.Edit_single_property_relational_collection_of_uint64();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_nullable_int32()
        => base.Edit_single_property_relational_collection_of_nullable_int32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_nullable_int32_set_to_null()
        => base.Edit_single_property_relational_collection_of_nullable_int32_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_enum()
        => base.Edit_single_property_relational_collection_of_enum();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_enum_with_int_converter()
        => base.Edit_single_property_relational_collection_of_enum_with_int_converter();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_nullable_enum()
        => base.Edit_single_property_relational_collection_of_nullable_enum();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_nullable_enum_set_to_null()
        => base.Edit_single_property_relational_collection_of_nullable_enum_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_nullable_enum_with_int_converter()
        => base.Edit_single_property_relational_collection_of_nullable_enum_with_int_converter();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_nullable_enum_with_int_converter_set_to_null()
        => base.Edit_single_property_relational_collection_of_nullable_enum_with_int_converter_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_nullable_enum_with_converter_that_handles_nulls()
        => base.Edit_single_property_relational_collection_of_nullable_enum_with_converter_that_handles_nulls();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_relational_collection_of_nullable_enum_with_converter_that_handles_nulls_set_to_null()
        => base.Edit_single_property_relational_collection_of_nullable_enum_with_converter_that_handles_nulls_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_bool()
        => base.Edit_single_property_collection_of_collection_of_bool();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_char()
        => base.Edit_single_property_collection_of_collection_of_char();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_double()
        => base.Edit_single_property_collection_of_collection_of_double();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_int16()
        => base.Edit_single_property_collection_of_collection_of_int16();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_int32()
        => base.Edit_single_property_collection_of_collection_of_int32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_int64()
        => base.Edit_single_property_collection_of_collection_of_int64();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_single()
        => base.Edit_single_property_collection_of_collection_of_single();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_nullable_int32()
        => base.Edit_single_property_collection_of_collection_of_nullable_int32();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_nullable_int32_set_to_null()
        => base.Edit_single_property_collection_of_collection_of_nullable_int32_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_nullable_enum_set_to_null()
        => base.Edit_single_property_collection_of_collection_of_nullable_enum_set_to_null();

    [ConditionalFact(Skip = SkipAllTypesReason)]
    public override Task Edit_single_property_collection_of_collection_of_nullable_enum_with_int_converter()
        => base.Edit_single_property_collection_of_collection_of_nullable_enum_with_int_converter();

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [InlineData(null)]
    [InlineData(true)]
    [InlineData(false)]
    public override Task Add_and_update_top_level_optional_owned_collection_to_JSON(bool? value)
        => base.Add_and_update_top_level_optional_owned_collection_to_JSON(value);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [InlineData(null)]
    [InlineData(true)]
    [InlineData(false)]
    public override Task Add_and_update_nested_optional_owned_collection_to_JSON(bool? value)
        => base.Add_and_update_nested_optional_owned_collection_to_JSON(value);

    [ConditionalTheory(Skip = SkipAllTypesReason)]
    [InlineData(null)]
    [InlineData(true)]
    [InlineData(false)]
    public override Task Add_and_update_nested_optional_primitive_collection(bool? value)
        => base.Add_and_update_nested_optional_primitive_collection(value);

    #endregion

    #region Skipped tests - Converter tests (JsonEntitiesConverters)

    private const string SkipConvertersReason = "JsonEntitiesConverters tests need verification";

    [ConditionalFact(Skip = SkipConvertersReason)]
    public override Task Edit_single_property_with_converter_bool_to_int_zero_one()
        => base.Edit_single_property_with_converter_bool_to_int_zero_one();

    [ConditionalFact(Skip = SkipConvertersReason)]
    public override Task Edit_single_property_with_converter_bool_to_string_True_False()
        => base.Edit_single_property_with_converter_bool_to_string_True_False();

    [ConditionalFact(Skip = SkipConvertersReason)]
    public override Task Edit_single_property_with_converter_bool_to_string_Y_N()
        => base.Edit_single_property_with_converter_bool_to_string_Y_N();

    [ConditionalFact(Skip = SkipConvertersReason)]
    public override Task Edit_single_property_with_converter_int_zero_one_to_bool()
        => base.Edit_single_property_with_converter_int_zero_one_to_bool();

    [ConditionalFact(Skip = SkipConvertersReason)]
    public override Task Edit_single_property_with_converter_string_True_False_to_bool()
        => base.Edit_single_property_with_converter_string_True_False_to_bool();

    [ConditionalFact(Skip = SkipConvertersReason)]
    public override Task Edit_single_property_with_converter_string_Y_N_to_bool()
        => base.Edit_single_property_with_converter_string_Y_N_to_bool();

    #endregion

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public class JsonUpdateBigQueryFixture : JsonUpdateFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => BigQueryTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            // BigQuery-specific: Ignore unsupported nested collections on entity
            modelBuilder.Entity<JsonEntityAllTypes>(
                b =>
                {
                    b.Ignore(j => j.TestEnumCollection);
                    b.Ignore(j => j.TestUnsignedInt16Collection);
                    b.Ignore(j => j.TestNullableEnumCollection);
                    b.Ignore(j => j.TestNullableEnumWithIntConverterCollection);
                    b.Ignore(j => j.TestCharacterCollection);
                    b.Ignore(j => j.TestNullableInt32Collection);
                    b.Ignore(j => j.TestUnsignedInt64Collection);
                    b.Ignore(j => j.TestByteCollection);
                    b.Ignore(j => j.TestBooleanCollection);
                    b.Ignore(j => j.TestDateTimeOffsetCollection);
                    b.Ignore(j => j.TestDoubleCollection);
                    b.Ignore(j => j.TestInt16Collection);
                    b.Ignore(j => j.TestInt64Collection);
                    b.Ignore(j => j.TestGuidCollection);

                    b.Ignore(j => j.TestDefaultStringCollectionCollection);
                    b.Ignore(j => j.TestMaxLengthStringCollectionCollection);
                    b.Ignore(j => j.TestInt16CollectionCollection);
                    b.Ignore(j => j.TestInt32CollectionCollection);
                    b.Ignore(j => j.TestInt64CollectionCollection);
                    b.Ignore(j => j.TestDoubleCollectionCollection);
                    b.Ignore(j => j.TestSingleCollectionCollection);
                    b.Ignore(j => j.TestCharacterCollectionCollection);
                    b.Ignore(j => j.TestBooleanCollectionCollection);
                    b.Ignore(j => j.TestNullableInt32CollectionCollection);
                    b.Ignore(j => j.TestNullableEnumCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithIntConverterCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithConverterThatHandlesNullsCollection);
                });

            modelBuilder.Entity<JsonEntityAllTypes>().OwnsOne(
                x => x.Reference, b =>
                {
                    b.Ignore(j => j.TestDefaultStringCollectionCollection);
                    b.Ignore(j => j.TestMaxLengthStringCollectionCollection);
                    b.Ignore(j => j.TestInt16CollectionCollection);
                    b.Ignore(j => j.TestInt32CollectionCollection);
                    b.Ignore(j => j.TestInt64CollectionCollection);
                    b.Ignore(j => j.TestDoubleCollectionCollection);
                    b.Ignore(j => j.TestSingleCollectionCollection);
                    b.Ignore(j => j.TestBooleanCollectionCollection);
                    b.Ignore(j => j.TestCharacterCollectionCollection);
                    b.Ignore(j => j.TestNullableInt32CollectionCollection);
                    b.Ignore(j => j.TestNullableEnumCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithIntConverterCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithConverterThatHandlesNullsCollection);
                });

            modelBuilder.Entity<JsonEntityAllTypes>().OwnsMany(
                x => x.Collection, b =>
                {
                    b.Ignore(j => j.TestDefaultStringCollectionCollection);
                    b.Ignore(j => j.TestMaxLengthStringCollectionCollection);
                    b.Ignore(j => j.TestInt16CollectionCollection);
                    b.Ignore(j => j.TestInt32CollectionCollection);
                    b.Ignore(j => j.TestInt64CollectionCollection);
                    b.Ignore(j => j.TestDoubleCollectionCollection);
                    b.Ignore(j => j.TestSingleCollectionCollection);
                    b.Ignore(j => j.TestBooleanCollectionCollection);
                    b.Ignore(j => j.TestCharacterCollectionCollection);
                    b.Ignore(j => j.TestNullableInt32CollectionCollection);
                    b.Ignore(j => j.TestNullableEnumCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithIntConverterCollectionCollection);
                    b.Ignore(j => j.TestNullableEnumWithConverterThatHandlesNullsCollection);
                });
        }

        protected override Task SeedAsync(JsonQueryContext context)
        {
            // Custom seeding that skips JsonEntityAllTypes due to EF Core WriteJson issues
            var jsonEntitiesBasic = JsonQueryData.CreateJsonEntitiesBasic();
            var jsonEntitiesInheritance = JsonQueryData.CreateJsonEntitiesInheritance();
            var jsonEntitiesConverters = JsonQueryData.CreateJsonEntitiesConverters();

            context.JsonEntitiesBasic.AddRange(jsonEntitiesBasic);
            context.JsonEntitiesInheritance.AddRange(jsonEntitiesInheritance);
            context.JsonEntitiesConverters.AddRange(jsonEntitiesConverters);

            return context.SaveChangesAsync();
        }
    }
}
