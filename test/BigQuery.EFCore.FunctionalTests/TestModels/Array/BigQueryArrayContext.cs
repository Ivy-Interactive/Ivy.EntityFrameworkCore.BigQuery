using Microsoft.EntityFrameworkCore;

namespace Ivy.EntityFrameworkCore.BigQuery.TestModels.Array;

public class BigQueryArrayContext : DbContext
{
    public BigQueryArrayContext(DbContextOptions<BigQueryArrayContext> options)
        : base(options)
    {
    }

    public DbSet<ArrayEntity> ArrayEntities { get; set; } = null!;
    public DbSet<ArrayContainerEntity> ArrayContainers { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ArrayEntity>(entity =>
        {
            entity.HasKey(e => e.Id);

            entity.Property(e => e.IntArray).IsRequired();
            entity.Property(e => e.IntList).IsRequired();
            entity.Property(e => e.LongArray).IsRequired();
            entity.Property(e => e.StringArray).IsRequired();
            entity.Property(e => e.StringList).IsRequired();
            entity.Property(e => e.DoubleArray).IsRequired();
            entity.Property(e => e.DoubleList).IsRequired();
            entity.Property(e => e.BoolArray).IsRequired();
            entity.Property(e => e.ByteArray).IsRequired();
            entity.Property(e => e.Name).IsRequired();
        });

        modelBuilder.Entity<ArrayContainerEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.HasMany(e => e.ArrayEntities)
                .WithOne()
                .HasForeignKey("ContainerId")
                .IsRequired(false);
        });
    }

    public static async Task SeedAsync(BigQueryArrayContext context)
    {
        var arrayEntities = ArrayData.CreateArrayEntities().ToList();

        context.ArrayEntities.AddRange(arrayEntities);
        await context.SaveChangesAsync();

        context.ArrayContainers.Add(
            new ArrayContainerEntity
            {
                Id = 1,
                ArrayEntities = arrayEntities
            });

        await context.SaveChangesAsync();
    }
}