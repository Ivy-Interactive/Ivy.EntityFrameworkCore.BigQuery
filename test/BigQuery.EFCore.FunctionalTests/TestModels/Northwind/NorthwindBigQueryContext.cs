using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;

namespace Ivy.EntityFrameworkCore.BigQuery.TestModels.Northwind
{
    public class NorthwindBigQueryContext(DbContextOptions options) : NorthwindRelationalContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // Clear for parallel test
            modelBuilder.HasDefaultSchema(null);

            base.OnModelCreating(modelBuilder);

            // Clear for parallel test
            var model = modelBuilder.Model;
            foreach (var entityType in model.GetEntityTypes())
            {
                entityType.SetSchema(null);
            }

            modelBuilder.Entity<Product>(b =>
            {
                b.Property(p => p.UnitPrice).HasColumnType("BIGNUMERIC(57, 28)");
            });

            modelBuilder.Entity<OrderDetail>(b =>
            {
                b.Property(p => p.UnitPrice).HasColumnType("BIGNUMERIC(57, 28)");

            });

            modelBuilder.Entity<Order>(b =>
            {
                b.Property(o => o.OrderDate).HasColumnType("TIMESTAMP");
            });

            modelBuilder.Entity<Customer>(b =>
            {
                b.ToTable("Customers");
            });


            //modelBuilder.Entity<CustomerQuery>().ToSqlQuery(
            //    "SELECT * FROM `Customers`"
            //);
        }
    }
}