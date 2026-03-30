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
                b.Property(p => p.UnitPrice).HasColumnType("NUMERIC");
            });

            modelBuilder.Entity<OrderDetail>(b =>
            {
                b.Property(p => p.UnitPrice).HasColumnType("NUMERIC");
            });

            modelBuilder.Entity<Order>(b =>
            {
                b.Property(o => o.OrderDate).HasColumnType("TIMESTAMP");
            });

            modelBuilder.Entity<Customer>(b =>
            {
                b.ToTable("Customers");
            });

            // Override base NorthwindRelationalContext's SQL Server syntax with BigQuery backticks
            modelBuilder.Entity<CustomerQuery>().ToSqlQuery(
                "SELECT `c`.`CustomerID`, `c`.`Address`, `c`.`City`, `c`.`CompanyName`, `c`.`ContactName`, `c`.`ContactTitle`, `c`.`Country`, `c`.`Fax`, `c`.`Phone`, `c`.`PostalCode`, `c`.`Region` FROM `Customers` AS `c`");

            modelBuilder.Entity<OrderQuery>().ToSqlQuery("SELECT * FROM `Orders`");
        }
    }
}