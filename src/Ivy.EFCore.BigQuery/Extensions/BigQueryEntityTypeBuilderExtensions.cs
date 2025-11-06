using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Ivy.EntityFrameworkCore.BigQuery.Metadata.Internal;

namespace Ivy.EntityFrameworkCore.BigQuery.Extensions
{
    /// <summary>
    /// BigQuery-specific extension methods for <see cref="EntityTypeBuilder{TEntity}"/>.
    /// </summary>
    public static class BigQueryEntityTypeBuilderExtensions
    {
        /// <summary>
        /// Configures the table to be created with CREATE OR REPLACE.
        /// </summary>
        /// <typeparam name="TEntity">The entity type being configured.</typeparam>
        /// <param name="entityTypeBuilder">The builder for the entity type being configured.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static EntityTypeBuilder<TEntity> HasBigQueryCreateOrReplace<TEntity>(
            this EntityTypeBuilder<TEntity> entityTypeBuilder)
            where TEntity : class
        {
            entityTypeBuilder.Metadata[BigQueryAnnotationNames.CreateOrReplace] = true;
            return entityTypeBuilder;
        }

        /// <summary>
        /// Configures the table to be created with IF NOT EXISTS.
        /// </summary>
        /// <typeparam name="TEntity">The entity type being configured.</typeparam>
        /// <param name="entityTypeBuilder">The builder for the entity type being configured.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static EntityTypeBuilder<TEntity> HasBigQueryIfNotExists<TEntity>(
            this EntityTypeBuilder<TEntity> entityTypeBuilder)
            where TEntity : class
        {
            entityTypeBuilder.Metadata[BigQueryAnnotationNames.IfNotExists] = true;
            return entityTypeBuilder;
        }

        /// <summary>
        /// Configures the table to be created as a temporary table.
        /// </summary>
        /// <typeparam name="TEntity">The entity type being configured.</typeparam>
        /// <param name="entityTypeBuilder">The builder for the entity type being configured.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static EntityTypeBuilder<TEntity> HasBigQueryTempTable<TEntity>(
            this EntityTypeBuilder<TEntity> entityTypeBuilder)
            where TEntity : class
        {
            entityTypeBuilder.Metadata[BigQueryAnnotationNames.TempTable] = true;
            return entityTypeBuilder;
        }

        /// <summary>
        /// Configures the table to be created as a temporary table with IF NOT EXISTS.
        /// </summary>
        /// <typeparam name="TEntity">The entity type being configured.</typeparam>
        /// <param name="entityTypeBuilder">The builder for the entity type being configured.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static EntityTypeBuilder<TEntity> HasBigQueryTempTableIfNotExists<TEntity>(
            this EntityTypeBuilder<TEntity> entityTypeBuilder)
            where TEntity : class
        {
            entityTypeBuilder.Metadata[BigQueryAnnotationNames.TempTable] = true;
            entityTypeBuilder.Metadata[BigQueryAnnotationNames.IfNotExists] = true;
            return entityTypeBuilder;
        }

        /// <summary>
        /// Configures the table to be created with CREATE OR REPLACE as a temporary table.
        /// </summary>
        /// <typeparam name="TEntity">The entity type being configured.</typeparam>
        /// <param name="entityTypeBuilder">The builder for the entity type being configured.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static EntityTypeBuilder<TEntity> HasBigQueryCreateOrReplaceTempTable<TEntity>(
            this EntityTypeBuilder<TEntity> entityTypeBuilder)
            where TEntity : class
        {
            entityTypeBuilder.Metadata[BigQueryAnnotationNames.CreateOrReplace] = true;
            entityTypeBuilder.Metadata[BigQueryAnnotationNames.TempTable] = true;
            return entityTypeBuilder;
        }
    }
}
