using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Ivy.EntityFrameworkCore.BigQuery.Metadata.Internal;

namespace Ivy.EntityFrameworkCore.BigQuery.Extensions
{
    /// <summary>
    /// BigQuery-specific extension methods for <see cref="IndexBuilder"/>.
    /// </summary>
    public static class BigQueryIndexBuilderExtensions
    {
        /// <summary>
        /// Configures the index as a BigQuery search index.
        /// </summary>
        /// <param name="indexBuilder">The builder for the index being configured.</param>
        /// <param name="ifNotExists">If true, adds IF NOT EXISTS to the CREATE SEARCH INDEX statement.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static IndexBuilder HasBigQuerySearchIndex(
            this IndexBuilder indexBuilder,
            bool ifNotExists = false)
        {
            indexBuilder.Metadata[BigQueryAnnotationNames.SearchIndex] = true;
            if (ifNotExists)
            {
                indexBuilder.Metadata[BigQueryAnnotationNames.IfNotExists] = true;
            }
            return indexBuilder;
        }

        /// <summary>
        /// Configures the index as a BigQuery search index on all columns.
        /// </summary>
        /// <param name="indexBuilder">The builder for the index being configured.</param>
        /// <param name="ifNotExists">If true, adds IF NOT EXISTS to the CREATE SEARCH INDEX statement.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static IndexBuilder HasBigQuerySearchIndexOnAllColumns(
            this IndexBuilder indexBuilder,
            bool ifNotExists = false)
        {
            indexBuilder.Metadata[BigQueryAnnotationNames.SearchIndex] = true;
            indexBuilder.Metadata[BigQueryAnnotationNames.AllColumns] = true;
            if (ifNotExists)
            {
                indexBuilder.Metadata[BigQueryAnnotationNames.IfNotExists] = true;
            }
            return indexBuilder;
        }

        /// <summary>
        /// Configures BigQuery search index options.
        /// </summary>
        /// <param name="indexBuilder">The builder for the index being configured.</param>
        /// <param name="options">The options string in the format "name=value, name2=value2".</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static IndexBuilder HasBigQuerySearchIndexOptions(
            this IndexBuilder indexBuilder,
            string options)
        {
            indexBuilder.Metadata[BigQueryAnnotationNames.IndexOptions] = options;
            return indexBuilder;
        }

        /// <summary>
        /// Configures BigQuery search index column options.
        /// </summary>
        /// <param name="indexBuilder">The builder for the index being configured.</param>
        /// <param name="columnOptions">Dictionary mapping column names to their options strings.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static IndexBuilder HasBigQuerySearchIndexColumnOptions(
            this IndexBuilder indexBuilder,
            IDictionary<string, string> columnOptions)
        {
            indexBuilder.Metadata[BigQueryAnnotationNames.IndexColumnOptions] = columnOptions;
            return indexBuilder;
        }

        /// <summary>
        /// Configures BigQuery search index column options for a specific column.
        /// </summary>
        /// <param name="indexBuilder">The builder for the index being configured.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="options">The options string for the column.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static IndexBuilder HasBigQuerySearchIndexColumnOptions(
            this IndexBuilder indexBuilder,
            string columnName,
            string options)
        {
            var existingOptions = indexBuilder.Metadata[BigQueryAnnotationNames.IndexColumnOptions] as IDictionary<string, string>
                ?? new Dictionary<string, string>();
            
            existingOptions[columnName] = options;
            indexBuilder.Metadata[BigQueryAnnotationNames.IndexColumnOptions] = existingOptions;
            return indexBuilder;
        }

        /// <summary>
        /// Configures the index to be dropped with IF EXISTS.
        /// </summary>
        /// <param name="indexBuilder">The builder for the index being configured.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public static IndexBuilder HasBigQueryDropIfExists(
            this IndexBuilder indexBuilder)
        {
            indexBuilder.Metadata[BigQueryAnnotationNames.IfExists] = true;
            return indexBuilder;
        }
    }
}
