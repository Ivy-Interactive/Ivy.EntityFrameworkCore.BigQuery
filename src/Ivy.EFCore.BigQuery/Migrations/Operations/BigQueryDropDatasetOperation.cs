using Microsoft.EntityFrameworkCore.Migrations.Operations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Migrations.Operations
{
    /// <summary>
    /// Represents an operation to drop a BigQuery dataset during a migration process.
    /// </summary>
    /// <remarks><see href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#drop_schema_statement"/></remarks>
    [DebuggerDisplay("DROP SCHEMA {Name}")]
    public class BigQueryDropDatasetOperation : MigrationOperation
    {
        /// <summary>
        /// Name of the dataset to drop.
        /// </summary>
        public string Name { get; set; } = null!;
        /// <summary>
        /// If no dataset exists with that name, the statement has no effect.
        /// </summary>
        public bool IfExists { get; set; } = true;

        //todo EXTERNAL?

        /// <summary>
        /// Gets or sets the behavior to apply when dropping a BigQuery dataset.
        /// </summary>
        public BigQueryDropDatasetBehavior Behavior { get; set; } = BigQueryDropDatasetBehavior.Restrict;

        /// <summary>
        /// The ID of the Google Cloud project that contains the dataset to drop.
        /// </summary>
        public string? ProjectId { get; internal set; }

        public enum BigQueryDropDatasetBehavior
        {
            /// <summary>
            /// Deletes the dataset and all resources within the dataset, such as tables, views, and functions.
            /// You must have permission to delete the resources, or else the statement returns an error. 
            /// For a list of BigQuery permissions, see 
            /// <see href="https://cloud.google.com/bigquery/docs/access-control"> Predefined roles and permissions.</see>
            /// </summary>
            Cascade,
            Restrict
        }
    }
}
