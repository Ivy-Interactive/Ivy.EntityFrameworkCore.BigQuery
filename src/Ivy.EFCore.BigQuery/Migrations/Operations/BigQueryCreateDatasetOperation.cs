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
    /// Represents an operation to create a BigQuery dataset.
    /// <see href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement"/>
    [DebuggerDisplay("CREATE SCHEMA {Name}")]
    public class BigQueryCreateDatasetOperation : MigrationOperation
    {

        /// <summary>
        /// Name of the dataset to create.
        /// </summary>
        public string Name { get; set; } = null!;

        /// <summary>
        /// The project ID where the dataset will be created.
        /// </summary>
        public string? ProjectId { get; set; }

        //todo: OPTIONS

        /// <summary>
        /// If any dataset exists with the same name, the CREATE statement has no effect.
        /// </summary>
        public bool IfNotExists { get; set; } = true;
    }
}
