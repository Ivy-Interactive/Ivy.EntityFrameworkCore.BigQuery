using Microsoft.EntityFrameworkCore.Storage;
using System.Text;

namespace Ivy.EFCore.BigQuery.Storage.Internal
{
    public class BigQuerySqlGenerationHelper : RelationalSqlGenerationHelper
    {
        //private string _escapeIdentifier(string identifier) => identifier.Replace("`", "``");

        public BigQuerySqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies) : base(dependencies)
        {
        }

        public override string DelimitIdentifier(string name, string? schema)
            => schema == null
                ? DelimitIdentifier(name)
                : $"{DelimitIdentifier(schema)}.{DelimitIdentifier(name)}";

        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
        public override string DelimitIdentifier(string name) => $"`{name.Replace("`", "``")}`";

        public override void DelimitIdentifier(StringBuilder builder, string identifier)
        {
            builder.Append('`');
            builder.Append(identifier.Replace("`", "``"));
            builder.Append('`');
        }
        

        public override void GenerateParameterName(StringBuilder builder, string name)
            => builder.Append('@').Append(name);

        public override string BatchTerminator => "";

        public override string StartTransactionStatement
            => "BEGIN TRANSACTION" + StatementTerminator;
    }
}
