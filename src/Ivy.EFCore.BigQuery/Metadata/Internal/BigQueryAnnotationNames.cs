namespace Ivy.EntityFrameworkCore.BigQuery.Metadata.Internal
{
	/// <summary>
	/// BigQuery provider-specific annotation names.
	/// </summary>
	public static class BigQueryAnnotationNames
	{
		public const string Prefix = "BigQuery:";

		// Search index
		public const string SearchIndex = Prefix + "SearchIndex";
		public const string IfNotExists = Prefix + "IfNotExists";
		public const string IfExists = Prefix + "IfExists";
		public const string AllColumns = Prefix + "AllColumns";
		public const string IndexOptions = Prefix + "IndexOptions";
		public const string IndexColumnOptions = Prefix + "IndexColumnOptions";

		// Table
		public const string CreateOrReplace = Prefix + "CreateOrReplace";
		public const string TempTable = Prefix + "TempTable";
	}
}