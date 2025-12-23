using System;

namespace Ivy.EntityFrameworkCore.BigQuery.Metadata;

/// <summary>
/// Set entity as BigQuery STRUCT&lt;...&gt;
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class BigQueryStructAttribute : Attribute
{
}
