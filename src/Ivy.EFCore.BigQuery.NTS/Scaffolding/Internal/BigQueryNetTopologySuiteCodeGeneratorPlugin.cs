using System.Reflection;
using Ivy.EntityFrameworkCore.BigQuery.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;

namespace Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Scaffolding.Internal;

/// <summary>
/// Code generator plugin that adds UseNetTopologySuite() to scaffolded DbContext.
/// </summary>
public class BigQueryNetTopologySuiteCodeGeneratorPlugin : ProviderCodeGeneratorPlugin
{
    private static readonly MethodInfo UseNetTopologySuiteMethodInfo
        = typeof(BigQueryNetTopologySuiteDbContextOptionsBuilderExtensions).GetRuntimeMethod(
            nameof(BigQueryNetTopologySuiteDbContextOptionsBuilderExtensions.UseNetTopologySuite),
            [typeof(BigQueryDbContextOptionsBuilder)])!;

    /// <inheritdoc />
    public override MethodCallCodeFragment GenerateProviderOptions()
        => new(UseNetTopologySuiteMethodInfo);
}