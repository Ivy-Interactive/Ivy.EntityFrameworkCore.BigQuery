using Ivy.EntityFrameworkCore.BigQuery.Extensions;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;

namespace Ivy.EntityFrameworkCore.BigQuery.Scaffolding.Internal
{
    public class BigQueryCodeGenerator : ProviderCodeGenerator
    {
        public BigQueryCodeGenerator(ProviderCodeGeneratorDependencies dependencies) 
            : base(dependencies) { }

        public override MethodCallCodeFragment GenerateUseProvider(string connectionString, MethodCallCodeFragment? providerOptions)
             => new(nameof(BigQueryDbContextOptionsBuilderExtensions.UseBigQuery),
                providerOptions == null
                    ? [connectionString]
                    : [connectionString, new NestedClosureCodeFragment("x", providerOptions)]);
    }
}