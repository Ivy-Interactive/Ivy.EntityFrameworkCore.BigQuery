using Microsoft.EntityFrameworkCore.Scaffolding;

namespace Ivy.EntityFrameworkCore.BigQuery.Scaffolding.Internal
{
    public class BigQueryModelCodeGeneratorSelector : IModelCodeGeneratorSelector
    {
        private readonly IEnumerable<IModelCodeGenerator> _generators;

        public BigQueryModelCodeGeneratorSelector(IEnumerable<IModelCodeGenerator> generators)
        {
            _generators = generators;
        }

        public IModelCodeGenerator Select(string? language)
        {
            var bigQueryGenerator = _generators.FirstOrDefault(g => g is BigQueryModelCodeGenerator);

            if (bigQueryGenerator != null)
            {
                return bigQueryGenerator;
            }

            var fallback = _generators.FirstOrDefault(g => g.Language == language);
            return fallback ?? throw new InvalidOperationException($"No model code generator found for language {language}");
        }
    }
}