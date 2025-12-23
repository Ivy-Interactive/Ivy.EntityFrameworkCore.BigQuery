using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Scaffolding;
using System.Text.RegularExpressions;

#pragma warning disable EF1001 // Internal EF Core API usage.
namespace Ivy.EntityFrameworkCore.BigQuery.Scaffolding.Internal
{
    /// <summary>
    /// Custom model code generator that handles STRUCT types specially
    /// </summary>
    public class BigQueryModelCodeGenerator : ModelCodeGenerator
    {
        private readonly IServiceProvider _serviceProvider;

        public BigQueryModelCodeGenerator(
            ModelCodeGeneratorDependencies dependencies,
            IServiceProvider serviceProvider)
            : base(dependencies)
        {
            _serviceProvider = serviceProvider;
        }

        public override string Language => "C#";

        public override ScaffoldedModel GenerateModel(
            IModel model,
            ModelCodeGenerationOptions options)
        {

            var defaultGenerator = new Microsoft.EntityFrameworkCore.Scaffolding.Internal.CSharpModelGenerator(
                Dependencies,
                _serviceProvider.GetService(typeof(Microsoft.EntityFrameworkCore.Design.Internal.IOperationReporter))
                    as Microsoft.EntityFrameworkCore.Design.Internal.IOperationReporter
                    ?? throw new InvalidOperationException("IOperationReporter not found"),
                _serviceProvider);

            var scaffoldedModel = defaultGenerator.GenerateModel(model, options);

            PostProcessStructEntityFiles(scaffoldedModel, model);

            PostProcessDbContext(scaffoldedModel, model);

            return scaffoldedModel;
        }

        /// <summary>
        /// Add [BigQueryStruct] and remove other attributes
        /// </summary>
        private void PostProcessStructEntityFiles(ScaffoldedModel scaffoldedModel, IModel model)
        {
            var filesToReplace = new List<(int index, ScaffoldedFile newFile)>();

            var structEntityTypes = model.GetEntityTypes()
                .Where(e => e.FindAnnotation("BigQuery:IsStructType")?.Value as bool? == true)
                .ToList();

            for (int i = 0; i < scaffoldedModel.AdditionalFiles.Count; i++)
            {
                var file = scaffoldedModel.AdditionalFiles[i];

                var structEntity = structEntityTypes.FirstOrDefault(e =>
                    file.Path.Equals($"{e.Name}.cs", StringComparison.OrdinalIgnoreCase));

                if (structEntity != null)
                {
                    var modifiedCode = TransformStructEntity(file.Code, structEntity);
                    filesToReplace.Add((i, new ScaffoldedFile(file.Path, modifiedCode)));
                }
                else
                {
                    var regularEntity = model.GetEntityTypes().FirstOrDefault(e =>
                    {
                        if (!file.Path.Equals($"{e.Name}.cs", StringComparison.OrdinalIgnoreCase))
                            return false;

                        return e.FindAnnotation("BigQuery:IsStructType")?.Value as bool? != true; // Only regular entities
                    });

                    if (regularEntity != null)
                    {
                        var modifiedCode = ReplaceStructPropertyTypes(file.Code, regularEntity);
                        if (modifiedCode != file.Code)
                        {
                            filesToReplace.Add((i, new ScaffoldedFile(file.Path, modifiedCode)));
                        }
                    }
                }
            }

            foreach (var (index, newFile) in filesToReplace)
            {
                scaffoldedModel.AdditionalFiles[index] = newFile;
            }
        }

        /// <summary>
        /// Transform a STRUCT entity class to add [BigQueryStruct] and other attributes
        /// </summary>
        private string TransformStructEntity(string code, IEntityType entityType)
        {
            var lines = code.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
            var result = new System.Text.StringBuilder();
            bool hasStructAttribute = false;
            bool hasAddedUsing = code.Contains("using Ivy.EntityFrameworkCore.BigQuery.Metadata;");
            int lastUsingLineIndex = -1;

            for (int i = 0; i < lines.Length; i++)
            {
                if (lines[i].Trim().StartsWith("using "))
                {
                    lastUsingLineIndex = i;
                }
            }

            for (int i = 0; i < lines.Length; i++)
            {
                var line = lines[i];

                if (line.Trim().StartsWith("[Table("))
                {
                    continue;
                }

                if (line.Trim().StartsWith("[PrimaryKey"))
                {
                    continue;
                }

                if (line.Trim().StartsWith("[Index("))
                {
                    continue;
                }

                if (line.Trim().StartsWith("[Keyless"))
                {
                    continue;
                }

                if (!hasAddedUsing && i == lastUsingLineIndex)
                {
                    result.AppendLine(line);
                    result.AppendLine("using Ivy.EntityFrameworkCore.BigQuery.Metadata;");
                    hasAddedUsing = true;
                    continue;
                }

                if (line.Contains($"public partial class {entityType.Name}"))
                {
                    if (!hasStructAttribute)
                    {
                        var indent = GetIndentation(line);
                        result.AppendLine($"{indent}[BigQueryStruct]");
                        hasStructAttribute = true;
                    }
                }

                result.AppendLine(line);
            }

            return result.ToString();
        }

        /// <summary>
        /// Replace IDictionary<string, object> with STRUCT class names in regular entities
        /// </summary>
        private string ReplaceStructPropertyTypes(string code, IEntityType entityType)
        {
            var modifiedCode = code;

            foreach (var property in entityType.GetProperties())
            {
                var structClassName = property.FindAnnotation("BigQuery:StructClassName")?.Value as string;
                if (!string.IsNullOrEmpty(structClassName))
                {
                    var propertyName = property.Name;

                    var replacements = new[]
                    {
                        ($"public IDictionary<string, object>? {propertyName} {{ get; set; }}",
                         $"public {structClassName}? {propertyName} {{ get; set; }}"),
                        ($"public IDictionary<string, object> {propertyName} {{ get; set; }}",
                         $"public {structClassName} {propertyName} {{ get; set; }}"),
                        ($"public IDictionary<string, object> {propertyName} {{ get; set; }} = null!;",
                         $"public {structClassName} {propertyName} {{ get; set; }} = null!;")
                    };

                    foreach (var (oldPattern, newPattern) in replacements)
                    {
                        if (modifiedCode.Contains(oldPattern))
                        {
                            modifiedCode = modifiedCode.Replace(oldPattern, newPattern);
                        }
                    }
                }
            }

            return modifiedCode;
        }

        /// <summary>
        /// Remove DbSets
        /// </summary>
        private void PostProcessDbContext(ScaffoldedModel scaffoldedModel, IModel model)
        {
            var structEntityNames = model.GetEntityTypes()
                .Where(e => e.FindAnnotation("BigQuery:IsStructType")?.Value as bool? == true)
                .Select(e => e.Name)
                .ToHashSet();

            var modifiedCode = scaffoldedModel.ContextFile.Code;
            var lines = modifiedCode.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
            var result = new System.Text.StringBuilder();

            bool hasAddedUsing = modifiedCode.Contains("using Ivy.EntityFrameworkCore.BigQuery.Extensions;");
            int lastUsingLineIndex = -1;

            for (int i = 0; i < lines.Length; i++)
            {
                if (lines[i].Trim().StartsWith("using "))
                {
                    lastUsingLineIndex = i;
                }
            }

            for (int i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                bool shouldSkip = false;

                if (!hasAddedUsing && i == lastUsingLineIndex)
                {
                    result.AppendLine(line);
                    result.AppendLine("using Ivy.EntityFrameworkCore.BigQuery.Extensions;");
                    hasAddedUsing = true;
                    continue;
                }

                if (structEntityNames.Any())
                {
                    foreach (var structName in structEntityNames)
                    {
                        if (Regex.IsMatch(line.Trim(), $@"^public\s+(virtual\s+)?DbSet<{Regex.Escape(structName)}>\s+\w+\s*{{\s*get;\s*set;\s*}}"))
                        {
                            shouldSkip = true;
                            break;
                        }
                    }
                }

                if (!shouldSkip)
                {
                    result.AppendLine(line);
                }
            }

            scaffoldedModel.ContextFile = new ScaffoldedFile(
                scaffoldedModel.ContextFile.Path,
                result.ToString());
        }

        private static string GetIndentation(string line)
        {
            int count = 0;
            foreach (char c in line)
            {
                if (c == ' ' || c == '\t')
                {
                    count++;
                }
                else
                {
                    break;
                }
            }
            return line.Substring(0, count);
        }
    }
}