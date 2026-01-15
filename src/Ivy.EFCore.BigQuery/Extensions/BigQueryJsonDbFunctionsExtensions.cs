using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

// ReSharper disable once CheckNamespace
namespace Ivy.EntityFrameworkCore.BigQuery.Extensions
{
    /// <summary>
    /// Provides BigQuery-specific extensions to <see cref="DbFunctions"/> for JSON operations.
    /// </summary>
    public static class BigQueryJsonDbFunctionsExtensions
    {
        /// <summary>
        /// Returns the type of the outermost JSON value as a text string.
        /// Possible types are: "object", "array", "string", "number", "boolean", and "null".
        /// Maps to BigQuery's JSON_TYPE function.
        /// </summary>
        /// <param name="_">The DbFunctions instance.</param>
        /// <param name="json">A JSON value (can be JsonElement, JsonDocument, or user POCO mapped to JSON).</param>
        /// <returns>The type of the JSON value as a string.</returns>
        public static string JsonType(this DbFunctions _, object json)
            => throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(JsonType)));

        /// <summary>
        /// Returns an array of unique keys from a JSON object.
        /// Maps to BigQuery's JSON_KEYS function.
        /// </summary>
        /// <param name="_">The DbFunctions instance.</param>
        /// <param name="json">A JSON object (can be JsonElement, JsonDocument, or user POCO mapped to JSON).</param>
        /// <returns>An array of key names from the JSON object.</returns>
        public static string[] JsonKeys(this DbFunctions _, object json)
            => throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(JsonKeys)));
    }
}
