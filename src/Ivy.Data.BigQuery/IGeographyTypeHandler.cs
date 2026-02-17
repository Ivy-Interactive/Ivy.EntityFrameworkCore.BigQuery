namespace Ivy.Data.BigQuery;

/// <summary>
/// Interface for handling GEOGRAPHY type conversion in the ADO.NET provider.
/// Implement this interface in a plugin assembly to provide support for
/// spatial types without requiring a hard dependency on NetTopologySuite.
/// </summary>
public interface IGeographyTypeHandler
{
    /// <summary>
    /// Determines if the given CLR type is a geography type that this handler supports.
    /// </summary>
    /// <param name="type">The CLR type to check.</param>
    /// <returns>True if this handler can process the type, false otherwise.</returns>
    bool IsGeographyType(Type type);

    /// <summary>
    /// Tries to convert a value to a GEOGRAPHY parameter value (typically WKT string).
    /// </summary>
    /// <param name="value">The value to convert.</param>
    /// <param name="convertedValue">The converted value (WKT string) if successful.</param>
    /// <returns>True if the conversion was successful, false otherwise.</returns>
    bool TryConvertToGeography(object? value, out object? convertedValue);

    /// <summary>
    /// Tries to convert a GEOGRAPHY value from BigQuery to the target CLR type.
    /// </summary>
    /// <param name="value">The value from BigQuery (BigQueryGeography, WKT string, or dictionary).</param>
    /// <param name="targetType">The target CLR type to convert to.</param>
    /// <param name="result">The converted geometry object if successful.</param>
    /// <returns>True if the conversion was successful, false otherwise.</returns>
    bool TryConvertFromGeography(object? value, Type targetType, out object? result);
}