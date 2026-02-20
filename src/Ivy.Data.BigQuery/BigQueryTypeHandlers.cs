namespace Ivy.Data.BigQuery;

/// <summary>
/// Static registry for BigQuery type handler plugins.
/// Use this to register custom type handlers for GEOGRAPHY and other extensible types.
/// </summary>
public static class BigQueryTypeHandlers
{
    private static IGeographyTypeHandler? _geographyHandler;
    private static readonly object _lock = new();

    /// <summary>
    /// Gets the currently registered geography type handler, or null if none is registered.
    /// </summary>
    public static IGeographyTypeHandler? GeographyHandler
    {
        get
        {
            lock (_lock)
            {
                return _geographyHandler;
            }
        }
    }

    /// <summary>
    /// Registers a geography type handler for the ADO.NET provider.
    /// This enables support for spatial types like NetTopologySuite Geometry.
    /// </summary>
    /// <param name="handler">The handler to register.</param>
    /// <exception cref="ArgumentNullException">Thrown when handler is null.</exception>
    public static void UseGeographyHandler(IGeographyTypeHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        lock (_lock)
        {
            _geographyHandler = handler;
        }
    }

    /// <summary>
    /// Removes the currently registered geography type handler.
    /// </summary>
    public static void ClearGeographyHandler()
    {
        lock (_lock)
        {
            _geographyHandler = null;
        }
    }

    /// <summary>
    /// Resets all type handlers to their default state (no handlers registered).
    /// Useful for testing scenarios.
    /// </summary>
    public static void Reset()
    {
        lock (_lock)
        {
            _geographyHandler = null;
        }
    }
}