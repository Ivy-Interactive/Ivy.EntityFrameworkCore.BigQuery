using System.Data.Common;
using System.Linq.Expressions;
using Ivy.Data.BigQuery;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace Ivy.EntityFrameworkCore.BigQuery.NetTopologySuite.Storage.Internal;

/// <summary>
/// Type mapping for NetTopologySuite Geometry types to BigQuery GEOGRAPHY.
/// BigQuery only supports GEOGRAPHY (spherical, WGS84), not GEOMETRY (planar).
/// </summary>
public class BigQueryGeographyTypeMapping : RelationalTypeMapping
{
    private readonly NtsGeometryServices _geometryServices;
    private readonly WKTReader _wktReader;
    private readonly WKTWriter _wktWriter;

    /// <summary>
    /// Creates a new instance.
    /// </summary>
    public BigQueryGeographyTypeMapping(Type clrType, NtsGeometryServices geometryServices, string storeType = "GEOGRAPHY")
        : base(CreateParameters(clrType, geometryServices, storeType))
    {
        _geometryServices = geometryServices;
        _wktReader = new WKTReader(geometryServices);
        _wktWriter = new WKTWriter();
    }

    /// <summary>
    /// Creates a new instance from existing parameters.
    /// </summary>
    protected BigQueryGeographyTypeMapping(
        RelationalTypeMappingParameters parameters,
        NtsGeometryServices geometryServices)
        : base(parameters)
    {
        _geometryServices = geometryServices;
        _wktReader = new WKTReader(geometryServices);
        _wktWriter = new WKTWriter();
    }

    private static RelationalTypeMappingParameters CreateParameters(
        Type clrType,
        NtsGeometryServices geometryServices,
        string storeType)
    {
        var wktReader = new WKTReader(geometryServices);
        var wktWriter = new WKTWriter();

        var converter = CreateGeometryConverter(clrType, wktReader, wktWriter);
        var comparer = CreateGeometryComparer(clrType);
        var jsonReaderWriter = CreateGeometryJsonReaderWriter(clrType, wktReader, wktWriter);

        return new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                clrType,
                converter,
                comparer,
                jsonValueReaderWriter: jsonReaderWriter),
            storeType,
            StoreTypePostfix.None,
            System.Data.DbType.Object);
    }

    /// <inheritdoc />
    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new BigQueryGeographyTypeMapping(parameters, _geometryServices);

    /// <inheritdoc />
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // When there's a value converter, EF Core may pass either:
        // - The original Geometry object (for model value)
        // - The converted WKT string (for provider value)
        string wkt;
        if (value is Geometry geometry)
        {
            wkt = _wktWriter.Write(geometry);
        }
        else if (value is string wktString)
        {
            wkt = wktString;
        }
        else
        {
            throw new InvalidOperationException($"Cannot generate SQL literal for value of type {value.GetType()}");
        }

        return $"ST_GEOGFROMTEXT('{wkt}')";
    }

    /// <inheritdoc />
    protected override void ConfigureParameter(DbParameter parameter)
    {
        base.ConfigureParameter(parameter);

        if (parameter is BigQueryParameter bqParam)
        {
            bqParam.BigQueryDbType = Google.Cloud.BigQuery.V2.BigQueryDbType.Geography;
        }
    }

    /// <summary>
    /// Creates a value converter for the specified geometry CLR type.
    /// Must build expressions dynamically to support Point, LineString, etc. not just Geometry.
    /// </summary>
    private static ValueConverter CreateGeometryConverter(Type clrType, WKTReader wktReader, WKTWriter wktWriter)
    {
        // Build: (TGeometry g) => wktWriter.Write(g)
        var geometryParam = Expression.Parameter(clrType, "g");
        var writeMethod = typeof(WKTWriter).GetMethod(nameof(WKTWriter.Write), new[] { typeof(Geometry) })!;
        var writeCall = Expression.Call(Expression.Constant(wktWriter), writeMethod, geometryParam);
        var toProviderLambda = Expression.Lambda(writeCall, geometryParam);

        // Build: (string s) => s == null ? null : (TGeometry)wktReader.Read(s)
        var stringParam = Expression.Parameter(typeof(string), "s");
        var readMethod = typeof(WKTReader).GetMethod(nameof(WKTReader.Read), new[] { typeof(string) })!;
        var readCall = Expression.Call(Expression.Constant(wktReader), readMethod, stringParam);
        var castRead = Expression.Convert(readCall, clrType);
        var nullCheck = Expression.Condition(
            Expression.Equal(stringParam, Expression.Constant(null, typeof(string))),
            Expression.Constant(null, clrType),
            castRead);
        var fromProviderLambda = Expression.Lambda(nullCheck, stringParam);

        var converterType = typeof(ValueConverter<,>).MakeGenericType(clrType, typeof(string));
        return (ValueConverter)Activator.CreateInstance(
            converterType,
            toProviderLambda,
            fromProviderLambda,
            null)!;
    }

    /// <summary>
    /// Creates a value comparer for the specified geometry CLR type.
    /// Uses EF Core's built-in GeometryValueComparer which uses EqualsExact (not EqualsTopologically)
    /// because EqualsTopologically throws for GeometryCollection types.
    /// </summary>
    private static ValueComparer CreateGeometryComparer(Type clrType)
    {
        var comparerType = typeof(GeometryValueComparer<>).MakeGenericType(clrType);
        return (ValueComparer)Activator.CreateInstance(comparerType)!;
    }

    /// <summary>
    /// Creates a JSON reader/writer for the specified geometry CLR type.
    /// For now, returns null since JSON columns with geography are uncommon in BigQuery.
    /// </summary>
    private static JsonValueReaderWriter? CreateGeometryJsonReaderWriter(Type clrType, WKTReader wktReader, WKTWriter wktWriter)
    {
        // JSON reader/writer is optional - return null for now
        // BigQuery geography columns are typically not stored inside JSON
        return null;
    }
}