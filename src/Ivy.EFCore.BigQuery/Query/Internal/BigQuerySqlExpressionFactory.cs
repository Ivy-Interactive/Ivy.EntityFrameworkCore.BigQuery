
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Diagnostics.CodeAnalysis;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQuerySqlExpressionFactory : SqlExpressionFactory
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQuerySqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : base(dependencies)
    {
        _typeMappingSource = dependencies.TypeMappingSource;
    }

    /// <summary>
    /// STRUCT field access expression: struct.field
    /// </summary>
    public virtual BigQueryStructAccessExpression StructAccess(
        SqlExpression instance,
        string fieldName,
        Type type,
        RelationalTypeMapping? typeMapping)
    {
        return new BigQueryStructAccessExpression(instance, fieldName, type, typeMapping);
    }

    /// <summary>
    /// STRUCT constructor expression: STRUCT&lt;...&gt;(...) or STRUCT(... AS ...)
    /// </summary>
    public virtual BigQueryStructConstructorExpression StructConstructor(
        IReadOnlyList<SqlExpression> arguments,
        IReadOnlyList<string> fieldNames,
        IReadOnlyList<RelationalTypeMapping?> fieldTypeMappings,
        Type type,
        RelationalTypeMapping? typeMapping,
        string? explicitType = null)
    {
        return new BigQueryStructConstructorExpression(
            arguments,
            fieldNames,
            fieldTypeMappings,
            type,
            typeMapping,
            explicitType);
    }

    /// <summary>
    /// Array element access expression: array[OFFSET(index)]
    /// </summary>
    public virtual BigQueryArrayIndexExpression ArrayIndex(
        SqlExpression array,
        SqlExpression index,
        RelationalTypeMapping? elementTypeMapping = null)
    {
        // Validate array is actually an array type
        if (array.TypeMapping is not Storage.Internal.Mapping.BigQueryArrayTypeMapping arrayMapping)
        {
            throw new InvalidOperationException($"Array indexing requires an array type, but got {array.TypeMapping?.GetType().Name}");
        }

        var mapping = elementTypeMapping ?? arrayMapping.ElementTypeMapping;
        var elementType = arrayMapping.ClrType.IsArray
            ? arrayMapping.ClrType.GetElementType()!
            : arrayMapping.ClrType.GetGenericArguments().FirstOrDefault() ?? typeof(object);

        return new BigQueryArrayIndexExpression(array, index, elementType, mapping);
    }

    /// <summary>
    /// Array literal expression: ARRAY&lt;type&gt;[expr1, expr2, ...] or optimized SqlConstantExpression
    /// </summary>
    public virtual SqlExpression ArrayLiteral(
        IReadOnlyList<SqlExpression> elements,
        Type arrayType,
        Type elementType,
        RelationalTypeMapping? arrayTypeMapping = null,
        RelationalTypeMapping? elementTypeMapping = null)
    {
        // Optimization: if all elements are constants, create a SqlConstantExpression with the actual array
        if (elements.All(e => e is SqlConstantExpression))
        {
            var constantValues = elements
                .Cast<SqlConstantExpression>()
                .Select(e => e.Value)
                .ToArray();

            var array = Array.CreateInstance(elementType, constantValues.Length);
            for (int i = 0; i < constantValues.Length; i++)
            {
                array.SetValue(constantValues[i], i);
            }

            return Constant(array, arrayTypeMapping);
        }

        return new BigQueryArrayLiteralExpression(
            elements,
            arrayType,
            elementType,
            arrayTypeMapping,
            elementTypeMapping);
    }

    /// <summary>
    /// UNNEST table expression: UNNEST(array) AS alias
    /// </summary>
    public virtual BigQueryUnnestExpression Unnest(
        SqlExpression array,
        string alias,
        bool withOffset = false,
        string? offsetAlias = null)
    {
        if (array.TypeMapping is not Storage.Internal.Mapping.BigQueryArrayTypeMapping arrayMapping)
        {
            throw new InvalidOperationException($"UNNEST requires an array type, but got {array.TypeMapping?.GetType().Name}");
        }

        return new BigQueryUnnestExpression(
            array,
            alias,
            arrayMapping.ClrType.IsArray
                ? arrayMapping.ClrType.GetElementType()!
                : arrayMapping.ClrType.GetGenericArguments().FirstOrDefault() ?? typeof(object),
            arrayMapping.ElementTypeMapping,
            withOffset,
            offsetAlias);
    }

    public override SqlExpression Convert(SqlExpression operand, Type type, RelationalTypeMapping? typeMapping)
    {
        if (UnwrapNullableType(type) == typeof(DateTime) && UnwrapNullableType(operand.Type) == typeof(DateTimeOffset))
        {
            return new SqlFunctionExpression(
                "DATETIME",
                new[] { operand },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                type,
                typeMapping);
        }

        return base.Convert(operand, type, typeMapping);
    }

    /// <summary>
    /// IN UNNEST expression: item IN UNNEST(array)
    /// </summary>
    public virtual BigQueryInUnnestExpression InUnnest(
        SqlExpression item,
        SqlExpression array)
    {
        return new BigQueryInUnnestExpression(item, array, _typeMappingSource.FindMapping(typeof(bool)));
    }

    public static Type UnwrapNullableType(Type type)
    => Nullable.GetUnderlyingType(type) ?? type;

    /// <summary>
    /// Creates a JSON traversal expression for BigQuery JSON column navigation
    /// </summary>
    public virtual BigQueryJsonTraversalExpression JsonTraversal(
        SqlExpression column,
        IReadOnlyList<SqlExpression> path,
        Type type,
        RelationalTypeMapping? typeMapping = null)
    {
        return new BigQueryJsonTraversalExpression(column, path, type, typeMapping);
    }

    /// <summary>
    /// Creates a JSON traversal expression with an empty path (direct column access)
    /// </summary>
    public virtual BigQueryJsonTraversalExpression JsonTraversal(
        SqlExpression column,
        Type type,
        RelationalTypeMapping typeMapping)
    {
        return new BigQueryJsonTraversalExpression(column, Array.Empty<SqlExpression>(), type, typeMapping);
    }


}
