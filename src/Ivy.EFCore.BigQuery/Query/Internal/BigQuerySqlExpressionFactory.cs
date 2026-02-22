
using Ivy.EntityFrameworkCore.BigQuery.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQuerySqlExpressionFactory : SqlExpressionFactory
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BigQuerySqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : base(dependencies)
    {
        _typeMappingSource = dependencies.TypeMappingSource;
    }

    /// <summary>
    /// Override MakeBinary to handle DateTime/DateTimeOffset type coercion.
    /// BigQuery cannot compare TIMESTAMP with DATETIME directly, so we need to cast.
    /// </summary>
    public override SqlExpression? MakeBinary(
        ExpressionType operatorType,
        SqlExpression left,
        SqlExpression right,
        RelationalTypeMapping? typeMapping,
        SqlExpression? existingExpression = null)
    {
        // Handle comparison operators between DateTime and DateTimeOffset
        if (IsComparisonOperator(operatorType))
        {
            (left, right) = ApplyDateTimeCoercion(left, right);
        }

        return base.MakeBinary(operatorType, left, right, typeMapping, existingExpression);
    }

    private static bool IsComparisonOperator(ExpressionType operatorType)
        => operatorType is ExpressionType.Equal
            or ExpressionType.NotEqual
            or ExpressionType.LessThan
            or ExpressionType.LessThanOrEqual
            or ExpressionType.GreaterThan
            or ExpressionType.GreaterThanOrEqual;

    /// <summary>
    /// Applies type coercion when comparing DateTime (DATETIME) with DateTimeOffset (TIMESTAMP).
    /// BigQuery requires both sides to be the same type for comparison.
    /// We cast DATETIME to TIMESTAMP using the TIMESTAMP() function.
    ///
    /// Note: We only apply coercion when CURRENT_TIMESTAMP() or CURRENT_DATETIME() functions
    /// are involved, to avoid breaking cases where model configuration might not match
    /// the actual database schema.
    /// </summary>
    private (SqlExpression left, SqlExpression right) ApplyDateTimeCoercion(SqlExpression left, SqlExpression right)
    {
        var leftIsCurrentTimestamp = IsCurrentTimestampFunction(left);
        var rightIsCurrentTimestamp = IsCurrentTimestampFunction(right);
        var leftIsCurrentDatetime = IsCurrentDatetimeFunction(left);
        var rightIsCurrentDatetime = IsCurrentDatetimeFunction(right);

        // Only coerce when one of the temporal functions is involved
        // Case 1: CURRENT_TIMESTAMP() compared with DATETIME (parameter, constant, column, or CURRENT_DATETIME())
        if (leftIsCurrentTimestamp && !rightIsCurrentTimestamp && IsDateTimeExpression(right))
        {
            right = CastToTimestamp(right);
        }
        else if (rightIsCurrentTimestamp && !leftIsCurrentTimestamp && IsDateTimeExpression(left))
        {
            left = CastToTimestamp(left);
        }
        // Case 2: CURRENT_DATETIME() compared with TIMESTAMP (DateTimeOffset CLR type)
        else if (leftIsCurrentDatetime && IsDateTimeOffsetExpression(right))
        {
            left = CastToTimestamp(left);
        }
        else if (rightIsCurrentDatetime && IsDateTimeOffsetExpression(left))
        {
            right = CastToTimestamp(right);
        }

        return (left, right);
    }

    /// <summary>
    /// Checks if expression is the CURRENT_TIMESTAMP() function.
    /// </summary>
    private static bool IsCurrentTimestampFunction(SqlExpression expression)
        => expression is SqlFunctionExpression func &&
           func.Name.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Checks if expression is the CURRENT_DATETIME() function.
    /// </summary>
    private static bool IsCurrentDatetimeFunction(SqlExpression expression)
        => expression is SqlFunctionExpression func &&
           func.Name.Equals("CURRENT_DATETIME", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Determines if an expression is a DateTime type (for coercion with CURRENT_TIMESTAMP).
    /// Only considers CLR type and basic type mapping, not column store types.
    /// </summary>
    private static bool IsDateTimeExpression(SqlExpression expression)
    {
        // CURRENT_DATETIME() is DateTime
        if (IsCurrentDatetimeFunction(expression))
            return true;

        var clrType = UnwrapNullableType(expression.Type);

        // DateTime CLR type without DateTimeOffset mapping
        if (clrType == typeof(DateTime))
        {
            // Make sure it's not mapped to TIMESTAMP
            if (expression.TypeMapping is BigQueryDateTimeOffsetTypeMapping)
                return false;

            return true;
        }

        return false;
    }

    /// <summary>
    /// Determines if an expression is a DateTimeOffset type (for coercion with CURRENT_DATETIME).
    /// </summary>
    private static bool IsDateTimeOffsetExpression(SqlExpression expression)
    {
        // CURRENT_TIMESTAMP() is DateTimeOffset/TIMESTAMP
        if (IsCurrentTimestampFunction(expression))
            return true;

        var clrType = UnwrapNullableType(expression.Type);
        return clrType == typeof(DateTimeOffset);
    }

    private SqlExpression CastToTimestamp(SqlExpression expression)
    {
        var timestampMapping = _typeMappingSource.FindMapping(typeof(DateTimeOffset));
        var datetimeMapping = _typeMappingSource.FindMapping(typeof(DateTime));

        // Apply type mapping to the argument if it doesn't have one
        var typedExpression = expression.TypeMapping == null
            ? ApplyTypeMapping(expression, datetimeMapping)
            : expression;

        return new SqlFunctionExpression(
            "TIMESTAMP",
            new[] { typedExpression },
            nullable: true,
            argumentsPropagateNullability: new[] { true },
            typeof(DateTimeOffset),
            timestampMapping);
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
