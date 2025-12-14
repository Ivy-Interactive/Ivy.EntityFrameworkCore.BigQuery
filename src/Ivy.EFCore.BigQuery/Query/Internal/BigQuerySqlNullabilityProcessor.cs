using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Ivy.EntityFrameworkCore.BigQuery.Query.Internal;

public class BigQuerySqlNullabilityProcessor : SqlNullabilityProcessor
{
    public BigQuerySqlNullabilityProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
    }

    protected override SqlExpression VisitCustomSqlExpression(
        SqlExpression sqlExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        switch (sqlExpression)
        {
            case BigQueryStructAccessExpression structAccessExpression:
            {
                var visitedStruct = Visit(structAccessExpression.Struct, allowOptimizedExpansion, out var structNullable);

                nullable = structNullable;

                return structAccessExpression.Update((SqlExpression)visitedStruct);
            }

            case BigQueryStructConstructorExpression structConstructorExpression:
            {
                var visitedArguments = new List<SqlExpression>();
                var anyNullable = false;

                foreach (var argument in structConstructorExpression.Arguments)
                {
                    var visitedArg = Visit(argument, allowOptimizedExpansion, out var argNullable);
                    visitedArguments.Add((SqlExpression)visitedArg);
                    anyNullable |= argNullable;
                }

                nullable = anyNullable;

                return structConstructorExpression.Update(visitedArguments);
            }

            default:
                return base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable);
        }
    }
}
