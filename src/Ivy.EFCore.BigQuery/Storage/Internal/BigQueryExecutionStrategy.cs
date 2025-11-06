using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal
{
    public class BigQueryExecutionStrategy : IExecutionStrategy
    {
        private ExecutionStrategyDependencies Dependencies { get; }

        public BigQueryExecutionStrategy(ExecutionStrategyDependencies dependencies)
        {
            Dependencies = dependencies;
        }

        public virtual bool RetriesOnFailure => false;

        public virtual TResult Execute<TState, TResult>(
            TState state,
            Func<DbContext, TState, TResult> operation,
            Func<DbContext, TState, ExecutionResult<TResult>>? verifySucceeded)
        {
            return operation(Dependencies.CurrentContext.Context, state);
        }

        public virtual async Task<TResult> ExecuteAsync<TState, TResult>(
            TState state,
            Func<DbContext, TState, CancellationToken, Task<TResult>> operation,
            Func<DbContext, TState, CancellationToken, Task<ExecutionResult<TResult>>>? verifySucceeded,
            CancellationToken cancellationToken)
        {
            return await operation(Dependencies.CurrentContext.Context, state, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
