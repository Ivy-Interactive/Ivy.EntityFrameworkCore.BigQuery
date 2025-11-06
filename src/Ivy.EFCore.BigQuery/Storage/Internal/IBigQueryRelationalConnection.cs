using Microsoft.EntityFrameworkCore.Storage;

namespace Ivy.EntityFrameworkCore.BigQuery.Storage.Internal;

public interface IBigQueryRelationalConnection : IRelationalConnection
{
    IBigQueryRelationalConnection CreateAdminConnection();
}