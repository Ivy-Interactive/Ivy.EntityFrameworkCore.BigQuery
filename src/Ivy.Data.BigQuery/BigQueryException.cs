using System.Data.Common;

namespace Ivy.Data.BigQuery;

[Serializable]
public class BigQueryException : DbException
{
    public Google.Apis.Bigquery.v2.Data.ErrorProto? ErrorProto { get; }

    public BigQueryException() { }
    public BigQueryException(string message) : base(message) { }
    public BigQueryException(string message, Exception? inner) : base(message, inner) { }

    public BigQueryException(string message, Google.Apis.Bigquery.v2.Data.ErrorProto? errorProto, Exception? inner = null) : base(message, inner)
    {
        ErrorProto = errorProto;
    }

#pragma warning disable SYSLIB0051 // Type or member is obsolete
    protected BigQueryException(
        System.Runtime.Serialization.SerializationInfo info,
        System.Runtime.Serialization.StreamingContext context) : base(info, context)
    {
    }
#pragma warning restore SYSLIB0051 // Type or member is obsolete
}