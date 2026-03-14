using System.Text;

namespace Ivy.Data.BigQuery;

internal static class BigQueryExceptionHelper
{
    public static BigQueryException CreateException(Google.Apis.Bigquery.v2.Data.ErrorProto? errorProto, string? messagePrefix = null, Exception? inner = null)
    {
        if (errorProto == null)
        {
            var genericMessage = string.IsNullOrEmpty(messagePrefix) ? "An unspecified BigQuery job error occurred." : $"{messagePrefix}: An unspecified BigQuery job error occurred.";
            return new BigQueryException(genericMessage, inner);
        }
        var message = FormatErrorMessage(errorProto, messagePrefix);
        return new BigQueryException(message, errorProto, inner);
    }

    public static BigQueryException CreateException(Google.GoogleApiException apiException, string? messagePrefix = null)
    {
        var message = string.IsNullOrEmpty(messagePrefix)
            ? $"BigQuery API Error: {apiException.Message}"
            : $"{messagePrefix}: {apiException.Message}";

        return new BigQueryException(message, apiException);
    }

    private static string FormatErrorMessage(Google.Apis.Bigquery.v2.Data.ErrorProto errorProto, string? messagePrefix)
    {
        var sb = new StringBuilder();
        if (!string.IsNullOrEmpty(messagePrefix))
        {
            sb.Append(messagePrefix);
            sb.Append(": ");
        }

        sb.Append(errorProto.Message ?? "Unknown error");

        if (string.IsNullOrEmpty(errorProto.Reason)) return sb.ToString();

        sb.Append($" (Reason: {errorProto.Reason}");
        if (!string.IsNullOrEmpty(errorProto.Location))
        {
            sb.Append($", Location: {errorProto.Location}");
        }
        sb.Append(')');
        return sb.ToString();
    }
}