using System.Diagnostics;
using Kusto.Data.Common;

namespace AzureDataExplorer;

/// <summary>
/// Extension methods for <see cref="ICslAdminProvider" />.
/// </summary>
internal static class CslAdminProviderExtensions
{
    /// <summary>
    /// Register a cancellation token for a query or control command.
    /// </summary>
    /// <param name="cslAdminProvider">The <see cref="ICslAdminProvider" />.</param>
    /// <param name="databaseName">The name of the database upon which the query or control command will be executed.</param>
    /// <param name="properties">The client request properties for the query or control command.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The results of the command.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="cslAdminProvider"/> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="properties"/> is null.</exception>
    public static IAsyncDisposable RegisterCancellationToken(
        this ICslAdminProvider cslAdminProvider,
        string? databaseName,
        ref ClientRequestProperties? properties,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cslAdminProvider);

        if (!cancellationToken.CanBeCanceled)
        {
            return new NullCancellationTokenRegistration();
        }

        return cancellationToken.Register(async (state, ct) => await IssueCancellationAsync(state, ct), (object?)(cslAdminProvider, databaseName, properties));
    }

    private static async Task IssueCancellationAsync(object? state, CancellationToken cancellationToken)
    {
        try
        {
            if (state == null)
            {
                return;
            }

            var (cslAdminProvider, databaseName, originalClientRequestProperties) = (((ICslAdminProvider, string?, ClientRequestProperties))state);

            var cancelQueryCommand = CslCommandGenerator.GenerateQueryCancelCommand(originalClientRequestProperties.ClientRequestId);
            var cancelClientRequestProperties = new ClientRequestProperties
            {
                ClientRequestId = Guid.NewGuid().ToString(),
            };

            // Set request options to not obfuscate client request parameters for debugging purposes
            cancelClientRequestProperties.SetOption(
                Kusto.Data.Common.ClientRequestProperties.OptionQueryLogQueryParameters,
                true);

            using var _ = await cslAdminProvider.ExecuteControlCommandAsync(databaseName, cancelQueryCommand, cancelClientRequestProperties);
        }
        catch (Exception ex)
        {
            Trace.WriteLine($"Unhandled exception: {ex}", $"{nameof(CslAdminProviderExtensions)}.{nameof(CslAdminProviderExtensions.IssueCancellationAsync)}");
        }
    }

    private class NullCancellationTokenRegistration : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
