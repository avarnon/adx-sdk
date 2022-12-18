using System.Data;
using Kusto.Data.Common;

namespace AzureDataExplorer;

/// <summary>
/// An interface representing an ADX query provider.
/// </summary>
public interface IAdxQueryProvider
{
    /// <summary>
    /// Gets or sets the default database name.
    /// </summary>
    string DefaultDatabaseName { get; set; }

    /// <summary>
    /// Execute a query.
    /// </summary>
    /// <param name="databaseName">The database upon which to execute the query.</param>
    /// <param name="query">The query</param>
    /// <param name="properties">The client request properties.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data reader representing the results of the query.</returns>
    /// <exception cref="ArgumentException"><paramref name="query" /> is null or empty.</exception>
    Task<IEnumerable<IDataReader>> ExecuteQueryAsync(string? databaseName, string query, ClientRequestProperties? properties, CancellationToken cancellationToken = default);
}
