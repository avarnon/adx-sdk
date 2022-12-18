using Kusto.Data;
using Kusto.Data.Net.Client;

namespace AzureDataExplorer;

/// <summary>
/// An ADX client factory.
/// </summary>
public class AdxClientFactory : IAdxClientFactory
{
    private readonly KustoConnectionStringBuilder _builder;

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="builder">The Kusto connection string builder.</param>
    /// <exception cref="ArgumentNullException"><paramref name="builder" /> is null.</exception>
    public AdxClientFactory(KustoConnectionStringBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        _builder = builder;
    }

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <exception cref="ArgumentException"><paramref name="connectionString" /> is null or empty.</exception>
    public AdxClientFactory(string connectionString)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionString);

        _builder = new KustoConnectionStringBuilder(connectionString);
    }

    /// <inheritdoc />
    public IAdxQueryProvider CreateQueryProvider()
    {
        return new AdxQueryProvider(
            KustoClientFactory.CreateCslQueryProvider(_builder),
            KustoClientFactory.CreateCslAdminProvider(_builder));
    }
}
