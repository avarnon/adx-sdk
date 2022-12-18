namespace AzureDataExplorer;

/// <summary>
/// An interface representing an ADX client factory.
/// </summary>
public interface IAdxClientFactory
{
    /// <summary>
    /// Create a query provider.
    /// </summary>
    /// <returns>The query provider.</returns>
    IAdxQueryProvider CreateQueryProvider();
}
