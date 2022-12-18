using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Text;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Results;

namespace AzureDataExplorer;

/// <summary>
/// An ADX query provider.
/// </summary>
public class AdxQueryProvider : IAdxQueryProvider
{
    private readonly ICslQueryProvider _cslQueryProvider;
    private readonly ICslAdminProvider _cslAdminProvider;

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="cslQueryProvider">The Cousteau Semantic Language query provider.</param>
    /// <param name="cslAdminProvider">The Cousteau Semantic Language administrative provider.</param>
    /// <exception cref="ArgumentNullException"><paramref name="cslQueryProvider" /> is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="cslAdminProvider" /> is null.</exception>
    public AdxQueryProvider(ICslQueryProvider cslQueryProvider, ICslAdminProvider cslAdminProvider)
    {
        ArgumentNullException.ThrowIfNull(cslQueryProvider);
        ArgumentNullException.ThrowIfNull(cslAdminProvider);

        _cslQueryProvider = cslQueryProvider;
        _cslAdminProvider = cslAdminProvider;
    }

    /// <inheritdoc />
    public string DefaultDatabaseName
    {
        get => _cslQueryProvider.DefaultDatabaseName;
        set
        {
            _cslAdminProvider.DefaultDatabaseName = value;
            _cslQueryProvider.DefaultDatabaseName = value;
        }
    }

    /// <inheritdoc />
    public async Task<IEnumerable<IDataReader>> ExecuteQueryAsync(string? databaseName, string query, ClientRequestProperties? properties, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(query);

        await using var _ = _cslAdminProvider.RegisterCancellationToken(databaseName, ref properties, cancellationToken);
        return ProcessProgressiveDataSetAsync(await _cslQueryProvider.ExecuteQueryV2Async(databaseName, query, properties), cancellationToken);
    }

    /// <summary>
    /// Process a progressive data set.
    /// </summary>
    /// <param name="progressiveDataSet">The progressive data set.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The data reader for the primary result.</returns>
    /// <exception cref="TaskCanceledException">The query was cancelled.</exception>
    private IEnumerable<IDataReader> ProcessProgressiveDataSetAsync(ProgressiveDataSet progressiveDataSet, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(progressiveDataSet);

        const string traceCategory = $"{nameof(AdxQueryProvider)}.{nameof(AdxQueryProvider.ProcessProgressiveDataSetAsync)}";
        var tablesById = new ConcurrentDictionary<int, DataTable>();
        var tableKindsById = new ConcurrentDictionary<int, WellKnownDataSet>();

        try
        {
            var frames = progressiveDataSet.GetFrames();

            // Walk the frames while accumulating data
            while (frames.MoveNext())
            {
                cancellationToken.ThrowIfCancellationRequested();

                var currentFrame = frames.Current;
                switch (currentFrame.FrameType)
                {
                    case FrameType.DataSetHeader:
                        {
                            var frameEx = (ProgressiveDataSetHeaderFrame)currentFrame;

                            Trace.WriteLine(
                                $"DataSet/HeaderFrame: Version={frameEx.Version}, IsProgressive={frameEx.IsProgressive}",
                                traceCategory);
                        }

                        break;

                    case FrameType.TableHeader:
                        {
                            var frameEx = (ProgressiveDataSetDataTableSchemaFrame)currentFrame;
                            var dataTable = ToEmptyDataTable(frameEx, cancellationToken);

                            if (dataTable == null)
                            {
                                throw new UnreachableException($"Could not create {nameof(DataTable)} with schema for {nameof(ProgressiveDataSetDataTableSchemaFrame)}");
                            }

                            if (!tablesById.TryAdd(frameEx.TableId, dataTable))
                            {
                                throw new UnreachableException($"Could not add {nameof(DataTable)} for {nameof(ProgressiveDataSetDataTableSchemaFrame)}");
                            }

                            if (!tableKindsById.TryAdd(frameEx.TableId, frameEx.TableKind))
                            {
                                throw new UnreachableException($"Could not add {nameof(WellKnownDataSet)} for {nameof(ProgressiveDataSetDataTableSchemaFrame)}");
                            }

                            Trace.WriteLine(
                                $"DataTable/SchemaFrame: TableId={frameEx.TableId}, TableName={frameEx.TableName}, TableKind={frameEx.TableKind}",
                                traceCategory);
                        }

                        break;

                    case FrameType.TableFragment:
                        {
                            var frameEx = (ProgressiveDataSetDataTableFragmentFrame)currentFrame;
                            if (!tablesById.TryGetValue(frameEx.TableId, out var dataTable))
                            {
                                throw new UnreachableException($"Could not find {nameof(DataTable)} for {nameof(ProgressiveDataSetDataTableFragmentFrame)}");
                            }

                            dataTable.BeginLoadData();
                            var record = new object[frameEx.FieldCount];
                            while (frameEx.GetNextRecord(record))
                            {
                                cancellationToken.ThrowIfCancellationRequested();

                                dataTable.Rows.Add(record);
                            }

                            dataTable.EndLoadData();
                            dataTable.AcceptChanges();

                            Trace.WriteLine(
                                $"DataTable/FragmentFrame: TableId={frameEx.TableId}, FieldCount={frameEx.FieldCount}, FrameSubType={frameEx.FrameSubType}",
                                traceCategory);
                        }

                        break;

                    case FrameType.TableCompletion:
                        {
                            var frameEx = (ProgressiveDataSetTableCompletionFrame)currentFrame;

                            Trace.WriteLine(
                                $"DataTable/TableCompletionFrame: TableId={frameEx.TableId}, RowCount={frameEx.RowCount}",
                                traceCategory);
                        }

                        break;

                    case FrameType.TableProgress:
                        {
                            var frameEx = (ProgressiveDataSetTableProgressFrame)currentFrame;

                            Trace.WriteLine(
                                $"DataTable/TableProgressFrame: TableId={frameEx.TableId}, TableProgress={frameEx.TableProgress}",
                                traceCategory);
                        }

                        break;

                    case FrameType.DataTable:
                        {
                            var frameEx = (ProgressiveDataSetDataTableFrame)currentFrame;
                            var dataTable = ToEmptyDataTable(frameEx, cancellationToken);

                            if (dataTable == null)
                            {
                                throw new UnreachableException($"Could not create {nameof(DataTable)} with schema for {nameof(ProgressiveDataSetDataTableFrame)}");
                            }

                            dataTable.Load(frameEx.TableData);
                            dataTable.AcceptChanges();

                            if (!tablesById.TryAdd(frameEx.TableId, dataTable))
                            {
                                throw new UnreachableException($"Could not add {nameof(DataTable)} for {nameof(ProgressiveDataSetDataTableFrame)}");
                            }

                            if (!tableKindsById.TryAdd(frameEx.TableId, frameEx.TableKind))
                            {
                                throw new UnreachableException($"Could not add {nameof(WellKnownDataSet)} for {nameof(ProgressiveDataSetDataTableFrame)}");
                            }

                            Trace.WriteLine(
                                $"DataTable/DataTableFrame: TableId={frameEx.TableId}, TableName={frameEx.TableName}, TableKind={frameEx.TableKind}",
                                traceCategory);
                        }

                        break;

                    case FrameType.DataSetCompletion:
                        {
                            var frameEx = (ProgressiveDataSetCompletionFrame)currentFrame;

                            Trace.WriteLine(
                                $"DataSet/CompletionFrame: HasErrors={frameEx.HasErrors}, Cancelled={frameEx.Cancelled}, Exception={frameEx.Exception}",
                                traceCategory);

                            if (frameEx.Cancelled)
                            {
                                throw new TaskCanceledException();
                            }

                            if (frameEx.HasErrors)
                            {
                                if (frameEx.Exception == null)
                                {
                                    throw new UnreachableException($"{nameof(ProgressiveDataSetCompletionFrame)} indicates an error occurred but no exception was provided");
                                }

                                ExceptionDispatchInfo.Capture(frameEx.Exception).Throw();
                            }
                        }

                        break;

                    case FrameType.LastInvalid:
                    default:
                        throw new UnreachableException($"Invalid {nameof(FrameType)} {currentFrame.FrameType}");
                }
            }

            foreach (var tableId in tablesById.Keys)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (!tableKindsById.TryGetValue(tableId, out var tableKind))
                {
                    throw new UnreachableException($"TableId exists for {nameof(DataTable)} but not for {nameof(WellKnownDataSet)}");
                }

                var dataTable = tablesById[tableId];

                switch (tableKind)
                {
                    case WellKnownDataSet.PrimaryResult:
                        yield return  dataTable.Clone().CreateDataReader();
                        break;

                    case WellKnownDataSet.QueryCompletionInformation:
                    case WellKnownDataSet.QueryTraceLog:
                    case WellKnownDataSet.QueryPerfLog:
                    case WellKnownDataSet.TableOfContents:
                    case WellKnownDataSet.QueryProperties:
                    case WellKnownDataSet.QueryPlan:
                    case WellKnownDataSet.Unknown:
                    case WellKnownDataSet.LastInvalid:
                        var rowIdx = -1;

                        foreach (var dataRow in dataTable.Rows.OfType<DataRow>())
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            var builder = new StringBuilder($"DataTable/{tableKind}[{++rowIdx}]: ");
                            for (var i = 0; i < dataTable.Columns.Count; i++)
                            {
                                cancellationToken.ThrowIfCancellationRequested();

                                if (i > 0)
                                {
                                    builder.Append(", ");
                                }

                                builder.Append($"{dataTable.Columns[i].ColumnName}={dataRow[i]}");
                            }

                            Trace.WriteLine(builder.ToString(), traceCategory);
                        }

                        break;

                    default:
                        throw new UnreachableException($"Invalid {nameof(WellKnownDataSet)} {tableKind}");
                }
            }
        }
        finally
        {
            // Cleanup

            progressiveDataSet.Dispose();

            foreach (var dataTable in tablesById.Values)
            {
                dataTable.Dispose();
            }
        }
    }

    /// <summary>
    /// Convert a <see cref="ProgressiveDataSetDataTableSchemaFrame" /> to a <see cref="DataTable" />.
    /// </summary>
    /// <param name="tableHeader">The table header.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data table whose schema matches the table header.</returns>
    /// <remarks>
    /// Adapted from <see cref="ExtendedTableFragment" />.
    /// </remarks>
    private static DataTable? ToEmptyDataTable(ProgressiveDataSetDataTableSchemaFrame tableHeader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tableHeader);

        return CreateMatchingDataTable(tableHeader.TableSchema, tableHeader.TableName, cancellationToken);
    }

    /// <summary>
    /// Convert a <see cref="ProgressiveDataSetDataTableFrame" /> to a <see cref="DataTable" />.
    /// </summary>
    /// <param name="tableFrame">The table frame.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data table whose schema and data matches the table frame.</returns>
    /// <remarks>
    /// Adapted from <see cref="ExtendedTableFragment" />.
    /// </remarks>
    private static DataTable? ToEmptyDataTable(ProgressiveDataSetDataTableFrame tableFrame, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tableFrame);

        return CreateMatchingDataTable(tableFrame.TableData.GetSchemaTable(), tableFrame.TableName, cancellationToken);
    }

    /// <summary>
    /// Create a matching data table for a schema data table.
    /// </summary>
    /// <param name="schema">The schema data table.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The matching data table.</returns>
    /// <remarks>
    /// Adapted from <see cref="ExtendedTableFragment" />.
    /// </remarks>
    private static DataTable CreateMatchingDataTable(DataTable? schema, string tableName, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var dataTable = new DataTable(tableName ?? "Results");
        if (IsValidSchema(schema, schema.Rows.Count))
        {
            foreach (var row in schema.Rows.OfType<DataRow>())
            {
                cancellationToken.ThrowIfCancellationRequested();

                var columnType = row.Field<string?>("ColumnType");
                var columnName = row.Field<string?>("ColumnName");
                var correspondingClrType = CslType.FromCslType(columnType).GetCorrespondingClrType();

                dataTable.Columns.Add(columnName, correspondingClrType);
            }
        }

        dataTable.AcceptChanges();

        return dataTable;
    }

    /// <summary>
    /// Check if a schema data table is valid.
    /// </summary>
    /// <param name="schema">The schema data table.</param>
    /// <param name="expectedRowCount">The expected row count.</param>
    /// <returns>True if the schema data table is valid; otherwise, false.</returns>
    /// <remarks>
    /// Adapted from <see cref="ExtendedTableFragment" />.
    /// </remarks>
    private static bool IsValidSchema(DataTable? schema, int expectedRowCount)
    {
        return schema != null &&
               schema.Rows.Count == expectedRowCount &&
               schema.Columns.Contains("ColumnName") &&
               schema.Columns.Contains("ColumnType");
    }
}
