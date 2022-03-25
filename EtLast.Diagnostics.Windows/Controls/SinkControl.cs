namespace FizzCode.EtLast.Diagnostics.Windows;

using System;
using System.Collections.Generic;
using System.Drawing;
using System.Windows.Forms;
using BrightIdeasSoftware;
using FizzCode.EtLast.Diagnostics.Interface;

internal class SinkControl
{
    public Control Container { get; }
    public DiagContext Context { get; }
    public TrackedSink Store { get; }
    public ObjectListView ListView { get; }
    public TextBox SearchBox { get; }
    private readonly Dictionary<string, int> _columnIndexes = new(StringComparer.OrdinalIgnoreCase);
    private readonly int _fixColumnCount;

    public SinkControl(Control container, DiagContext context, TrackedSink store)
    {
        Container = container;
        Context = context;
        Store = store;

        SearchBox = new TextBox()
        {
            Parent = container,
            Bounds = new Rectangle(10, 10, 150, 20),
        };

        SearchBox.TextChanged += SearchBox_TextChanged;

        ListView = ControlUpdater<string>.CreateListView(container);
        ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
        ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);
        ListView.FormatCell += ListView_FormatCell;
        ListView.UseCellFormatEvents = true;

        ListView.AllColumns.Add(new OLVColumn()
        {
            Text = "ID",
            AspectGetter = x => (x as StoredRowModel)?.RowUid,
        });
        ListView.AllColumns.Add(new OLVColumn()
        {
            Text = "Process",
            AspectGetter = x => (x as StoredRowModel).ProcessName,
        });

        ListView.Columns.AddRange(ListView.AllColumns.ToArray());

        _fixColumnCount = ListView.Columns.Count;
    }

    private void ListView_FormatCell(object sender, FormatCellEventArgs e)
    {
        if (string.IsNullOrEmpty(e.Column.Text))
            e.SubItem.ForeColor = Color.DarkGray;
    }

    private void SearchBox_TextChanged(object sender, EventArgs e)
    {
        var text = (sender as TextBox).Text;
        ListView.AdditionalFilter = !string.IsNullOrEmpty(text)
            ? TextMatchFilter.Contains(ListView, text)
            : null;
    }

    public void Refresh()
    {
        ListView.BeginUpdate();
        try
        {
            ListView.Items.Clear();
            var modelList = new List<StoredRowModel>();

            var newColumns = new List<OLVColumn>();

            Context.Index.EnumerateThroughSink(Store.UID, evt =>
            {
                if (!Context.WholePlaybook.ProcessList.TryGetValue(evt.ProcessInvocationUID, out var process))
                    return;

                for (var i = 0; i < evt.Values.Length; i++)
                {
                    var columnName = evt.Values[i].Key;
                    if (!_columnIndexes.TryGetValue(columnName, out var columnIndex))
                    {
                        columnIndex = ListView.AllColumns.Count - _fixColumnCount;

                        var newColumn = new OLVColumn()
                        {
                            Text = columnName,
                            AspectGetter = x =>
                            {
                                return (x is StoredRowModel r && columnIndex < r.Values.Length)
                                    ? r.Values[columnIndex]
                                    : null;
                            },
                            AspectToStringConverter = FormattingHelpers.ToDisplayValue,
                        };

                        ListView.AllColumns.Add(newColumn);
                        newColumns.Add(newColumn);

                        newColumn = new OLVColumn()
                        {
                            Text = "",
                            AspectGetter = x =>
                            {
                                return (x is StoredRowModel r && columnIndex < r.Values.Length)
                                    ? r.Values[columnIndex]?.GetType()
                                    : null;
                            },
                            AspectToStringConverter = value => ((Type)value)?.GetFriendlyTypeName(),
                        };

                        ListView.AllColumns.Add(newColumn);
                        newColumns.Add(newColumn);

                        _columnIndexes.Add(columnName, columnIndex);
                    }
                }

                var model = new StoredRowModel()
                {
                    RowUid = evt.RowUid,
                    ProcessName = process.DisplayName,
                    Values = new object[ListView.AllColumns.Count - _fixColumnCount],
                    Types = new string[ListView.AllColumns.Count - _fixColumnCount],
                };

                for (var i = 0; i < evt.Values.Length; i++)
                {
                    var kvp = evt.Values[i];
                    var columnIndex = _columnIndexes[kvp.Key];

                    model.Values[columnIndex] = kvp.Value;
                    model.Types[columnIndex] = kvp.Value?.GetType().GetFriendlyTypeName();
                }

                modelList.Add(model);
            });

            ListView.Columns.AddRange(newColumns.ToArray());
            ListView.SetObjects(modelList);

            ControlUpdater<int>.ResizeListViewWithRows(ListView);
        }
        finally
        {
            ListView.EndUpdate();
        }
    }

    private class StoredRowModel
    {
        public int RowUid { get; set; }
        public string ProcessName { get; set; }
        public object[] Values { get; set; }
        public string[] Types { get; set; }
    }
}
