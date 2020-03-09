namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Windows.Forms;
    using BrightIdeasSoftware;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ProcessRowListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Control Container { get; }
        public TrackedProcessInvocation Process { get; }
        public List<TrackedRow> Rows { get; }
        public ObjectListView ListView { get; }
        public TextBox SearchBox { get; }
        private readonly Dictionary<string, int> _columnIndexes = new Dictionary<string, int>();
        private readonly int _fixColumnCount;

        public ProcessRowListControl(Control container, TrackedProcessInvocation process, List<TrackedRow> rows)
        {
            Container = container;
            Process = process;
            Rows = rows;

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
            ListView.CellToolTipShowing += ListView_CellToolTipShowing;

            ListView.AllColumns.Add(new OLVColumn()
            {
                Text = "ID",
                AspectGetter = x => (x as ProcessRowModel)?.RowUid,
            });

            ListView.AllColumns.Add(new OLVColumn()
            {
                Text = "Previous process",
                AspectGetter = x => (x as ProcessRowModel)?.TrackedRow.PreviousProcess?.Name,
            });

            ListView.AllColumns.Add(new OLVColumn()
            {
                Text = "Next process",
                AspectGetter = x => (x as ProcessRowModel)?.TrackedRow.NextProcess?.Name,
            });

            ListView.Columns.AddRange(ListView.AllColumns.ToArray());

            _fixColumnCount = ListView.Columns.Count;

            var newColumns = new List<OLVColumn>();
            var items = new List<ProcessRowModel>();
            foreach (var row in rows)
            {
                foreach (var kvp in row.NewValues)
                {
                    AddColumnByValue(newColumns, kvp);
                }

                if (row.PreviousValues != null)
                {
                    foreach (var kvp in row.PreviousValues)
                    {
                        AddColumnByValue(newColumns, kvp);
                    }
                }

                var item = new ProcessRowModel()
                {
                    TrackedRow = row,
                    RowUid = row.Uid,
                    NewValues = new object[newColumns.Count / 2],
                    NewTypes = new string[newColumns.Count / 2],
                    PreviousValues = row.PreviousValues != null ? new object[newColumns.Count / 2] : null,
                    PreviousTypes = row.PreviousValues != null ? new string[newColumns.Count / 2] : null,
                };

                if (row.PreviousValues != null)
                {
                    foreach (var kvp in row.PreviousValues)
                    {
                        var valueIndex = _columnIndexes[kvp.Key] / 2;
                        item.PreviousValues[valueIndex] = kvp.Value;
                        item.PreviousTypes[valueIndex] = kvp.Value?.GetType().GetFriendlyTypeName();
                    }
                }

                foreach (var kvp in row.NewValues)
                {
                    var valueIndex = _columnIndexes[kvp.Key] / 2;
                    item.NewValues[valueIndex] = kvp.Value;
                    item.NewTypes[valueIndex] = kvp.Value?.GetType().GetFriendlyTypeName();
                }

                items.Add(item);
            }

            ListView.Columns.AddRange(newColumns.ToArray());
            ListView.AddObjects(items);

            ControlUpdater<int>.ResizeListView(ListView);
        }

        private void AddColumnByValue(List<OLVColumn> newColumns, KeyValuePair<string, object> kvp)
        {
            var columnName = kvp.Key;
            if (!_columnIndexes.TryGetValue(columnName, out var columnIndex))
            {
                columnIndex = ListView.AllColumns.Count - _fixColumnCount;
                var valueIndex = columnIndex / 2;

                var newColumn = new OLVColumn()
                {
                    Text = columnName,
                    AspectGetter = x =>
                    {
                        return (x is ProcessRowModel r && valueIndex < r.NewValues.Length)
                            ? r.NewValues[valueIndex]
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
                        return (x is ProcessRowModel r && valueIndex < r.NewValues.Length)
                            ? r.NewValues[valueIndex]?.GetType()
                            : null;
                    },
                    AspectToStringConverter = value => ((Type)value)?.GetFriendlyTypeName(),
                };

                ListView.AllColumns.Add(newColumn);
                newColumns.Add(newColumn);

                _columnIndexes.Add(columnName, columnIndex);
            }
        }

        private void ListView_CellToolTipShowing(object sender, ToolTipShowingEventArgs e)
        {
            if (e.Model is ProcessRowModel model && model.PreviousValues != null)
            {
                var columnIndex = e.ColumnIndex - _fixColumnCount;
                if (columnIndex >= 0)
                {
                    var valueIndex = columnIndex / 2;

                    var previousValue = valueIndex < model.PreviousValues.Length ? model.PreviousValues[valueIndex] : null;
                    var newValue = valueIndex < model.NewValues.Length ? model.NewValues[valueIndex] : null;
                    if (previousValue != newValue)
                    {
                        e.Text = "previous value: " + FormattingHelpers.ToDisplayValue(previousValue)
                            + (previousValue == null ? "" : " (" + previousValue.GetType().GetFriendlyTypeName() + ")")
                            + "\r\nnew value: " + FormattingHelpers.ToDisplayValue(newValue)
                            + (newValue == null ? "" : " (" + newValue.GetType().GetFriendlyTypeName() + ")");
                    }
                }
            }
        }

        private void ListView_FormatCell(object sender, FormatCellEventArgs e)
        {
            if (string.IsNullOrEmpty(e.Column.Text))
                e.SubItem.ForeColor = Color.DarkGray;

            if (e.Model is ProcessRowModel model && model.PreviousValues != null)
            {
                var columnIndex = e.ColumnIndex - _fixColumnCount;
                if (columnIndex >= 0)
                {
                    var valueIndex = columnIndex / 2;

                    var previousValue = valueIndex < model.PreviousValues.Length ? model.PreviousValues[valueIndex] : null;
                    var newValue = valueIndex < model.NewValues.Length ? model.NewValues[valueIndex] : null;

                    if (valueIndex % 2 == 0)
                    {
                        if (!DefaultValueComparer.ValuesAreEqual(previousValue, newValue))
                        {
                            e.SubItem.BackColor = Color.LightBlue;
                        }
                    }
                    else
                    {
                        if ((previousValue == null != (newValue == null))
                            || (previousValue != null && newValue != null && previousValue.GetType() != newValue.GetType()))
                        {
                            e.SubItem.BackColor = Color.LightBlue;
                        }
                    }
                }
            }
        }

        private void SearchBox_TextChanged(object sender, EventArgs e)
        {
            var text = (sender as TextBox).Text;
            ListView.AdditionalFilter = !string.IsNullOrEmpty(text)
                ? TextMatchFilter.Contains(ListView, text)
                : null;
        }

        private class ProcessRowModel
        {
            public TrackedRow TrackedRow { get; set; }
            public int RowUid { get; set; }
            public object[] NewValues { get; set; }
            public string[] NewTypes { get; set; }

            public object[] PreviousValues { get; set; }
            public string[] PreviousTypes { get; set; }
        }
    }
}