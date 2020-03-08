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
                foreach (var kvp in row.Values)
                {
                    var columnName = kvp.Key;
                    if (!_columnIndexes.TryGetValue(columnName, out var columnIndex))
                    {
                        columnIndex = ListView.AllColumns.Count - _fixColumnCount;

                        var newColumn = new OLVColumn()
                        {
                            Text = columnName,
                            AspectGetter = x =>
                            {
                                return (x is ProcessRowModel r && columnIndex < r.Values.Length)
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
                                return (x is ProcessRowModel r && columnIndex < r.Values.Length)
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

                var item = new ProcessRowModel()
                {
                    TrackedRow = row,
                    RowUid = row.Uid,
                    Values = new object[ListView.AllColumns.Count - _fixColumnCount],
                    Types = new string[ListView.AllColumns.Count - _fixColumnCount],
                };

                foreach (var kvp in row.Values)
                {
                    var columnIndex = _columnIndexes[kvp.Key];

                    item.Values[columnIndex] = kvp.Value;
                    item.Types[columnIndex] = kvp.Value?.GetType().GetFriendlyTypeName();
                }

                items.Add(item);
            }

            ListView.Columns.AddRange(newColumns.ToArray());
            ListView.AddObjects(items);

            ControlUpdater<int>.ResizeListView(ListView);
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

        private class ProcessRowModel
        {
            public TrackedRow TrackedRow { get; set; }
            public int RowUid { get; set; }
            public object[] Values { get; set; }
            public string[] Types { get; set; }
        }
    }
}