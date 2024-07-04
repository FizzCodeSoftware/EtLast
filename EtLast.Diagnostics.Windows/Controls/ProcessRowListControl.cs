namespace FizzCode.EtLast.Diagnostics.Windows;

internal class ProcessRowListControl
{
    public Control Container { get; }
    public TrackedProcess Process { get; }
    public List<TrackedEtlRow> Rows { get; }

    // filters
    public RadioButton ShowAll { get; }
    public RadioButton ShowRemoved { get; }
    public RadioButton ShowChanged { get; }
    public RadioButton ShowUnChanged { get; }

    private readonly Dictionary<string, int> _columnIndexes = new(StringComparer.OrdinalIgnoreCase);
    private readonly int _fixColumnCount;

    public ControlUpdater<ProcessRowModel> Updater { get; }

    public ProcessRowListControl(Control container, TrackedProcess process, List<TrackedEtlRow> rows)
    {
        Container = container;
        Process = process;
        Rows = rows;

        Updater = new ControlUpdater<ProcessRowModel>(null, Container, -1, 10)
        {
            ItemFilter = ItemFilter,
            ContainsRows = true,
        };

        Updater.CreateSearchBox(10, 10);

        ShowAll = new RadioButton()
        {
            Parent = container,
            Bounds = new Rectangle(Updater.SearchBox.Right + 20, Updater.SearchBox.Top, 60, Updater.SearchBox.Height),
            Text = "All",
            CheckAlign = ContentAlignment.MiddleLeft,
            Checked = process.Kind != "mutator",
        };

        ShowChanged = new RadioButton()
        {
            Parent = container,
            Bounds = new Rectangle(ShowAll.Right + 20, Updater.SearchBox.Top, 75, Updater.SearchBox.Height),
            Text = "Changed",
            CheckAlign = ContentAlignment.MiddleLeft,
            Checked = process.Kind == "mutator",
        };

        ShowUnChanged = new RadioButton()
        {
            Parent = container,
            Bounds = new Rectangle(ShowChanged.Right + 20, Updater.SearchBox.Top, 100, Updater.SearchBox.Height),
            Text = "Unchanged",
            CheckAlign = ContentAlignment.MiddleLeft,
            Checked = false,
        };

        ShowRemoved = new RadioButton()
        {
            Parent = container,
            Bounds = new Rectangle(ShowUnChanged.Right + 20, Updater.SearchBox.Top, 75, Updater.SearchBox.Height),
            Text = "Removed",
            CheckAlign = ContentAlignment.MiddleLeft,
            Checked = false,
        };

        ShowAll.CheckedChanged += (s, a) => Updater.RefreshItems(true);
        ShowRemoved.CheckedChanged += (s, a) => Updater.RefreshItems(true);
        ShowChanged.CheckedChanged += (s, a) => Updater.RefreshItems(true);
        ShowUnChanged.CheckedChanged += (s, a) => Updater.RefreshItems(true);

        Updater.ListView.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom;
        Updater.ListView.Bounds = new Rectangle(Container.ClientRectangle.Left, Container.ClientRectangle.Top + 40, Container.ClientRectangle.Width, Container.ClientRectangle.Height - 40);
        Updater.ListView.FormatCell += ListView_FormatCell;
        Updater.ListView.UseCellFormatEvents = true;
        Updater.ListView.CellToolTipShowing += ListView_CellToolTipShowing;

        Updater.ListView.AllColumns.Add(new OLVColumn()
        {
            Text = "ID",
            AspectGetter = x => (x as ProcessRowModel)?.TrackedRow.Id,
        });

        Updater.ListView.AllColumns.Add(new OLVColumn()
        {
            Text = "Previous process",
            AspectGetter = x => (x as ProcessRowModel)?.TrackedRow.PreviousProcess?.Name,
        });

        Updater.ListView.AllColumns.Add(new OLVColumn()
        {
            Text = "Next process",
            AspectGetter = x => (x as ProcessRowModel)?.TrackedRow.NextProcess?.Name,
        });

        Updater.ListView.Columns.AddRange(Updater.ListView.AllColumns.ToArray());

        _fixColumnCount = Updater.ListView.Columns.Count;

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

                if (row.NewValues.Count == row.PreviousValues.Count)
                {
                    foreach (var kvp in row.NewValues)
                    {
                        row.PreviousValues.TryGetValue(kvp.Key, out var previousValue);
                        row.NewValues.TryGetValue(kvp.Key, out var newValue);

                        if (!DefaultValueComparer.ValuesAreEqual(previousValue, newValue))
                        {
                            item.Changed = true;
                        }
                    }
                }
                else
                {
                    item.Changed = true;
                }
            }

            foreach (var kvp in row.NewValues)
            {
                var valueIndex = _columnIndexes[kvp.Key] / 2;
                item.NewValues[valueIndex] = kvp.Value;
                item.NewTypes[valueIndex] = kvp.Value?.GetType().GetFriendlyTypeName();
            }

            Updater.AddItem(item);
        }

        Updater.ListView.Columns.AddRange(newColumns.ToArray());

        Updater.Start();
    }

    private bool ItemFilter(ProcessRowModel item)
    {
        if (ShowAll.Checked)
            return true;
        else if (ShowChanged.Checked)
            return item.Changed;
        else if (ShowUnChanged.Checked)
            return !item.Changed;
        else if (ShowRemoved.Checked)
            return item.TrackedRow.NextProcess == null;

        return true;
    }

    private void AddColumnByValue(List<OLVColumn> newColumns, KeyValuePair<string, object> kvp)
    {
        var columnName = kvp.Key;
        if (!_columnIndexes.TryGetValue(columnName, out var columnIndex))
        {
            columnIndex = Updater.ListView.AllColumns.Count - _fixColumnCount;
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

            Updater.ListView.AllColumns.Add(newColumn);
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

            Updater.ListView.AllColumns.Add(newColumn);
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
                else if ((previousValue == null != (newValue == null))
                    || (previousValue != null && newValue != null && previousValue.GetType() != newValue.GetType()))
                {
                    e.SubItem.BackColor = Color.LightBlue;
                }
            }
        }
    }

    public class ProcessRowModel
    {
        public TrackedEtlRow TrackedRow { get; set; }
        public object[] NewValues { get; set; }
        public string[] NewTypes { get; set; }

        public object[] PreviousValues { get; set; }
        public string[] PreviousTypes { get; set; }

        public bool Changed { get; set; }
    }
}
