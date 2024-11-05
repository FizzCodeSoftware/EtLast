namespace FizzCode.EtLast.Diagnostics.Windows;

public delegate void OnProcessListSelectionChanged(TrackedProcess process);

internal class ContextProcessListControl
{
    public DiagContext Context { get; }
    public MyListView ListView { get; }
    public OnProcessListSelectionChanged OnSelectionChanged { get; set; }

    private readonly System.Threading.Timer _processStatUpdaterTimer;

    private Color IsSelectedForeColor { get; } = Color.White;
    private Color IsSelectedBackColor { get; } = Color.FromArgb(100, 100, 200);
    private Color IsOutputBackColor { get; } = Color.FromArgb(180, 255, 180);
    private Color IsInputBackColor { get; } = Color.FromArgb(255, 230, 185);

    private readonly List<ListViewItem> _allItems = [];
    private readonly Dictionary<long, ListViewItem> _itemsByProcessId = [];

    public ContextProcessListControl(Control container, DiagContext context)
    {
        Context = context;

        ListView = new MyListView()
        {
            View = View.Details,
            Parent = container,
            HeaderStyle = ColumnHeaderStyle.Nonclickable,
            GridLines = true,
            AllowColumnReorder = false,
            FullRowSelect = false,
            Width = 1200,
            BorderStyle = BorderStyle.FixedSingle,
            ShowItemToolTips = true,
            MultiSelect = false,
            HideSelection = false,
        };

        ListView.MouseMove += ListView_MouseMove;
        ListView.MouseLeave += (s, a) => ToolTipSingleton.Remove(ListView);
        ListView.MouseUp += ListView_MouseUp;

        const int fix = 40 + 60 + 60 + 140;
        ListView.Columns.Add("#", 40);
        ListView.Columns.Add("time", 60);

        ListView.Columns.Add("process", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;
        ListView.Columns.Add("kind", 60).TextAlign = HorizontalAlignment.Left;
        ListView.Columns.Add("type", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;

        ListView.Columns.Add("IN", 140).TextAlign = HorizontalAlignment.Right;
        ListView.Columns.Add("+", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
        ListView.Columns.Add("-", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
        ListView.Columns.Add("sink", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
        ListView.Columns.Add("pending", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
        ListView.Columns.Add("OUT", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;

        ListView.MouseDoubleClick += ListView_MouseDoubleClick;

        _processStatUpdaterTimer = new System.Threading.Timer((state) => UpdateProcessStats());
        _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);

        context.WholePlaybook.OnProcessStarted += OnProcessStarted;

        ListView.ItemSelectionChanged += ListView_ItemSelectionChanged;
    }

    private void ListView_MouseDoubleClick(object sender, MouseEventArgs e)
    {
        var list = sender as ListView;
        var info = list.HitTest(e.X, e.Y);

        if (info.Item?.Tag is TrackedProcess process)
        {
            var relevantRowIdList = Context.Index.GetProcessRowMap(process.Id);

            var finishedRows = new HashSet<long>();
            var currentProcesses = new Dictionary<long, TrackedProcess>();

            var rows = new Dictionary<long, TrackedEtlRow>();
            Context.Index.EnumerateThroughRowEvents(e =>
            {
                if (relevantRowIdList.Contains(e.RowId) && !finishedRows.Contains(e.RowId))
                {
                    if (e is RowCreatedEvent rce)
                    {
                        var creatorProc = Context.WholePlaybook.ProcessList[rce.ProcessId];
                        var row = new TrackedEtlRow()
                        {
                            Id = rce.RowId,
                            CreatorProcess = creatorProc,
                            PreviousProcess = null,
                        };

                        if (creatorProc == process)
                        {
                            row.NewValues = new Dictionary<string, object>(rce.Values, StringComparer.OrdinalIgnoreCase);
                        }
                        else
                        {
                            row.PreviousValues = new Dictionary<string, object>(rce.Values, StringComparer.OrdinalIgnoreCase);
                        }

                        currentProcesses[row.Id] = creatorProc;

                        row.AllEvents.Add(rce);
                        rows.Add(row.Id, row);
                    }
                    else if (e is RowValueChangedEvent rvce)
                    {
                        var row = rows[rvce.RowId];
                        row.AllEvents.Add(rvce);

                        var values = currentProcesses[row.Id] == process
                            ? row.NewValues
                            : row.PreviousValues;

                        foreach (var kvp in rvce.Values)
                        {
                            if (kvp.Value is EtlRowRemovedValue)
                                values.Remove(kvp.Key);
                            else
                                values[kvp.Key] = kvp.Value;
                        }
                    }
                    else if (e is RowOwnerChangedEvent roce)
                    {
                        var newProc = roce.NewProcessId != null
                            ? Context.WholePlaybook.ProcessList[roce.NewProcessId.Value]
                            : null;

                        var row = rows[roce.RowId];
                        row.AllEvents.Add(roce);

                        var currentProcess = currentProcesses[row.Id];

                        if (newProc == process)
                        {
                            row.NewValues = new Dictionary<string, object>(row.PreviousValues, StringComparer.OrdinalIgnoreCase);
                            row.PreviousProcess = currentProcess;
                        }
                        else if (currentProcess == process)
                        {
                            finishedRows.Add(row.Id);
                            row.NextProcess = newProc;
                        }

                        currentProcesses[row.Id] = newProc;
                    }

                    return finishedRows.Count < relevantRowIdList.Count;
                }

                return true;
            }, DiagnosticsEventKind.RowCreated, DiagnosticsEventKind.RowValueChanged, DiagnosticsEventKind.RowOwnerChanged);

            if (rows.Values.Count > 0)
            {
                using (var form = new Form())
                {
                    form.FormBorderStyle = FormBorderStyle.Sizable;
                    form.WindowState = FormWindowState.Normal;
                    form.StartPosition = FormStartPosition.Manual;
                    form.Bounds = new Rectangle(Screen.PrimaryScreen.Bounds.Left + 100, Screen.PrimaryScreen.Bounds.Top + 100, Screen.PrimaryScreen.Bounds.Width - 200, Screen.PrimaryScreen.Bounds.Height - 200);
                    form.KeyPreview = true;
                    form.KeyPress += (s, e) =>
                    {
                        if (e.KeyChar == (char)Keys.Escape)
                        {
                            form.Close();
                        }
                    };

                    var control = new ProcessRowListControl(form, process, [.. rows.Values]);
                    control.Updater.RefreshStarted += (sender, args) => form.Text = "LOADING...";
                    control.Updater.RefreshFinished += (sender, args) => form.Text = "Process output: " + process.Name;

                    ToolTipSingleton.Remove(ListView);
                    form.ShowDialog();
                }
            }
        }
    }

    internal void SelectProcess(TrackedProcess process)
    {
        var item = _allItems.Find(x => x.Tag == process);
        if (item?.Selected == false)
        {
            ListView.EnsureVisible(item.Index);

            ListView.Focus();
            item.Selected = true;
        }
    }

    private void ListView_ItemSelectionChanged(object sender, ListViewItemSelectionChangedEventArgs e)
    {
        if (!e.IsSelected)
            return;

        var selectedProcess = e.Item.Tag as TrackedProcess;

        OnSelectionChanged?.Invoke(selectedProcess);

        foreach (var item in _allItems)
        {
            var itemProcess = item.Tag as TrackedProcess;

            if (selectedProcess != null)
            {
                item.UseItemStyleForSubItems = false;

                var itemIsSelected = itemProcess == selectedProcess;
                var itemIsInput = itemProcess.InputRowCountByPreviousProcess.ContainsKey(selectedProcess.Id);
                var itemIsOutput = selectedProcess.InputRowCountByPreviousProcess.ContainsKey(itemProcess.Id);

                for (var i = 0; i < item.SubItems.Count; i++)
                {
                    var subItem = item.SubItems[i];
                    if (itemIsSelected)
                    {
                        subItem.BackColor = IsSelectedBackColor;
                        if (i is not 5 and not 10)
                            subItem.ForeColor = IsSelectedForeColor;
                    }
                    else
                    {
                        subItem.BackColor = ListView.BackColor;
                        if (i is not 5 and not 10)
                            subItem.ForeColor = ListView.ForeColor;
                    }
                }

                if (itemIsSelected)
                {
                    item.SubItems[5].BackColor = IsOutputBackColor;
                    item.SubItems[10].BackColor = IsInputBackColor;
                }
                else if (itemIsInput)
                {
                    item.SubItems[2].BackColor = item.SubItems[5].BackColor = IsInputBackColor;
                    item.SubItems[10].BackColor = item.BackColor;
                }
                else if (itemIsOutput)
                {
                    item.SubItems[2].BackColor = item.SubItems[10].BackColor = IsOutputBackColor;
                    item.SubItems[5].BackColor = item.BackColor;
                }
                else
                {
                    item.SubItems[2].BackColor = item.SubItems[5].BackColor = item.SubItems[10].BackColor = item.BackColor;
                }
            }
            else
            {
                item.UseItemStyleForSubItems = true;
            }
        }
    }

    private void OnProcessStarted(Playbook playbook, TrackedProcess process)
    {
        var item = new ListViewItem(process.Id.ToString("D", CultureInfo.InvariantCulture))
        {
            Tag = process,
        };

        item.SubItems.Add("-");
        item.SubItems.Add(process.IdentedName);
        item.SubItems.Add(process.KindToString());
        item.SubItems.Add(process.Type);
        item.SubItems.Add("0");
        item.SubItems.Add("0");
        item.SubItems.Add("0");
        item.SubItems.Add("0");
        item.SubItems.Add("0");
        item.SubItems.Add("0").Tag = new Func<string>(() => process.GetFormattedRowFlow(Context));
        item.Tag = process;

        if (ListView.SelectedItems.Count == 0)
        {
            item.Selected = true;
        }

        if (process.Caller != null && _itemsByProcessId.TryGetValue(process.Caller.Id, out var invokerItem))
        {
            var nextIndex = invokerItem.Index + 1;
            while (nextIndex < _allItems.Count)
            {
                var p = _allItems[nextIndex].Tag as TrackedProcess;
                if (!p.HasParent(process.Caller))
                    break;

                nextIndex++;
            }

            _allItems.Insert(nextIndex, item);
            ListView.Items.Insert(nextIndex, item);
        }
        else
        {
            _allItems.Add(item);
            ListView.Items.Add(item);
        }

        _itemsByProcessId.Add(process.Id, item);
    }

    private void UpdateProcessStats()
    {
        _processStatUpdaterTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);

        ListView.Invoke(new Action(() =>
        {
            var changed = false;
            foreach (var item in _allItems)
            {
                var process = item.Tag as TrackedProcess;

                if (item.SubItems[1].Text != process.NetTimeAfterFinishedAsString)
                {
                    changed = true;
                    break;
                }

                if (item.SubItems[5].Text != process.GetFormattedInputRowCount()
                    || item.SubItems[6].Text != process.CreatedRowCount.FormatToStringNoZero()
                    || item.SubItems[7].Text != process.DroppedRowCount.FormatToStringNoZero()
                    || item.SubItems[8].Text != process.WrittenRowCount.FormatToStringNoZero()
                    || item.SubItems[9].Text != process.AliveRowCount.FormatToStringNoZero()
                    || item.SubItems[10].Text != process.PassedRowCount.FormatToStringNoZero())
                {
                    changed = true;
                    break;
                }
            }

            if (changed)
            {
                ListView.BeginUpdate();
                try
                {
                    foreach (var item in _allItems)
                    {
                        var process = item.Tag as TrackedProcess;
                        item.SubItems[1].SetIfChanged(process.NetTimeAfterFinishedAsString);
                        item.SubItems[5].SetIfChanged(process.GetFormattedInputRowCount());
                        item.SubItems[6].SetIfChanged(process.CreatedRowCount.FormatToStringNoZero());
                        item.SubItems[7].SetIfChanged(process.DroppedRowCount.FormatToStringNoZero());
                        item.SubItems[8].SetIfChanged(process.WrittenRowCount.FormatToStringNoZero());
                        item.SubItems[9].SetIfChanged(process.AliveRowCount.FormatToStringNoZero());
                        item.SubItems[10].SetIfChanged(process.PassedRowCount.FormatToStringNoZero());
                    }
                }
                finally
                {
                    ListView.EndUpdate();
                }
            }
        }));

        if (!Context.FullyLoaded)
        {
            _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);
        }
    }

    private void ListView_MouseMove(object sender, MouseEventArgs e)
    {
        var info = ListView.HitTest(e.X, e.Y);

        if (info.SubItem?.Tag != null)
        {
            ToolTipSingleton.Show(info.SubItem.Tag, ListView, e.X, e.Y);
        }
        else
        {
            ToolTipSingleton.Remove(ListView);
        }
    }

    private void ListView_MouseUp(object sender, MouseEventArgs e)
    {
        var list = sender as ListView;
        var info = list.HitTest(e.X, e.Y);
        if (info.Item != null)
        {
            if (!info.Item.Selected)
                info.Item.Selected = true;
        }
    }
}
