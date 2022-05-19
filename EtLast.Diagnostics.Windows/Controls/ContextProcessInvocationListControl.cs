namespace FizzCode.EtLast.Diagnostics.Windows;

public delegate void OnProcessInvocationListSelectionChanged(TrackedProcessInvocation process);

internal class ContextProcessInvocationListControl
{
    public DiagContext Context { get; }
    public MyListView ListView { get; }
    public OnProcessInvocationListSelectionChanged OnSelectionChanged { get; set; }

    private readonly System.Threading.Timer _processStatUpdaterTimer;

    private Color IsSelectedForeColor { get; } = Color.White;
    private Color IsSelectedBackColor { get; } = Color.FromArgb(100, 100, 200);
    private Color IsOutputBackColor { get; } = Color.FromArgb(180, 255, 180);
    private Color IsInputBackColor { get; } = Color.FromArgb(255, 230, 185);
    private Color IsSameTopicBackColor { get; } = Color.FromArgb(220, 220, 255);

    private readonly List<ListViewItem> _allItems = new();
    private readonly Dictionary<int, ListViewItem> _itemsByProcessInvocationUid = new();

    public ContextProcessInvocationListControl(Control container, DiagContext context)
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

        var fix = 40 + 60 + 60 + 140;
        ListView.Columns.Add("#", 40);
        ListView.Columns.Add("time", 60);

        ListView.Columns.Add("topic", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Right;
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

        context.WholePlaybook.OnProcessInvoked += OnProcessInvoked;

        ListView.ItemSelectionChanged += ListView_ItemSelectionChanged;
    }

    private void ListView_MouseDoubleClick(object sender, MouseEventArgs e)
    {
        var list = sender as ListView;
        var info = list.HitTest(e.X, e.Y);

        if (info.Item?.Tag is TrackedProcessInvocation process)
        {
            var relevantRowUids = Context.Index.GetProcessRowMap(process.InvocationUid);

            var finishedRows = new HashSet<int>();
            var currentProcesses = new Dictionary<int, TrackedProcessInvocation>();

            var rows = new Dictionary<int, TrackedEtlRow>();
            Context.Index.EnumerateThroughRowEvents(e =>
            {
                if (relevantRowUids.Contains(e.RowUid) && !finishedRows.Contains(e.RowUid))
                {
                    if (e is RowCreatedEvent rce)
                    {
                        var creatorProc = Context.WholePlaybook.ProcessList[rce.ProcessInvocationUid];
                        var row = new TrackedEtlRow()
                        {
                            Uid = rce.RowUid,
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

                        currentProcesses[row.Uid] = creatorProc;

                        row.AllEvents.Add(rce);
                        rows.Add(row.Uid, row);
                    }
                    else if (e is RowValueChangedEvent rvce)
                    {
                        var row = rows[rvce.RowUid];
                        row.AllEvents.Add(rvce);

                        var values = currentProcesses[row.Uid] == process
                            ? row.NewValues
                            : row.PreviousValues;

                        foreach (var kvp in rvce.Values)
                        {
                            if (kvp.Value == null)
                                values.Remove(kvp.Key);
                            else
                                values[kvp.Key] = kvp.Value;
                        }
                    }
                    else if (e is RowOwnerChangedEvent roce)
                    {
                        var newProc = roce.NewProcessInvocationUid != null
                            ? Context.WholePlaybook.ProcessList[roce.NewProcessInvocationUid.Value]
                            : null;

                        var row = rows[roce.RowUid];
                        row.AllEvents.Add(roce);

                        var currentProcess = currentProcesses[row.Uid];

                        if (newProc == process)
                        {
                            row.NewValues = new Dictionary<string, object>(row.PreviousValues, StringComparer.OrdinalIgnoreCase);
                            row.PreviousProcess = currentProcess;
                        }
                        else if (currentProcess == process)
                        {
                            finishedRows.Add(row.Uid);
                            row.NextProcess = newProc;
                        }

                        currentProcesses[row.Uid] = newProc;
                    }

                    return finishedRows.Count < relevantRowUids.Count;
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

                    var control = new ProcessRowListControl(form, process, rows.Values.ToList());
                    control.Updater.RefreshStarted += (sender, args) =>
                    {
                        form.Text = "LOADING...";
                    };
                    control.Updater.RefreshFinished += (sender, args) =>
                    {
                        form.Text = "Process output: " + process.Name;
                    };

                    ToolTipSingleton.Remove(ListView);
                    form.ShowDialog();
                }
            }
        }
    }

    internal void SelectProcess(TrackedProcessInvocation process)
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

        var selectedProcess = e.Item.Tag as TrackedProcessInvocation;

        OnSelectionChanged?.Invoke(selectedProcess);

        foreach (var item in _allItems)
        {
            var itemProcess = item.Tag as TrackedProcessInvocation;

            if (selectedProcess != null)
            {
                item.UseItemStyleForSubItems = false;

                var itemIsSelected = itemProcess == selectedProcess;
                var itemIsInput = itemProcess.InputRowCountByPreviousProcess.ContainsKey(selectedProcess.InvocationUid);
                var itemIsOutput = selectedProcess.InputRowCountByPreviousProcess.ContainsKey(itemProcess.InvocationUid);
                var isSameTopic = selectedProcess.Topic == itemProcess.Topic/* || itemProcess.HasParentWithTopic(selectedProcess.Topic)*/;

                for (var i = 0; i < item.SubItems.Count; i++)
                {
                    var subItem = item.SubItems[i];
                    if (itemIsSelected)
                    {
                        subItem.BackColor = IsSelectedBackColor;
                        if (i is not 6 and not 11)
                            subItem.ForeColor = IsSelectedForeColor;
                    }
                    else
                    {
                        subItem.BackColor = ListView.BackColor;
                        if (i is not 6 and not 11)
                            subItem.ForeColor = ListView.ForeColor;
                    }
                }

                if (!itemIsSelected)
                {
                    item.SubItems[2].BackColor = isSameTopic
                        ? IsSameTopicBackColor
                        : item.BackColor;
                }

                if (itemIsSelected)
                {
                    item.SubItems[6].BackColor = IsOutputBackColor;
                    item.SubItems[11].BackColor = IsInputBackColor;
                }
                else if (itemIsInput)
                {
                    item.SubItems[3].BackColor = item.SubItems[6].BackColor = IsInputBackColor;
                    item.SubItems[11].BackColor = item.BackColor;
                }
                else if (itemIsOutput)
                {
                    item.SubItems[3].BackColor = item.SubItems[11].BackColor = IsOutputBackColor;
                    item.SubItems[6].BackColor = item.BackColor;
                }
                else if (isSameTopic)
                {
                    item.SubItems[3].BackColor = IsSameTopicBackColor;
                    item.SubItems[6].BackColor = item.SubItems[11].BackColor = item.BackColor;
                }
                else
                {
                    item.SubItems[3].BackColor = item.SubItems[6].BackColor = item.SubItems[11].BackColor = item.BackColor;
                }
            }
            else
            {
                item.UseItemStyleForSubItems = true;
            }
        }
    }

    private void OnProcessInvoked(Playbook playbook, TrackedProcessInvocation process)
    {
        var item = new ListViewItem(process.InstanceUID.ToString("D", CultureInfo.InvariantCulture)
            + (process.InvocationCounter > 1
                ? "/" + process.InvocationCounter.ToString("D", CultureInfo.InvariantCulture)
                : ""))
        {
            Tag = process,
        };

        item.SubItems.Add("-");
        item.SubItems.Add(process.Topic);
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

        if (process.Invoker != null && _itemsByProcessInvocationUid.TryGetValue(process.Invoker.InvocationUid, out var invokerItem))
        {
            var nextIndex = invokerItem.Index + 1;
            while (nextIndex < _allItems.Count)
            {
                var p = _allItems[nextIndex].Tag as TrackedProcessInvocation;
                if (!p.HasParent(process.Invoker))
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

        _itemsByProcessInvocationUid.Add(process.InvocationUid, item);
    }

    private void UpdateProcessStats()
    {
        _processStatUpdaterTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);

        ListView.Invoke(new Action(() =>
        {
            var changed = false;
            foreach (var item in _allItems)
            {
                var process = item.Tag as TrackedProcessInvocation;

                if (item.SubItems[1].Text != process.NetTimeAfterFinishedAsString)
                {
                    changed = true;
                    break;
                }

                if (item.SubItems[6].Text != process.GetFormattedInputRowCount()
                    || item.SubItems[7].Text != process.CreatedRowCount.FormatToStringNoZero()
                    || item.SubItems[8].Text != process.DroppedRowCount.FormatToStringNoZero()
                    || item.SubItems[9].Text != process.WrittenRowCount.FormatToStringNoZero()
                    || item.SubItems[10].Text != process.AliveRowCount.FormatToStringNoZero()
                    || item.SubItems[11].Text != process.PassedRowCount.FormatToStringNoZero())
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
                        var process = item.Tag as TrackedProcessInvocation;
                        item.SubItems[1].SetIfChanged(process.NetTimeAfterFinishedAsString);
                        item.SubItems[6].SetIfChanged(process.GetFormattedInputRowCount());
                        item.SubItems[7].SetIfChanged(process.CreatedRowCount.FormatToStringNoZero());
                        item.SubItems[8].SetIfChanged(process.DroppedRowCount.FormatToStringNoZero());
                        item.SubItems[9].SetIfChanged(process.WrittenRowCount.FormatToStringNoZero());
                        item.SubItems[10].SetIfChanged(process.AliveRowCount.FormatToStringNoZero());
                        item.SubItems[11].SetIfChanged(process.PassedRowCount.FormatToStringNoZero());
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
