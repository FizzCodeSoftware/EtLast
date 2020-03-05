namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using FizzCode.EtLast;
    using OfficeOpenXml;

    public class EpPlusSingleExcelStreamWriterMutator<TState> : AbstractMutator, IRowWriter
        where TState : BaseExcelWriterState, new()
    {
        public Stream Stream { get; set; }
        public Action<ExcelPackage, TState> Initialize { get; set; }
        public Action<IRow, ExcelPackage, TState> Action { get; set; }
        public Action<ExcelPackage, TState> Finalize { get; set; }
        public ExcelPackage ExistingPackage { get; set; }
        private TState _state;
        private ExcelPackage _package;
        private int? _storeUid;

        public EpPlusSingleExcelStreamWriterMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _state = new TState();
        }

        protected override void CloseMutator()
        {
            if (_state.LastWorksheet != null)
            {
                Finalize?.Invoke(_package, _state);
            }

            if (ExistingPackage == null && _package != null)
            {
                _package.Save();
                _package.Dispose();
                _package = null;
            }

            _state = null;
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (_package == null) // lazy load here instead of prepare
            {
#pragma warning disable CA2000 // Dispose objects before losing scope
                _package = ExistingPackage ?? new ExcelPackage(Stream);
#pragma warning restore CA2000 // Dispose objects before losing scope
                Initialize?.Invoke(_package, _state);
            }

            try
            {
                Action.Invoke(row, _package, _state);

                if (_storeUid != null)
                    Context.OnRowStored?.Invoke(this, row, _storeUid.Value);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, row, "error raised during writing an excel stream", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel stream, message: {0}, row: {1}",
                    ex.Message, row.ToDebugString()));

                exception.Data.Add("FileName", Stream);
                throw exception;
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (Stream == null)
                throw new ProcessParameterNullException(this, nameof(Stream));

            if (Action == null)
                throw new ProcessParameterNullException(this, nameof(Action));
        }

        public void AddWorkSheet(string name)
        {
            _state.LastWorksheet = _package.Workbook.Worksheets.Add(name);
            _storeUid = Context.GetStoreUid(new List<KeyValuePair<string, string>>()
            {
                new KeyValuePair<string, string>("Sheet", name),
            });
        }
    }
}