namespace FizzCode.EtLast.EPPlus
{
    using OfficeOpenXml;
    using System;
    using System.IO;

    public class EpPlusSingleExcelFileWriterOperation<TState> : AbstractRowOperation
        where TState : new()
    {
        public string FileName { get; set; }
        public Action<ExcelPackage, TState> Initialize { get; set; }
        public Action<IRow, ExcelPackage, TState> Action { get; set; }
        public Action<ExcelPackage, TState> Finalize { get; set; }
        public ExcelPackage Source { get; set; }

        private ExcelPackage _excelPackage;
        private TState _state;

        public override void Apply(IRow row)
        {
            if (_excelPackage == null) // lazy load here instead of prepare
            {
                _excelPackage = Source ?? new ExcelPackage(new FileInfo(FileName));
                Initialize?.Invoke(_excelPackage, _state);
            }

            try
            {
                Action.Invoke(row, _excelPackage, _state);
            }
            catch (Exception ex)
            {
                var exception = new OperationExecutionException(Process, this, row, "error raised during writing an excel file", ex);
                exception.AddOpsMessage(string.Format("error raised during writing an excel file, file name: {0}, message: {1}, row: {2}", FileName, ex.Message, row.ToDebugString()));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }
        }

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(FileName)) throw new InvalidOperationParameterException(this, nameof(FileName), FileName, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (Action == null) throw new InvalidOperationParameterException(this, nameof(Action), Action, InvalidOperationParameterException.ValueCannotBeNullMessage);

            _state = new TState();
        }

        public override void Shutdown()
        {
            Finalize?.Invoke(_excelPackage, _state);

            base.Shutdown();

            _state = default(TState);

            _excelPackage.Save();
            _excelPackage.Dispose();
            _excelPackage = null;
        }
    }
}