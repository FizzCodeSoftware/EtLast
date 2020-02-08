namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class MutatorBuilder
    {
        public IEvaluable InputProcess { get; set; }
        public List<IMutator> Mutators { get; set; }

        public IEvaluable BuildEvaluable()
        {
            if (Mutators.Count == 0)
                return InputProcess;

            var last = InputProcess;
            foreach (var process in Mutators)
            {
                process.InputProcess = last;
                last = process;
            }

            return last;
        }
    }
}