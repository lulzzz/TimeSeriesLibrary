using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class stores a date/value pair.  A timeseries can then be composed
    /// from an array of such objects.  The class is designed for use by .NET.
    /// COM callers will instead use the equivalent TSDateValueStruct struct.
    /// </summary>
    public class TimeSeriesValue
    {
        public DateTime Date;
        public double Value;


        static public explicit operator TimeSeriesValue(TSDateValueStruct tsdvs)
        {
            return new TimeSeriesValue { Date = tsdvs.Date, Value = tsdvs.Value };
        }
        static public explicit operator TSDateValueStruct(TimeSeriesValue tsv)
        {
            return new TSDateValueStruct { Date = tsv.Date, Value = tsv.Value };
        }

    }
}
