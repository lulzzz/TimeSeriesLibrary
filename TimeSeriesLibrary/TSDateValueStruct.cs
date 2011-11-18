using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This struct stores a date/value pair.  A timeseries can then be composed
    /// from an array of such structs.  The struct is designed for use by C/C++ COM code.
    /// .NET callers will instead use the equivalent TimeSeriesValue class.
    /// </summary>
    public struct TSDateValueStruct
    {
        public DateTime Date;
        public double Value;


    }
}
