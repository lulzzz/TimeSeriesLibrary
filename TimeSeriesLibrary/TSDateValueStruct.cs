using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This struct stores a date/value pair.  A timeseries can then be composed
    /// from an array of such structs.  The struct is designed for use by C/C++ COM code.
    /// .NET-based callers will instead use the equivalent TimeSeriesValue class.
    /// </summary>
    public struct TSDateValueStruct
    {
        /// <summary>
        /// The date of the time step
        /// </summary>
        public DateTime Date;
        /// <summary>
        /// The value of the time series on the time step
        /// </summary>
        public double Value;
    }
}
