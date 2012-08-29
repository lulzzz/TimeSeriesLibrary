using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This struct stores a date/value pair.  A timeseries can then be composed
    /// from an array of such structs.  The struct is designed for use by C/C++ COM code.
    /// .NET-based callers will instead use the equivalent TimeSeriesValue class.
    /// </summary>
    [Guid("b03b028a-d2fa-4bbb-9c18-822552edf68b")]
    [ComVisible(true)]
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
