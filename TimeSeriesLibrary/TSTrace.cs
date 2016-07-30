using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This interface describes an object that describes one trace of a time series.
    /// The interface is designed to be used in Oasis.Base by EntityObjects.
    /// </summary>
    public interface ITimeSeriesTrace
    {
        /// <summary>
        /// The trace number that identifies the trace
        /// </summary>
        Int32 TraceNumber { get; set; }
        /// <summary>
        /// The number of time steps that are stored in the ValueBlob property
        /// </summary>
        Int32 TimeStepCount { get; set; }
        /// <summary>
        /// The date of the last time step that is stored in the ValueBlob property
        /// </summary>
        DateTime EndDate { get; set; }
        /// <summary>
        /// The BLOB (byte array) that stores all the values (for regular time series) or the date/value 
        /// pairs (for irregular time series) of all the time steps in this time series trace.
        /// </summary>
        Byte[] ValueBlob { get; set; }
        /// <summary>
        /// The checksum computed from the BLOB and trace number
        /// </summary>
        Byte[] Checksum { get; set; }
    }

    /// <summary>
    /// This provides a concrete implementation of ITimeSeriesTrace for use
    /// internal to TimeSeriesLibrary.
    /// </summary>
    public class TSTrace : ITimeSeriesTrace
    {
        /// <summary>
        /// The trace number that identifies the trace
        /// </summary>
        public Int32 TraceNumber { get; set; }
        /// <summary>
        /// The number of time steps that are stored in the ValueBlob property
        /// </summary>
        public Int32 TimeStepCount { get; set; }
        /// <summary>
        /// The date of the last time step that is stored in the ValueBlob property
        /// </summary>
        public DateTime EndDate { get; set; }
        /// <summary>
        /// The BLOB (byte array) that stores all the values (for regular time series) or the date/value 
        /// pairs (for irregular time series) of all the time steps in this time series trace.
        /// </summary>
        public Byte[] ValueBlob { get; set; }
        /// <summary>
        /// The checksum computed from the BLOB and trace number
        /// </summary>
        public Byte[] Checksum { get; set; }
    }
}
