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
        /// <summary>
        /// The date of the time step
        /// </summary>
        public DateTime Date { get; set; }
        /// <summary>
        /// The value of the time series on the time step
        /// </summary>
        public double Value { get; set; }

        /// <summary>
        /// Operator for converting from TSDateValueStruct struct instance to TimeSeriesValue object.
        /// The operator is explicit only.  Example usage:
        ///     // set up for usage
        ///     TimeSeriesValue tsv = new TimeSeriesValue();
        ///     TSDateValueStruct tsvds = new TSDateValueStruct({ Date=someDate, Value=someValue });
        ///     // example usage of operator
        ///     tsv = (TimeSeriesValue)tsvds;
        /// </summary>
        /// <param name="tsdvs">The struct instance to convert</param>
        /// <returns>the value of the struct instance converted to a TimeSeriesValue object</returns>
        static public explicit operator TimeSeriesValue(TSDateValueStruct tsdvs)
        {
            return new TimeSeriesValue { Date = tsdvs.Date, Value = tsdvs.Value };
        }
        /// <summary>
        /// Operator for converting from TimeSeriesValue object to TSDateValueStruct struct instance.
        /// The operator is explicit only.  Example usage:
        ///     // set up for usage
        ///     TimeSeriesValue tsv = new TimeSeriesValue({ Date=someDate, Value=someValue });
        ///     TSDateValueStruct tsvds = new TSDateValueStruct();
        ///     // example usage of operator
        ///     tsvds = (TSDateValueStruct)tsv;
        /// </summary>
        /// <param name="tsv">The TimeSeriesValue object to convert</param>
        /// <returns>the value of the object converted to an instance of TSDateValueStruct</returns>
        static public explicit operator TSDateValueStruct(TimeSeriesValue tsv)
        {
            return new TSDateValueStruct { Date = tsv.Date, Value = tsv.Value };
        }

        /// <summary>
        /// This method checks whether the values of all properties of the given object are the
        /// same as the properties of this object.  The given object may be of any type, so the
        /// method checks whether it is of type TimeSeriesValue (if not, it returns false).
        /// </summary>
        /// <param name="obj">the object to compare to this object</param>
        /// <returns>true if the given object has the same property values as this object, 
        /// otherwise false.</returns>
        public Boolean ValueEquals(Object obj)
        {
            // If we have the very same instance of the object, then no need to go further
            if (this == obj) return true;
            // if obj is null
            if (obj == null) return false;
            // if obj is not of matching type
            if ((obj is TimeSeriesValue) == false) return false;
            // recast so that we can access the properties that we'll need to compare
            TimeSeriesValue obj2 = (TimeSeriesValue)obj;

            // Compare individual properties
            if (this.Date != obj2.Date) return false;
            if (this.Value != obj2.Value) return false;

            return true;
        }

    }
}
