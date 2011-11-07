using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class contains meta-data about one time series record that was imported.  This
    /// meta-data can be used by functions outside of TimeSeriesLibrary to finish processing
    /// the time series that were imported from an XML file by the TSXml class.  In this way,
    /// the outside functions can handle features of the time series that TimeSeriesLibrary
    /// was not designed to handle.
    /// </summary>
    public class TSImport
    {
        public String Name;
        public Guid Id;
        public String UnprocessedElements;
    }
}
