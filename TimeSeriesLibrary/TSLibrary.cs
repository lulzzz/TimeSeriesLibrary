using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// TSLibrary is the main class for the TimeSeriesLibrary.  It contains the callable
    /// functions of the library.
    /// </summary>
    public class TSLibrary
    {
        /// <summary>
        /// TSConnection object maintains a list of connections that have been opened by the library
        /// </summary>
        public TSConnection ConnxObject = new TSConnection();

        #region Public Methods for Connection
        /// <summary>
        /// Opens a new connection for the time series library to use.  The new connection 
        /// is added to a list and assigned a serial number.  The method returns the
        /// serial number of the new connection.
        /// </summary>
        /// <param name="connectionString">The connection string used to open the connection.</param>
        /// <returns>The serial number that was automatically assigned to the new connection.</returns>
        public int OpenConnection(String connectionString)
        {
            // all the logic is found in the TSConnection object.
            return ConnxObject.OpenConnection(connectionString);
        }
        /// <summary>
        /// Closes the connection identified with the given serial number.  When the
        /// connection is closed, it is removed from the list of connections available to
        /// the time series library, and the serial number no longer refers to this
        /// connection.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection to be closed</param>
        public void CloseConnection(int connectionNumber)
        {
            // all the logic is found in the TSConnection object.
            ConnxObject.CloseConnection(connectionNumber);
        }
        /// <summary>
        /// Returns the SqlConnection object corresponding to the given connection number.
        /// </summary>
        /// <param name="connectionNumber">serial number of the connection within the collection</param>
        /// <returns>The SqlConnection object corresponding to the given connection number</returns>
        public SqlConnection GetConnectionFromId(int connectionNumber)
        {
            SqlConnection connx;
            try
            {
                connx = ConnxObject.TSConnectionsCollection[connectionNumber];
            }
            catch
            {
                throw new TSLibraryException(ErrCode.Enum.Connection_Not_Found,
                                String.Format("TimeSeriesLibrary does not have an open connection number {0}", 
                                connectionNumber));
            }
            return connx;
        }
        #endregion


        #region Public methods for READING time series

        // usage: ??
        public int ReadValues(
                int connectionNumber, String tableName, Guid id,
                int nReqValues, ref List<double> valueList, DateTime reqStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            double[] valueArray = new double[nReqValues];
            int ret = ts.ReadValuesRegular(id, nReqValues, valueArray, reqStartDate);
            valueList = valueArray.ToList<double>();
            return ret;
        }
        
        // usage: for onevar to read model output, b/c it does not need dates for each timeseries
        public unsafe int ReadValuesUnsafe(
                int connectionNumber, String tableName, Guid id,
                int nReqValues, double[] valueArray, DateTime reqStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            return ts.ReadValuesRegular(id, nReqValues, valueArray, reqStartDate);
        }
        // usage: general model/onevar input
        public unsafe int ReadDatesValuesUnsafe(
                int connectionNumber, String tableName, Guid id,
                int nReqValues, TimeSeriesValue[] dateValueArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            int nValuesRead = 0;
            // Read the meta-parameters of the time series so that we'll know if it's regular or irregular
            if (!ts.IsInitialized) ts.Initialize(id);

            // The operations will differ for regular and irregular time series
            if (ts.TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                // IRREGULAR TIME SERIES

                // Read the date/value array from the database
                nValuesRead = ts.ReadValuesIrregular(id, nReqValues, dateValueArray, reqStartDate, reqEndDate);
            }
            else
            {
                // REGULAR TIME SERIES

                // Allocate an array to hold the time series' data values
                double[] valueArray = new double[nReqValues];
                // Read the data values from the database
                nValuesRead = ts.ReadValuesRegular(id, nReqValues, valueArray, reqStartDate);
                // Allocate an array to hold the time series' date values
                DateTime[] dateArray = new DateTime[nValuesRead];
                // Fill the array with the date values corresponding to the time steps defined
                // for this time series in the database.
                ts.FillDateArray(id, nValuesRead, dateArray, reqStartDate);
                // Loop through all values, filling the array of date/value pairs from the
                // primitive array of dates and primitive array of values.
                int i;
                for (i = 0; i < nValuesRead; i++)
                {
                    dateValueArray[i].Date = dateArray[i];
                    dateValueArray[i].Value = valueArray[i];
                    // So far we have ignored the requested end date.  However, at this
                    // stage we won't make the list any longer than was requested by the caller.
                    if (dateValueArray[i].Date >= reqEndDate)
                    {
                        i++;
                        break;
                    }
                }
                nValuesRead = i;
            }
            //return ts.ReadValuesRegular(id, nReqValues, valueArray, reqStartDate);
            return nValuesRead;
        }

        // usage: for GUI to retrieve an entire time series.  The length of the list is allocated in this method.
        public int ReadAllDatesValues(
                int connectionNumber, String tableName, Guid id,
                ref List<TimeSeriesValue> dateValueList)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            // Read the meta-parameters of the time series so that we'll know its date and list-length limits
            if (!ts.IsInitialized) ts.Initialize(id);

            // let the sister function do the rest of the work
            return ReadLimitedDatesValues(connectionNumber, tableName, id,
                        ts.TimeStepCount, ref dateValueList, ts.BlobStartDate, ts.BlobEndDate);
        }
        
        // usage: for GUI to retrieve a time series, with date and list-length limits.
        // The length of the list is allocated in this method.
        public int ReadLimitedDatesValues(
                int connectionNumber, String tableName, Guid id,
                int nReqValues, ref List<TimeSeriesValue> dateValueList, DateTime reqStartDate, DateTime reqEndDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            int nValuesRead = 0;
            // Read the meta-parameters of the time series so that we'll know if it's regular or irregular
            if(!ts.IsInitialized) ts.Initialize(id);

            // Caller has the option of passing nReqValues==0, indicating that there should be no
            // limit on the list length
            if (nReqValues == 0) nReqValues = ts.TimeStepCount;

            // The operations will differ for regular and irregular time series
            if (ts.TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                // IRREGULAR TIME SERIES

                // Allocate an array of date/value pairs that will be used by the caller
                TimeSeriesValue[] dateValueArray = new TimeSeriesValue[nReqValues];
                // Read the date/value list from the database
                nValuesRead = ts.ReadValuesIrregular(id, nReqValues, dateValueArray, reqStartDate, reqEndDate);
                // resize the array so that the list that we make from it will have exactly the right size
                if(nValuesRead!=nReqValues)
                    Array.Resize<TimeSeriesValue>(ref dateValueArray, nValuesRead);
                // Convert the array of date/value pairs into the list that will be used by the caller
                dateValueList = dateValueArray.ToList<TimeSeriesValue>();
            }
            else
            {
                // REGULAR TIME SERIES

                // Allocate an array to hold the time series' data values
                double[] valueArray = new double[nReqValues];
                // Read the data values from the database
                nValuesRead = ts.ReadValuesRegular(id, nReqValues, valueArray, reqStartDate);
                // Allocate an array to hold the time series' date values
                DateTime[] dateArray = new DateTime[nValuesRead];
                // Fill the array with the date values corresponding to the time steps defined
                // for this time series in the database.
                ts.FillDateArray(id, nValuesRead, dateArray, reqStartDate);
                // Allocate a list of date/value pairs that will be used by the caller
                dateValueList = new List<TimeSeriesValue>(nValuesRead);
                // Loop through all values, building the list of date/value pairs out of the
                // primitive array of dates and primitive array of values.
                TimeSeriesValue tsv = new TimeSeriesValue();
                int i;
                for (i = 0; i < nValuesRead; i++)
                {
                    tsv.Date = dateArray[i];
                    tsv.Value = valueArray[i];
                    dateValueList.Add(tsv);
                    // So far we have ignored the requested end date.  However, at this
                    // stage we won't make the list any longer than was requested by the caller.
                    if (tsv.Date >= reqEndDate)
                    {
                        i++;
                        break;
                    }
                }
                // If the number of requested values is much smaller than the capacity of the list,
                // then we'll reallocate the list so as not to waste memory.
                if (i < nValuesRead * 0.4)
                {
                    dateValueList.TrimExcess();
                }
                nValuesRead = i;
            }
            return nValuesRead;
        }

        #endregion


        #region Public methods for WRITING time series

        // TODO: test the effectiveness of safe/unsafe versions by making a C++ sandbox.
        public Guid WriteValues(
                    int connectionNumber, String tableName,
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, List<double> valueList, DateTime OutStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            return ts.WriteValuesRegular(timeStepUnit, timeStepQuantity, nOutValues, valueList.ToArray<double>(), OutStartDate);
        }
        public unsafe Guid WriteValuesUnsafe(
                    int connectionNumber, String tableName,
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, double[] valueArray, DateTime OutStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            return ts.WriteValuesRegular(timeStepUnit, timeStepQuantity, nOutValues, valueArray, OutStartDate);
        }

        #endregion


        #region Public methods for DELETING time series

        public bool DeleteSeries(
                int connectionNumber, String tableName, Guid id)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            return ts.DeleteSeries(id);
        }

        public bool DeleteMatchingSeries(
                int connectionNumber, String tableName, String whereClause)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            return ts.DeleteMatchingSeries(whereClause);
        }

        #endregion


        #region Public methods for XML import
        public int XmlImportWithList(int connectionNumber, String tableName, String xmlFileName,
                        List<TSImport> tsImportList)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TSXml tsXml = new TSXml(connx, tableName);

            return tsXml.ReadAndStore(xmlFileName, tsImportList);
        }
        public int XmlImport(int connectionNumber, String tableName, String xmlFileName)
        {
            return XmlImportWithList(connectionNumber, tableName, xmlFileName, new List<TSImport>());
        }
        #endregion

    }
}
