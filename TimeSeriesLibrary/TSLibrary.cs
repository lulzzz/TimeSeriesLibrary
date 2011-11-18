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
    /// This class contains the callable functions of the library for .NET code.
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
        /// is added to a list and assigned a serial number within the list.  The method returns
        /// the serial number of the new connection.
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

        /// <summary>
        /// This method reads the time series matching the given GUID, using the given
        /// database connection number and database table name, and stores the values into
        /// the given List of double-precision floats.  The method starts populating the
        /// list at the given start date, filling in no more than the number of values
        /// that are requested.  An exception is thrown if the time series is not found
        /// to have regular time steps.
        /// 
        /// This method is designed to be used from managed code such as C#.  This method
        /// does not return values for the dates of each time step.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to read the time series</param>
        /// <param name="tableName">The name of the database table that contains the time series</param>
        /// <param name="id">GUID value identifying the time series to read</param>
        /// <param name="nReqValues">The maximum number of values that the method will fill into the list</param>
        /// <param name="valueList">The List that the method will fill</param>
        /// <param name="reqStartDate">The earliest date that the method will enter into the list</param>
        /// <returns>The number of values that the method added to the list</returns>
        public int ReadValuesRegular(
                int connectionNumber, String tableName, Guid id,
                int nReqValues, ref List<double> valueList, DateTime reqStartDate, DateTime reqEndDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            double[] valueArray = new double[nReqValues];
            int ret = ts.ReadValuesRegular(id, nReqValues, valueArray, reqStartDate, reqEndDate);
            valueList = valueArray.ToList<double>();
            return ret;
        }

        /// <summary>
        /// This method reads the time series matching the given GUID, using the given
        /// database connection number and database table name, and stores the values into
        /// the given list of TimeSeriesValue structs (date/value pairs).  The method will 
        /// read regular or irregular time series.
        /// 
        /// This method is designed to be used from managed code such as C#.  This
        /// method makes the list as long as is needed to store every time step value that
        /// is stored in the database.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to read the time series</param>
        /// <param name="tableName">The name of the database table that contains the time series</param>
        /// <param name="id">GUID value identifying the time series to read</param>
        /// <param name="dateValueList">The list that the method will fill</param>
        /// <returns>The number of values that the method added to the list</returns>
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

        /// <summary>
        /// This method reads the time series matching the given GUID, using the given
        /// database connection number and database table name, and stores the values into
        /// the given list of TimeSeriesValue structs (date/value pairs).  The method starts populating the
        /// array at the given start date, filling in no more than the number of values
        /// that are requested, and will not add any values that come after the given end date.
        /// The method will read regular or irregular time series.
        /// 
        /// This method is designed to be used from managed code such as C#.  This
        /// method will not add any values to the list that are earlier than the given start date,
        /// later than the given end date, or that exceed the given list length.  The given
        /// list is instantiated by the method.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to read the time series</param>
        /// <param name="tableName">The name of the database table that contains the time series</param>
        /// <param name="id">GUID value identifying the time series to read</param>
        /// <param name="nReqValues">The maximum number of values that the method will fill into the array.
        /// If zero is given, then no maximum number of values is applied.</param>
        /// <param name="dateValueList">The list that the method will fill</param>
        /// <param name="reqStartDate">The earliest date that the method will enter into the array</param>
        /// <param name="reqEndDate">The latest date that the method will enter into the array</param>
        /// <returns>The number of values that the method added to the list</returns>
        // usage: for GUI to retrieve a time series, with date and list-length limits.
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
                nValuesRead = ts.ReadValuesRegular(id, nReqValues, valueArray, reqStartDate, reqEndDate);
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
        // TODO: Create methods WriteValues (take list of date/value pairs)

        /// <summary>
        /// This method saves the given time series list as a new database record, using the given
        /// database connection number and database table name.  The method only writes regular
        /// time series.  The given list is expressed only as values without explicit dates, so
        /// the method requires that the dates be defined by a given time step unit, quanitity
        /// of units per time step, the number of time steps in the array, and the date of the first
        /// time step.
        /// 
        /// This method is designed to be used from managed code such as C#.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write the time series</param>
        /// <param name="tableName">The name of the database table that time series will be written to</param>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, or Year</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="nOutValues">The number of values in the list to be written to the database</param>
        /// <param name="valueArray">list of time series values to be written to database</param>
        /// <param name="outStartDate">date of the first time step in the series</param>
        /// <returns>GUID value identifying the database record that was created</returns>
        public Guid WriteValuesRegular(
                    int connectionNumber, String tableName,
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, List<double> valueList, DateTime outStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            return ts.WriteValuesRegular(timeStepUnit, timeStepQuantity, nOutValues, valueList.ToArray<double>(), outStartDate);
        }
        
        /// <summary>
        /// This method saves the given time series as a new database record, using the given
        /// database connection number and database table name.  The given time series can be
        /// either regular or irregular.  The caller must pass in the parameters for the time step
        /// unit type, and the number of units per time step.  If the series is irregular, then
        /// the parameter for the number of units per time step is ignored.  The time series is
        /// given as a list of date/value pairs, so it determines all date parameters from the
        /// list itself.
        /// 
        /// This method is designed to be used from managed code such as C#.  The method can write
        /// either regular or irregular time series.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write the time series</param>
        /// <param name="tableName">The name of the database table that time series will be written to</param>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.  If the timeStepUnit is Irregular, then
        /// the method will ignore the timeStepQuantity value.</param>
        /// <param name="dateValueArray">the list of time series date/value pairs to be written to database</param>
        /// <returns>GUID value identifying the database record that was created</returns>
        public Guid WriteValues(
                    int connectionNumber, String tableName,
                    short timeStepUnit, short timeStepQuantity,
                    List<TimeSeriesValue> dateValueList)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            // The TS object's methods will require certain parameter values which we can
            // determine from the list of date/value pairs.
            int nOutValues = dateValueList.Count;
            DateTime outStartDate = dateValueList[0].Date;

            if ((TSDateCalculator.TimeStepUnitCode)timeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                return ts.WriteValuesIrregular(nOutValues, dateValueList.ToArray());
            }
            else
            {
                double[] valueArray = dateValueList.Select(dv => dv.Value).ToArray();
                return ts.WriteValuesRegular(timeStepUnit, timeStepQuantity, nOutValues, valueArray, outStartDate);
            }
        }

        #endregion


        #region Public methods for DELETING time series

        /// <summary>
        /// This method deletes a record for a single time series from the database, using the
        /// given database connection number and database table name.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to access the time series</param>
        /// <param name="tableName">The name of the database table that time series will be deleted from</param>
        /// <param name="id">The GUID identifying the record to delete</param>
        /// <returns>true if a record was deleted, false if no records were deleted</returns>
        public bool DeleteSeries(
                int connectionNumber, String tableName, Guid id)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            return ts.DeleteSeries(id);
        }

        /// <summary>
        /// This method deletes any records from the table which match the given WHERE
        /// clause of a SQL command, using the given database connection number
        /// and database table name.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to access the time series</param>
        /// <param name="tableName">The name of the database table that time series will be deleted from</param>
        /// <param name="whereClause">The WHERE clause of a SQL command, not including the word WHERE.
        /// For example, to delete delete all records where Id > 55, use the text "Id > 55".</param>
        /// <returns>true if one or more records were deleted, false if no records were deleted</returns>
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

        /// <summary>
        /// This method reads the given XML file and stores any time series that are defined in the
        /// XML file to the database using the given database connection number and database table name.
        /// For each time series that the method adds to the database, it adds a TSImport object to the
        /// given List of TSImport objects.  Each TSImport object records fields that were read from the
        /// XML file, but which TSLibrary does not process.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write to the database</param>
        /// <param name="tableName">The name of the database table that time series will be written to</param>
        /// <param name="xmlFileName">The file name (with path) of an XML file that defines one or more time series to import</param>
        /// <param name="tsImportList">A List of TSImport objects that the method adds to--one item for each time series
        /// that is saved to the database.  The List must already be instantiated before calling this method.
        /// The method does not change any items that are already in the List.</param>
        /// <returns>The number of time series records that were successfully stored</returns>
        public int XmlImportWithList(int connectionNumber, String tableName, String xmlFileName,
                        List<TSImport> tsImportList)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TSXml object with SqlConnection object and table name
            TSXml tsXml = new TSXml(connx, tableName);

            return tsXml.ReadAndStore(xmlFileName, tsImportList);
        }
        /// <summary>
        /// This method reads the given XML file and stores any time series that are defined in the
        /// XML file to the database using the given database connection number and database table name.
        /// The method does not process a List of TSImport objects.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write to the database</param>
        /// <param name="tableName">The name of the database table that time series will be written to</param>
        /// <param name="xmlFileName">The file name (with path) of an XML file that defines one or more time series to import</param>
        /// <returns>The number of time series records that were successfully stored</returns>
        public int XmlImport(int connectionNumber, String tableName, String xmlFileName)
        {
            // Simply let the sister method do all the processing,
            // but pass it a local List<TSImport> instance that won't be saved.
            return XmlImportWithList(connectionNumber, tableName, xmlFileName, new List<TSImport>());
        }
        #endregion


        #region Public methods for computing date values
        DateTime IncrementDate(DateTime startDate, TSDateCalculator.TimeStepUnitCode unit,
                    short stepSize, int numSteps)
        {
            return TSDateCalculator.IncrementDate(startDate, unit, stepSize, numSteps);
        }
        public void FillDateArray(
                    TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    int nReqValues, DateTime[] dateArray, DateTime reqStartDate)
        {
            TSDateCalculator.FillDateArray(timeStepUnit, timeStepQuantity, 
                                nReqValues, dateArray, reqStartDate);
        }
        public void FillSeriesDateArray(
                    int connectionNumber, String tableName, Guid id,
                    int nReqValues, DateTime[] dateArray, DateTime reqStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            ts.FillDateArray(id, nReqValues, dateArray, reqStartDate);
        }
        public int CountTimeSteps(DateTime startDate, DateTime endDate,
            short unit, short stepSize)
        {
            return TSDateCalculator.CountSteps(startDate, endDate, (TSDateCalculator.TimeStepUnitCode)unit, stepSize);
        }
        #endregion



    }
}
