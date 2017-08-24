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
    /// This class is used to do operations on one time series at a time.  It is not designed to hold
    /// information about a time series from one function call to another.
    /// </summary>
    public class TS
    {
        #region Fields
        /// <summary>
        /// Container of the SqlConnection objects used by this TS object
        /// </summary>
        private TSConnection TSConnection;
        /// <summary>
        /// name of the database table that stores parameters of this time series
        /// </summary>
        private String ParametersTableName;
        /// <summary>
        /// name of the database table that stores the BLOB for a single trace of this time series
        /// </summary>
        private String TraceTableName;
        /// <summary>
        /// This value is true if the meta parameters have been read from the database.
        /// </summary>
        public Boolean IsInitialized;

        /// <summary>
        /// unique identifier for the database record
        /// </summary>
        public int Id;
        /// <summary>
        /// object that contains the meta-parameter values that TimeSeriesLibrary must maintain alongside the BLOB
        /// </summary>
        public TSParameters TSParameters = new TSParameters();
        /// <summary>
        /// Checksum computed from the BLOB and meta-parameters when the timeseries is saved to database.
        /// </summary>
        public Byte[] Checksum;
        #endregion

        #region Properties linked to TSParameters field
        /// <summary>
        /// code for the units that measure the regular time step (e.g. hour, day, month)
        /// </summary>
        // This property simply refers to a field of the TSParameters object
        public TSDateCalculator.TimeStepUnitCode TimeStepUnit
        {
            get { return TSParameters.TimeStepUnit; }
            set { TSParameters.TimeStepUnit = value; }
        }
        /// <summary>
        /// number of units per time step (e.g. Quantity=6 for 6-hour time steps)
        /// </summary>
        // This property simply refers to a field of the TSParameters object
        public short TimeStepQuantity
        {
            get { return TSParameters.TimeStepQuantity; }
            set { TSParameters.TimeStepQuantity = value; }
        }
        /// <summary>
        /// Date of the first time step stored in the database
        /// </summary>
        // This property simply refers to a field of the TSParameters object
        public DateTime BlobStartDate
        {
            get { return TSParameters.BlobStartDate; }
            set { TSParameters.BlobStartDate = value; }
        }
        /// <summary>
        /// The compression code that indicates what compression algorithm is used to compress the BLOB
        /// </summary>
        // This property simply refers to a field of the TSParameters object
        public int CompressionCode
        {
            get { return TSParameters.CompressionCode; }
            set { TSParameters.CompressionCode = value; }
        }
        #endregion


        #region Class Constructor
        /// <summary>
        /// Class constructor that should be used if the TS object will read or write to database
        /// </summary>
        /// <param name="connx">SqlConnection object that this object will use</param>
        /// <param name="tsConnection">Object that manages connection objects for TimeSeriesLibrary</param>
        /// <param name="paramTableName">Name of the table in the database that stores this object's record</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        public TS(TSConnection tsConnection, String paramTableName, String traceTableName)
        {
            // Store the method parameters in class fields
            TSConnection = tsConnection;
            ParametersTableName = paramTableName;
            TraceTableName = traceTableName;
            // Mark this time series as uninitialized
            // (because the meta parameters have not yet been read from the database)
            IsInitialized = false;
        }
        /// <summary>
        /// Class constructor that should be used if the TS object will not read or write to database
        /// </summary>
        public TS()
        {
            // These field values reflect that the database is not accessed
            ParametersTableName = null;
            TraceTableName = null;
            // Mark this time series as uninitialized
            // (because the meta parameters have not yet been read from the database)
            IsInitialized = false;
        }
        #endregion


        #region Initialize() Method
        /// <summary>
        /// Method reads the database record from the parameters table to get the parameters 
        /// of the time series.  It does not read the time series data values themselves.
        /// Therefore, this method can be called efficiently both by functions that will read 
        /// the time series or write the time series, whether or not those functions will need 
        /// the entire set of data values.
        /// </summary>
        /// <param name="id">ID of the time series record</param>
        /// <returns>true if a record was found, false if no record was found</returns>
        public Boolean Initialize(int id)
        {
            // store the method's input parameters
            Id = id;
            // Define the SQL query

            String comm = String.Format("select TimeStepUnit,TimeStepQuantity," +
                                        "StartDate,CompressionCode " +
                                        "from {0} where Id='{1}'", ParametersTableName, Id);
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, TSConnection.Connection))
            {
                DataTable dTable = new DataTable();
                // Execute the query to fill the DataTable object
                try
                {
                    adp.Fill(dTable);
                }
                catch(Exception e)
                {   // The query failed.
                    throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                    "Table '" + ParametersTableName + "' could not be opened using query:\n\n" + comm, e);
                }
                // There should be at least 1 row in the DataTable object
                if (dTable.Rows.Count < 1)
                {
                    throw new TSLibraryException(ErrCode.Enum.Record_Not_Found_Table,
                                "Found zero records using query:\n\n." + comm);
                }
                // Assign properties from table to this object
                TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)dTable.Rows[0].Field<int>("TimeStepUnit");
                TimeStepQuantity = (short)dTable.Rows[0].Field<int>("TimeStepQuantity");
                BlobStartDate = dTable.Rows[0].Field<DateTime>("StartDate");
                CompressionCode = dTable.Rows[0].Field<int>("CompressionCode");
                dTable.Dispose();
            }
            IsInitialized = true;
            return true;
        }
        #endregion

        #region BuildStringForEmptyParametersDataTable()
        /// <summary>
        /// Method returns a string for querying the database table and returning an empty result set.
        /// The subsequent query can be used to create an empty DataTable object, with the necessary
        /// columns defined.  Because the query names all required fields of the database table, the
        /// subsequent query will raise an exception if any fields are missing.
        /// </summary>
        /// <returns>The SQL command that returns an empty resultset</returns>
        private String BuildStringForEmptyParametersDataTable()
        {
            // note: by including 'where 1=0', we ensure that an empty resultset will be returned.
            return String.Format("select" +
                                 "  Id, TimeStepUnit, TimeStepQuantity, TimeStepCount, StartDate, EndDate, Checksum" +
                                 "  from {0} where 1=0", ParametersTableName);
        }
        #endregion

        #region GetNewSqlCommand method
        /// <summary>
        /// This method returns a new SqlCommand object that is initialized with the given text.
        /// </summary>
        /// <param name="text">the initial text of the SQL command</param>
        protected SqlCommand GetNewSqlCommand(String text)
        {
            // CommandTimeout value of 600 was chosen to compensate for problems with very large number
            // of records in OASIS's OutputTimeSeries and OutputTimeSeriesTraces table

            return new SqlCommand(text, TSConnection.Connection) { CommandTimeout = 600 };
        }
        #endregion

        #region ReadValuesRegular() Method
        /// <summary>
        /// Method reads the time series matching the given ID and trace number, storing the values into
        /// the given array of double-precision floats.  The method starts populating the
        /// array at the given start date, filling in no more than the number of values
        /// that are requested.
        /// </summary>
        /// <param name="id">ID of the time series</param>
        /// <param name="traceNumber">number of the trace to read</param>
        /// <param name="nReqValues">number of values requested to read</param>
        /// <param name="valueArray">array requested to fill with values</param>
        /// <param name="reqStartDate">The earliest date in the time series that will be written to the array of values</param>
        /// <param name="reqEndDate">The latest date in the time series that will be written to the array of values</param>
        /// <returns>The number of values actually filled into the array</returns>
        public unsafe int ReadValuesRegular(int id, int traceNumber,
            int nReqValues, double[] valueArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            // Initialize class fields other than the BLOB of data values
            if (!IsInitialized) Initialize(id);

            // This method can only process regular-time-step series
            if (TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                throw new TSLibraryException(ErrCode.Enum.Record_Not_Regular,
                                String.Format("The method can only process regular time series, but" +
                                "the record with Id {0} is irregular.", id));
            }
            // If the end date requested by the caller is such that the stored time series
            // does not overlap, then we don't need to go any further.
            if (reqEndDate < BlobStartDate)
                return 0;

            // byte array (the BLOB) that will be read from the database.
            Byte[] blobData = null;
            // method ReadValues reads data from the database into the byte array
            int timeStepCount = ReadValues(id, traceNumber, ref blobData);
            // Convert the BLOB into an array of double values (valueArray)
            return TSBlobCoder.ConvertBlobToArrayRegular(TimeStepUnit, TimeStepQuantity,
                                timeStepCount, BlobStartDate, true,
                                nReqValues, reqStartDate, reqEndDate, 
                                blobData, valueArray,
                                CompressionCode);
        }
        #endregion

        #region ReadValuesIrregular() Method
        /// <summary>
        /// Method reads the irregular time series matching the given ID and trace number, storing the dates and
        /// values into the given array of TSDateValueStruct (a struct containing the date/value pair).
        /// The method starts populating the array at the given start date, filling in no more than
        /// the number of values, that are requested, and not reading past the given end date
        /// </summary>
        /// <param name="id">ID id of the time series</param>
        /// <param name="traceNumber">number of the trace to read</param>
        /// <param name="nReqValues">number of values requested to read</param>
        /// <param name="dateValueArray">array requrested to fill with date/value pairs</param>
        /// <param name="reqStartDate">start date requested</param>
        /// <param name="reqEndDate">end date requested</param>
        /// <returns>The number of values actually filled into the array</returns>
        public unsafe int ReadValuesIrregular(int id, int traceNumber,
            int nReqValues, TSDateValueStruct[] dateValueArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            // Initialize class fields other than the BLOB of data values
            if (!IsInitialized) Initialize(id);

            // This method can only process irregular-time-step series
            if (TimeStepUnit != TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                throw new TSLibraryException(ErrCode.Enum.Record_Not_Irregular,
                                String.Format("The method can only process irregular time series, but" +
                                "the record with Id {0} is regular.", id));
            }
            // If the end date requested by the caller is such that the stored time series
            // does not overlap, then we don't need to go any further.
            if (reqEndDate < BlobStartDate)
                return 0;

            // byte array (the BLOB) that will be read from the database.
            Byte[] blobData = null;
            // method ReadValues reads data from the database into the byte array
            int timeStepCount = ReadValues(id, traceNumber, ref blobData);
            // convert the byte array into date/value pairs
            return TSBlobCoder.ConvertBlobToArrayIrregular(timeStepCount, true, nReqValues,
                            reqStartDate, reqEndDate,
                            blobData, dateValueArray, CompressionCode);

        }
        #endregion

        #region ReadValues() Method
        /// <summary>
        /// This method contains the operations to read the BLOB for a single trace 
        /// from the database table.
        /// </summary>
        /// <param name="id">ID identifying the time series record to read</param>
        /// <param name="traceNumber">number of the trace to read</param>
        /// <param name="blobData">the byte array that is populated from the database BLOB</param>
        private unsafe int ReadValues(int id, int traceNumber, ref byte[] blobData)
        {
            // SQL statement that will only give us the BLOB of data values
            String comm = String.Format("select ValueBlob,TimeStepCount "
                            + "from {0} where TimeSeries_Id='{1}' and TraceNumber='{2}' ",
                                    TraceTableName, Id, traceNumber);
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, TSConnection.Connection))
            using (DataTable dTable = new DataTable())
            {
                // Execute the query to fill the DataTable object
                try
                {
                    adp.Fill(dTable);
                }
                catch (Exception e)
                {   // The query failed
                    throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                    "Table '" + TraceTableName + "' could not be opened using query:\n\n" + comm, e);
                }
                // There should be at least 1 row in the table
                if (dTable.Rows.Count < 1)
                {
                    throw new TSLibraryException(ErrCode.Enum.Record_Not_Found_Table,
                                    "Found zero records using query:\n\n." + comm);
                }

                // DataRow object represents the current row of the DataTable object, which in turn is our result set
                DataRow currentRow = dTable.Rows[0];
                // Cast the BLOB as an array of bytes
                blobData = (Byte[])currentRow["ValueBlob"];
                return (int)currentRow["TimeStepCount"];
            }
        }
        #endregion

        #region WriteParameters methods
        /// <summary>
        /// This method writes a new record to the database table for a regular time series.
        /// The method will record extra parameters (other than those that are saved
        /// as class-level properties of this object) into the database record using the strings
        /// in the method parameters extraParamNames and extraParamValues.  This method does not
        /// make any changes to the trace table.
        /// </summary>
        /// <param name="doWriteToDB">true if the method should actually save the timeseries to the database</param>
        /// <param name="tsImport">TSImport object into which the method will record values that it has computed.
        /// If this parameter is null, then the method will skip the recording of such paramters to an object.</param>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, or Year</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="timeStepCount">The number of time steps in the time series</param>
        /// <param name="outStartDate">date of the first time step in the series</param>
        /// <param name="extraParamNames">A list of field names that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamValues.</param>
        /// <param name="extraParamValues">A list of field values that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamNames.</param>
        /// <returns>the primary key Id value of the new record that was created</returns>
        public unsafe int WriteParametersRegular(
                    bool doWriteToDB, TSImport tsImport,
                    short timeStepUnit, short timeStepQuantity,
                    int timeStepCount, DateTime outStartDate,
                    String extraParamNames, String extraParamValues)
        {
            ErrorCheckWriteValues(doWriteToDB, tsImport);
            // The method's parameters are used to compute the meta-parameters of this time series
            TSParameters.SetParametersRegular(
                    (TSDateCalculator.TimeStepUnitCode)timeStepUnit, timeStepQuantity,
                    timeStepCount, outStartDate,
                    // new time series are always compressed by the current compression technique            
                    TSBlobCoder.currentCompressionCode);
            IsInitialized = true;
            // Compute the Checksum for this time series ensemble.  Because this is a newly
            // written series, there are not yet any traces to incorporate into the checksum
            // (presumably those will be added later).
            Checksum = TSBlobCoder.ComputeChecksum(TSParameters, new List<ITimeSeriesTrace>());
            // WriteValues method will handle all of the database interaction
            if (doWriteToDB)
                WriteParameters(extraParamNames, extraParamValues);

            return Id;
        }
        /// <summary>
        /// This method writes a new record to the database table for an irregular time series.
        /// The method will record extra parameters (other than those that are saved
        /// as class-level properties of this object) into the database record using the strings
        /// in the method parameters extraParamNames and extraParamValues.  This method does not
        /// make any changes to the trace table.
        /// </summary>
        /// <param name="doWriteToDB">true if the method should actually save the timeseries to the database</param>
        /// <param name="tsImport">TSImport object into which the method will record values that it has computed.
        /// If this parameter is null, then the method will skip the recording of such paramters to an object.</param>
        /// <param name="timeStepCount">The number of time steps in the time series</param>
        /// <param name="outStartDate">date of the first time step in the series</param>
        /// <param name="outEndDate">date of the last time step in the series</param>
        /// <param name="extraParamNames">A list of field names that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamValues.</param>
        /// <param name="extraParamValues">A list of field values that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamNames.</param>
        /// <returns>the primary key Id value of the new record that was created</returns>
        public unsafe int WriteParametersIrregular(
                    bool doWriteToDB, TSImport tsImport,
                    int timeStepCount, DateTime outStartDate, DateTime outEndDate,
                    String extraParamNames, String extraParamValues)
        {
            ErrorCheckWriteValues(doWriteToDB, tsImport);
            // The method's parameters are used to compute the meta-parameters of this time series
            TSParameters.SetParametersIrregular(timeStepCount, outStartDate, outEndDate, 
                            // new time series are always compressed by the current compression technique            
                            TSBlobCoder.currentCompressionCode);
            IsInitialized = true;
            // Compute the Checksum for this time series ensemble.  Because this is a newly
            // written series, there are not yet any traces to incorporate into the checksum
            // (presumably those will be added later).
            Checksum = TSBlobCoder.ComputeChecksum(TSParameters, new List<ITimeSeriesTrace>());
            // WriteValues method will handle all of the database interaction
            if (doWriteToDB)
                WriteParameters(extraParamNames, extraParamValues);

            return Id;
        }
        /// <summary>
        /// This method writes a new record to the database table, using the parameters of the
        /// time series that have been saved as properties of this object prior to calling the
        /// method.  The method will record extra parameters (other than those that are saved
        /// as class-level properties of this object) into the database record using the strings
        /// in the method parameters extraParamNames and extraParamValues.  This method does not
        /// make any changes to the trace table.
        /// </summary>
        /// <param name="extraParamNames">A list of field names that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamValues.</param>
        /// <param name="extraParamValues">A list of field values that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamNames.</param>
        /// <returns>the primary key Id value of the new record that was created</returns>
        private unsafe int WriteParameters(String extraParamNames, String extraParamValues)
        {
            // Initialize the string of column names and column values with the first pair.
            String colString = "TimeStepUnit";
            String valString = ((short)TimeStepUnit).ToString();
            // Add the rest of the column names and column values such that each is preceded by a comma
            AppendStringPair(ref colString, ref valString, "TimeStepQuantity", TimeStepQuantity.ToString());
            AppendStringPair(ref colString, ref valString, "StartDate", "'" + BlobStartDate.ToString() + "'");
            AppendStringPair(ref colString, ref valString, "Checksum", ByteArrayToString(Checksum));
            AppendStringPair(ref colString, ref valString, "CompressionCode", CompressionCode.ToString());
            // Now our strings contain all of the columns that TimeSeriesLibrary is responsible for
            // handling.  The caller may pass in additional column names and values that we now add
            // to our strings.
            AppendStringPair(ref colString, ref valString, extraParamNames, extraParamValues);
            // Create a SQL INSERT command.  The "select SCOPE_IDENTITY" at the end of the command
            // ensures that the command will return the ID of the new record.
            String comm = String.Format("insert into {0} ({1}) values ({2}); select SCOPE_IDENTITY()",
                            ParametersTableName, colString, valString);
            // SqlCommand object can execute the query for us
            using (SqlCommand sqlCommand = GetNewSqlCommand(comm))
            {
                try
                {
                    Id = (int)(decimal)sqlCommand.ExecuteScalar();
                }
                catch (Exception e)
                {   // The query failed
                    throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                    "Could not execute query:\n\n." + comm, e);
                }
            }
            return Id;
        }
        /// <summary>
        /// This method contains error checks on the input parameters of this class's methods
        /// WriteValuesRegular() and WriteValuesIrregular(), so both of those methods call
        /// this private method before they undertake any other operations.
        /// </summary>
        /// <param name="doWriteToDB">true if the 'Write' method should actually save the timeseries to the database</param>
        /// <param name="tsImport">TSImport object into which the method will record values that it has computed.
        /// If this parameter is null, then the method will skip the recording of such paramters to an object.</param>
        private void ErrorCheckWriteValues(bool doWriteToDB, TSImport tsImport)
        {
            // TODO: create an error code and throw exception
            if (doWriteToDB && (ParametersTableName == null 
                                    || TSConnection.Connection == null 
                                    || TraceTableName == null))
            {
            }
        }
        #endregion

        #region WriteTrace methods
        /// <summary>
        /// This method writes a new record to the trace table for a regular time step series.
        /// The method converts the given valueArray into the BLOB that is actually stored in
        /// the table.  The method computes the checksum of the trace, and updates the checksum
        /// in the parameters table to reflect the fact that a new trace has been added to the ensemble.
        /// </summary>
        /// <param name="id">identifying primary key value of the the parameters table for the record 
        /// that this trace belongs to</param>
        /// <param name="doWriteToDB">true if the method should actually save the timeseries to the database</param>
        /// <param name="tsImport">TSImport object into which the method will record values that it has computed.
        /// If this parameter is null, then the method will skip the recording of such paramters to an object.</param>
        /// <param name="traceNumber">number of the trace to write</param>
        /// <param name="valueArray">The array of values to be written to the database</param>
        public unsafe void WriteTraceRegular(int id,
                    bool doWriteToDB, TSImport tsImport,
                    int traceNumber,
                    double[] valueArray)
        {
            // Initialize class fields other than the BLOB of data values
            if (!IsInitialized) Initialize(id);

            // This method can only process regular-time-step series
            if (TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                throw new TSLibraryException(ErrCode.Enum.Record_Not_Regular,
                                String.Format("The method can only process regular time series, but" +
                                "the record with Id {0} is irregular.", id));
            }
            // Create a trace object
            int timeStepCount = valueArray.Count();
            ITimeSeriesTrace traceObject = new TSTrace
            {
                TraceNumber = traceNumber,
                TimeStepCount = timeStepCount,
                EndDate = TSDateCalculator.IncrementDate(BlobStartDate, 
                                TimeStepUnit, TimeStepQuantity, timeStepCount - 1)
            };
            if (tsImport != null)
                tsImport.TraceList.Add(traceObject);
            // Convert the array of double values into a byte array...a BLOB
            TSBlobCoder.ConvertArrayToBlobRegular(valueArray, CompressionCode, traceObject);

            // Write a new record to the trace table
            if(doWriteToDB)
                WriteTrace(traceObject);
            // update the checksum in the parameters table
            UpdateParametersChecksum(doWriteToDB, tsImport);
        }
        /// <summary>
        /// This method writes a new record to the trace table for an irregular time step series.
        /// The method converts the given dateValueArray into the BLOB that is actually stored in
        /// the table.  The method computes the checksum of the trace, and updates the checksum
        /// in the parameters table to reflect the fact that a new trace has been added to the ensemble.
        /// </summary>
        /// <param name="id">identifying primary key value of the the parameters table for the record 
        /// that this trace belongs to</param>
        /// <param name="doWriteToDB">true if the method should actually save the timeseries to the database</param>
        /// <param name="tsImport">TSImport object into which the method will record values that it has computed.
        /// If this parameter is null, then the method will skip the recording of such paramters to an object.</param>
        /// <param name="traceNumber">number of the trace to write</param>
        /// <param name="dateValueArray">The array of values to be written to the database</param>
        public unsafe void WriteTraceIrregular(int id,
                    bool doWriteToDB, TSImport tsImport,
                    int traceNumber,
                    TSDateValueStruct[] dateValueArray)
        {
            // Initialize class fields other than the BLOB of data values
            if (!IsInitialized) Initialize(id);

            // This method can only process irregular-time-step series
            if (TimeStepUnit != TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                throw new TSLibraryException(ErrCode.Enum.Record_Not_Irregular,
                                String.Format("The method can only process irregular time series, but" +
                                "the record with Id {0} is regular.", id));
            }
            // Create a trace object
            ITimeSeriesTrace traceObject = new TSTrace
            {
                TraceNumber = traceNumber,
                TimeStepCount = dateValueArray.Count(),
                EndDate = dateValueArray.Any() ? dateValueArray.Last().Date : BlobStartDate
            };
            if (tsImport != null)
                tsImport.TraceList.Add(traceObject);
            // Convert the array of double values into a byte array...a BLOB
            TSBlobCoder.ConvertArrayToBlobIrregular(dateValueArray, CompressionCode, traceObject);

            // Write a new record to the trace table
            if (doWriteToDB)
                WriteTrace(traceObject);
            // update the checksum in the parameters table
            UpdateParametersChecksum(doWriteToDB, tsImport);
        }
        /// <summary>
        /// This method writes the data contained in the traceObject parameter into a single
        /// record of the trace table.  The properties of the traceObject, including the
        /// checksum and BLOB, should be populated before calling this method.
        /// </summary>
        /// <param name="traceObject"></param>
        private unsafe void WriteTrace(ITimeSeriesTrace traceObject)
        {
            //String keyString = "WriTrc";

            DataTable dataTable;
            // Attempt to get the existing DataTable object from the collection that is kept by
            // the TSConnection object.  If this fails, then we'll create a new DataTable.
            if (TSConnection.BulkCopyDataTables.TryGetValue(TraceTableName, out dataTable) == false)
            {
                // Create the DataTable object and add columns that match the columns
                // of the database table.
                dataTable = new DataTable();
                dataTable.Columns.Add("TimeSeries_Id", typeof(int));
                dataTable.Columns.Add("TraceNumber", typeof(int));
                dataTable.Columns.Add("TimeStepCount", typeof(int));
                dataTable.Columns.Add("EndDate", typeof(DateTime));
                dataTable.Columns.Add("ValueBlob", typeof(byte[]));
                dataTable.Columns.Add("Checksum", typeof(byte[]));
                // Add the DataTable to a collection that is kept in the TSConnection object.
                TSConnection.BulkCopyDataTables.Add(TraceTableName, dataTable);
            }
            // Add the trace as a new DataRow object in the DataTable.  In the 'Add' method,
            // the parameters must be entered in the same order as the columns were created
            // in the code immediately above.
            dataTable.Rows.Add(Id, traceObject.TraceNumber,
                                   traceObject.TimeStepCount, traceObject.EndDate,
                                   traceObject.ValueBlob, traceObject.Checksum);

            //SqlCommand sqlCommand;
            //// Find a SqlCommand that was previously cached for this purpose
            //var commandContainer = TSConnection.PreparedSqlCommands
            //        .Where(o => o.TableName == TraceTableName 
            //                    && o.KeyString == keyString).FirstOrDefault();
            //// If no SqlCommand has been created yet
            //if (commandContainer == null)
            //    // Then create one.
            //    sqlCommand = CreateWriteTraceSqlCommand(keyString);
            //else
            //    sqlCommand = commandContainer.SqlCommand;
            //
            //// Supply parameter values for the SqlCommand
            //sqlCommand.Parameters["@TimeSeries_Id"].Value = Id;
            //sqlCommand.Parameters["@TraceNumber"].Value = traceObject.TraceNumber;
            //sqlCommand.Parameters["@TimeStepCount"].Value = traceObject.TimeStepCount;
            //sqlCommand.Parameters["@EndDate"].Value = traceObject.EndDate;
            //sqlCommand.Parameters["@ValueBlob"].Value = traceObject.ValueBlob;
            //sqlCommand.Parameters["@Checksum"].Value = traceObject.Checksum;
            //// Execute the SQL command
            //try
            //{
            //    sqlCommand.ExecuteNonQuery();
            //}
            //catch (Exception e)
            //{   // The query failed
            //    throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
            //                    "Table '" + TraceTableName + "' could not be opened using query:\n\n"
            //                    + sqlCommand.CommandText, e);
            //}
        }
        /// <summary>
        /// This method updates the value in the Checksum field of the parameters table.
        /// It does not modify any other fields.
        /// </summary>
        /// <param name="doWriteToDB">true if the method should actually
        /// save the timeseries to the database</param>
        /// <param name="tsImport">TSImport object into which the method will record values
        /// that it has computed. If this parameter is null, then the method will skip the recording
        /// of such paramters to an object.</param>
        private void UpdateParametersChecksum(bool doWriteToDB, TSImport tsImport)
        {
            // This List will contain one item for each trace for this time series.  The primary
            // purpose of the list is to store the checksum for each trace, since the checksum
            // of the timeseries is computed from the list of checksums from each of its traces.
            List<ITimeSeriesTrace> traceObjects;
            if (tsImport != null)
                traceObjects = tsImport.TraceList;
            else
                traceObjects = new List<ITimeSeriesTrace>();

            if (doWriteToDB)
            {
                //
                // Query the Trace table to get all traces for this time series
                String keyString = "SelTrcChk";
                SqlCommand sqlCommand;
                // Find a SqlCommand that was previously cached for this purpose
                var commandContainer = TSConnection.PreparedSqlCommands
                        .Where(o => o.TableName == TraceTableName
                                    && o.KeyString == keyString).FirstOrDefault();
                // If no SqlCommand has been created yet
                if (commandContainer == null)
                    // Then create one.
                    sqlCommand = CreateSelectTraceChecksumSqlCommand(keyString);
                else
                    sqlCommand = commandContainer.SqlCommand;
                // Supply parameter values for the SqlCommand
                sqlCommand.Parameters["@TimeSeries_Id"].Value = Id;

                // SqlDataAdapter object will use the query to fill the DataTable
                using (SqlDataAdapter adp = new SqlDataAdapter(sqlCommand))
                // SqlCommandBuilder object must be instantiated in order for us to call
                // the Fill method of the SqlDataAdapter.  Interestingly, we only need to
                // instantiate this object--we don't need to use it in any other way.
                using (SqlCommandBuilder bld = new SqlCommandBuilder(adp))
                using (DataTable dTable = new DataTable())
                {
                    // Execute the query to fill the DataTable object
                    try
                    {
                        adp.Fill(dTable);
                    }
                    catch (Exception e)
                    {   // The query failed
                        throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                        "Table '" + TraceTableName + "' could not be opened using query:\n\n"
                                        + sqlCommand.CommandText, e);
                    }
                    // Loop through all traces, and record the values that will be needed to compute the
                    // checksum of the ensemble time series.  Note that this does not include the BLOB 
                    // of the trace.
                    foreach (DataRow row in dTable.Rows)
                    {
                        ITimeSeriesTrace traceObject = new TSTrace();
                        traceObject.TraceNumber = (int)row["TraceNumber"];
                        traceObject.Checksum = (byte[])row["Checksum"];
                        traceObjects.Add(traceObject);
                    }
                }
            }
            // Compute the new checksum of the ensemble
            Checksum = TSBlobCoder.ComputeChecksum(TimeStepUnit, TimeStepQuantity,
                                     BlobStartDate, traceObjects);
            if (doWriteToDB)
            {
                // 
                // Write the new value to the parameters table
                String keyString = "UpdChk";
                SqlCommand sqlCommand;
                // Find a SqlCommand that was previously cached for this purpose
                var commandContainer = TSConnection.PreparedSqlCommands
                        .Where(o => o.TableName == TraceTableName
                                    && o.KeyString == keyString).FirstOrDefault();
                // If no SqlCommand has been created yet
                if (commandContainer == null)
                    // Then create one.
                    sqlCommand = CreateUpdateChecksumSqlCommand(keyString);
                else
                    sqlCommand = commandContainer.SqlCommand;
                // Supply parameter values for the SqlCommand
                sqlCommand.Parameters["@Id"].Value = Id;
                sqlCommand.Parameters["@Checksum"].Value = Checksum;
                sqlCommand.ExecuteNonQuery();
            }
        }
        /// <summary>
        /// Create a new SqlCommand object for the WriteTrace method, so that the
        /// SqlCommand can be cached and reused by the database system.
        /// </summary>
        private SqlCommand CreateWriteTraceSqlCommand(String keyString)
        {
            // Instantiate the SqlCommand object using a SQL INSERT command
            SqlCommand command = GetNewSqlCommand("INSERT INTO " + TraceTableName
                                + "(TimeSeries_Id, TraceNumber, TimeStepCount, EndDate, "
                                                    + "ValueBlob, Checksum) "
                                + "VALUES (@TimeSeries_Id, @TraceNumber, @TimeStepCount, @EndDate, "
                                                    + "@ValueBlob, @Checksum)");

            // Add parameters to the command
            command.Parameters.Add("@TimeSeries_Id", SqlDbType.Int);
            command.Parameters.Add("@TraceNumber", SqlDbType.Int);
            command.Parameters.Add("@TimeStepCount", SqlDbType.Int);
            command.Parameters.Add("@EndDate", SqlDbType.DateTime);
            command.Parameters.Add("@ValueBlob", SqlDbType.VarBinary, -1);
            command.Parameters.Add("@Checksum", SqlDbType.Binary, 16);

            // add the new command to a container that keeps the command object wrapped together
            // with identifying information.  Note that the container's constructor will call
            // the command's Prepare method.
            var container = new TSSqlCommandContainer(TraceTableName, keyString, command);
            // The TSConnectionManager object will keep this container in a collection
            TSConnection.PreparedSqlCommands.Add(container);

            return command;
        }
        /// <summary>
        /// Create a new SqlCommand object for the UpdateParametersChecksum method, so that the
        /// SqlCommand can be cached and reused by the database system.
        /// </summary>
        private SqlCommand CreateSelectTraceChecksumSqlCommand(String keyString)
        {
            // Instantiate the SqlCommand object using a SQL INSERT command
            SqlCommand command = GetNewSqlCommand("SELECT  TraceNumber, Checksum from "
                        + TraceTableName+" where TimeSeries_Id=@TimeSeries_Id order by TraceNumber");

            // Add parameters to the command
            command.Parameters.Add("@TimeSeries_Id", SqlDbType.Int);

            // add the new command to a container that keeps the command object wrapped together
            // with identifying information.  Note that the container's constructor will call
            // the command's Prepare method.
            var container = new TSSqlCommandContainer(TraceTableName, keyString, command);
            // The TSConnectionManager object will keep this container in a collection
            TSConnection.PreparedSqlCommands.Add(container);

            return command;
        }
        /// <summary>
        /// Create a new SqlCommand object for the UpdateParametersChecksum method, so that the
        /// SqlCommand can be cached and reused by the database system.
        /// </summary>
        private SqlCommand CreateUpdateChecksumSqlCommand(String keyString)
        {
            // Instantiate the SqlCommand object using a SQL INSERT command
            SqlCommand command = GetNewSqlCommand("UPDATE " + ParametersTableName
                                    + " SET Checksum=@Checksum WHERE Id=@Id");

            // Add parameters to the command
            command.Parameters.Add("@Checksum", SqlDbType.Binary, 16);
            command.Parameters.Add("@Id", SqlDbType.Int);

            // add the new command to a container that keeps the command object wrapped together
            // with identifying information.  Note that the container's constructor will call
            // the command's Prepare method.
            var container = new TSSqlCommandContainer(TraceTableName, keyString, command);
            // The TSConnectionManager object will keep this container in a collection
            TSConnection.PreparedSqlCommands.Add(container);

            return command;
        }
        #endregion

        #region DeleteSeries() Method
        /// <summary>
        /// Method deletes the single record from the table which matches the given ID
        /// </summary>
        /// <param name="id">The ID identifying the record to delete</param>
        /// <returns>true if a record was deleted, false if no records were deleted</returns>
        public Boolean DeleteSeries(int id)
        {
            Id = id; String comm; SqlCommand sqlCommand;

            //
            // First delete from the Trace Table

            // Simple SQL statement to delete the selected record
            comm = String.Format("delete from {0} where TimeSeries_Id='{1}' ",
                                    TraceTableName, Id);
            // SqlCommand object allows us to execute the command
            using (sqlCommand = GetNewSqlCommand(comm))
            {
                // This method executes the SQL command and returns the number of rows that were affected
                sqlCommand.ExecuteNonQuery();
            }

            //
            // Second delete from the Parameters Table

            // Simple SQL statement to delete the selected record
            comm = String.Format("delete from {0} where Id='{1}' ",
                                    ParametersTableName, Id);
            // SqlCommand object allows us to execute the command
            using (sqlCommand = GetNewSqlCommand(comm))
            {
                // This method executes the SQL command and returns the number of rows that were affected
                int numRowsAffected = sqlCommand.ExecuteNonQuery();
                // Return value reflects whether anything was actually deleted
                if (numRowsAffected > 0)
                    return true;
            }

            return false;
        } 
        #endregion

        #region DeleteMatchingSeries() Method
        /// <summary>
        /// Method deletes any records from the table which match the given WHERE
        /// clause of a SQL command.
        /// </summary>
        /// <param name="whereClause">The WHERE clause of a SQL command, not including the word WHERE.
        /// For example, to delete delete all records where Id > 55, use the text "Id > 55".</param>
        /// <returns>true if one or more records were deleted, false if no records were deleted</returns>
        public Boolean DeleteMatchingSeries(String whereClause)
        {
            String comm; SqlCommand sqlCommand;

            //
            // First delete from the Trace Table

            // Simple SQL statement to delete the selected record
            comm = String.Format("delete t from {0} t inner join {1} p on p.Id = t.TimeSeries_Id and {2} ",
                                    TraceTableName, ParametersTableName, whereClause);
            // SqlCommand object allows us to execute the command
            using (sqlCommand = GetNewSqlCommand(comm))
            {
                // This method executes the SQL command and returns the number of rows that were affected
                sqlCommand.ExecuteNonQuery();
            }

            //
            // Second delete from the Parameters Table

            // Simple SQL statement to delete the selected record
            comm = String.Format("delete from {0} where {1}",
                                    ParametersTableName, whereClause);
            // SqlCommand object allows us to execute the command
            using (sqlCommand = GetNewSqlCommand(comm))
            {
                // This method executes the SQL command and returns the number of rows that were affected
                int numRowsAffected = sqlCommand.ExecuteNonQuery();
                // Return value reflects whether anything was actually deleted
                if (numRowsAffected > 0)
                    return true;
            }
            return false;
        }
        #endregion

        #region FillDateArray() Method
        /// <summary>
        /// Method fills in the values of the given array of DateTime values with the dates for
        /// each time step in the time series that matches the given ID.  The array is filled
        /// starting at the given start date, or the start date of the database record, whichever
        /// is earliest.  The array is filled up the the given number of values.
        /// </summary>
        /// <param name="id">ID of the time series</param>
        /// <param name="nReqValues">number of values requested to be filled</param>
        /// <param name="dateArray">array requested to fill with values</param>
        /// <param name="reqStartDate">start date requested</param>
        public unsafe void FillDateArray(int id,
            int nReqValues, DateTime[] dateArray, DateTime reqStartDate)
        {
            // Initialize class fields
            if (!IsInitialized) Initialize(id);

            int i;
            DateTime revisedStartDate;
            // The time steps boundaries are defined by the database record, so
            // we can't rely on the reqStartDate to have the correct time step boundary.
            // Therefore, begin counting at the first date in the database record.
            if (reqStartDate > BlobStartDate)
            {
                i = TSDateCalculator.CountSteps(BlobStartDate, reqStartDate, TimeStepUnit, TimeStepQuantity);
                revisedStartDate = TSDateCalculator.IncrementDate(BlobStartDate, TimeStepUnit, TimeStepQuantity, i);
            }
            else
            {
                revisedStartDate = BlobStartDate;
            }

            TSDateCalculator.FillDateArray(TimeStepUnit, TimeStepQuantity,
                                nReqValues, dateArray, revisedStartDate);
        }
        #endregion

        #region ByteArrayToString Method
        private static String ByteArrayToString(byte[] ba)
        {
            StringBuilder hex = new StringBuilder(ba.Length * 2);
            foreach (byte b in ba)
                hex.AppendFormat("{0:x2}", b);
            return "0x" + hex.ToString();
        } 
        #endregion

        #region AppendStringPair Method
        private static void AppendStringPair(ref String string1, ref String string2, String append1, String append2)
        {
            string1 += ", " + append1;
            string2 += ", " + append2;
        }
        #endregion

    }
}
