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
        /// <summary>
        /// Object handles the connection to the database
        /// </summary>
        private SqlConnection Connx;
        /// <summary>
        /// name of the database table that stores this time series
        /// </summary>
        private String TableName;
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
        public TSParameters tsParameters = new TSParameters();
        /// <summary>
        /// MD5 Checksum computed from the BLOB and meta-parameters when the timeseries is saved to database.
        /// </summary>
        public Byte[] Checksum;


        #region Properties linked to tsParameters field
        /// <summary>
        /// code for the units that measure the regular time step (e.g. hour, day, month)
        /// </summary>
        // This property simply refers to a field of the TSParameters object
        public TSDateCalculator.TimeStepUnitCode TimeStepUnit
        {
            get { return tsParameters.TimeStepUnit; }
            set { tsParameters.TimeStepUnit = value; }
        }
        /// <summary>
        /// number of units per time step (e.g. Quantity=6 for 6-hour time steps)
        /// </summary>
        // This property simply refers to a field of the TSParameters object
        public short TimeStepQuantity
        {
            get { return tsParameters.TimeStepQuantity; }
            set { tsParameters.TimeStepQuantity = value; }
        }
        /// <summary>
        /// Date of the first time step stored in the databasepublic 
        /// </summary>
        // This property simply refers to a field of the TSParameters object
        public DateTime BlobStartDate
        {
            get { return tsParameters.BlobStartDate; }
            set { tsParameters.BlobStartDate = value; }
        }
        /// <summary>
        /// Date of the last time step stored in the database
        /// </summary>
        // This property simply refers to a field of the TSParameters object
        public DateTime BlobEndDate
        {
            get { return tsParameters.BlobEndDate; }
            set { tsParameters.BlobEndDate = value; }
        }
        /// <summary>
        /// The number of time steps stored in the database
        /// </summary>
        // This property simply refers to a field of the TSParameters object
        public int TimeStepCount
        {
            get { return tsParameters.TimeStepCount; }
            set { tsParameters.TimeStepCount = value; }
        }
        #endregion


        #region Static Constructor
        /// <summary>
        /// Static constructor is called before the first instance of the class is initialized--not when an
        /// individual instance of the class is initialized.  That is, the static constructor is only called
        /// once per run.  The static constructor can only set static data for the class.
        /// </summary>
        static TS()
        {
            // Can be deleted ?  Stuff that we were doing here has be moved elsewhere.
        }
        #endregion


        #region Class Constructor
        /// <summary>
        /// Class constructor that should be used if the TS object will read or write to database
        /// </summary>
        /// <param name="connx">SqlConnection object that this object will use</param>
        /// <param name="tableName">Name of the table in the database that stores this object's record</param>
        public TS(SqlConnection connx, String tableName)
        {
            // Store the method parameters in class fields
            Connx = connx;
            TableName = tableName;
            // Mark this time series as uninitialized
            // (because the meta parameters have not yet been read from the database)
            IsInitialized = false;
        }
        /// <summary>
        /// Class constructor that should be used if the TS object will not read or write to database
        /// </summary>
        /// <param name="connx">SqlConnection object that this object will use</param>
        /// <param name="tableName">Name of the table in the database that stores this object's record</param>
        public TS()
        {
            // These field values reflect that the database is not accessed
            Connx = null;
            TableName = null;
            // Mark this time series as uninitialized
            // (because the meta parameters have not yet been read from the database)
            IsInitialized = false;
        }
        #endregion


        #region Initialize() Method
        /// <summary>
        /// Method reads the database record to get the definition of the time series.
        /// It does not read the time series data values themselves.  Therefore, this
        /// method can be called efficiently both by functions that will read the time series
        /// or write the time series, whether or not those functions will need the entire
        /// set of data values.
        /// </summary>
        /// <param name="id">ID of the time series record</param>
        public Boolean Initialize(int id)
        {
            // store the method's input parameters
            Id = id;
            // Define the SQL query
            String comm = String.Format("select TimeStepUnit,TimeStepQuantity,RecordCount,StartDate,EndDate " +
                                        "from {0} where Id='{1}'", TableName, Id);
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, Connx))
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
                                    "Table '" + TableName + "' could not be opened using query:\n\n." + comm, e);
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
                TimeStepCount = dTable.Rows[0].Field<int>("RecordCount");
                BlobStartDate = dTable.Rows[0].Field<DateTime>("StartDate");
                BlobEndDate = dTable.Rows[0].Field<DateTime>("EndDate");
            }
            IsInitialized = true;
            return true;
        }
        #endregion


        #region BuildStringForEmptyDataTable()
        /// <summary>
        /// Method returns a string for querying the database table and returning an empty result set.
        /// The subsequent query can be used to create an empty DataTable object, with the necessary
        /// columns defined.  Because the query names all required fields of the database table, the
        /// subsequent query will raise an exception if any fields are missing.
        /// </summary>
        /// <returns>The SQL command that returns an empty resultset</returns>
        String BuildStringForEmptyDataTable()
        {
            // note: by including 'where 1=0', we ensure that an empty resultset will be returned.
            return String.Format("select" +
                                 "  Id, TimeStepUnit, TimeStepQuantity, RecordCount, StartDate, EndDate, Checksum, ValueBlob" +
                                 "  from {0} where 1=0", TableName);
        }
        #endregion


        #region ReadValuesRegular() Method
        /// <summary>
        /// Method reads the time series matching the given ID, storing the values into
        /// the given array of double-precision floats.  The method starts populating the
        /// array at the given start date, filling in no more than the number of values
        /// that are requested.
        /// </summary>
        /// <param name="id">ID of the time series</param>
        /// <param name="nReqValues">number of values requested to read</param>
        /// <param name="valueArray">array requested to fill with values</param>
        /// <param name="reqStartDate">The earliest date in the time series that will be written to the array of values</param>
        /// <param name="reqEndDate">The latest date in the time series that will be written to the array of values</param>
        /// <returns>The number of values actually filled into the array</returns>
        public unsafe int ReadValuesRegular(int id,
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
            // If the start or end date requested by the caller are such that the stored time series
            // does not overlap, then we don't need to go any further.
            if (reqStartDate > BlobEndDate || reqEndDate < BlobStartDate)
                return 0;

            // byte array (the BLOB) that will be read from the database.
            Byte[] blobData = null;
            // method ReadValues reads data from the database into the byte array
            ReadValues(id, ref blobData);
            // Convert the BLOB into an array of double values (valueArray)
            return TSBlobCoder.ConvertBlobToArrayRegular(TimeStepUnit, TimeStepQuantity,
                                BlobStartDate, true,
                                nReqValues, reqStartDate, reqEndDate, 
                                blobData, valueArray);
        }
        #endregion


        #region ReadValuesIrregular() Method
        /// <summary>
        /// Method reads the irregular time series matching the given ID, storing the dates and
        /// values into the given array of TSDateValueStruct (a struct containing the date/value pair).
        /// The method starts populating the array at the given start date, filling in no more than
        /// the number of values, that are requested, and not reading past the given end date
        /// </summary>
        /// <param name="id">ID id of the time series</param>
        /// <param name="nReqValues">number of values requested to read</param>
        /// <param name="dateValueArray">array requrested to fill with date/value pairs</param>
        /// <param name="reqStartDate">start date requested</param>
        /// <param name="reqEndDate">end date requested</param>
        /// <returns>The number of values actually filled into the array</returns>
        public unsafe int ReadValuesIrregular(int id,
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
            // If the start or end date requested by the caller are such that the stored time series
            // does not overlap, then we don't need to go any further.
            if (reqStartDate > BlobEndDate || reqEndDate < BlobStartDate)
                return 0;

            // byte array (the BLOB) that will be read from the database.
            Byte[] blobData = null;
            // method ReadValues reads data from the database into the byte array
            ReadValues(id, ref blobData);
            // convert the byte array into date/value pairs
            return TSBlobCoder.ConvertBlobToArrayIrregular(true, nReqValues, reqStartDate, reqEndDate,
                            blobData, dateValueArray);

        }
        #endregion


        #region ReadValues() Method
        /// <summary>
        /// This method contains the operations to read the BLOB from the database table.
        /// </summary>
        /// <param name="id">ID identifying the time series record to read</param>
        /// <param name="blobData">the byte array that is populated from the database BLOB</param>
        private unsafe void ReadValues(int id, ref byte[] blobData)
        {
            // SQL statement that will only give us the BLOB of data values
            String comm = String.Format("select ValueBlob from {0} where Id='{1}' ",
                                    TableName, Id);
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, Connx))
            {
                DataTable dTable = new DataTable();
                // Execute the query to fill the DataTable object
                try
                {
                    adp.Fill(dTable);
                }
                catch(Exception e)
                {   // The query failed
                    throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                    "Table '" + TableName + "' could not be opened using query:\n\n." + comm, e);
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
            }

        }
        #endregion


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
            if (doWriteToDB && (TableName == null || Connx == null))
            {
            }
        }

        #region WriteValuesRegular() Method
        /// <summary>
        /// Method writes the given array of values as a timeseries to the database with the given
        /// start date and time step descriptors.
        /// </summary>
        /// <param name="doWriteToDB">true if the method should actually save the timeseries to the database</param>
        /// <param name="tsImport">TSImport object into which the method will record values that it has computed.
        /// If this parameter is null, then the method will skip the recording of such paramters to an object.</param>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, or Year</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="nOutValues">The number of values in the array to be written to the database</param>
        /// <param name="outStartDate">The date of the first time step</param>
        /// <param name="valueArray">The array of values to be written to the database</param>
        /// <returns>ID value identifying the database record that was created</returns>
        public unsafe int WriteValuesRegular(
                    bool doWriteToDB, TSImport tsImport,
                    short timeStepUnit, short timeStepQuantity, 
                    int nOutValues, DateTime outStartDate, double[] valueArray)
        {
            ErrorCheckWriteValues(doWriteToDB, tsImport);
            // The method's parameters are used to compute the meta-parameters of this time series
            tsParameters.SetParametersRegular(
                    (TSDateCalculator.TimeStepUnitCode)timeStepUnit, timeStepQuantity, 
                    nOutValues, outStartDate);

            // Convert the array of double values into a byte array...a BLOB
            byte[] blobData = TSBlobCoder.ConvertArrayToBlobRegular(TimeStepCount, valueArray);
            // compute the Checksum
            Checksum = TSBlobCoder.ComputeChecksum(tsParameters, blobData);

            // WriteValues method will handle all of the database interaction
            if (doWriteToDB)
                WriteValues(blobData);
            // Save the information that this method has computed into a TSImport object
            if (tsImport != null)
                tsImport.RecordFromTS(this, blobData);

            return Id;
        } 
        #endregion


        #region WriteValuesIrregular() Method
        /// <summary>
        /// Method writes the given array of date/value pairs as an irregular timeseries to the database.
        /// The method determines the start and end date of the timeseries using the given array of 
        /// date/value pairs.  
        /// </summary>
        /// <param name="doWriteToDB">true if the method should actually save the timeseries to the database</param>
        /// <param name="tsImport">TSImport object into which the method will record values that it has computed.
        /// <param name="nOutValues">The number of values in the array to be written to the database</param>
        /// <param name="dateValueArray">The array of values to be written to the database</param>
        /// <returns>ID value identifying the database record that was created</returns>
        public unsafe int WriteValuesIrregular(
                    bool doWriteToDB, TSImport tsImport,
                    int nOutValues, TSDateValueStruct[] dateValueArray)
        {
            ErrorCheckWriteValues(doWriteToDB, tsImport);
            // The method's parameters are used to compute the meta-parameters of this time series
            tsParameters.SetParametersIrregular(nOutValues, dateValueArray);

            // Convert the array of double values into a byte array...a BLOB
            Byte[] blobData = TSBlobCoder.ConvertArrayToBlobIrregular(TimeStepCount, dateValueArray);
            // compute the Checksum
            Checksum = TSBlobCoder.ComputeChecksum(tsParameters, blobData);

            // WriteValues method will handle all of the database interaction
            if (doWriteToDB)
                return WriteValues(blobData);
            // Save the information that this method has computed into a TSImport object
            if (tsImport != null)
                tsImport.RecordFromTS(this, blobData);

            return Id;
        }
        #endregion


        #region WriteValues() Method
        /// <summary>
        /// This method contains the actual operations to write the BLOB and its meta-parameters to the database.
        /// </summary>
        /// <param name="blobData">the blob (byte array) of time series values to be written</param>
        /// <returns>ID value identifying the database record that was created</returns>
        private unsafe int WriteValues(byte[] blobData)
        {
            // SQL statement that gives us a resultset for the DataTable object.  Note that
            // this query is rigged so that it will always return 0 records.  This is because
            // we only want the resultset to define the fields of the DataTable object.
            String comm = BuildStringForEmptyDataTable();
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, Connx))
            {
                // SqlCommandBuilder object must be instantiated in order for us to call
                // the Update method of the SqlDataAdapter.  Interestingly, we only need to
                // instantiate this object--we don't need to use it in any other way.
                using (SqlCommandBuilder bld = new SqlCommandBuilder(adp))
                {
                    DataTable dTable = new DataTable();
                    // Execute the query to fill the DataTable object
                    try
                    {
                        adp.Fill(dTable);
                    }
                    catch(Exception e)
                    {   // The query failed
                        throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                        "Table '" + TableName + "' could not be opened using query:\n\n." + comm, e);
                    }

                    // DataRow object represents the current row of the DataTable object, which in turn
                    // represents a record that we will add to the database table.
                    DataRow currentRow = dTable.NewRow();

                    // transfer all of the data into the DataRow object
                    currentRow["TimeStepUnit"] = (short)TimeStepUnit;
                    currentRow["TimeStepQuantity"] = TimeStepQuantity;
                    currentRow["RecordCount"] = TimeStepCount;
                    currentRow["StartDate"] = BlobStartDate;
                    currentRow["EndDate"] = BlobEndDate;
                    currentRow["Checksum"] = Checksum;
                    currentRow["ValueBlob"] = blobData;
                    dTable.Rows.Add(currentRow);
                    // This event handler will make sure that we can get the ID after the record is updated
                    adp.RowUpdating += Adapter_RowUpdating;
                    // Save the DataRow object to the database
                    adp.Update(dTable);
                    // now get the id of the new record
                    Id = (int)currentRow["Id"];
                    // unsubscribe from the event handler
                    adp.RowUpdating -= Adapter_RowUpdating;
                }
            }

            return Id;
        }
        /// <summary>
        /// This event hanlder modifies the SqlDataAdapter's SQL command in such a way to ensure
        /// that we can read the ID on the record that was updated.  Source:
        /// http://stackoverflow.com/a/12105428
        /// </summary>
        private void Adapter_RowUpdating(object sender, SqlRowUpdatingEventArgs e)
        {
            e.Command.CommandText += "; SELECT ID = SCOPE_IDENTITY()";
            e.Command.UpdatedRowSource = UpdateRowSource.FirstReturnedRecord;
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
            Id = id;
            // Simple SQL statement to delete the selected record
            String comm = String.Format("delete from {0} where Id='{1}' ",
                                    TableName, Id);
            // SqlCommand object allows us to execute the command
            SqlCommand sqlCommand = new SqlCommand(comm, Connx);
            // This method executes the SQL command and returns the number of rows that were affected
            int numRowsAffected = sqlCommand.ExecuteNonQuery();

            // Return value reflects whether anything was actually deleted
            if (numRowsAffected > 0)
                return true;

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
            // Simple SQL statement to delete the selected record
            String comm = String.Format("delete from {0} where {1}",
                                    TableName, whereClause);
            // SqlCommand object allows us to execute the command
            SqlCommand sqlCommand = new SqlCommand(comm, Connx);
            int numRowsAffected;
            try
            {   // This method executes the SQL command and returns the number of rows that were affected
                numRowsAffected = sqlCommand.ExecuteNonQuery();
            }
            catch (SqlException e)
            {   // execution of the SQL command failed because it is invalid
                throw new TSLibraryException(ErrCode.Enum.Sql_Syntax_Error,
                                "SQL Command\n\n\"" + comm + "\"\n\n contains a syntax error.", e);
            }

            // Return value reflects whether anything was actually deleted
            if (numRowsAffected > 0)
                return true;

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

    }
}
