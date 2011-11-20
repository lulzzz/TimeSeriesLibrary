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
        // Fields with class scope may be shared between method calls.

        private SqlConnection Connx;     // Object handles the connection to the database
        private String TableName;       // name of the database table that stores this time series

        public Guid Id = new Guid();   // unique identifier for the database record
        public TSDateCalculator.TimeStepUnitCode TimeStepUnit;  // code for the units that measure the regular time step (e.g. hour, day, month)
        public short TimeStepQuantity;   // number of units per time step (e.g. Quantity=6 for 6-hour time steps)
        public DateTime BlobStartDate;   // Date of the first time step stored in the database
        public DateTime BlobEndDate;     // Date of the last time step stored in the database
        public int TimeStepCount;        // The number of time steps stored in the database

        public Boolean IsInitialized;


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
        /// Class constructor
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
        #endregion


        #region Initialize() Method
        /// <summary>
        /// Method reads the database record to get the definition of the time series.
        /// It does not read the time series data values themselves.  Therefore, this
        /// method can be called efficiently both by functions that will read the time series
        /// or write the time series, whether or not those functions will need the entire
        /// set of data values.
        /// </summary>
        /// <param name="id">GUID id of the time series record</param>
        public Boolean Initialize(Guid id)
        {
            // store the method's input parameters
            Id = id;
            // Define the SQL query
            String comm = String.Format("select TimeStepUnit,TimeStepQuantity,TimeStepCount,StartDate,EndDate " +
                                        "from {0} where Guid='{1}'", TableName, Id);
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, Connx))
            {
                DataTable dTable = new DataTable();
                // Execute the query to fill the DataTable object
                try
                {
                    adp.Fill(dTable);
                }
                catch
                {   // The query failed.
                    throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                    "Table '" + TableName + "' could not be opened using query:\n\n." + comm);
                }
                // There should be at least 1 row in the DataTable object
                if (dTable.Rows.Count < 1)
                {
                    throw new TSLibraryException(ErrCode.Enum.Record_Not_Found_Table,
                                    "Found zero records using query:\n\n." + comm);
                }
                // Assign properties from table to this object
                TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)dTable.Rows[0].Field<short>("TimeStepUnit");
                TimeStepQuantity = dTable.Rows[0].Field<short>("TimeStepQuantity");
                TimeStepCount = dTable.Rows[0].Field<int>("TimeStepCount");
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
                                 "  Guid, TimeStepUnit, TimeStepQuantity, TimeStepCount, StartDate, EndDate, Checksum, ValueBlob" +
                                 "  from {0} where 1=0", TableName);
        }
        #endregion


        #region ReadValuesRegular() Method
        /// <summary>
        /// Method reads the time series matching the given GUID, storing the values into
        /// the given array of double-precision floats.  The method starts populating the
        /// array at the given start date, filling in no more than the number of values
        /// that are requested.
        /// </summary>
        /// <param name="id">GUID id of the time series</param>
        /// <param name="nReqValues">number of values requested to read</param>
        /// <param name="valueArray">array requested to fill with values</param>
        /// <param name="reqStartDate">start date requested</param>
        /// <returns>The number of values actually filled into the array</returns>
        public unsafe int ReadValuesRegular(Guid id,
            int nReqValues, double[] valueArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            // Initialize class fields other than the BLOB of data values
            if (!IsInitialized) Initialize(id);

            // This method can only process regular-time-step series
            if (TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                throw new TSLibraryException(ErrCode.Enum.Record_Not_Regular,
                                String.Format("The method can only process regular time series, but" +
                                "the record with Guid {0} is irregular.", id));
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
        /// Method reads the irregular time series matching the given GUID, storing the dates and
        /// values into the given array of TSDateValueStruct (a struct containing the date/value pair).
        /// The method starts populating the array at the given start date, filling in no more than
        /// the number of values, that are requested, and not reading past the given end date
        /// </summary>
        /// <param name="id">GUID id of the time series</param>
        /// <param name="nReqValues">number of values requested to read</param>
        /// <param name="dateValueArray">array requrested to fill with date/value pairs</param>
        /// <param name="reqStartDate">start date requested</param>
        /// <param name="reqEndDate">end date requested</param>
        /// <returns>The number of values actually filled into the array</returns>
        public unsafe int ReadValuesIrregular(Guid id,
            int nReqValues, TSDateValueStruct[] dateValueArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            // Initialize class fields other than the BLOB of data values
            if (!IsInitialized) Initialize(id);

            // This method can only process irregular-time-step series
            if (TimeStepUnit != TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                throw new TSLibraryException(ErrCode.Enum.Record_Not_Irregular,
                                String.Format("The method can only process irregular time series, but" +
                                "the record with Guid {0} is regular.", id));
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
        public unsafe void ReadValues(Guid id, ref byte[] blobData)
        {
            // SQL statement that will only give us the BLOB of data values
            String comm = String.Format("select ValueBlob from {0} where Guid='{1}' ",
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
                catch
                {   // The query failed
                    throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                    "Table '" + TableName + "' could not be opened using query:\n\n." + comm);
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


        #region WriteValuesRegular() Method
        /// <summary>
        /// Method writes the given array of values as a timeseries to the database with the given
        /// start date and time step descriptors.
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, or Year</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="nOutValues">The number of values in the array to be written to the database</param>
        /// <param name="valueArray">The array of values to be written to the database</param>
        /// <param name="outStartDate">The date of the first time step</param>
        /// <returns>GUID value identifying the database record that was created</returns>
        public unsafe Guid WriteValuesRegular(
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, double[] valueArray, DateTime outStartDate)
        {
            // Record values from function parameters
            TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)timeStepUnit;
            TimeStepQuantity = timeStepQuantity;
            TimeStepCount = nOutValues;
            BlobStartDate = outStartDate;
            // Determine the date of the last time step
            BlobEndDate = TSDateCalculator.IncrementDate(outStartDate, TimeStepUnit, TimeStepQuantity, TimeStepCount-1);

            // Convert the array of double values into a byte array...a BLOB
            byte[] blobData = TSBlobCoder.ConvertArrayToBlobRegular(TimeStepCount, valueArray);

            // WriteValues method will handle all of the database interaction
            return WriteValues(blobData);
        } 
        #endregion


        #region WriteValuesIrregular() Method
        /// <summary>
        /// Method writes the given array of date/value pairs as an irregular timeseries to the database.
        /// The method determines the start and end date of the timeseries using the given array of 
        /// date/value pairs.  
        /// </summary>
        /// <param name="nOutValues">The number of values in the array to be written to the database</param>
        /// <param name="dateValueArray">The array of values to be written to the database</param>
        /// <returns>GUID value identifying the database record that was created</returns>
        public unsafe Guid WriteValuesIrregular(
                    int nOutValues, TSDateValueStruct[] dateValueArray)
        {
            // Fill in the class-level fields
            TimeStepUnit = TSDateCalculator.TimeStepUnitCode.Irregular;
            TimeStepQuantity = 0;
            TimeStepCount = nOutValues;
            // Determine the date of the first and last time step from the input array
            BlobStartDate = dateValueArray[0].Date;
            BlobEndDate = dateValueArray[TimeStepCount-1].Date;

            // Convert the array of double values into a byte array...a BLOB
            Byte[] blobData = TSBlobCoder.ConvertArrayToBlobIrregular(TimeStepCount, dateValueArray);

            // WriteValues method will handle all of the database interaction
            return WriteValues(blobData);
        }
        #endregion


        #region WriteValues() Method
        public unsafe Guid WriteValues(byte[] blobData)
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
                    catch
                    {   // The query failed
                        throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                        "Table '" + TableName + "' could not be opened using query:\n\n." + comm);
                    }

                    // DataRow object represents the current row of the DataTable object, which in turn
                    // represents a record that we will add to the database table.
                    DataRow currentRow = dTable.NewRow();

                    // compute the checksum
                    Byte[] checksum = TSBlobCoder.ComputeChecksum(TimeStepUnit, TimeStepQuantity,
                                    TimeStepCount, BlobStartDate, BlobEndDate, blobData);

                    // NewGuid method generates a GUID value that is virtually guaranteed to be unique
                    Id = Guid.NewGuid();
                    // transfer all of the data into the DataRow object
                    currentRow["Guid"] = Id;
                    currentRow["TimeStepUnit"] = (short)TimeStepUnit;
                    currentRow["TimeStepQuantity"] = TimeStepQuantity;
                    currentRow["TimeStepCount"] = TimeStepCount;
                    currentRow["StartDate"] = BlobStartDate;
                    currentRow["EndDate"] = BlobEndDate;
                    currentRow["Checksum"] = checksum;
                    currentRow["ValueBlob"] = blobData;
                    dTable.Rows.Add(currentRow);
                    // Save the DataRow object to the database
                    adp.Update(dTable);
                }
            }

            return Id;
        }
        #endregion


        #region DeleteSeries() Method
        /// <summary>
        /// Method deletes the single record from the table which matches the given GUID
        /// </summary>
        /// <param name="id">The GUID identifying the record to delete</param>
        /// <returns>true if a record was deleted, false if no records were deleted</returns>
        public Boolean DeleteSeries(Guid id)
        {
            Id = id;
            // Simple SQL statement to delete the selected record
            String comm = String.Format("delete from {0} where Guid='{1}' ",
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
            catch (SqlException)
            {   // execution of the SQL command failed because it is invalid
                throw new TSLibraryException(ErrCode.Enum.Sql_Syntax_Error,
                                "SQL Command\n\n\"" + comm + "\"\n\n contains a syntax error.");
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
        /// each time step in the time series that matches the given GUID id.  The array is filled
        /// starting at the given start date, or the start date of the database record, whichever
        /// is earliest.  The array is filled up the the given number of values.
        /// </summary>
        /// <param name="id">GUID id of the time series</param>
        /// <param name="nReqValues">number of values requested to be filled</param>
        /// <param name="dateArray">array requested to fill with values</param>
        /// <param name="reqStartDate">start date requested</param>
        public unsafe void FillDateArray(Guid id,
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
