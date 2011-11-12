using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;
using System.Security.Cryptography;

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

        public static int blobPadForChecksumLength;



        #region Static Constructor
        /// <summary>
        /// Static constructor is called before the first instance of the class is initialized--not when an
        /// individual instance of the class is initialized.  That is, the static constructor is only called
        /// once per run.  The static constructor can only set static data for the class.
        /// </summary>
        static TS()
        {
            // The binary array that the 'Write' methods build has padding at the front.  
            // The BLOB (representing the timeseries values) that is saved to the database only
            // includes the portion of the binary array that follows a padding at the front.
            // The padding at the front is filled with the values of meta parameters that
            // are needed to define the time series.  The full binary array--the BLOB data plus
            // the padding--is used to compute an MD5 checksum to fingerprint this timeseries.

            // It is crucial that the the meta parameters provided for in the padding as defined
            // here be consistent
            // with methods BuildStringForEmptyDataTable() and PadBlobForChecksum()
            blobPadForChecksumLength =
                // TimeStepUnit
                sizeof(TSDateCalculator.TimeStepUnitCode) +
                // TimeStepQuantity
                sizeof(short) +
                // TimeStepCount
                sizeof(int) +
                // StartDate and EndDate
                8 + 8;
        }
        #endregion


        #region ComputeChecksum() Method
        byte[] ComputeChecksum(byte[] blobData)
        {
            // The binary array that the 'Write' methods build has padding at the front.  
            // The BLOB (representing the timeseries values) that is saved to the database only
            // includes the portion of the binary array that follows a padding at the front.
            // The padding at the front is filled with the values of meta parameters that
            // are needed to define the time series.  The full binary array--the BLOB data plus
            // the padding--is used to compute an MD5 checksum to fingerprint this timeseries.

            // It is crucial that the the meta parameters provided for in the padding as defined
            // here be consistent with method BuildStringForEmptyDataTable() and the calculation
            // of static field blobPadForChecksumLength in this class's static constructor.


            MemoryStream blobStream = new MemoryStream(blobData);
            BinaryWriter blobWriter = new BinaryWriter(blobStream);

            // TimeStepUnit
            blobWriter.Write((short)TimeStepUnit);
            // TimeStepQuantity
            blobWriter.Write(TimeStepQuantity);
            // TimeStepCount
            blobWriter.Write(TimeStepCount);
            // StartDate and EndDate
            blobWriter.Write(BlobStartDate.ToBinary());
            blobWriter.Write(BlobEndDate.ToBinary());

            // For security, ensure that the padding for the meta parameters does not run into
            // the part of the array that is reserved for the BLOB.  This would indicate a coding error.
            if (blobWriter.BaseStream.Position > blobPadForChecksumLength)
            {
                throw new TSLibraryException(ErrCode.Enum.Internal_Error,
                                "INTERNAL ERROR: The padding for checksum data has been overrun.");
            }
            // MD5CryptoServiceProvider object has methods to compute the checksum
            MD5CryptoServiceProvider md5Hasher = new MD5CryptoServiceProvider();
            // compute and return the checksum
            return md5Hasher.ComputeHash(blobData);
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


        #region Method for Initializing time series meta parameters (before reading or writing data values)
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


        #region Method builds string for returning empty DataTable
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
            int nReqValues, double[] valueArray, DateTime reqStartDate)
        {
            int numReadValues = 0;
            // Initialize class fields other than the BLOB of data values
            if (!IsInitialized) Initialize(id);

            // Using the start date and number of values, compute the requested end date.
            //DateTime reqEndDate
            //    = TSDateCalculator.IncrementDate(reqStartDate, TimeStepUnit, TimeStepQuantity, nReqValues);

            //
            // Start the DataTable

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
                Byte[] blobData = (Byte[])currentRow["ValueBlob"];
                // MemoryStream and BinaryReader objects enable bulk copying of data from the BLOB
                MemoryStream blobStream = new MemoryStream(blobData);
                BinaryReader blobReader = new BinaryReader(blobStream);
                // How many elements of size 'double' are in the BLOB?
                int numBlobBin = (int)blobStream.Length;
                int numBlobValues = numBlobBin / sizeof(double);
                // Do we skip any values at the front of the BLOB in order to fullfil the requested start date?
                int numSkipValues = 0;
                if (reqStartDate > BlobStartDate)
                    numSkipValues = TSDateCalculator.CountSteps(BlobStartDate, reqStartDate, TimeStepUnit, TimeStepQuantity);
                // convert the number of skipped values from number of doubles to number of bytes
                int numSkipBin = numSkipValues * sizeof(double);
                // the number of values that can actually be read from the BLOB
                numReadValues = Math.Min(numBlobValues - numSkipValues, nReqValues);
                int numReadBin = numReadValues * sizeof(double);

                // Transfer the entire array of data as a block
                Buffer.BlockCopy(blobReader.ReadBytes(numBlobBin), numSkipBin, valueArray, 0, numReadBin);
            }
            return numReadValues;
        }
        #endregion


        #region ReadValuesIrregular() Method
        /// <summary>
        /// Method reads the irregular time series matching the given GUID, storing the dates and
        /// values into the given array of TimeSeriesValue (a struct containing the date/value pair).
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
            int nReqValues, TimeSeriesValue[] dateValueArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            int numReadValues = 0;
            // Initialize class fields other than the BLOB of data values
            if (!IsInitialized) Initialize(id);

            // If the start or end date requested by the caller are such that the stored time series
            // does not overlap, then we don't need to go any further.
            if (reqStartDate > BlobEndDate || reqEndDate < BlobStartDate)
                return 0;

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
                Byte[] blobData = (Byte[])currentRow["ValueBlob"];
                // MemoryStream and BinaryReader objects enable bulk copying of data from the BLOB
                MemoryStream blobStream = new MemoryStream(blobData);
                BinaryReader blobReader = new BinaryReader(blobStream);
                // How many elements of 'TimeSeriesValue' are in the BLOB?
                int numBlobBin = (int)blobStream.Length;
                int numBlobValues = numBlobBin / sizeof(TimeSeriesValue);
                DateTime currDate;
                int j = 0;
                // Loop through all time steps in the BLOB
                for (int i = 0; i < numBlobValues; i++)
                {
                    // First check the date of this time step
                    currDate = DateTime.FromBinary(blobReader.ReadInt64());
                    // If date is before or after the dates requested by the caller, then
                    // we won't record the date/value info to the output array.
                    if (currDate < reqStartDate) continue;
                    if (currDate > reqEndDate) break;
                    // Record the date and value to the output array.
                    dateValueArray[j].Date = currDate;
                    dateValueArray[j].Value = blobReader.ReadDouble();
                    j++;
                    // Don't overrun the array length specified by the caller
                    if (j >= nReqValues) break;
                }
                
                numReadValues = j;
            }
            return numReadValues;
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
        /// <param name="OutStartDate">The date of the first time step</param>
        /// <returns>GUID value identifying the database record that was created</returns>
        public unsafe Guid WriteValuesRegular(
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, double[] valueArray, DateTime OutStartDate)
        {
            // Record values from function parameters
            TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)timeStepUnit;
            TimeStepQuantity = timeStepQuantity;

            // Determine the date of the last time step
            DateTime OutEndDate
                = TSDateCalculator.IncrementDate(OutStartDate, TimeStepUnit, TimeStepQuantity, nOutValues);

            //
            // Start the DataTable

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

                    // The number of bytes required for the BLOB
                    int nBin = nOutValues * sizeof(double);
                    // Allocate an array for the BLOB--plus a bit of padding that is used to compute the checksum
                    Byte[] blobData = new Byte[nBin + blobPadForChecksumLength];
                    // Copy the array of doubles that was passed to the method into the byte array.  We skip
                    // a bit of padding at the beginning that is used to compute the checksum.  Thus, the
                    // byte array (without the padding for checksum) becomes the BLOB.
                    Buffer.BlockCopy(valueArray, 0, blobData, blobPadForChecksumLength, nBin);
                    // compute the checksum
                    Byte[] checksum = ComputeChecksum(blobData);

                    // NewGuid method generates a GUID value that is virtually guaranteed to be unique
                    Id = Guid.NewGuid();
                    // transfer all of the data into the DataRow object
                    currentRow["Guid"] = Id;
                    currentRow["TimeStepUnit"] = (short)TimeStepUnit;
                    currentRow["TimeStepQuantity"] = TimeStepQuantity;
                    currentRow["TimeStepCount"] = nOutValues;
                    currentRow["StartDate"] = OutStartDate;
                    currentRow["EndDate"] = OutEndDate;
                    currentRow["Checksum"] = checksum;
                    currentRow["ValueBlob"] = new ArraySegment<byte>(blobData, blobPadForChecksumLength, nBin);
                    dTable.Rows.Add(currentRow);
                    // Save the DataRow object to the database
                    adp.Update(dTable);
                }
            }

            return Id;
        } 
        #endregion


        #region WriteValuesIrregular() Method
        /// <summary>
        /// Method writes the given array of values as a timeseries to the database with the given
        /// start date and time step descriptors.
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, or Year</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="nOutValues">The number of values in the array to be written to the database</param>
        /// <param name="valueArray">The array of values to be written to the database</param>
        /// <param name="OutStartDate">The date of the first time step</param>
        /// <returns>GUID value identifying the database record that was created</returns>
        public unsafe Guid WriteValuesIrregular(
                    int nOutValues, TimeSeriesValue[] dateValueArray)
        {
            // Determine the date of the first and last time step
            DateTime OutStartDate = dateValueArray[0].Date;
            DateTime OutEndDate = dateValueArray[nOutValues-1].Date;

            //
            // Start the DataTable

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

                    //
                    // Fill the values into the DataTable's records

                    // start at first record
                    int nBin = nOutValues * sizeof(TimeSeriesValue);
                    Byte[] blobData = new Byte[nBin];

                    MemoryStream blobStream = new MemoryStream(blobData);
                    BinaryWriter blobWriter = new BinaryWriter(blobStream);
                    for (int i = 0; i < nOutValues; i++)
                    {
                        blobWriter.Write(dateValueArray[i].Date.ToBinary());
                        blobWriter.Write(dateValueArray[i].Value);
                    }
                    // NewGuid method generates a GUID value that is virtually guaranteed to be unique
                    Id = Guid.NewGuid();
                    // now save meta-parameters to the main table
                    currentRow["Guid"] = Id;
                    currentRow["TimeStepUnit"] = (short)TSDateCalculator.TimeStepUnitCode.Irregular;
                    currentRow["TimeStepQuantity"] = 1;
                    currentRow["TimeStepCount"] = nOutValues;
                    currentRow["StartDate"] = OutStartDate;
                    currentRow["EndDate"] = OutEndDate;
                    currentRow["Checksum"] = new Byte[16];
                    currentRow["ValueBlob"] = blobData;
                    dTable.Rows.Add(currentRow);
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
            // The time steps boundaries are defined by the database record, so
            // we can't rely on the reqStartDate to have the correct time step boundary.
            // Therefore, begin counting at the first date in the database record.
            if (reqStartDate > BlobStartDate)
            {
                i = TSDateCalculator.CountSteps(BlobStartDate, reqStartDate, TimeStepUnit, TimeStepQuantity);
                dateArray[0] = TSDateCalculator.IncrementDate(BlobStartDate, TimeStepUnit, TimeStepQuantity, i);
            }
            else
            {
                dateArray[0] = BlobStartDate;
            }

            // Loop through the length of the array and fill in the date values
            for (i = 1; i < nReqValues; i++)
            {
                dateArray[i] = TSDateCalculator.IncrementDate(dateArray[i-1], TimeStepUnit, TimeStepQuantity, 1);
            }
        }
        #endregion

    }
}
