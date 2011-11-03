using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;

namespace TimeSeriesLibrary
{
    public class TS
    {
        #region Private Fields
        private TSConnection ConnxObject = new TSConnection();

        private ErrCodes.Enum ErrorCode;
        private int Id;
        private String TableName;
        private TSDateCalculator.TimeStepUnitCode TimeStepUnit;
        private short TimeStepQuantity;
        private DateTime BlobStartDate;
        private DateTime BlobEndDate;
        private Boolean IsEmpty;

        private SqlConnection Connx;
        private SqlDataAdapter adp;
        private DataTable dTable; 
        #endregion


        #region Public Methods for Connection
        /// <summary>
        /// Opens a new connection for the time series library to use.  The new connection 
        /// is added to a list and assigned a serial number.  The method returns the
        /// serial number of the new connection.
        /// </summary>
        /// <param name="connectionString">The connection string used to open the connection.</param>
        /// <returns>The serial number of the new connection.</returns>
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
        #endregion


        #region Private ClearProperties method
        /// <summary>
        /// Method clears some critical properties of the object, to enforce the rule
        /// that every public method should begin by calling Initialize().  Any method
        /// that initializes the properties should call ClearProperties upon completion.
        /// </summary>
        private void ClearProperties()
        {
            Id = -1;
            TableName = "";
        } 
        #endregion


        #region Private Initialize method
        /// <summary>
        /// Method reads the given record from the given table to get defining parameters of the
        /// time series.  This process is preliminary to reading or writing the actual time series values.
        /// </summary>
        /// <param name="connectionNumber">Serial number of the connection to the database that contains the time series</param>
        /// <param name="tableName">Name of the table that contains the time series</param>
        /// <param name="id">ID number of the time series record</param>
        /// <returns>true if successful, false if there was an error</returns>
        Boolean Initialize(int connectionNumber, String tableName, int id)
        {
            Id = id;
            TableName = tableName;
            // Turn the connection key into a connection object
            try
            {
                Connx = ConnxObject.TSConnectionsCollection[connectionNumber];
            }
            catch
            {
                ErrorCode = ErrCodes.Enum.Connection_Not_Found;
                return false;
            }
            // Define the SQL query and get a resultset
            String comm = String.Format("select TimeStepUnit,TimeStepQuantity,StartDate,EndDate " +
                                        "from {0} where Id='{1}'", TableName, Id);
            adp = new SqlDataAdapter(comm, Connx);
            dTable = new DataTable();
            try
            {
                adp.Fill(dTable);
            }
            catch
            {
                ErrorCode = ErrCodes.Enum.Could_Not_Open_Main_Table;
                return false;
            }
            // There should be at least 1 row in the table
            if (dTable.Rows.Count < 1)
            {
                ErrorCode = ErrCodes.Enum.Record_Not_Found_Main_Table;
                return false;
            }
            // Assign properties from table to this object
            try
            {
                TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)dTable.Rows[0].Field<short>("TimeStepUnit");
                TimeStepQuantity = dTable.Rows[0].Field<short>("TimeStepQuantity");
                if (dTable.Rows[0]["StartDate"] is DBNull)
                {
                    // TODO: this IsEmpty business is probaby obsolete
                    IsEmpty = true;
                }
                else
                {
                    IsEmpty = false;
                    BlobStartDate = dTable.Rows[0].Field<DateTime>("StartDate");
                    BlobEndDate = dTable.Rows[0].Field<DateTime>("EndDate");
                }
            }
            catch
            {
                ErrorCode = ErrCodes.Enum.Missing_Fields_From_Main_Table;
                return false;
            }
            return true;
        } 
        #endregion


        #region Public methods overrides for ReadValues
        /// <summary>
        /// Reads values of a time series and returns the array of values of the time series,
        /// but does not return an array of date values.  The selected values are determined
        /// by the start date and a number of values (the array length).
        /// </summary>
        /// <param name="connectionNumber">Serial number of the connection to the database that contains the time series</param>
        /// <param name="tableName">Name of the table that contains the time series</param>
        /// <param name="id">ID number of the time series record</param>
        /// <param name="nMaxValues">The number of values to read into the array</param>
        /// <param name="valueArray">The array of time series values that the method populates</param>
        /// <param name="reqStartDate">The first date from which the method is to fetch time series values</param>
        /// <returns>The number of values that were actually populated into the valueArray.  Negative number indicates an error code</returns>
        public unsafe int ReadValues(
            int connectionNumber, String tableName, int id,
            int nReqValues, double[] valueArray, DateTime reqStartDate)
        {
            return ReadValuesCore(connectionNumber, tableName, id,
                        nReqValues, valueArray, new DateTime[nReqValues], false,
                        reqStartDate, new DateTime(), false);
        }
        /// <summary>
        /// Reads values of a time series and returns the array of values of the time series,
        /// but does not return an array of date values.  The selected values are determined
        /// by the start date and an end date.  However, an array length must be specified.
        /// The array is not filled past the end date *or* the array length, whichever comes
        /// up shorter.
        /// </summary>
        /// <param name="connectionNumber">Serial number of the connection to the database that contains the time series</param>
        /// <param name="tableName">Name of the table that contains the time series</param>
        /// <param name="id">ID number of the time series record</param>
        /// <param name="nMaxValues">The upper bound on the number of values to read into the array</param>
        /// <param name="valueArray">The array of time series values that the method populates</param>
        /// <param name="reqStartDate">The first date from which the method is to fetch time series values</param>
        /// <param name="reqEndDate">The last date from which the method is to fetch time series values</param>
        /// <returns>The number of values that were actually populated into the valueArray.  Negative number indicates an error code</returns>
        public unsafe int ReadValues(
            int connectionNumber, String tableName, int id,
            int nMaxValues, double[] valueArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            return ReadValuesCore(connectionNumber, tableName, id,
                        nMaxValues, valueArray, new DateTime[nMaxValues], false,
                        reqStartDate, reqEndDate, true);
        }
        /// <summary>
        /// Reads values of a time series and returns the array of values of the time series,
        /// plus an array of date values for the time series.  The selected values are determined
        /// by the start date and a number of values (the array length).
        /// </summary>
        /// <param name="connectionNumber">Serial number of the connection to the database that contains the time series</param>
        /// <param name="tableName">Name of the table that contains the time series</param>
        /// <param name="id">ID number of the time series record</param>
        /// <param name="nMaxValues">The number of values to read into the array</param>
        /// <param name="valueArray">The array of time series values that the method populates</param>
        /// <param name="dateArray">The array of time series values that the method populates</param>
        /// <param name="reqStartDate">The first date from which the method is to fetch time series values</param>
        /// <returns>The number of values that were actually populated into the valueArray.  Negative number indicates an error code</returns>
        public unsafe int ReadValues(
            int connectionNumber, String tableName, int id,
            int nReqValues, double[] valueArray, DateTime[] dateArray, DateTime reqStartDate)
        {
            return ReadValuesCore(connectionNumber, tableName, id,
                        nReqValues, valueArray, dateArray, true,
                        reqStartDate, new DateTime(), false);
        }
        /// <summary>
        /// Reads values of a time series and returns the array of values of the time series,
        /// plus an array of date values for the time series.  The selected values are determined
        /// by the start date and an end date.  However, an array length must be specified.
        /// The array is not filled past the end date *or* the array length, whichever comes
        /// up shorter.
        /// </summary>
        /// <param name="connectionNumber">Serial number of the connection to the database that contains the time series</param>
        /// <param name="tableName">Name of the table that contains the time series</param>
        /// <param name="id">ID number of the time series record</param>
        /// <param name="nMaxValues">The upper bound on the number of values to read into the array</param>
        /// <param name="valueArray">The array of time series values that the method populates</param>
        /// <param name="dateArray"></param>
        /// <param name="reqStartDate">The first date from which the method is to fetch time series values</param>
        /// <param name="reqEndDate">The last date from which the method is to fetch time series values</param>
        /// <returns>The number of values that were actually populated into the valueArray.  Negative number indicates an error code</returns>
        public unsafe int ReadValues(
            int connectionNumber, String tableName, int id,
            int nMaxValues, double[] valueArray, DateTime[] dateArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            return ReadValuesCore(connectionNumber, tableName, id,
                        nMaxValues, valueArray, dateArray, true,
                        reqStartDate, reqEndDate, true);
        }
        #endregion


        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionNumber">key number of the connection</param>
        /// <param name="tableName">name of the main table</param>
        /// <param name="id">id number of the time series</param>
        /// <param name="nMaxValues">number of values requested to read</param>
        /// <param name="valueArray">array requested to fill with values</param>
        /// <param name="reqStartTime">start time requested</param>
        /// <returns>If positive, number of values actually filled into array.
        /// If negative, the error code.</returns>
        private unsafe int ReadValuesCore(
            int connectionNumber, String tableName, int id,
            int nMaxValues, double[] valueArray, DateTime[] dateArray, Boolean dateArrayIsReq,
            DateTime reqStartDate, DateTime reqEndDate, Boolean endDateIsSpecified)
        {
            int nValuesRead = 0;
            // Initialize class fields
            ErrorCode = ErrCodes.Enum.None;
            if (Initialize(connectionNumber, tableName, id) == false)
            {
                // If Initialize method returned an error code
                return (int)ErrorCode;
            }
            // Error trap
            if (nMaxValues < 1)
            {
                ErrorCode = ErrCodes.Enum.Array_Length_Less_Than_One;
                return (int)ErrorCode;
            }
            if (Connx.State == ConnectionState.Closed)
                Connx.Open();
            SqlCommand sqlCommand = new SqlCommand();
            sqlCommand.Connection = Connx;

            //
            // Compute parameters related to the time range of requested dates
            //

            int nReqValues = nMaxValues;
            // If the end date is specified
            if (endDateIsSpecified)
            {
                // Error trap
                if (reqEndDate < reqStartDate)
                {
                    ErrorCode = ErrCodes.Enum.End_Date_Precedes_Start_Date;
                    return (int)ErrorCode;
                }
                // The number of values that will be filled into the array is determined by the start date and end date
                nReqValues = TSDateCalculator.CountSteps(reqStartDate, reqEndDate, TimeStepUnit, TimeStepQuantity);
                // The number of values that will be filled into the array can not exceed the array size specified
                // in the method's parameter list.
                nReqValues = Math.Min(nReqValues, nMaxValues);
            }
            // If the end date is *not* specified
            else
            {
                // The end date must be computed from the start date and the array length (nMaxValues);
                reqEndDate = TSDateCalculator.IncrementDate
                                (reqStartDate, TimeStepUnit, TimeStepQuantity, nMaxValues);
            }
            // Check whether time range of requested dates is covered by time series in database
            // The check does not include whether the requested end date is past the date in database--in that
            // case, method will return truncated array.
            // TODO: CHECK PRECISION IMPLICATIONS of the IF statement
            if (reqEndDate < BlobStartDate || reqStartDate > BlobEndDate || reqStartDate < BlobStartDate)
            {
                ErrorCode = ErrCodes.Enum.Requested_Dates_Outside_Of_Range;
                return (int)ErrorCode;
            }


            // Get the file path of the SQL FILESTREAM BLOB.
            // Note that this file path is only recognized by SqlFileStream object.
            // It is not a regular disk directory path.
            sqlCommand.CommandText = String.Format("select ValueBlob.PathName() from {0} " +
                                                   "where Id='{1}' ", TableName, Id);
            Object pathObj = sqlCommand.ExecuteScalar();
            if (pathObj == DBNull.Value)
            {
                ErrorCode = ErrCodes.Enum.Record_Not_Found_Values_Table;
                return (int)ErrorCode;
            }
            String filePath = (string)pathObj;

            // For FILESTREAM BLOB operations, we must explicitly create a transaction context
            // to ensure data consistency during the read.
            sqlCommand.Transaction = Connx.BeginTransaction();
            sqlCommand.CommandText = "SELECT GET_FILESTREAM_TRANSACTION_CONTEXT()";
            byte[] txContext = (byte[])sqlCommand.ExecuteScalar();
            // SqlFileStream object is used to read the file
            SqlFileStream sqlFileStream = new SqlFileStream(filePath, txContext, FileAccess.Read);

            // Skip any values that precede the requested start date
            // TODO: CHECK PRECISION IMPLICATIONS
            int nSkipValues = 0;
            if (reqStartDate > BlobStartDate)
            {
                nSkipValues = TSDateCalculator.CountSteps
                                   (BlobStartDate, reqStartDate, TimeStepUnit, TimeStepQuantity);
                sqlFileStream.Seek(nSkipValues * sizeof(double), 0);
            }

            // Get the number of bytes that will be read
            int nBin = Math.Min(nReqValues * sizeof(double), (int)sqlFileStream.Length);
            // BinaryReader object gives us a block of bytes from the FILESTREAM
            BinaryReader blobReader = new BinaryReader(sqlFileStream);
            // Copy the entire block of memory into the array
            Buffer.BlockCopy(blobReader.ReadBytes(nBin), 0, valueArray, 0, nBin);

            nValuesRead = nBin / sizeof(double);
            // 
            if (dateArrayIsReq)
            {
                dateArray[0] = TSDateCalculator.IncrementDate
                                    (BlobStartDate, TimeStepUnit, TimeStepQuantity, nSkipValues);
                for (int t = 1; t < nValuesRead; t++)
                {
                    dateArray[t] = TSDateCalculator.IncrementDate
                                        (dateArray[t - 1], TimeStepUnit, TimeStepQuantity, 1);
                }
            }

            // Closes the C# FileStream class (does not necessarily close the the underlying FILESTREAM handle).
            sqlFileStream.Close();
            // Finalize the transaction
            sqlCommand.Transaction.Commit();

            ClearProperties();
            return nValuesRead;
        }



        /// <summary>
        /// 
        /// </summary>
        public unsafe int WriteValues(
                    int connectionNumber, String tableName,
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, double[] valueArray, DateTime OutStartDate)
        {
            ErrorCode = ErrCodes.Enum.None;

            TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)timeStepUnit;
            TimeStepQuantity = timeStepQuantity;
            TableName = tableName;

            int numStepsAddStart = 0;
            int numStepsAddEnd = 0;
            int t, tBlob;

            DateTime OutEndDate
                = TSDateCalculator.IncrementDate(OutStartDate, TimeStepUnit, TimeStepQuantity, nOutValues);

            //
            // Start the DataTable

            // SQL statement to return the values records
            String comm = String.Format("select * from {0} where 1=0", TableName);
            // Send SQL resultset to DataTable dTable
            Connx = ConnxObject.TSConnectionsCollection[connectionNumber];
            adp = new SqlDataAdapter(comm, Connx);
            SqlCommandBuilder bld = new SqlCommandBuilder(adp);
            dTable = new DataTable();
            try
            {
                adp.Fill(dTable);
            }
            catch
            {
                return (int)ErrCodes.Enum.Could_Not_Open_Values_Table;
            }
            IsEmpty = true;

            //
            // Determine if values records need to be added to the front and/or end of the values

            // No time series values have been entered yet
            if (IsEmpty)
            {
                BlobStartDate = OutStartDate;
                BlobEndDate = OutEndDate;
            }
            // The time series already contains values records
            else
            {   // TODO: This is not yet working!

                // Determine how many new records are needed at the front
                if (OutStartDate < BlobStartDate)
                {
                    numStepsAddStart
                        = TSDateCalculator.CountSteps(OutStartDate, BlobStartDate, TimeStepUnit, TimeStepQuantity);
                }
                // Determine how many new records are needed at the end
                if (OutEndDate < BlobEndDate)
                {
                    numStepsAddEnd
                        = TSDateCalculator.CountSteps(BlobEndDate, OutEndDate, TimeStepUnit, TimeStepQuantity);
                }
            }
            DataRow currentRow = dTable.NewRow();

            //
            // Fill the values into the DataTable's records

            // start at first record
            DateTime currentDate = OutStartDate;
            int nBin = nOutValues * sizeof(double);
            Byte[] blobData = new Byte[nBin];

            Buffer.BlockCopy(valueArray, 0, blobData, 0, nBin);


            // now save meta-parameters to the main table
            currentRow["TimeStepUnit"] = (short)TimeStepUnit;
            currentRow["TimeStepQuantity"] = TimeStepQuantity;
            currentRow["StartDate"] = OutStartDate;
            currentRow["EndDate"] = OutEndDate;
            currentRow["ValueBlob"] = blobData;
            dTable.Rows.Add(currentRow);
            adp.Update(dTable);

            ClearProperties();
            return 0;
        }

    }
}
