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
    // Differs from class TSFile as follows:
    //    TSFile follows same methods as TSMax
    //    TSFileS reads directly from file
    public class TSFileS
    {
        public TSConnection ConnxObject = new TSConnection();

        ErrCodes.Enum ErrorCode;
        int Id;
        String TableName;
        TSDateCalculator.TimeStepUnitCode TimeStepUnit;
        short TimeStepQuantity;
        DateTime BlobStartDate;
        DateTime BlobEndDate;
        Boolean IsEmpty;
        
        SqlConnection Connx;
        SqlDataAdapter adp;
        DataTable dTable;


        /// <summary>
        /// Method reads the record to get the definition of the time series
        /// </summary>
        /// <param name="id">id of the time series record</param>
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
            String comm = String.Format("select TimeStepUnit,TimeStepQuantity,StartDate,EndDate "+
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
            if(dTable.Rows.Count < 1)
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

        /// <summary>
        /// Method clears some critical properties of the object, to enforce the rule
        /// that every public method should begin by calling Initialize().
        /// </summary>
        void ClearProperties()
        {
            TableName="";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionNumber">key number of the connection</param>
        /// <param name="tableName">name of the main table</param>
        /// <param name="id">id number of the time series</param>
        /// <param name="nReqValues">number of values requested to read</param>
        /// <param name="valueArray">array requested to fill with values</param>
        /// <param name="reqStartTime">start time requested</param>
        /// <returns>If positive, number of values actually filled into array.
        /// If negative, the error code.</returns>
        public unsafe int ReadValues(
            int connectionNumber, String tableName, int id,
            int nReqValues, double[] valueArray, DateTime reqStartDate)
        {
            // Initialize class fields
            ErrorCode = ErrCodes.Enum.None;
            if (Initialize(connectionNumber, tableName, id) == false)
            {
                return (int)ErrorCode;
            }

            DateTime reqEndDate
                = TSDateCalculator.IncrementDate(reqStartDate, TimeStepUnit, TimeStepQuantity, nReqValues);

            int t, tBlob;
            SqlCommand sqlCommand = new SqlCommand();
            sqlCommand.Connection = Connx;
            if (Connx.State == ConnectionState.Closed)
                Connx.Open();

            //The first task is to retrieve the file path
            //of the SQL FILESTREAM BLOB that we want to
            //access in the application.
            sqlCommand.CommandText
                 = String.Format("select ValueBlob.PathName() from {0} where Id='{1}' ",
                                    TableName, Id);
            String filePath = null;
            Object pathObj = sqlCommand.ExecuteScalar();
            if (DBNull.Value != pathObj)
                filePath = (string)pathObj;
            else
            {
                return (int)ErrCodes.Enum.Record_Not_Found_Values_Table;
            }

            // The next task is to obtain a transaction context. All FILESTREAM BLOB 
            // operations occur within a transaction context to maintain data consistency.  
            // MARS-enabled connections have specific rules for batch scoped transactions,
            // which the Transact-SQL BEGIN TRANSACTION statement violates. To avoid this
            // issue, client applications should use appropriate API facilities for 
            // transaction management, management, such as the SqlTransaction class.
            SqlTransaction transaction = Connx.BeginTransaction();
            sqlCommand.Transaction = transaction;
            sqlCommand.CommandText = "SELECT GET_FILESTREAM_TRANSACTION_CONTEXT()";
            byte[] txContext = (byte[])sqlCommand.ExecuteScalar();
            // Get a handle that can be passed to the Win32 FILE APIs.
            SqlFileStream sqlFileStream = new SqlFileStream(filePath, txContext, FileAccess.Read);

            int nBin = Math.Min(nReqValues * sizeof(double), (int)sqlFileStream.Length);
            BinaryReader blobReader = new BinaryReader(sqlFileStream);
            Buffer.BlockCopy(blobReader.ReadBytes(nBin), 0, valueArray, 0, nBin);


            //// Skip any values that precede the requested start date
            //if (reqStartDate > BlobStartDate)
            //{
            //    int numSkipValues
            //        = TSDateCalculator.CountSteps(BlobStartDate, reqStartDate, TimeStepUnit, TimeStepQuantity);
            //    sqlFileStream.Seek(numSkipValues*sizeof(double), 0);
            //}

            // Loop through all values that need to be stored
            //for (t = 0; t < nReqValues; t++)
            //{
                //// If this value overruns the current record's blob
                //if (t >= blobLengthVals)
                //{
                //    t--;
                //    break;
                //}
                // Write the current value onto the blob data
            //    valueArray[t] = blobReader.ReadDouble();
            //}





            // Closes the C# FileStream class (does not necessarily close the the underlying FILESTREAM handle).
            sqlFileStream.Close();
            // Finalize the transaction
            sqlCommand.Transaction.Commit();
            //Connx.Close();

            ClearProperties();
            return 0; // return t;
        }


        ///// <summary>
        ///// 
        ///// </summary>
        //public unsafe int WriteValues(
        //            int connectionNumber, String tableName,
        //            short timeStepUnit, short timeStepQuantity,
        //            int nOutValues, double[] valueArray, DateTime OutStartDate)
        //{
        //    ErrorCode = ErrCodes.Enum.None;

        //    TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)timeStepUnit;
        //    TimeStepQuantity = timeStepQuantity;
        //    TableName = tableName;

        //    int numStepsAddStart=0;
        //    int numStepsAddEnd = 0;
        //    int t, tBlob;

        //    DateTime OutEndDate 
        //        = TSDateCalculator.IncrementDate(OutStartDate, TimeStepUnit, TimeStepQuantity, nOutValues);


        //    Connx = ConnxObject.TSConnectionsCollection[connectionNumber];
        //    SqlCommand sqlCommand = new SqlCommand();
        //    sqlCommand.Connection = Connx;
        //    if (Connx.State == ConnectionState.Closed)
        //        Connx.Open();

        //    //
        //    // Determine if values records need to be added to the front and/or end of the values

        //    // No time series values have been entered yet
        //    if (IsEmpty)
        //    {
        //        BlobStartDate = OutStartDate;
        //        BlobEndDate = OutEndDate;
        //    }
        //    // The time series already contains values records
        //    else
        //    {   // TODO: This is not yet working!

        //        // Determine how many new records are needed at the front
        //        if (OutStartDate < BlobStartDate)
        //        {
        //            numStepsAddStart
        //                = TSDateCalculator.CountSteps(OutStartDate, BlobStartDate, TimeStepUnit, TimeStepQuantity);
        //        }
        //        // Determine how many new records are needed at the end
        //        if (OutEndDate < BlobEndDate)
        //        {
        //            numStepsAddEnd
        //                = TSDateCalculator.CountSteps(BlobEndDate, OutEndDate, TimeStepUnit, TimeStepQuantity);
        //        }
        //    }
            
        //    //
        //    // Fill the values into the DataTable's records

        //    // start at first record
        //    DateTime currentDate = OutStartDate;
        //    //Byte[] blobData = new Byte[nOutValues * sizeof(double)];
        //    //MemoryStream blobStream = new MemoryStream(blobData);
        //    //BinaryWriter blobWriter = new BinaryWriter(blobStream);



        //    //// Loop through all values that need to be stored
        //    //for (t=0, tBlob=0; t < nOutValues; t++, tBlob++)
        //    //{
        //    //    // TODO: the loop is currently only fit to handle a time series that is not being overwritten

        //    //    // Write the current value onto the blob data
        //    //    blobWriter.Write(valueArray[t]);
        //    //}
            


        //    // Start the transaction
        //    SqlTransaction transaction = Connx.BeginTransaction();
        //    sqlCommand.Transaction = transaction;
        //    // Insert the new record into the table, filling all fields except the BLOB
        //    sqlCommand.CommandText = String.Format("INSERT INTO {4} " +
        //                "(TimeStepUnit, TimeStepQuantity, StartDate, EndDate, ValueBlob) " +
        //                "VALUES({0}, {1}, '{2:yyyy-MM-dd HH:mm:ss}', '{3:yyyy-MM-dd HH:mm:ss}', 0); " +
        //                "SELECT SCOPE_IDENTITY()",
        //                (int)TimeStepUnit, TimeStepQuantity, OutStartDate, OutEndDate, TableName);
        //    //String rowId = (String)sqlCommand.ExecuteScalar();
        //    Id = Convert.ToInt32(sqlCommand.ExecuteScalar());

        //    // Get the file path of the BLOB's FILESTREAM
        //    sqlCommand.CommandText
        //         = String.Format("select ValueBlob.PathName() from {0} where Id={1} ", TableName, Id);
        //    String filePath = null;
        //    Object pathObj = sqlCommand.ExecuteScalar();
        //    if (DBNull.Value != pathObj)
        //        filePath = (string)pathObj;
        //    else
        //    {
        //        return (int)ErrCodes.Enum.Record_Not_Found_Values_Table;
        //    }

        //    sqlCommand.CommandText = "SELECT GET_FILESTREAM_TRANSACTION_CONTEXT()";
        //    byte[] txContext = (byte[])sqlCommand.ExecuteScalar();
        //    // Get a handle that can be passed to the Win32 FILE APIs.
        //    SqlFileStream sqlFileStream = new SqlFileStream(filePath, txContext, FileAccess.Write);

        //    int nBin = nOutValues * sizeof(double);
        //    byte[] byteArray = new byte[nBin];
        //    Buffer.BlockCopy(valueArray, 0, byteArray, 0, nBin);
        //    sqlFileStream.Write(byteArray, 0, nBin);

        //    // Closes the C# FileStream class (does not necessarily close the the underlying FILESTREAM handle).
        //    sqlFileStream.Close();
        //    // Finalize the transaction
        //    sqlCommand.Transaction.Commit();

        //    ClearProperties();
        //    return 0;
        //}



        ///// <summary>
        ///// 
        ///// </summary>
        //public unsafe int WriteValues(
        //            int connectionNumber, String tableName,
        //            short timeStepUnit, short timeStepQuantity,
        //            int nOutValues, double[] valueArray, DateTime OutStartDate)
        //{
        //    ErrorCode = ErrCodes.Enum.None;

        //    TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)timeStepUnit;
        //    TimeStepQuantity = timeStepQuantity;
        //    TableName = tableName;

        //    int numStepsAddStart = 0;
        //    int numStepsAddEnd = 0;
        //    int t, tBlob;

        //    DateTime OutEndDate
        //        = TSDateCalculator.IncrementDate(OutStartDate, TimeStepUnit, TimeStepQuantity, nOutValues);


        //    Connx = ConnxObject.TSConnectionsCollection[connectionNumber];
        //    SqlCommand sqlCommand = new SqlCommand();
        //    sqlCommand.Connection = Connx;
        //    if (Connx.State == ConnectionState.Closed)
        //        Connx.Open();

        //    //
        //    // Determine if values records need to be added to the front and/or end of the values

        //    // No time series values have been entered yet
        //    if (IsEmpty)
        //    {
        //        BlobStartDate = OutStartDate;
        //        BlobEndDate = OutEndDate;
        //    }
        //    // The time series already contains values records
        //    else
        //    {   // TODO: This is not yet working!

        //        // Determine how many new records are needed at the front
        //        if (OutStartDate < BlobStartDate)
        //        {
        //            numStepsAddStart
        //                = TSDateCalculator.CountSteps(OutStartDate, BlobStartDate, TimeStepUnit, TimeStepQuantity);
        //        }
        //        // Determine how many new records are needed at the end
        //        if (OutEndDate < BlobEndDate)
        //        {
        //            numStepsAddEnd
        //                = TSDateCalculator.CountSteps(BlobEndDate, OutEndDate, TimeStepUnit, TimeStepQuantity);
        //        }
        //    }

        //    //
        //    // Fill the values into the DataTable's records

        //    // start at first record
        //    DateTime currentDate = OutStartDate;
        //    //Byte[] blobData = new Byte[nOutValues * sizeof(double)];
        //    //MemoryStream blobStream = new MemoryStream(blobData);
        //    //BinaryWriter blobWriter = new BinaryWriter(blobStream);



        //    //// Loop through all values that need to be stored
        //    //for (t=0, tBlob=0; t < nOutValues; t++, tBlob++)
        //    //{
        //    //    // TODO: the loop is currently only fit to handle a time series that is not being overwritten

        //    //    // Write the current value onto the blob data
        //    //    blobWriter.Write(valueArray[t]);
        //    //}



        //    // Start the transaction
        //    SqlTransaction transaction = Connx.BeginTransaction();
        //    sqlCommand.Transaction = transaction;
        //    // Insert the new record into the table, filling all fields except the BLOB
        //    sqlCommand.CommandText = String.Format("INSERT INTO {4} " +
        //                "(TimeStepUnit, TimeStepQuantity, StartDate, EndDate, ValueBlob) " +
        //                "VALUES({0}, {1}, '{2:yyyy-MM-dd HH:mm:ss}', '{3:yyyy-MM-dd HH:mm:ss}', 0); " +
        //                "SELECT SCOPE_IDENTITY()",
        //                (int)TimeStepUnit, TimeStepQuantity, OutStartDate, OutEndDate, TableName);
        //    //String rowId = (String)sqlCommand.ExecuteScalar();
        //    Id = Convert.ToInt32(sqlCommand.ExecuteScalar());

        //    // Get the file path of the BLOB's FILESTREAM
        //    sqlCommand.CommandText
        //         = String.Format("select ValueBlob.PathName() from {0} where Id={1} ", TableName, Id);
        //    String filePath = null;
        //    Object pathObj = sqlCommand.ExecuteScalar();
        //    if (DBNull.Value != pathObj)
        //        filePath = (string)pathObj;
        //    else
        //    {
        //        return (int)ErrCodes.Enum.Record_Not_Found_Values_Table;
        //    }

        //    sqlCommand.CommandText = "SELECT GET_FILESTREAM_TRANSACTION_CONTEXT()";
        //    byte[] txContext = (byte[])sqlCommand.ExecuteScalar();
        //    // Get a handle that can be passed to the Win32 FILE APIs.
        //    SqlFileStream sqlFileStream = new SqlFileStream(filePath, txContext, FileAccess.Write);

        //    //int nBin = nOutValues * sizeof(double);
        //    //byte[] byteArray = new byte[nBin];
        //    //Buffer.BlockCopy(valueArray, 0, byteArray, 0, nBin);
        //    //sqlFileStream.Write(byteArray, 0, nBin);

        //    BinaryFormatter binFormatter = new BinaryFormatter();
        //    binFormatter.Serialize(sqlFileStream, valueArray);
            

        //    // Closes the C# FileStream class (does not necessarily close the the underlying FILESTREAM handle).
        //    sqlFileStream.Close();
        //    // Finalize the transaction
        //    sqlCommand.Transaction.Commit();

        //    ClearProperties();
        //    return 0;
        //}




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
