using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;

namespace TimeSeriesLibrary
{
    public class TSMax
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
                                        "from {0} where Id={1}", TableName, Id);
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
            Id=-1;
            TableName="";
        }

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

            int t, tBlob;

            DateTime reqEndDate
                = TSDateCalculator.IncrementDate(reqStartDate, TimeStepUnit, TimeStepQuantity, nReqValues);

            //
            // Start the DataTable

            // SQL statement to return the values
            String comm = String.Format("select ValueBlob from {0} where Id={1} ",
                                    TableName, Id);
            // Send SQL resultset to DataTable dTable
            SqlDataAdapter adp = new SqlDataAdapter(comm, Connx);
            DataTable dTable = new DataTable();
            try
            {
                adp.Fill(dTable);
            }
            catch
            {
                return (int)ErrCodes.Enum.Could_Not_Open_Values_Table;
            }
            // There should be at least 1 row in the table
            if (dTable.Rows.Count < 1)
            {
                return (int)ErrCodes.Enum.Record_Not_Found_Values_Table;
            }

            DataRow currentRow = dTable.Rows[0];
            Byte[] blobData = (Byte[])currentRow["ValueBlob"];
            MemoryStream blobStream = new MemoryStream(blobData);
            BinaryReader blobReader = new BinaryReader(blobStream);
            int blobLengthVals = (int)blobStream.Length / sizeof(double);

            // In our first data blob, skip any values that precede the requested start date
            if (reqStartDate > BlobStartDate)
            {
                int numSkipValues
                    = TSDateCalculator.CountSteps(BlobStartDate, reqStartDate, TimeStepUnit, TimeStepQuantity);
                for (tBlob = 0; tBlob < numSkipValues; tBlob++)
                {
                    blobReader.ReadInt32();
                }
            }

            // Loop through all values that need to be stored
            for (t = 0; t < nReqValues; t++)
            {
                // If this value overruns the current record's blob
                if (t >= blobLengthVals)
                {
                    t--;
                    break;
                }
                // Write the current value onto the blob data
                valueArray[t] = blobReader.ReadDouble();
            }


            ClearProperties();
            return t;
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

            int numStepsAddStart=0;
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
            Byte[] blobData = new Byte[nOutValues * sizeof(double)];
            MemoryStream blobStream = new MemoryStream(blobData);
            BinaryWriter blobWriter = new BinaryWriter(blobStream);

            // Loop through all values that need to be stored
            for (t=0, tBlob=0; t < nOutValues; t++, tBlob++)
            {
                // TODO: the loop is currently only fit to handle a time series that is not being overwritten

                // Write the current value onto the blob data
                blobWriter.Write(valueArray[t]);
            }


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
