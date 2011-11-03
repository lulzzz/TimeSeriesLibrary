using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;

namespace TimeSeriesLibrary
{
    public class TS_old
    {
        const int BlobSizeBytes = 3072;
        const int BlobSizeVals = BlobSizeBytes / sizeof(double);

        public TSConnection ConnxObject = new TSConnection();

        ErrCodes.Enum ErrorCode;
        int Id;
        String TableName;
        TSDateCalculator.TimeStepUnitCode TimeStepUnit;
        short TimeStepQuantity;
        DateTime ValuesStartDate;
        DateTime ValuesEndDate;
        DateTime BlobStartDate;
        DateTime BlobEndDate;
        Boolean IsEmpty;
        SqlConnection Connx;

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
            String comm = String.Format("select * from {0} where Id={1}", TableName, Id);
            SqlDataAdapter adp = new SqlDataAdapter(comm, Connx);
            DataTable dTable = new DataTable();
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
                if (dTable.Rows[0]["ValuesStartDate"] is DBNull)
                {
                    IsEmpty = true;
                }
                else
                {
                    IsEmpty = false;
                    ValuesStartDate = dTable.Rows[0].Field<DateTime>("ValuesStartDate");
                    ValuesEndDate = dTable.Rows[0].Field<DateTime>("ValuesEndDate");
                    BlobStartDate = dTable.Rows[0].Field<DateTime>("BlobStartDate");
                    BlobEndDate = dTable.Rows[0].Field<DateTime>("BlobEndDate");
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
            int connectionNumber, String tableName, String valuesTableName, int id,
            int nReqValues, double[] valueArray, DateTime reqStartDate)
        {
            // Initialize class fields
            ErrorCode = ErrCodes.Enum.None;
            if (Initialize(connectionNumber, tableName, id) == false)
            {
                return (int)ErrorCode;
            }

            int i, t, tBlob;

            DateTime reqEndDate
                = TSDateCalculator.IncrementDate(reqStartDate, TimeStepUnit, TimeStepQuantity, nReqValues);

            //
            // Start the DataTable

            // SQL statement to return the values records
            String comm = String.Format("select * from {0} where TimeSeries_Id={1} " +
                            "and BlobStartDate between '{2:yyyyMMdd HH:mm}' and '{3:yyyyMMdd HH:mm}' " +
                            "or ValueEndDate between '{2:yyyyMMdd HH:mm}' and '{3:yyyyMMdd HH:mm}' " +
                            "order by BlobStartDate",
                            valuesTableName, Id, reqStartDate, reqEndDate);
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
            int dTableRowCount = dTable.Rows.Count;
            // There should be at least 1 row in the table
            if (dTableRowCount < 1)
            {
                return (int)ErrCodes.Enum.Record_Not_Found_Values_Table;
            }
            
            i = 0;
            DataRow currentRow = dTable.Rows[0];
            Byte[] blobData = (Byte[])currentRow["ValueBlob"];
            MemoryStream blobStream = new MemoryStream(blobData);
            BinaryReader blobReader = new BinaryReader(blobStream);

            // In our first data blob, skip any values that precede the requested start date
            DateTime rowBlobStartDate = (DateTime)currentRow["ValueStartDate"];
            if (reqStartDate > rowBlobStartDate)
            {
                int numSkipValues
                    = TSDateCalculator.CountSteps(rowBlobStartDate, reqStartDate, TimeStepUnit, TimeStepQuantity);
                for (tBlob = 0; tBlob < numSkipValues; tBlob++)
                {
                    blobReader.ReadInt32();
                }
            }

            // Loop through all values that need to be stored
            for (t = 0, tBlob = 0; t < nReqValues; t++, tBlob++)
            {
                // If this value overruns the current record's blob
                if (tBlob >= BlobSizeVals)
                {
                    if (i + 1 >= dTableRowCount)
                    {
                        t--;
                        break;
                    }
                    // Move to the next record and its blob
                    i++;
                    currentRow = dTable.Rows[i];
                    blobData = (Byte[])currentRow["ValueBlob"];
                    blobStream = new MemoryStream(blobData);
                    blobReader = new BinaryReader(blobStream);
                    tBlob = 0;
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
            int connectionNumber, String tableName, String valuesTableName, int id,
            int nOutValues, double[] valueArray, DateTime OutStartDate)
        {
            ErrorCode = ErrCodes.Enum.None;
            if (Initialize(connectionNumber, tableName, id) == false)
                return (int)ErrorCode;

            int numRecAddEnd = 0, numStepsAddStart=0;
            int numRecAddStart = 0, numStepsAddEnd = 0;
            int i, t, tBlob;

            DateTime OutEndDate 
                = TSDateCalculator.IncrementDate(OutStartDate, TimeStepUnit, TimeStepQuantity, nOutValues);

            //
            // Start the DataTable
            
            // SQL statement to return the values records
            String comm = String.Format("select * from {0} where TimeSeries_Id={1} "+
                            "and BlobStartDate between '{2:yyyyMMdd HH:mm}' and '{3:yyyyMMdd HH:mm}' "+
                            "or ValueEndDate between '{2:yyyyMMdd HH:mm}' and '{3:yyyyMMdd HH:mm}' "+
                            "order by BlobStartDate",
                            valuesTableName, Id, OutStartDate, OutEndDate);
            // Send SQL resultset to DataTable dTable
            SqlDataAdapter adp = new SqlDataAdapter(comm, Connx);
            SqlCommandBuilder bld = new SqlCommandBuilder(adp);
            DataTable dTable = new DataTable();
            try
            {
                adp.Fill(dTable);
            }
            catch
            {
                return (int)ErrCodes.Enum.Could_Not_Open_Values_Table;
            }

            //
            // Determine if values records need to be added to the front and/or end of the values

            // No time series values have been entered yet
            if (IsEmpty)
            {
                BlobStartDate = OutStartDate;
                numRecAddStart = (nOutValues - 1) / BlobSizeVals + 1;
                BlobEndDate = TSDateCalculator.IncrementDate(BlobStartDate, TimeStepUnit, TimeStepQuantity,
                                            numRecAddStart * BlobSizeVals);
            }
            // The time series already contains values records
            else
            {
                // Determine how many new records are needed at the front
                if (OutStartDate < BlobStartDate)
                {
                    numStepsAddStart
                        = TSDateCalculator.CountSteps(OutStartDate, BlobStartDate, TimeStepUnit, TimeStepQuantity);
                    numRecAddStart = (numStepsAddStart - 1) / BlobSizeVals + 1;
                    BlobStartDate = TSDateCalculator.IncrementDate(BlobStartDate, TimeStepUnit, TimeStepQuantity,
                                            -numRecAddStart * BlobSizeVals);
                }
                // Determine how many new records are needed at the end
                if (OutEndDate < BlobEndDate)
                {
                    numStepsAddEnd
                        = TSDateCalculator.CountSteps(BlobEndDate, OutEndDate, TimeStepUnit, TimeStepQuantity);
                    numRecAddEnd = (numStepsAddEnd - 1) / BlobSizeVals + 1;
                    BlobEndDate = TSDateCalculator.IncrementDate(BlobEndDate, TimeStepUnit, TimeStepQuantity,
                                            numRecAddEnd * BlobSizeVals);
                }
            }
            DataRow currentRow;
            // Add new records to front of the DataTable            
            for(i=0; i<numRecAddStart; i++)
            {
                currentRow = InitializedDataRow(dTable);
                dTable.Rows.InsertAt(currentRow, 0);
            }
            // Add new records to end of the DataTable            
            for (i = 0; i < numRecAddEnd; i++)
            {
                currentRow = InitializedDataRow(dTable);
                dTable.Rows.Add(currentRow, 0);
            }
            
            //
            // Fill the values into the DataTable's records

            // start at first record
            i=0;
            currentRow = dTable.Rows[0];
            DateTime rowBlobStartDate = BlobStartDate;
            DateTime rowBlobEndDate = TSDateCalculator.IncrementDate(BlobStartDate, TimeStepUnit, TimeStepQuantity,
                                            BlobSizeVals-1);
            DateTime currentDate = OutStartDate;
            Byte[] blobData = new Byte[BlobSizeBytes];
            MemoryStream blobStream = new MemoryStream(blobData);
            BinaryWriter blobWriter = new BinaryWriter(blobStream);

            // Loop through all values that need to be stored
            for (t=0, tBlob=0; t < nOutValues; t++, tBlob++)
            {
                // TODO: the loop is currently only fit to handle a time series that is not being overwritten

                // If this value overruns the current record's blob
                if (tBlob >= BlobSizeVals)
                {
                    // write the meta-parameters to the record in values table
                    FillDataRow(currentRow, 0, rowBlobStartDate, rowBlobStartDate, rowBlobEndDate, blobData);
                    blobData = new Byte[BlobSizeBytes];
                    blobStream = new MemoryStream(blobData);
                    blobWriter = new BinaryWriter(blobStream);
                    // Move to the next record and its blob
                    i++;
                    currentRow = dTable.Rows[i];
                    rowBlobStartDate = TSDateCalculator.IncrementDate(rowBlobEndDate, TimeStepUnit, TimeStepQuantity, 1);
                    rowBlobEndDate = TSDateCalculator.IncrementDate(rowBlobStartDate, TimeStepUnit, TimeStepQuantity, BlobSizeVals-1);
                    tBlob = 0;
                }
                // Write the current value onto the blob data
                blobWriter.Write(valueArray[t]);
            }
            FillDataRow(currentRow, 0, rowBlobStartDate, rowBlobStartDate, OutEndDate, blobData);
            adp.Update(dTable);


            // now save meta-parameters to the main table
            comm = String.Format("select * from {0} where Id={1}", TableName, Id);
            adp = new SqlDataAdapter(comm, Connx);
            dTable = new DataTable();
            bld = new SqlCommandBuilder(adp);
            adp.Fill(dTable); 
            currentRow = dTable.Rows[0];
            currentRow["ValuesStartDate"] = BlobStartDate;
            currentRow["ValuesEndDate"] = BlobEndDate;
            currentRow["BlobStartDate"] = BlobStartDate;
            currentRow["BlobEndDate"] = BlobEndDate;
            adp.Update(dTable);

            ClearProperties();
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        private DataRow InitializedDataRow(DataTable dTable)
        {
            DataRow currentRow = dTable.NewRow();
            currentRow["TimeSeries_Id"] = Id;
            currentRow["NumSkip"] = 0;
            currentRow["ValueBlob"] = null;
            return currentRow;
        }

        /// <summary>
        /// 
        /// </summary>
        void FillDataRow(DataRow currentRow, int numSkip, DateTime blobStartDate, DateTime valueStartDate,
                    DateTime valueEndDate, Byte[] blobData)
        {
            currentRow["NumSkip"] = numSkip;
            currentRow["BlobStartDate"] = blobStartDate;
            currentRow["ValueStartDate"] = valueStartDate;
            currentRow["ValueEndDate"] = valueEndDate;
            currentRow["ValueBlob"] = blobData;
        }
    }
}
