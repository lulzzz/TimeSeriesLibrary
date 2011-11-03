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
        public ErrCodes.Enum ErrorCode;
        public Guid Id;
        public String TableName;
        public TSDateCalculator.TimeStepUnitCode TimeStepUnit;
        public short TimeStepQuantity;
        public DateTime BlobStartDate;
        public DateTime BlobEndDate;

        private Boolean IsEmpty;
        private SqlConnection Connx;
        private SqlDataAdapter adp;
        private DataTable dTable;


        #region Class Constructor
        /// <summary>
        /// Class constructor
        /// </summary>
        /// <param name="connx">SqlConnection object that this object will use</param>
        /// <param name="tableName">Name of the table in the database that stores this object's record</param>
        public TS(SqlConnection connx, String tableName)
        {
            Connx = connx;
            TableName = tableName;
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
        /// <param name="id">id of the time series record</param>
        Boolean Initialize(Guid id)
        {
            // store the method's input parameters
            Id = id;
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


        #region Method for clearing object's properties
        /// <summary>
        /// Method clears some critical properties of the object, to enforce the rule
        /// that every public method should begin by calling Initialize().
        /// </summary>
        void ClearProperties()
        {
            TableName = "";
        } 
        #endregion


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id">id number of the time series</param>
        /// <param name="nMaxValues">number of values requested to read</param>
        /// <param name="valueArray">array requested to fill with values</param>
        /// <param name="reqStartTime">start time requested</param>
        /// <returns>If positive, number of values actually filled into array.
        /// If negative, the error code.</returns>
        public unsafe int ReadValues(Guid id,
            int nReqValues, double[] valueArray, DateTime reqStartDate)
        {
            // Initialize class fields
            ErrorCode = ErrCodes.Enum.None;
            if (Initialize(id) == false)
            {
                return (int)ErrorCode;
            }

            int t, tBlob;

            DateTime reqEndDate
                = TSDateCalculator.IncrementDate(reqStartDate, TimeStepUnit, TimeStepQuantity, nReqValues);

            //
            // Start the DataTable

            // SQL statement to return the values
            String comm = String.Format("select ValueBlob from {0} where Id='{1}' ",
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
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, double[] valueArray, DateTime OutStartDate)
        {
            ErrorCode = ErrCodes.Enum.None;

            TimeStepUnit = (TSDateCalculator.TimeStepUnitCode)timeStepUnit;
            TimeStepQuantity = timeStepQuantity;

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
