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
        public ErrCode.Enum ErrorCode;
        public Guid Id = new Guid();
        public String TableName;
        public TSDateCalculator.TimeStepUnitCode TimeStepUnit;
        public short TimeStepQuantity;
        public DateTime BlobStartDate;
        public DateTime BlobEndDate;

        private Boolean IsEmpty;
        private SqlConnection Connx;

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
        /// <param name="id">GUID id of the time series record</param>
        Boolean Initialize(Guid id)
        {
            // store the method's input parameters
            Id = id;
            // Define the SQL query
            String comm = String.Format("select TimeStepUnit,TimeStepQuantity,StartDate,EndDate " +
                                        "from {0} where Guid='{1}'", TableName, Id);
            // SqlDataAdapter object will use the query to fill the DataTable
            SqlDataAdapter adp = new SqlDataAdapter(comm, Connx);
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
        /// Method reads the time series matching the given GUID, storing the values into
        /// the given array of double-precision floats.  The method starts populating the
        /// array at the given start date, filling in no more than the number of values
        /// that are requested.
        /// </summary>
        /// <param name="id">GUID id of the time series</param>
        /// <param name="nReqValues">number of values requested to read</param>
        /// <param name="valueArray">array requested to fill with values</param>
        /// <param name="reqStartTime">start time requested</param>
        /// <returns>The number of values actually filled into array</returns>
        public unsafe int ReadValues(Guid id,
            int nReqValues, double[] valueArray, DateTime reqStartDate)
        {
            // Initialize class fields other than the BLOB of data values
            Initialize(id);

            int t;  // Counter for each value that is read
            int tSkip;  // Counter for each value that skipped at the beginning of the BLOB of values

            // Using the start date and number of values, compute the requested end date.
            DateTime reqEndDate
                = TSDateCalculator.IncrementDate(reqStartDate, TimeStepUnit, TimeStepQuantity, nReqValues);

            //
            // Start the DataTable

            // SQL statement that will only give us the BLOB of data values
            String comm = String.Format("select ValueBlob from {0} where Guid='{1}' ",
                                    TableName, Id);
            // SqlDataAdapter object will use the query to fill the DataTable
            SqlDataAdapter adp = new SqlDataAdapter(comm, Connx);
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
            // MemoryStream and BinaryReader objects allow us to read each value one-by-one.
            MemoryStream blobStream = new MemoryStream(blobData);
            BinaryReader blobReader = new BinaryReader(blobStream);
            // How many elements of size 'double' are in the blob?
            int blobLengthVals = (int)blobStream.Length / sizeof(double);

            // Skip any values that precede the requested start date
            tSkip = 0;
            if (reqStartDate > BlobStartDate)
            {
                int numSkipValues
                    = TSDateCalculator.CountSteps(BlobStartDate, reqStartDate, TimeStepUnit, TimeStepQuantity);
                for (; tSkip < numSkipValues; tSkip++)
                {
                    blobReader.ReadDouble();
                }
            }
            int blobLengthValsAdj = blobLengthVals - tSkip;

            // Loop through all values that need to be stored
            for (t = 0; t < nReqValues; t++)
            {
                // If this value overruns the current record's blob
                if (t >= blobLengthValsAdj)
                {
                    // we'll stop here
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
        public unsafe Guid WriteValues(
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, double[] valueArray, DateTime OutStartDate)
        {
            ErrorCode = ErrCode.Enum.None;

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
            SqlDataAdapter adp = new SqlDataAdapter(comm, Connx);
            SqlCommandBuilder bld = new SqlCommandBuilder(adp);
            DataTable dTable = new DataTable();
            try
            {
                adp.Fill(dTable);
            }
            catch
            {
                ErrorCode=ErrCode.Enum.Could_Not_Open_Values_Table;
                return new Guid();
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

            // NewGuid method generates a GUID value that is virtually guaranteed to be unique
            Id = Guid.NewGuid();
            // now save meta-parameters to the main table
            currentRow["Guid"] = Id;
            currentRow["TimeStepUnit"] = (short)TimeStepUnit;
            currentRow["TimeStepQuantity"] = TimeStepQuantity;
            currentRow["StartDate"] = OutStartDate;
            currentRow["EndDate"] = OutEndDate;
            currentRow["ValueBlob"] = blobData;
            dTable.Rows.Add(currentRow);
            adp.Update(dTable);

            ClearProperties();
            return Id;
        }

    }
}
