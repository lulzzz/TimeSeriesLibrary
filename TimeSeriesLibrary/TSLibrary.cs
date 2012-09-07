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
    /// This class contains the callable functions of the library for .NET code.
    /// </summary>
    public class TSLibrary
    {
        /// <summary>
        /// TSConnection object maintains a list of connections that have been opened by the library
        /// </summary>
        public TSConnection ConnxObject = new TSConnection();


        #region ConvertBlobToList() methods

        /// <summary>
        /// This method creates a List of TimeSeriesValue objects from the given BLOB (byte array)
        /// of time series values.  The method converts the entire BLOB into the list.  The sibling
        /// method ConvertBlobToListLimited can convert a selected portion of the BLOB into the list.
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.  If timeStepUnit is 
        /// Irregular, then this value is ignored.</param>
        /// <param name="blobStartDate">The DateTime value of the first time step in the BLOB. If 
        /// timeStepUnit is Irregular, then this value is ignored.</param>
        /// <param name="blobData">The BLOB (byte array) that this method will convert</param>
        /// <param name="dateValueList">The List of TimeSeriesValues that this method will create from the BLOB.</param>
        /// <returns>The number of time steps added to dateValueList</returns>
        public int ConvertBlobToListAll(
            TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
            DateTime blobStartDate,
            Byte[] blobData, ref List<TimeSeriesValue> dateValueList)
        {
            // The private method ConvertBlobToList() will do all the real work here.
            // This private method takes parameters for limiting a portion of the List to be
            // written to the BLOB.  The values below are dummies that will be passed to
            // the private method, which it will ignore.
            int nReqValues = 0;
            DateTime reqStartDate = blobStartDate;
            DateTime reqEndDate = blobStartDate;

            // Let the private core method do all the real work.
            // We pass it the 'applyLimits' value of false, to tell it to ignore the 'req' limit values.
            return ConvertBlobToList(timeStepUnit, timeStepQuantity,
                        blobStartDate, false,
                        nReqValues, reqStartDate, reqEndDate,
                        blobData, ref dateValueList);
        }

        /// <summary>
        /// This method creates a List of TimeSeriesValue objects from the given BLOB (byte array)
        /// of time series values.  The method takes parameters for a maximum number of values,
        /// an earliest date, and a latest date, so that only a portion of the BLOB might be 
        /// converted to the List.  The sibling method ConvertBlobToListAll can convert the entire
        /// BLOB without any limits.
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.  If timeStepUnit is 
        /// Irregular, then this value is ignored.</param>
        /// <param name="blobStartDate">The DateTime value of the first time step in the BLOB. If 
        /// timeStepUnit is Irregular, then this value is ignored.</param>
        /// <param name="nReqValues">The maximum number of time steps that should be added to dateValueList</param>
        /// <param name="reqStartDate">The earliest date that will be added to dateValueList</param>
        /// <param name="reqEndDate">The latest date that will be added to dateValueList</param>
        /// <param name="blobData">The BLOB (byte array) that this method will convert into a List</param>
        /// <param name="dateValueList">The List of TimeSeriesValues that this method will create from the BLOB.</param>
        /// <returns>The number of time steps added to dateValueList</returns>
        public int ConvertBlobToListLimited(
            TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
            DateTime blobStartDate,
            int nReqValues, DateTime reqStartDate, DateTime reqEndDate,
            Byte[] blobData, ref List<TimeSeriesValue> dateValueList)
        {
            // Let the private core method do all the real work.
            // We pass it the 'applyLimits' value of true.
            return ConvertBlobToList(timeStepUnit, timeStepQuantity,
                        blobStartDate, true, 
                        nReqValues, reqStartDate, reqEndDate,
                        blobData, ref dateValueList);
        }


        /// <summary>
        /// This private method creates a List of TimeSeriesValue objects from the given BLOB (byte array)
        /// of time series values.  The method takes parameters for a maximum number of values,
        /// an earliest date, and a latest date, so that only a portion of the BLOB might be 
        /// converted to the List.  This method is designed to do the operations that are common between
        /// the public methods ConvertBlobToListLimited() and ConvertBlobToListAll().
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.  If timeStepUnit is 
        /// Irregular, then this value is ignored.</param>
        /// <param name="blobStartDate">The DateTime value of the first time step in the BLOB. If 
        /// timeStepUnit is Irregular, then this value is ignored.</param>
        /// <param name="applyLimits">If value is true, then nReqValues, reqStartDate, and reqEndDate will be
        /// used to limit the portion of the BLOB that is converted to dateValueList.  If the value is false, then
        /// nReqValues, reqStartDate, and reqEndDate will be ignored.</param>
        /// <param name="nReqValues">The maximum number of time steps that should be added to dateValueList.
        /// If applyLimits==false, then this value is ignored.</param>
        /// <param name="reqStartDate">The earliest date that will be added to dateValueList.
        /// If applyLimits==false, then this value is ignored.</param>
        /// <param name="reqEndDate">The latest date that will be added to dateValueList.
        /// If applyLimits==false, then this value is ignored.</param>
        /// <param name="blobData">The BLOB (byte array) that this method will convert into a List</param>
        /// <param name="dateValueList">The List of TimeSeriesValues that this method will create from the BLOB.</param>
        /// <returns>The number of time steps added to dateValueList</returns>
        private unsafe int ConvertBlobToList(
            TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
            DateTime blobStartDate, Boolean applyLimits,
            int nReqValues, DateTime reqStartDate, DateTime reqEndDate,
            Byte[] blobData, ref List<TimeSeriesValue> dateValueList)
        {
            int nValuesRead = 0;

            if (timeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                // IRREGULAR TIME SERIES

                // If we're not limiting the output list (i.e., we're returning every time step from
                // the BLOB), then set the size of the intermediate array to match the size of the BLOB.
                if (applyLimits == false)
                    nReqValues = blobData.Length / sizeof(TSDateValueStruct);
                // Allocate an array of date/value pairs that TSBlobCoder method will fill
                TSDateValueStruct[] dateValueArray = new TSDateValueStruct[nReqValues];
                // Method in the TSBlobCoder class does the real work
                nValuesRead = TSBlobCoder.ConvertBlobToArrayIrregular(applyLimits, 
                                    nReqValues, reqStartDate, reqEndDate,
                                    blobData, dateValueArray);
                // resize the array so that the List that we make from it will have exactly the right size
                if(nValuesRead!=nReqValues)
                    Array.Resize<TSDateValueStruct>(ref dateValueArray, nValuesRead);
                // Convert the array of date/value pairs into the List that will be used by the caller
                dateValueList = dateValueArray
                        .Select(tsv => (TimeSeriesValue)tsv).ToList<TimeSeriesValue>();
            }
            else
            {
                // REGULAR TIME SERIES

                // If we're not limiting the output list (i.e., we're returning every time step from
                // the BLOB), then set the size of the intermediate array to match the size of the BLOB.
                if (applyLimits == false)
                    nReqValues = blobData.Length / sizeof(double);
                // Allocate an array of values that TSBlobCoder method will fill
                double[] valueArray = new double[nReqValues];
                // Method in the TSBlobCoder class does the real work
                nValuesRead = TSBlobCoder.ConvertBlobToArrayRegular(timeStepUnit, timeStepQuantity,
                                    blobStartDate, applyLimits,
                                    nReqValues, reqStartDate, reqEndDate,
                                    blobData, valueArray);
                // Allocate an array to hold the time series' date values
                DateTime[] dateArray = new DateTime[nValuesRead];
                // Fill the array with the date values corresponding to the time steps defined
                // for this time series in the database.
                TSDateCalculator.FillDateArray(timeStepUnit, timeStepQuantity, nValuesRead, dateArray, reqStartDate);
                // Allocate a List of date/value pairs that will be used by the caller
                dateValueList = new List<TimeSeriesValue>(nValuesRead);
                // Loop through all values, building the List of date/value pairs out of the
                // primitive array of dates and primitive array of values.
                int i;
                for (i = 0; i < nValuesRead; i++)
                {
                    dateValueList.Add(new TimeSeriesValue { Date = dateArray[i], Value = valueArray[i] });
                }
                nValuesRead = i;
            }
            return nValuesRead;
        }
        #endregion


        #region ConvertListToBlob() methods
        /// <summary>
        /// This method converts a List of TimeSeriesValue objects into a BLOB (byte array) of
        /// time series values.  The entire List is converted into the BLOB--i.e., the method
        /// does not take any parameters for limiting the size of the List that is created.
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="dateValueList">A List of TimeSeriesValue objects that will be converted to a BLOB</param>
        /// <returns>The BLOB (byte array) of time series values that was created from dateValueList</returns>
        public byte[] ConvertListToBlob(TSDateCalculator.TimeStepUnitCode timeStepUnit,
                            List<TimeSeriesValue> dateValueList)
        {
            int timeStepCount = dateValueList.Count;

            if (timeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                // IRREGULAR TIME SERIES

                // The method in TSBlobCoder can only process an array of TSDateValueStruct.  Therefore
                // we convert the List of objects to an Array of struct instances.
                TSDateValueStruct[] dateValueArray = dateValueList.Select(tsv => (TSDateValueStruct)tsv).ToArray();
                // Let the method in TSBlobCoder class do all the work
                return TSBlobCoder.ConvertArrayToBlobIrregular(timeStepCount, dateValueArray);
            }
            else
            {
                // REGULAR TIME SERIES

                // The method in TSBlobCoder can only process an array of double values.  Therefore
                // we convert the List of date/value objects to an Array values.
                double[] valueArray = dateValueList.Select(dv => dv.Value).ToArray();
                // Let the method in TSBlobCoder class do all the work
                return TSBlobCoder.ConvertArrayToBlobRegular(timeStepCount, valueArray);
            }
        }
        /// <summary>
        /// This method converts a List of TimeSeriesValue objects into a BLOB (byte array) of
        /// time series values and computes a checksum from the BLOB and its meta parameters.
        /// The entire List is converted into the BLOB--i.e., the method does not take any 
        /// parameters for limiting the size of the List that is created.  This method will
        /// throw exceptions if the meta-parameters that are passed in are not consistent
        /// with the List of TimeSeriesValue objects.
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="timeStepCount">The number of time steps stored in the BLOB</param>
        /// <param name="blobStartDate">Date of the first time step in the BLOB</param>
        /// <param name="blobEndDate">Date of the last time step in the BLOB</param>
        /// <param name="dateValueList">A List of TimeSeriesValue objects that will be converted to a BLOB</param>
        /// <param name="checksum">The checksum (a 16-byte array) that will be computed by the method</param>
        /// <returns>The BLOB (byte array) of time series values that was created from dateValueList</returns>
        public byte[] ConvertListToBlobWithChecksum(
                    TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    int timeStepCount, DateTime blobStartDate, DateTime blobEndDate,
                    List<TimeSeriesValue> dateValueList,
                    ref byte[] checksum)
        {

            throw new NotImplementedException();
            /*
            // Error checks
            if (dateValueList.Count != timeStepCount)
                throw new TSLibraryException(ErrCode.Enum.Checksum_Improper_Count);
            if (dateValueList[0].Date != blobStartDate)
                throw new TSLibraryException(ErrCode.Enum.Checksum_Improper_StartDate);
            if (dateValueList.Last().Date != blobEndDate)
                throw new TSLibraryException(ErrCode.Enum.Checksum_Improper_EndDate);
            
            // Convert the List dateValueList into a BLOB.  The sibling method does all the work.
            byte[] blobData = ConvertListToBlob(timeStepUnit, dateValueList);
            // Method in TSBlobCoder class computes the checksum
            checksum = TSBlobCoder.ComputeChecksum(timeStepUnit, timeStepQuantity,
                        timeStepCount, blobStartDate, blobEndDate, blobData);
            
            return blobData;*/
        }
        #endregion


        #region Public methods for XML import

        /// <summary>
        /// This method reads the given XML file and stores each time series that is defined in the
        /// XML file to a new TSImport object that is added to the given List of TSImport objects.
        /// </summary>
        /// <param name="xmlFileName">The file name (with path) of an XML file that defines one or more time series to import</param>
        /// <param name="tsImportList">A List of TSImport objects that the method adds to.  One item is added to the List 
        /// for each time series that is processed in the XML file.  The List must already be instantiated before calling 
        /// this method.  The method does not change any items that are already in the List.</param>
        /// <returns>The number of time series records that were successfully read and added to tsImportList</returns>
        public int XmlImport(String xmlFileName, List<TSImport> tsImportList)
        {
            // Construct new TSXml object without SqlConnection object and table name
            TSXml tsXml = new TSXml();
            // Method in the TSXML object does all the work
            return tsXml.ReadAndStore(xmlFileName, null, tsImportList, false, true);
        }
        /// <summary>
        /// This method reads the given XML file and stores any time series that are defined in the
        /// XML file to the database using the given database connection number and database table name.
        /// Each time series is also stored in a new TSImport object that is addded to the given List 
        /// of TSImport objects.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write to the database</param>
        /// <param name="tableName">The name of the database table that time series will be written to</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="xmlFileName">The file name (with path) of an XML file that defines one or more time series to import</param>
        /// <param name="tsImportList">A List of TSImport objects that the method adds to.  One item is added to the List 
        /// for each time series that is processed in the XML file.  The List must already be instantiated before calling 
        /// this method.  The method does not change any items that are already in the List.</param>
        /// <returns>The number of time series records that were successfully stored</returns>
        public int XmlImportAndSaveToDB(int connectionNumber, String tableName, String traceTableName,
                        String xmlFileName, List<TSImport> tsImportList)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TSXml object with SqlConnection object and table name
            TSXml tsXml = new TSXml(connx, tableName, traceTableName);
            // Method in the TSXML object does all the work
            return tsXml.ReadAndStore(xmlFileName, null, tsImportList, true, true);
        }
        #endregion


        #region Public methods for computing date values
        DateTime IncrementDate(DateTime startDate, TSDateCalculator.TimeStepUnitCode unit,
                    short stepSize, int numSteps)
        {
            return TSDateCalculator.IncrementDate(startDate, unit, stepSize, numSteps);
        }
        public void FillDateArray(
                    TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    int nReqValues, DateTime[] dateArray, DateTime reqStartDate)
        {
            TSDateCalculator.FillDateArray(timeStepUnit, timeStepQuantity,
                                nReqValues, dateArray, reqStartDate);
        }
        public void FillSeriesDateArray(
                    int connectionNumber, String tableName, String traceTableName, int id,
                    int nReqValues, DateTime[] dateArray, DateTime reqStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName, traceTableName);

            ts.FillDateArray(id, nReqValues, dateArray, reqStartDate);
        }
        public int CountTimeSteps(DateTime startDate, DateTime endDate,
            short unit, short stepSize)
        {
            return TSDateCalculator.CountSteps(startDate, endDate, (TSDateCalculator.TimeStepUnitCode)unit, stepSize);
        }
        #endregion


        #region Public Methods for Database Connection
        /// <summary>
        /// Opens a new connection for the time series library to use.  The new connection 
        /// is added to a list and assigned a serial number within the list.  The method returns
        /// the serial number of the new connection.
        /// </summary>
        /// <param name="connectionString">The connection string used to open the connection.</param>
        /// <returns>The serial number that was automatically assigned to the new connection.</returns>
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
        /// <summary>
        /// Returns the SqlConnection object corresponding to the given connection number.
        /// </summary>
        /// <param name="connectionNumber">serial number of the connection within the collection</param>
        /// <returns>The SqlConnection object corresponding to the given connection number</returns>
        public SqlConnection GetConnectionFromId(int connectionNumber)
        {
            SqlConnection connx;
            try
            {
                connx = ConnxObject.TSConnectionsCollection[connectionNumber];
            }
            catch
            {
                throw new TSLibraryException(ErrCode.Enum.Connection_Not_Found,
                                String.Format("TimeSeriesLibrary does not have an open connection number {0}", 
                                connectionNumber));
            }
            return connx;
        }
        #endregion


        #region Public methods for READING time series from database

        /// <summary>
        /// This method reads the time series matching the given ID, using the given
        /// database connection number and database table name, and stores the values into
        /// the given List of double-precision floats.  The method starts populating the
        /// list at the given start date, filling in no more than the number of values
        /// that are requested.  An exception is thrown if the time series is not found
        /// to have regular time steps.
        /// 
        /// This method is designed to be used from managed code such as C#.  This method
        /// does not return values for the dates of each time step.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to read the time series</param>
        /// <param name="tableName">The name of the database table that contains the time series</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="id">ID value identifying the time series to read</param>
        /// <param name="traceNumber">number of the trace to read</param>
        /// <param name="nReqValues">The maximum number of values that the method will fill into the list</param>
        /// <param name="valueList">The List that the method will fill</param>
        /// <param name="reqStartDate">The earliest date that the method will enter into the list</param>
        /// <param name="reqEndDate">The latest date that the method will enter into the list</param>
        /// <returns>The number of values that the method added to the list</returns>
        public int ReadValuesRegular(
                int connectionNumber, String tableName, String traceTableName, int id, int traceNumber,
                int nReqValues, ref List<double> valueList, DateTime reqStartDate, DateTime reqEndDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName, traceTableName);

            // allocate an array of doubles, since the ReadValuesRegular method works from arrays (not Lists)
            double[] valueArray = new double[nReqValues];
            // The real work gets done in ReadValuesRegular method of the TS object
            int ret = ts.ReadValuesRegular(id, traceNumber, nReqValues, valueArray, reqStartDate, reqEndDate);
            // convert the array that ReadValuesRegular filled into a List
            valueList = valueArray.ToList<double>();
            return ret;
        }

        /// <summary>
        /// This method reads the time series matching the given ID, using the given
        /// database connection number and database table name, and stores the values into
        /// the given list of TimeSeriesValue objects (date/value pairs).  The method will 
        /// read regular or irregular time series.
        /// 
        /// This method is designed to be used from managed code such as C#.  This
        /// method makes the list as long as is needed to store every time step value that
        /// is stored in the database.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to read the time series</param>
        /// <param name="tableName">The name of the database table that contains the time series</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="id">ID value identifying the time series to read</param>
        /// <param name="traceNumber">number of the trace to read</param>
        /// <param name="dateValueList">The list that the method will fill</param>
        /// <returns>The number of values that the method added to the list</returns>
        // usage: for GUI to retrieve an entire time series.  The length of the list is allocated in this method.
        public int ReadAllDatesValues(
                int connectionNumber, String tableName, String traceTableName, int id, int traceNumber,
                ref List<TimeSeriesValue> dateValueList)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName, traceTableName);

            // Read the meta-parameters of the time series so that we'll know its date and list-length limits
            if (!ts.IsInitialized) ts.Initialize(id);

            // Let the sister function do the rest of the work.  This function will limit the length
            // of the TimeSeriesValue List that it returns, but we tell it that the limits are the
            // beginning, end, and length of the timeseries as stored in the database.  Therefore, 
            // it will return the entire time series as found in the database.
            return ReadLimitedDatesValues(connectionNumber, tableName, traceTableName, id, traceNumber,
                        ts.TimeStepCount, ref dateValueList, ts.BlobStartDate, ts.BlobEndDate);
        }

        /// <summary>
        /// This method reads the time series matching the given ID, using the given
        /// database connection number and database table name, and stores the values into
        /// the given List of TimeSeriesValue objects (date/value pairs).  The method starts populating
        /// the array at the given start date, filling in no more than the number of values
        /// that are requested, and will not add any values that come after the given end date.
        /// The method will read regular or irregular time series.
        /// 
        /// This method is designed to be used from managed code such as C#.  This
        /// method will not add any values to the list that are earlier than the given start date,
        /// later than the given end date, or that exceed the given list length.  The given
        /// list is instantiated by the method.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to read the time series</param>
        /// <param name="tableName">The name of the database table that contains the time series</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="id">ID value identifying the time series to read</param>
        /// <param name="traceNumber">number of the trace to read</param>
        /// <param name="nReqValues">The maximum number of values that the method will fill into the array.
        /// If 0 is given, then no maximum number of values is applied.</param>
        /// <param name="dateValueList">The list that the method will fill</param>
        /// <param name="reqStartDate">The earliest date that the method will enter into the array</param>
        /// <param name="reqEndDate">The latest date that the method will enter into the array</param>
        /// <returns>The number of values that the method added to the list</returns>
        public int ReadLimitedDatesValues(
                int connectionNumber, String tableName, String traceTableName, int id, int traceNumber,
                int nReqValues, ref List<TimeSeriesValue> dateValueList, DateTime reqStartDate, DateTime reqEndDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName, traceTableName);

            int nValuesRead = 0;
            // Read the meta-parameters of the time series so that we'll know if it's regular or irregular
            if(!ts.IsInitialized) ts.Initialize(id);

            // Caller has the option of passing nReqValues==0, indicating that there should be no
            // limit on the list length
            if (nReqValues == 0) nReqValues = ts.TimeStepCount;

            // The operations will differ for regular and irregular time series
            if (ts.TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                // IRREGULAR TIME SERIES

                // Allocate an array of date/value pairs for ReadValuesIrregular to fill
                TSDateValueStruct[] dateValueArray = new TSDateValueStruct[nReqValues];
                // Read the date/value array from the database
                nValuesRead = ts.ReadValuesIrregular(id, traceNumber, nReqValues, dateValueArray, reqStartDate, reqEndDate);
                // resize the array so that the List that we make from it will have exactly the right size
                if(nValuesRead!=nReqValues)
                    Array.Resize<TSDateValueStruct>(ref dateValueArray, nValuesRead);
                // Convert the array of date/value pairs into the List that will be used by the caller
                dateValueList = dateValueArray
                        .Select(tsv => (TimeSeriesValue)tsv).ToList<TimeSeriesValue>();
            }
            else
            {
                // REGULAR TIME SERIES

                // Allocate an array to hold the time series' data values
                double[] valueArray = new double[nReqValues];
                // Read the data values from the database
                nValuesRead = ts.ReadValuesRegular(id, traceNumber, nReqValues, valueArray, reqStartDate, reqEndDate);
                // Allocate an array to hold the time series' date values
                DateTime[] dateArray = new DateTime[nValuesRead];
                // Fill the array with the date values corresponding to the time steps defined
                // for this time series in the database.
                ts.FillDateArray(id, nValuesRead, dateArray, reqStartDate);
                // Allocate a List of date/value pairs that will be used by the caller
                dateValueList = new List<TimeSeriesValue>(nValuesRead);
                // Loop through all values, building the List of date/value pairs out of the
                // primitive array of dates and primitive array of values.
                int i;
                for (i = 0; i < nValuesRead; i++)
                {
                    dateValueList.Add(new TimeSeriesValue
                                { Date = dateArray[i], Value = valueArray[i] });
                }
                nValuesRead = i;
            }
            return nValuesRead;
        }

        #endregion


        #region Public methods for WRITING time series to database

        /// <summary>
        /// This method saves the given time series list as a new database record, using the given
        /// database connection number and database table name.  The method only writes regular
        /// time series.  The given list is expressed only as values without explicit dates, so
        /// the method requires that the dates be defined by a given time step unit, quanitity
        /// of units per time step, the number of time steps in the array, and the date of the first
        /// time step.
        /// 
        /// This method is designed to be used from managed code such as C#.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write the time series</param>
        /// <param name="tableName">The name of the database table that time series will be written to</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, or Year</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="nOutValues">The number of values in the list to be written to the database</param>
        /// <param name="valueList">list of time series values to be written to database</param>
        /// <param name="outStartDate">date of the first time step in the series</param>
        /// <returns>ID value identifying the database record that was created</returns>
        public int WriteValuesRegular(
                    int connectionNumber, String tableName, String traceTableName, int traceNumber,
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, List<double> valueList, DateTime outStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName, traceTableName);
            // Call the method in the TS object that does all the work.
            return ts.WriteValuesRegular(true, null, traceNumber, timeStepUnit, timeStepQuantity, nOutValues, 
                            outStartDate, valueList.ToArray<double>());
        }
        
        /// <summary>
        /// This method saves the given time series as a new database record, using the given
        /// database connection number and database table name.  The given time series can be
        /// either regular or irregular.  The caller must pass in the parameters for the time step
        /// unit type, and the number of units per time step.  If the series is irregular, then
        /// the parameter for the number of units per time step is ignored.  The time series is
        /// given as a list of date/value pairs, so it determines all date parameters from the
        /// list itself.
        /// 
        /// This method is designed to be used from managed code such as C#.  The method can write
        /// either regular or irregular time series.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write the time series</param>
        /// <param name="tableName">The name of the database table that time series will be written to</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.  If the timeStepUnit is Irregular, then
        /// the method will ignore the timeStepQuantity value.</param>
        /// <param name="dateValueList">the list of time series date/value pairs to be written to database</param>
        /// <returns>ID value identifying the database record that was created</returns>
        public int WriteValues(
                    int connectionNumber, String tableName, String traceTableName, int traceNumber,
                    short timeStepUnit, short timeStepQuantity,
                    List<TimeSeriesValue> dateValueList)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName, traceTableName);

            // The TS object's methods will require certain parameter values which we can
            // determine from the list of date/value pairs.
            int nOutValues = dateValueList.Count;
            DateTime outStartDate = dateValueList[0].Date;

            if ((TSDateCalculator.TimeStepUnitCode)timeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                // IRREGULAR TIME SERIES

                // A method in the TS object does all the work.  We pass it an array of date/value pairs
                // that is equivalent to the List that we received from the caller.
                return ts.WriteValuesIrregular(true, null, traceNumber, nOutValues, 
                            dateValueList.Select(tsv => (TSDateValueStruct)tsv).ToArray());
            }
            else
            {
                // REGULAR TIME SERIES

                // Create an array of values that is extracted from the List of date/value pairs
                // that we received from the caller.  This is all that the method for storing
                // regular time series will need to (and be able to) process.
                double[] valueArray = dateValueList.Select(dv => dv.Value).ToArray();
                // A method in the TS object does all the work.
                return ts.WriteValuesRegular(true, null, traceNumber, 
                                timeStepUnit, timeStepQuantity, nOutValues, outStartDate, valueArray);
            }
        }

        #endregion


        #region Public methods for DELETING time series from database

        /// <summary>
        /// This method deletes a record for a single time series from the database, using the
        /// given database connection number and database table name.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to access the time series</param>
        /// <param name="tableName">The name of the database table that time series will be deleted from</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="id">The ID identifying the record to delete</param>
        /// <returns>true if a record was deleted, false if no records were deleted</returns>
        public bool DeleteSeries(
                int connectionNumber, String tableName, String traceTableName, int id)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName, traceTableName);

            return ts.DeleteSeries(id);
        }

        /// <summary>
        /// This method deletes any records from the table which match the given WHERE
        /// clause of a SQL command, using the given database connection number
        /// and database table name.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to access the time series</param>
        /// <param name="tableName">The name of the database table that time series will be deleted from</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="whereClause">The WHERE clause of a SQL command, not including the word WHERE.
        /// For example, to delete delete all records where Id > 55, use the text "Id > 55".</param>
        /// <returns>true if one or more records were deleted, false if no records were deleted</returns>
        public bool DeleteMatchingSeries(
                int connectionNumber, String tableName, String traceTableName, String whereClause)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName, traceTableName);

            return ts.DeleteMatchingSeries(whereClause);
        }

        #endregion

    }
}
