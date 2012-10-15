using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;  // for the COM callable wrapper

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This interface gives the signatures of methods in the ComTSLibrary class
    /// for the COM callable wrapper.  The interface should have the same name
    /// as the class that implements it, with _ added to the beginning.
    /// </summary>
    [Guid("752F96CB-377C-41fe-8FB0-2045C69DA0D3")]
    [InterfaceType(ComInterfaceType.InterfaceIsDual)]
    [ComVisible(true)]
    public unsafe interface _ComTSLibrary
    {
        [ComVisible(true)]
        bool GetHasError();
        [ComVisible(true)]
        sbyte *GetErrorMessage();
        [ComVisible(true)]
        int OpenConnection(sbyte *pConnectionString);
        [ComVisible(true)]
        void CloseConnection(int connectionNumber);
        [ComVisible(true)]
        System.Data.SqlClient.SqlConnection GetConnectionFromId(int connectionNumber);

        [ComVisible(true)]
        int ReadDatesValues(int connectionNumber, sbyte *pParamTableName, sbyte *pTraceTableName, int id, int traceNumber, int nReqValues, ref TSDateValueStruct[] dateValueArray, DateTime reqStartDate, DateTime reqEndDate);
        [ComVisible(true)]
        int ReadValuesRegular(int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName, int id, int traceNumber, int nReqValues, double[] valueArray, DateTime reqStartDate, DateTime reqEndDate);

        [ComVisible(true)]
        int WriteParametersIrregular(int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName,
                        int nOutValues, DateTime outStartDate, DateTime outEndDate,
                        sbyte *pExtraParamNames, sbyte *pExtraParamValues);
        [ComVisible(true)]
        int WriteParametersRegular(int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName,
                        short timeStepUnit, short timeStepQuantity, int nOutValues, DateTime outStartDate,
                        sbyte *pExtraParamNames, sbyte *pExtraParamValues);
        [ComVisible(true)]
        void WriteTraceIrregular(int connectionNumber, sbyte* pParamTableName, sbyte *pTraceTableName,
                        int id, int traceNumber, TSDateValueStruct[] dateValueArray);
        [ComVisible(true)]
        void WriteTraceRegular(int connectionNumber, sbyte* pParamTableName, sbyte *pTraceTableName, int id, int traceNumber, double[] valueArray);

        [ComVisible(true)]
        bool DeleteMatchingSeries(int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName, sbyte* pWhereClause);
        [ComVisible(true)]
        bool DeleteSeries(int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName, int id);

        //[ComVisible(true)]
        //int XmlImport(int connectionNumber, string paramTableName, String traceTableName, string xmlFileName);
        ////[ComVisible(true)]
        ////int XmlImportWithList(int connectionNumber, string paramTableName, string xmlFileName, System.Collections.Generic.List<TSImport> tsImportList);

        //[ComVisible(true)]
        //DateTime IncrementDate(DateTime startDate, short unit, short stepSize, int numSteps);
        //[ComVisible(true)]
        //void FillDateArray(short timeStepUnit, short timeStepQuantity, int nReqValues, DateTime[] dateArray, DateTime reqStartDate);
        //[ComVisible(true)]
        //void FillSeriesDateArray(int connectionNumber, string paramTableName, String traceTableName, int id, int nReqValues, DateTime[] dateArray, DateTime reqStartDate);
        //[ComVisible(true)]
        //int CountTimeSteps(DateTime startDate, DateTime endDate, short unit, short stepSize);
    } 
    
    /// <summary>
    /// This class contains the callable functions of the library for COM code.
    /// As such, it actually wraps an object of the TSLibrary class, in which
    /// the callable functions are designed to be called by .NET code.
    /// The class implements an interface in order to use the COM callable wrapper.
    /// </summary>
    [Guid("6AC55A17-C27E-4a53-9D2A-8A9F9369070D")]
    [ClassInterface(ClassInterfaceType.None)]
    [ComVisible(true)]
    public unsafe class ComTSLibrary : _ComTSLibrary
    {
        /// <summary>
        /// The ComTSLibrary class wraps an instance of the TSLibrary class.
        /// </summary>
        private TSLibrary TSLib;
        /// <summary>
        /// TSConnection object maintains a list of connections that have been opened by the library
        /// </summary>
        public TSConnection ConnxObject;

        #region Error Handling
        private bool _hasError = false;
        public bool GetHasError()
        {
            //GC.Collect();
            return _hasError;
        }
        private byte[] _errorMessageSbyte;
        private String _errorMessage;
        private String ErrorMessage
        {
            get { return _errorMessage; }
            set
            {
                _errorMessage = value;
                _hasError = true;
                _errorMessageSbyte = System.Text.Encoding.ASCII.GetBytes(_errorMessage);
            }
        }
        public sbyte* GetErrorMessage()
        {
            fixed (byte* pErrorMessage = _errorMessageSbyte)
            {
                return (sbyte*)pErrorMessage;
            }
        } 
        #endregion

        #region Constructor
        /// <summary>
        /// Class constructor.
        /// In accord with the COM callable wrapper, this constructor can not take any parameters,
        /// and there can be no other constructors besides this one.
        /// </summary>
        public ComTSLibrary()
        {
            // The ComTSLibrary class wraps an instance of the TSLibrary class
            TSLib = new TSLibrary();
            // This class's ConnxObject field is simply a reference to the ConnxObject
            // field in the TSLibrary object that this class wraps.
            ConnxObject = TSLib.ConnxObject;
        } 
        #endregion


        #region Public Methods for Connection
        /// <summary>
        /// Opens a new connection for the time series library to use.  The new connection 
        /// is added to a list and assigned a serial number within the list.  The method returns
        /// the serial number of the new connection.
        /// </summary>
        /// <param name="connectionString">The connection string used to open the connection.</param>
        /// <returns>The serial number that was automatically assigned to the new connection.</returns>
        [ComVisible(true)]
        public int OpenConnection(sbyte *pConnectionString)
        {
            // Convert from simple character byte array to .Net String object
            String connectionString = new String(pConnectionString);

            try
            {
                // let the sibling method in the wrapped TSLibrary object contain the logic
                return TSLib.OpenConnection(connectionString);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
                return 0;
            }
        }
        /// <summary>
        /// Closes the connection identified with the given serial number.  When the
        /// connection is closed, it is removed from the list of connections available to
        /// the time series library, and the serial number no longer refers to this
        /// connection.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection to be closed</param>
        [ComVisible(true)]
        public void CloseConnection(int connectionNumber)
        {
            try
            {
                // let the sibling method in the wrapped TSLibrary object contain the logic
                TSLib.CloseConnection(connectionNumber);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
            }
        }
        /// <summary>
        /// Returns the SqlConnection object corresponding to the given connection number.
        /// </summary>
        /// <param name="connectionNumber">serial number of the connection within the collection</param>
        /// <returns>The SqlConnection object corresponding to the given connection number</returns>
        [ComVisible(true)]
        public SqlConnection GetConnectionFromId(int connectionNumber)
        {
            try
            {
                // let the sibling method in the wrapped TSLibrary object contain the logic
                return TSLib.GetConnectionFromId(connectionNumber);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
                return null;
            }
        }
        #endregion


        #region Public methods for READING time series
        /// <summary>
        /// This method reads the time series matching the given ID, using the given
        /// database connection number and database table name, and stores the values into
        /// the given array of double-precision floats.  The method starts populating the
        /// array at the given start date, filling in no more than the number of values
        /// that are requested.  An exception is thrown if the time series is not found
        /// to have regular time steps.
        /// 
        /// This method is designed to be used from unmanaged code such as C/C++.  This method
        /// does not return values for the dates of each time step.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to read the time series</param>
        /// <param name="paramTableName">The name of the database table that contains the time series</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="id">ID value identifying the time series to read</param>
        /// <param name="traceNumber">number of the trace to read</param>
        /// <param name="nReqValues">The maximum number of values that the method will fill into the array</param>
        /// <param name="valueArray">The array that the method will fill</param>
        /// <param name="reqStartDate">The earliest date that the method will enter into the array</param>
        /// <param name="reqEndDate">The latest date that the method will enter into the array</param>
        /// <returns>The number of values that the method added to the array</returns>
        // usage: for onevar to read model output, b/c it does not need dates for each timeseries
        [ComVisible(true)]
        public int ReadValuesRegular(
                int connectionNumber, sbyte *pParamTableName, sbyte* pTraceTableName, int id, int traceNumber,
                int nReqValues, double[] valueArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            try
            {
                // Convert from simple character byte array to .Net String object
                String paramTableName = new String(pParamTableName);
                String traceTableName = new String(pTraceTableName);
                // Get the connection that we'll pass along.
                SqlConnection connx = GetConnectionFromId(connectionNumber);
                // Construct new TS object with SqlConnection object and table name
                TS ts = new TS(connx, paramTableName, traceTableName);

                return ts.ReadValuesRegular(id, traceNumber, nReqValues, valueArray, reqStartDate, reqEndDate);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
                return 0;
            }

        }
        /// <summary>
        /// This method reads the time series matching the given ID, using the given
        /// database connection number and database table name, and stores the values into
        /// the given array of TSDateValueStruct structs (date/value pairs).  The method starts populating the
        /// array at the given start date, filling in no more than the number of values
        /// that are requested.  The method will read regular or irregular time series.
        /// 
        /// This method is designed to be used from unmanaged code such as C/C++.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to read the time series</param>
        /// <param name="paramTableName">The name of the database table that contains the time series</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="id">ID value identifying the time series to read</param>
        /// <param name="traceNumber">number of the trace to read</param>
        /// <param name="nReqValues">The maximum number of values that the method will fill into the array</param>
        /// <param name="dateValueArray">The array that the method will fill</param>
        /// <param name="reqStartDate">The earliest date that the method will enter into the array</param>
        /// <param name="reqEndDate">The latest date that the method will enter into the array</param>
        /// <returns>The number of values that the method added to the array</returns>
        // usage: general model/onevar input
        [ComVisible(true)]
        public int ReadDatesValues(
                int connectionNumber, sbyte *pParamTableName, sbyte *pTraceTableName, int id, int traceNumber,
                int nReqValues, ref TSDateValueStruct[] dateValueArray, DateTime reqStartDate, DateTime reqEndDate)
        {
            try
            {
                // Convert from simple character byte array to .Net String object
                String paramTableName = new String(pParamTableName);
                String traceTableName = new String(pTraceTableName);
                // Get the connection that we'll pass along.
                SqlConnection connx = GetConnectionFromId(connectionNumber);
                // Construct new TS object with SqlConnection object and table name
                TS ts = new TS(connx, paramTableName, traceTableName);

                int nValuesRead = 0;
                // Read the parameters of the time series so that we'll know if it's regular or irregular
                if (!ts.IsInitialized) ts.Initialize(id);

                // The operations will differ for regular and irregular time series
                if (ts.TimeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
                {
                    // IRREGULAR TIME SERIES

                    // Read the date/value array from the database
                    nValuesRead = ts.ReadValuesIrregular(id, traceNumber, nReqValues, dateValueArray, reqStartDate, reqEndDate);
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
                    // Loop through all values, filling the array of date/value pairs from the
                    // primitive array of dates and primitive array of values.
                    int i;
                    for (i = 0; i < nValuesRead; i++)
                    {
                        dateValueArray[i].Date = dateArray[i];
                        dateValueArray[i].Value = valueArray[i];
                        // So far we have ignored the requested end date.  However, at this
                        // stage we won't make the list any longer than was requested by the caller.
                        if (dateValueArray[i].Date >= reqEndDate)
                        {
                            i++;
                            break;
                        }
                    }
                    nValuesRead = i;
                }
                return nValuesRead;
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
                return 0;
            }
        }
        #endregion


        #region Public methods for WRITING time series
        /// <summary>
        /// This method saves the given time series parameters to a new database record, using the given
        /// database connection number and database table name.  The method does not store anything to 
        /// the corresponding trace table in the database.  The method only writes regular time series.
        /// 
        /// This method is designed to be used from unmanaged code such as C/C++.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write the time series</param>
        /// <param name="paramTableName">The name of the database table that time series will be written to</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, or Year</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="nOutValues">The number of values in the array to be written to the database</param>
        /// <param name="outStartDate">date of the first time step in the series</param>
        /// <param name="extraParamNames">A list of field names that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamValues.</param>
        /// <param name="extraParamValues">A list of field values that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamNames.</param>
        /// <returns>ID value identifying the database record that was created</returns>
        [ComVisible(true)]
        public int WriteParametersRegular(
                    int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName,
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, DateTime outStartDate,
                    sbyte *pExtraParamNames, sbyte *pExtraParamValues)
        {
            try
            {
                // Convert from simple character byte array to .Net String object
                String paramTableName = new String(pParamTableName);
                String traceTableName = new String(pTraceTableName);
                String extraParamNames = new String(pExtraParamNames);
                String extraParamValues = new String(pExtraParamValues);
                // Get the connection that we'll pass along.
                SqlConnection connx = GetConnectionFromId(connectionNumber);
                // Construct new TS object with SqlConnection object and table name
                TS ts = new TS(connx, paramTableName, traceTableName);

                return ts.WriteParametersRegular(true, null, timeStepUnit, timeStepQuantity,
                                nOutValues, outStartDate,
                                extraParamNames, extraParamValues);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
                return 0;
            }
        }
        /// <summary>
        /// This method saves the given time series array to a new database record, using the given
        /// database connection number and database table name.  The method creates a new record in
        /// the trace table, but does not create a record in the parameters table.  The method does
        /// ensure that the checksum in the parameters table is updated.  The method only writes regular
        /// time series.  The given array is expressed only as values without explicit dates.
        /// 
        /// This method is designed to be used from unmanaged code such as C/C++.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write the time series</param>
        /// <param name="paramTableName">The name of the database table that time series will be written to</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="id">the primary key value of the record in the parameters table that this trace is associated with</param>
        /// <param name="traceNumber">number of the trace to write</param>
        /// <param name="valueArray">array of time series values to be written to database</param>
        /// <returns>ID value identifying the database record that was created</returns>
        [ComVisible(true)]
        public void WriteTraceRegular(
                    int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName, 
                    int id, int traceNumber, double[] valueArray)
        {
            try
            {
                // Convert from simple character byte array to .Net String object
                String paramTableName = new String(pParamTableName);
                String traceTableName = new String(pTraceTableName);
                // Get the connection that we'll pass along.
                SqlConnection connx = GetConnectionFromId(connectionNumber);
                // Construct new TS object with SqlConnection object and table name
                TS ts = new TS(connx, paramTableName, traceTableName);

                ts.WriteTraceRegular(id, true, null, traceNumber, valueArray);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
            }
        }
        /// <summary>
        /// This method saves the given time series parameters to a new database record, using the given
        /// database connection number and database table name.  The method does not store anything to 
        /// the corresponding trace table in the database.  The method only writes irregular time series.
        /// 
        /// This method is designed to be used from unmanaged code such as C/C++.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write the time series</param>
        /// <param name="paramTableName">The name of the database table that time series will be written to</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="nOutValues">The number of values in the array to be written to the database</param>
        /// <param name="outStartDate">date of the first time step in the series</param>
        /// <param name="outEndDate">date of the last time step in the series</param>
        /// <param name="extraParamNames">A list of field names that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamValues.</param>
        /// <param name="extraParamValues">A list of field values that the the method should fill, in addition
        /// to the fields that the TimeSeriesLibrary is designed to maintain.  Every item in this list must
        /// be matched to an item in extraParamNames.</param>
        /// <returns>ID value identifying the database record that was created</returns>
        [ComVisible(true)]
        public int WriteParametersIrregular(
                    int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName, 
                    int nOutValues, DateTime outStartDate, DateTime outEndDate,
                    sbyte *pExtraParamNames, sbyte *pExtraParamValues)
        {
            try
            {
                // Convert from simple character byte array to .Net String object
                String paramTableName = new String(pParamTableName);
                String traceTableName = new String(pTraceTableName);
                String extraParamNames = new String(pExtraParamNames);
                String extraParamValues = new String(pExtraParamValues);
                // Get the connection that we'll pass along.
                SqlConnection connx = GetConnectionFromId(connectionNumber);
                // Construct new TS object with SqlConnection object and table name
                TS ts = new TS(connx, paramTableName, traceTableName);

                return ts.WriteParametersIrregular(true, null, nOutValues, outStartDate, outEndDate,
                                extraParamNames, extraParamValues);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
                return 0;
            }
        }
        /// <summary>
        /// This method saves the given time series array to a new database record, using the given
        /// database connection number and database table name.  The method creates a new record in
        /// the trace table, but does not create a record in the parameters table.  The method does
        /// ensure that the checksum in the parameters table is updated.  The method only writes irregular
        /// time series.  The given array is expressed only as values without explicit dates.
        /// 
        /// This method is designed to be used from unmanaged code such as C/C++.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write the time series</param>
        /// <param name="paramTableName">The name of the database table that time series will be written to</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="id">the primary key value of the record in the parameters table that this trace is associated with</param>
        /// <param name="traceNumber">number of the trace to write</param>
        /// <param name="dateValueArray">the array of time series date/value pairs to be written to database</param>
        /// <returns>ID value identifying the database record that was created</returns>
        [ComVisible(true)]
        public void WriteTraceIrregular(
                    int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName, 
                    int id, int traceNumber, TSDateValueStruct[] dateValueArray)
        {
            try
            {
                // Convert from simple character byte array to .Net String object
                String paramTableName = new String(pParamTableName);
                String traceTableName = new String(pTraceTableName);
                // Get the connection that we'll pass along.
                SqlConnection connx = GetConnectionFromId(connectionNumber);
                // Construct new TS object with SqlConnection object and table name
                TS ts = new TS(connx, paramTableName, traceTableName);

                ts.WriteTraceIrregular(id, true, null, traceNumber, dateValueArray);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
            }
        }
        #endregion


        #region Public methods for DELETING time series
        /// <summary>
        /// This method deletes a record for a single time series from the database, using the
        /// given database connection number and database table name.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to access the time series</param>
        /// <param name="paramTableName">The name of the database table that time series will be deleted from</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="id">The ID identifying the record to delete</param>
        /// <returns>true if a record was deleted, false if no records were deleted</returns>
        [ComVisible(true)]
        public bool DeleteSeries(
                int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName, int id)
        {
            try
            {
                // Convert from simple character byte array to .Net String object
                String paramTableName = new String(pParamTableName);
                String traceTableName = new String(pTraceTableName);
                // Get the connection that we'll pass along.
                SqlConnection connx = GetConnectionFromId(connectionNumber);
                // Construct new TS object with SqlConnection object and table name
                TS ts = new TS(connx, paramTableName, traceTableName);

                return ts.DeleteSeries(id);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
                return false;
            }
        }
        /// <summary>
        /// This method deletes any records from the table which match the given WHERE
        /// clause of a SQL command, using the given database connection number
        /// and database table name.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to access the time series</param>
        /// <param name="paramTableName">The name of the database table that time series will be deleted from</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="whereClause">The WHERE clause of a SQL command, not including the word WHERE.
        /// For example, to delete delete all records where Id > 55, use the text "Id > 55".</param>
        /// <returns>true if one or more records were deleted, false if no records were deleted</returns>
        [ComVisible(true)]
        public bool DeleteMatchingSeries(
                int connectionNumber, sbyte* pParamTableName, sbyte* pTraceTableName, sbyte* pWhereClause)
        {
            try
            {
                // Convert from simple character byte array to .Net String object
                String paramTableName = new String(pParamTableName);
                String traceTableName = new String(pTraceTableName);
                String whereClause = new String(pWhereClause);
                // Get the connection that we'll pass along.
                SqlConnection connx = GetConnectionFromId(connectionNumber);
                // Construct new TS object with SqlConnection object and table name
                TS ts = new TS(connx, paramTableName, traceTableName);

                return ts.DeleteMatchingSeries(whereClause);
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
                return false;
            }
        }
        #endregion


        #region Public methods for XML import

        /// <summary>
        /// This method reads the given XML file and stores any time series that are defined in the
        /// XML file to the database using the given database connection number and database table name.
        /// For each time series that the method adds to the database, it adds a TSImport object to the
        /// given List of TSImport objects.  Each TSImport object records fields that were read from the
        /// XML file, but which TSLibrary does not process.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write to the database</param>
        /// <param name="paramTableName">The name of the database table that time series will be written to</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="xmlFileName">The file name (with path) of an XML file that defines one or more time series to import</param>
        /// <param name="tsImportList">A List of TSImport objects that the method adds to--one item for each time series
        /// that is saved to the database.  The List must already be instantiated before calling this method.
        /// The method does not change any items that are already in the List.</param>
        /// <returns>The number of time series records that were successfully stored</returns>
        //[ComVisible(true)]
        // TODO: this is not COM friendly due to the List<> object
        public int XmlImportWithList(int connectionNumber, String paramTableName, String traceTableName, String xmlFileName,
                        List<TSImport> tsImportList)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TSXml object with SqlConnection object and table name
            TSXml tsXml = new TSXml(connx, paramTableName, traceTableName);

            return tsXml.ReadAndStore(xmlFileName, null, tsImportList, true, false);
        }
        /// <summary>
        /// This method reads the given XML file and stores any time series that are defined in the
        /// XML file to the database using the given database connection number and database table name.
        /// The method does not process a List of TSImport objects.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection that is used to write to the database</param>
        /// <param name="paramTableName">The name of the database table that time series will be written to</param>
        /// <param name="traceTableName">The name of the database table that stores the BLOB for a single trace</param>
        /// <param name="xmlFileName">The file name (with path) of an XML file that defines one or more time series to import</param>
        /// <returns>The number of time series records that were successfully stored</returns>
        [ComVisible(true)]
        public int XmlImport(int connectionNumber, String paramTableName, String traceTableName, String xmlFileName)
        {
            // Simply let the sister method do all the processing,
            // but pass it a local List<TSImport> instance that won't be saved.
            return XmlImportWithList(connectionNumber, paramTableName, traceTableName, xmlFileName, new List<TSImport>());
        }
        #endregion


        #region Public methods for computing date values
        [ComVisible(true)]
        public DateTime IncrementDate(DateTime startDate, short unit,
                    short stepSize, int numSteps)
        {
            return TSDateCalculator.IncrementDate(startDate, (TSDateCalculator.TimeStepUnitCode)unit, stepSize, numSteps);
        }
        [ComVisible(true)]
        public void FillDateArray(
                    short timeStepUnit, short timeStepQuantity,
                    int nReqValues, DateTime[] dateArray, DateTime reqStartDate)
        {
            TSDateCalculator.FillDateArray((TSDateCalculator.TimeStepUnitCode)timeStepUnit, timeStepQuantity,
                                nReqValues, dateArray, reqStartDate);
        }
        [ComVisible(true)]
        public void FillSeriesDateArray(
                    int connectionNumber, String paramTableName, String traceTableName, int id,
                    int nReqValues, DateTime[] dateArray, DateTime reqStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);
            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, paramTableName, traceTableName);

            ts.FillDateArray(id, nReqValues, dateArray, reqStartDate);
        }
        [ComVisible(true)]
        public int CountTimeSteps(DateTime startDate, DateTime endDate,
            short unit, short stepSize)
        {
            return TSDateCalculator.CountSteps(startDate, endDate, (TSDateCalculator.TimeStepUnitCode)unit, stepSize);
        }
        #endregion



    }


}
