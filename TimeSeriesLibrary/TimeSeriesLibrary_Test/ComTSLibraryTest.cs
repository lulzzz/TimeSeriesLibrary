using TimeSeriesLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data.SqlClient;
using System.Data;

namespace TimeSeriesLibrary_Test
{
    
    
    /// <summary>
    ///This is a test class for ComTSLibraryTest and is intended
    ///to contain all ComTSLibraryTest Unit Tests
    ///</summary>
    [TestClass()]
    public class ComTSLibraryTest
    {
        static ComTSLibrary _lib;
        static int _connxNumber;
        static SqlConnection _connx;
        const String _paramTableName = "RunOutputTimeSeries";
        const String _traceTableName = "OutputTimeSeriesTraces";

        #region TestContext
        private TestContext testContextInstance;
        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        } 
        #endregion

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //
        //Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            _lib = new ComTSLibrary();
            _connxNumber = _lib.OpenConnection("Data Source=.; Database=ObjectModel; Trusted_Connection=yes;");
            _connx = _lib.GetConnectionFromId(_connxNumber);
        }
        
        //Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            _lib.CloseConnection(_connxNumber);
            _lib = null;
        }
        
        //Use TestInitialize to run code before running each test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}
        //
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion


        #region Test WriteParameters methods
        // WriteParametersRegularUnsafe
        [TestMethod()]
        public void WriteParametersRegular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            int timeStepCount = 50;
            short timeStepUnit = (short)TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuantity = 2;
            int id;

            String[] extraParamNames = new String[] { "TimeSeriesType", "Unit_Id", "RunGUID", "VariableType", "VariableId" };
            String[] extraParamValues = new String[] { "0", "1", "'A9974079-884E-4FE4-B752-65AF2245E978'", "0", "0" };
            // The method being tested
            id = _lib.WriteParametersRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                    timeStepUnit, timeStepQuantity, timeStepCount, startDate, extraParamNames, extraParamValues);

            String comm = String.Format("select * from {0} where Id={1}", _paramTableName, id);
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, _connx))
            {
                DataTable dTable = new DataTable();
                // Execute the query to fill the DataTable object
                adp.Fill(dTable);
                DataRow row = dTable.Rows[0];
                // 
                Assert.AreEqual(timeStepQuantity, row.Field<int>("TimeStepQuantity"));
                Assert.AreEqual(timeStepUnit, row.Field<int>("TimeStepUnit"));
                Assert.AreEqual(timeStepCount, row.Field<int>("RecordCount"));
                Assert.AreEqual(startDate, row.Field<DateTime>("StartDate"));
                Assert.AreEqual(0, row.Field<int>("TimeSeriesType"));
                Assert.AreEqual(1, row.Field<int>("Unit_Id"));
            }

        }
        // WriteParametersIrregularUnsafe
        [TestMethod()]
        public void WriteParametersIrregular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 50;
            short timeStepUnit = (short)TSDateCalculator.TimeStepUnitCode.Irregular;
            short timeStepQuantity = 0;
            int id;

            String[] extraParamNames = new String[] { "TimeSeriesType", "Unit_Id", "RunGUID", "VariableType", "VariableId" };
            String[] extraParamValues = new String[] { "1", "3", "'A9974079-884E-4FE4-B752-65AF2245E978'", "0", "0" };
            // The method being tested
            id = _lib.WriteParametersIrregularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                    timeStepCount, startDate, endDate, extraParamNames, extraParamValues);

            String comm = String.Format("select * from {0} where Id={1}", _paramTableName, id);
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, _connx))
            {
                DataTable dTable = new DataTable();
                // Execute the query to fill the DataTable object
                adp.Fill(dTable);
                DataRow row = dTable.Rows[0];
                // 
                Assert.AreEqual(timeStepQuantity, row.Field<int>("TimeStepQuantity"));
                Assert.AreEqual(timeStepUnit, row.Field<int>("TimeStepUnit"));
                Assert.AreEqual(timeStepCount, row.Field<int>("RecordCount"));
                Assert.AreEqual(startDate, row.Field<DateTime>("StartDate"));
                Assert.AreEqual(endDate, row.Field<DateTime>("EndDate"));
                Assert.AreEqual(1, row.Field<int>("TimeSeriesType"));
                Assert.AreEqual(3, row.Field<int>("Unit_Id"));
            }

        } 
        #endregion

        #region Test WriteTrace methods
        // WriteTraceRegularUnsafe
        [TestMethod()]
        public void WriteTraceRegular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            int timeStepCount = 40;
            short timeStepUnit = (short)TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuantity = 2;
            int id, traceNumber = 27;

            String[] extraParamNames = new String[] { "TimeSeriesType", "Unit_Id", "RunGUID", "VariableType", "VariableId" };
            String[] extraParamValues = new String[] { "0", "1", "'A9974079-884E-4FE4-B752-65AF2245E978'", "0", "0" };
            id = _lib.WriteParametersRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                    timeStepUnit, timeStepQuantity, timeStepCount, startDate, extraParamNames, extraParamValues);

            double[] valArray = new double[timeStepCount], testValArray = new double[timeStepCount];
            double x = 10.0;
            for (int i = 0; i < timeStepCount; i++)
            {
                valArray[i] = x;
                x *= 1.2;
            }
            // The method being tested
            _lib.WriteTraceRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, traceNumber, valArray);

            String comm = String.Format("select * from {0} where TimeSeries_Id={1} and TraceNumber={2}",
                            _traceTableName, id, traceNumber);
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, _connx))
            {
                DataTable dTable = new DataTable();
                // Execute the query to fill the DataTable object
                adp.Fill(dTable);
                DataRow row = dTable.Rows[0];
                byte[] blob = row.Field<byte[]>("ValueBlob");
                TSBlobCoder.ConvertBlobToArrayRegular((TSDateCalculator.TimeStepUnitCode)timeStepUnit, timeStepQuantity,
                        startDate, false, 0, startDate, startDate, blob, testValArray);
                // 
                for (int i = 0; i < timeStepCount; i++)
                    Assert.AreEqual(valArray[i], testValArray[i]);
            }

        }
        // WriteTraceIrregularUnsafe
        [TestMethod()]
        public void WriteTraceIrregular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 40;
            int id, traceNumber = 27;

            String[] extraParamNames = new String[] { "TimeSeriesType", "Unit_Id", "RunGUID", "VariableType", "VariableId" };
            String[] extraParamValues = new String[] { "0", "1", "'A9974079-884E-4FE4-B752-65AF2245E978'", "0", "0" };
            id = _lib.WriteParametersIrregularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                    timeStepCount, startDate, endDate, extraParamNames, extraParamValues);

            TSDateValueStruct[] dateValArray = new TSDateValueStruct[timeStepCount],
                                testDateValArray = new TSDateValueStruct[timeStepCount];
            double x = 10.0;
            double y = 1.0;
            DateTime curDate = startDate;
            for (int i = 0; i < timeStepCount; i++)
            {
                dateValArray[i].Value = x;
                dateValArray[i].Date = curDate;
                x *= 1.2;
                y += 0.5;
                curDate = curDate.AddDays(y);
            }
            // The method being tested
            _lib.WriteTraceIrregularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, traceNumber, dateValArray);

            String comm = String.Format("select * from {0} where TimeSeries_Id={1} and TraceNumber={2}",
                            _traceTableName, id, traceNumber);
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, _connx))
            {
                DataTable dTable = new DataTable();
                // Execute the query to fill the DataTable object
                adp.Fill(dTable);
                DataRow row = dTable.Rows[0];
                byte[] blob = row.Field<byte[]>("ValueBlob");
                TSBlobCoder.ConvertBlobToArrayIrregular(
                            false, 0, startDate, startDate, blob, testDateValArray);
                // 
                for (int i = 0; i < timeStepCount; i++)
                    Assert.AreEqual(dateValArray[i], testDateValArray[i]);
            }

        }
        #endregion    

        #region Test ReadValues methods
        // ReadValuesRegularUnsafe
        [TestMethod()]
        public void ReadValuesRegular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            int timeStepCount = 40;
            short timeStepUnit = (short)TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuantity = 2;
            int id, traceNumber = 27;

            String[] extraParamNames = new String[] { "TimeSeriesType", "Unit_Id", "RunGUID", "VariableType", "VariableId" };
            String[] extraParamValues = new String[] { "0", "1", "'A9974079-884E-4FE4-B752-65AF2245E978'", "0", "0" };
            id = _lib.WriteParametersRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                    timeStepUnit, timeStepQuantity, timeStepCount, startDate, extraParamNames, extraParamValues);

            double[] valArray = new double[timeStepCount], testValArray = new double[timeStepCount];
            double x = 10.0;
            for (int i = 0; i < timeStepCount; i++)
            {
                valArray[i] = x;
                x *= 1.2;
            }
            DateTime endDate = TSDateCalculator.IncrementDate(startDate, (TSDateCalculator.TimeStepUnitCode)timeStepUnit,
                                    timeStepQuantity, timeStepCount - 1);
            _lib.WriteTraceRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, traceNumber, valArray);

            // The method being tested
            _lib.ReadValuesRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, traceNumber, timeStepCount, testValArray, startDate, endDate);

            // 
            for (int i = 0; i < timeStepCount; i++)
                Assert.AreEqual(valArray[i], testValArray[i]);

        }
        // ReadDatesValuesUnsafe
        [TestMethod()]
        public void ReadDatesValuesRegular()
        {
            DateTime startDate = DateTime.Parse("1/10/1996");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 70;
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Hour;
            short timeStepQuantity = 6;
            int id, traceNumber = 13;

            String[] extraParamNames = new String[] { "TimeSeriesType", "Unit_Id", "RunGUID", "VariableType", "VariableId" };
            String[] extraParamValues = new String[] { "0", "1", "'A9974079-884E-4FE4-B752-65AF2245E978'", "0", "0" };
            id = _lib.WriteParametersRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                    (short)timeStepUnit, timeStepQuantity, timeStepCount, startDate, extraParamNames, extraParamValues);

            double[] valArray = new double[timeStepCount];
            TSDateValueStruct[] dateValArray = new TSDateValueStruct[timeStepCount],
                                testDateValArray = new TSDateValueStruct[timeStepCount];
            double x = 5.25;
            DateTime curDate = startDate;
            for (int i = 0; i < timeStepCount; i++)
            {
                valArray[i] = x;
                dateValArray[i].Value = x;
                dateValArray[i].Date = curDate;
                x += 1.75;
                curDate = TSDateCalculator.IncrementDate(curDate, timeStepUnit, timeStepQuantity, 1);
            }
            _lib.WriteTraceRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, traceNumber, valArray);

            // The method being tested
            _lib.ReadDatesValuesUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, traceNumber, timeStepCount, ref testDateValArray, startDate, endDate);

            // 
            for (int i = 0; i < timeStepCount; i++)
                Assert.AreEqual(dateValArray[i], testDateValArray[i]);

        }
        // ReadDatesValuesUnsafe
        [TestMethod()]
        public void ReadDatesValuesIrregular()
        {
            DateTime startDate = DateTime.Parse("8/10/1999");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 40;
            int id, traceNumber = 4;

            String[] extraParamNames = new String[] { "TimeSeriesType", "Unit_Id", "RunGUID", "VariableType", "VariableId" };
            String[] extraParamValues = new String[] { "0", "1", "'A9974079-884E-4FE4-B752-65AF2245E978'", "0", "0" };
            id = _lib.WriteParametersIrregularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                    timeStepCount, startDate, endDate, extraParamNames, extraParamValues);

            TSDateValueStruct[] dateValArray = new TSDateValueStruct[timeStepCount],
                                testDateValArray = new TSDateValueStruct[timeStepCount];
            double x = 10.0;
            double y = 1.0;
            DateTime curDate = startDate;
            for (int i = 0; i < timeStepCount; i++)
            {
                dateValArray[i].Value = x;
                dateValArray[i].Date = curDate;
                x *= 1.5;
                y += 0.25;
                curDate = curDate.AddDays(y);
            }
            _lib.WriteTraceIrregularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, traceNumber, dateValArray);

            // The method being tested
            _lib.ReadDatesValuesUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, traceNumber, timeStepCount, ref testDateValArray, startDate, endDate);

            // 
            for (int i = 0; i < timeStepCount; i++)
                Assert.AreEqual(dateValArray[i], testDateValArray[i]);

        }
        #endregion

        // ReadDatesValuesUnsafe
        [TestMethod()]
        public void Delete()
        {
            DateTime startDate = DateTime.Parse("1/10/1996");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 70;
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Hour;
            short timeStepQuantity = 6;
            int id;

            String[] extraParamNames = new String[] { "TimeSeriesType", "Unit_Id", "RunGUID", "VariableType", "VariableId" };
            String[] extraParamValues = new String[] { "0", "1", "'A9974079-884E-4FE4-B752-65AF2245E978'", "0", "0" };
            id = _lib.WriteParametersRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                    (short)timeStepUnit, timeStepQuantity, timeStepCount, startDate, extraParamNames, extraParamValues);

            double[] valArray = new double[timeStepCount];
            double x = 5.25;
            for (int i = 0; i < timeStepCount; i++)
            {
                valArray[i] = x;
                x += 1.75;
            }
            _lib.WriteTraceRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, 5, valArray);
            _lib.WriteTraceRegularUnsafe(_connxNumber, _paramTableName, _traceTableName,
                        id, 6, valArray);

            String comm = String.Format("select count(1) from {0} where TimeSeries_Id={1}", _traceTableName, id);
            SqlCommand sqlCommand = new SqlCommand(comm, _connx);
            int rowCount = (int)sqlCommand.ExecuteScalar();
            Assert.AreEqual(rowCount, 2);

            // The method being tested
            _lib.DeleteSeries(_connxNumber, _paramTableName, _traceTableName, id);

            rowCount = (int)sqlCommand.ExecuteScalar();
            Assert.AreEqual(rowCount, 0);

            comm = String.Format("select count(1) from {0} where Id={1}", _paramTableName, id);
            rowCount = (int)sqlCommand.ExecuteScalar();
            Assert.AreEqual(rowCount, 0);

        }

    }
}
