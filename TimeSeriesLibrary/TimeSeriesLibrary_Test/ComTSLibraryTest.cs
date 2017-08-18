using System;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oasis.TestFoundation;
using TimeSeriesLibrary;

namespace TimeSeriesLibrary_Test
{
    
    
    /// <summary>
    ///This is a test class for ComTSLibraryTest and is intended
    ///to contain all ComTSLibraryTest Unit Tests
    ///</summary>
    [TestClass()]
    [DeploymentItem("lzfx.dll")]
    [DeploymentItem("lzfx64.dll")]
    [DeploymentItem("lz4_32.dll")]
    [DeploymentItem("lz4_64.dll")]
    public unsafe class ComTSLibraryTest
    {
        TransactionScope _transactionScope;
        static ComTSLibrary _lib;
        static int _connxNumber;
        static SqlConnection _connx;
        const String _paramTableName = "OutputTimeSeries";
        const String _traceTableName = "OutputTimeSeriesTraces";

        #region GetSbyte helper method
        /// <summary>
        /// This method converts the given string to a byte array, which is equivalent
        /// to a char[] array used in native C++ code.
        /// </summary>
        private static sbyte* GetSbyte(String s)
        {
            byte[] b = System.Text.Encoding.ASCII.GetBytes(s);
            
            fixed (sbyte* sb = new sbyte[b.Length])
            {
                for (int i = 0; i < b.Length; i++)
                {
                    sb[i] = (sbyte)b[i];
                }
                return sb;
            }
        }
        #endregion

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
            _connxNumber = _lib.OpenConnection(GetSbyte("Data Source=.; Database=ObjectModel; Trusted_Connection=yes;"));
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
        [TestInitialize()]
        public void MyTestInitialize()
        {
            _transactionScope = TestHelper.GetNewTransactionScope();
        }
        //Use TestCleanup to run code after each test has run
        [TestCleanup()]
        public void MyTestCleanup()
        {
            if (_transactionScope != null)
                _transactionScope.Dispose();
        }
        #endregion


        #region Test WriteParameters methods
        // WriteParametersRegular
        [TestMethod()]
        public void WriteParametersRegular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            int timeStepCount = 50;
            short timeStepUnit = (short)TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuantity = 2;
            int id;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            // The method being tested
            id = _lib.WriteParametersRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    timeStepUnit, timeStepQuantity, timeStepCount, startDate, 
                    GetSbyte(extraParamNames), GetSbyte(extraParamValues));

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
                Assert.AreEqual(startDate, row.Field<DateTime>("StartDate"));
                Assert.AreEqual(0, row.Field<int>("TimeSeriesType"));
                Assert.AreEqual(1, row.Field<int>("Unit_Id"));
            }

        }
        // WriteParametersIrregular
        [TestMethod()]
        public void WriteParametersIrregular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 50;
            short timeStepUnit = (short)TSDateCalculator.TimeStepUnitCode.Irregular;
            short timeStepQuantity = 0;
            int id;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "1, 3, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            // The method being tested
            id = _lib.WriteParametersIrregular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    timeStepCount, startDate, endDate, GetSbyte(extraParamNames), GetSbyte(extraParamValues));

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
                Assert.AreEqual(startDate, row.Field<DateTime>("StartDate"));
                Assert.AreEqual(1, row.Field<int>("TimeSeriesType"));
                Assert.AreEqual(3, row.Field<int>("Unit_Id"));
            }

        } 
        #endregion

        #region Test WriteTrace methods
        // WriteTraceRegular
        [TestMethod()]
        public void WriteTraceRegular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            int timeStepCount = 40;
            short timeStepUnit = (short)TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuantity = 2;
            int id, traceNumber = 27;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            id = _lib.WriteParametersRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    timeStepUnit, timeStepQuantity, timeStepCount, startDate, 
                    GetSbyte(extraParamNames), GetSbyte(extraParamValues));

            double[] valArray = new double[timeStepCount], testValArray = new double[timeStepCount];
            double x = 10.0;
            for (int i = 0; i < timeStepCount; i++)
            {
                valArray[i] = x;
                x *= 1.2;
            }
            // The method being tested
            _lib.WriteTraceRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
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
                        timeStepCount, startDate, false, 0, startDate, startDate, blob,
                        testValArray, TSBlobCoder.currentCompressionCode);
                // 
                for (int i = 0; i < timeStepCount; i++)
                    Assert.AreEqual(valArray[i], testValArray[i]);
            }

        }
        // WriteTraceIrregular
        [TestMethod()]
        public void WriteTraceIrregular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 40;
            int id, traceNumber = 27;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            id = _lib.WriteParametersIrregular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    timeStepCount, startDate, endDate, GetSbyte(extraParamNames), GetSbyte(extraParamValues));

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
            _lib.WriteTraceIrregular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
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
                TSBlobCoder.ConvertBlobToArrayIrregular(timeStepCount,
                            false, 0, startDate, startDate, blob, testDateValArray, TSBlobCoder.currentCompressionCode);
                // 
                for (int i = 0; i < timeStepCount; i++)
                    Assert.AreEqual(dateValArray[i], testDateValArray[i]);
            }

        }
        #endregion    

        #region Test ReadValues methods
        // ReadValuesRegular
        [TestMethod()]
        public void ReadValuesRegular()
        {
            DateTime startDate = DateTime.Parse("2/10/2000");
            int timeStepCount = 40;
            short timeStepUnit = (short)TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuantity = 2;
            int id, traceNumber = 27;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            id = _lib.WriteParametersRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    timeStepUnit, timeStepQuantity, timeStepCount, startDate, GetSbyte(extraParamNames), GetSbyte(extraParamValues));

            double[] valArray = new double[timeStepCount], testValArray = new double[timeStepCount];
            double x = 10.0;
            for (int i = 0; i < timeStepCount; i++)
            {
                valArray[i] = x;
                x *= 1.2;
            }
            DateTime endDate = TSDateCalculator.IncrementDate(startDate, (TSDateCalculator.TimeStepUnitCode)timeStepUnit,
                                    timeStepQuantity, timeStepCount - 1);
            _lib.WriteTraceRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, traceNumber, valArray);

            // The method being tested
            _lib.ReadValuesRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, traceNumber, timeStepCount, testValArray, startDate, endDate);

            // 
            for (int i = 0; i < timeStepCount; i++)
                Assert.AreEqual(valArray[i], testValArray[i]);

        }
        // ReadDatesValues
        [TestMethod()]
        public void ReadDatesValuesRegular()
        {
            DateTime startDate = DateTime.Parse("1/10/1996");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 70;
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Hour;
            short timeStepQuantity = 6;
            int id, traceNumber = 13;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            id = _lib.WriteParametersRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    (short)timeStepUnit, timeStepQuantity, timeStepCount, startDate, 
                    GetSbyte(extraParamNames), GetSbyte(extraParamValues));

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
            _lib.WriteTraceRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, traceNumber, valArray);

            // The method being tested
            _lib.ReadDatesValues(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, traceNumber, timeStepCount, ref testDateValArray, startDate, endDate);

            // 
            for (int i = 0; i < timeStepCount; i++)
                Assert.AreEqual(dateValArray[i], testDateValArray[i]);

        }
        // ReadDatesValues
        [TestMethod()]
        public void ReadDatesValuesIrregular()
        {
            DateTime startDate = DateTime.Parse("8/10/1999");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 40;
            int id, traceNumber = 4;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            id = _lib.WriteParametersIrregular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    timeStepCount, startDate, endDate, GetSbyte(extraParamNames), GetSbyte(extraParamValues));

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
            _lib.WriteTraceIrregular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, traceNumber, dateValArray);

            // The method being tested
            _lib.ReadDatesValues(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, traceNumber, timeStepCount, ref testDateValArray, startDate, endDate);

            // 
            for (int i = 0; i < timeStepCount; i++)
                Assert.AreEqual(dateValArray[i], testDateValArray[i]);

        }
        #endregion

        #region Test Delete methods
        // DeleteSeries method
        [TestMethod()]
        public void DeleteSeries()
        {
            DateTime startDate = DateTime.Parse("1/10/1996");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 70;
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Hour;
            short timeStepQuantity = 6;
            int id;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            id = _lib.WriteParametersRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    (short)timeStepUnit, timeStepQuantity, timeStepCount, startDate,
                    GetSbyte(extraParamNames), GetSbyte(extraParamValues));

            double[] valArray = new double[timeStepCount];
            double x = 5.25;
            for (int i = 0; i < timeStepCount; i++)
            {
                valArray[i] = x;
                x += 1.75;
            }
            _lib.WriteTraceRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, 5, valArray);
            _lib.WriteTraceRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, 6, valArray);

            String comm = String.Format("select count(1) from {0} where TimeSeries_Id={1}", _traceTableName, id);
            SqlCommand sqlCommand = new SqlCommand(comm, _connx);
            int rowCount = (int)sqlCommand.ExecuteScalar();
            Assert.AreEqual(rowCount, 2);

            // The method being tested
            _lib.DeleteSeries(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName), id);

            rowCount = (int)sqlCommand.ExecuteScalar();
            Assert.AreEqual(rowCount, 0);

            comm = String.Format("select count(1) from {0} where Id={1}", _paramTableName, id);
            rowCount = (int)sqlCommand.ExecuteScalar();
            Assert.AreEqual(rowCount, 0);

        } 
        // DeleteMatchingSeries method
        [TestMethod()]
        public void DeleteMatchingSeries()
        {
            DateTime startDate = DateTime.Parse("1/10/1996");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 70;
            TSDateCalculator.TimeStepUnitCode timeStepUnit = TSDateCalculator.TimeStepUnitCode.Hour;
            short timeStepQuantity = 6;
            int id;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '0000000E-000E-000E-000E-00000000000E'";
            id = _lib.WriteParametersRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    (short)timeStepUnit, timeStepQuantity, timeStepCount, startDate,
                    GetSbyte(extraParamNames), GetSbyte(extraParamValues));

            double[] valArray = new double[timeStepCount];
            double x = 5.25;
            for (int i = 0; i < timeStepCount; i++)
            {
                valArray[i] = x;
                x += 1.75;
            }
            _lib.WriteTraceRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, 5, valArray);
            _lib.WriteTraceRegular(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, 6, valArray);

            String comm = String.Format("select count(1) from {0} where TimeSeries_Id={1}", _traceTableName, id);
            SqlCommand sqlCommand = new SqlCommand(comm, _connx);
            int rowCount = (int)sqlCommand.ExecuteScalar();
            Assert.AreEqual(rowCount, 2);

            // The method being tested
            bool ret = _lib.DeleteMatchingSeries(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                    GetSbyte("RunGUID='A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E'"));

            Assert.AreEqual(ret, true);

            rowCount = (int)sqlCommand.ExecuteScalar();
            Assert.AreEqual(rowCount, 0);

            comm = String.Format("select count(1) from {0} where Id={1}", _paramTableName, id);
            rowCount = (int)sqlCommand.ExecuteScalar();
            Assert.AreEqual(rowCount, 0);

        } 
        #endregion 

        // Test Error Handling
        [TestMethod()]
        public void ResetErrorHandler()
        {
            _lib.ResetErrorHandler();
            Assert.IsFalse(_lib.GetHasError(), "GetHasError should have returned False after calling the reset method");

            sbyte[] errorMessage = new sbyte[4096];
            String errorString;
            fixed (sbyte* pErrorMessage = &errorMessage[0])
            {
                _lib.GetErrorMessage(pErrorMessage);
                errorString = new String(pErrorMessage);
            }
            Assert.IsTrue(errorString.Length == 0, "GetErrorMessage should not return any message after calling the reset method");
        }
        [TestMethod()]
        public void ErrorHandling1()
        {
            _lib.ResetErrorHandler();
            // This should cause an error
            _lib.DeleteSeries(_connxNumber, GetSbyte("TableThatDoesNotExist"), GetSbyte("TableThatWillNeverExist"), 3);

            Assert.IsTrue(_lib.GetHasError(), "GetHasError should have returned True after an error was triggered");

            sbyte[] errorMessage = new sbyte[4096];
            String errorString;
            fixed (sbyte* pErrorMessage = &errorMessage[0])
            {
                _lib.GetErrorMessage(pErrorMessage);
                errorString = new String(pErrorMessage);
            }
            Assert.IsTrue(errorString.Length > 0);
        }
        [TestMethod()]
        public void ErrorHandling2()
        {
            _lib.ResetErrorHandler();

            TSDateValueStruct[] dateValStructArray = new TSDateValueStruct[10];
            // This should cause an error
            _lib.ReadDatesValues(_connxNumber, GetSbyte("TableThatDoesNotExist"), GetSbyte("TableThatWillNeverExist"), 3,
                                    -33, 10, ref dateValStructArray, DateTime.Parse("1/10/1996"), DateTime.Parse("1/10/1996"));

            Assert.IsTrue(_lib.GetHasError(), "GetHasError should have returned True after an error was triggered");

            sbyte[] errorMessage = new sbyte[4096];
            String errorString;
            fixed (sbyte* pErrorMessage = &errorMessage[0])
            {
                _lib.GetErrorMessage(pErrorMessage);
                errorString = new String(pErrorMessage);
            }
            Assert.IsTrue(errorString.Length > 0);
        } 

    }
}
