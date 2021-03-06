﻿using System;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oasis.TestFoundation;
using TimeSeriesLibrary;
using System.Collections.Generic;
using Oasis.Foundation.Infrastructure;

namespace TimeSeriesLibrary_Test
{
    
    
    /// <summary>
    ///This is a test class for ComTSLibraryTest and is intended
    ///to contain all ComTSLibraryTest Unit Tests
    ///</summary>
    [TestClass()]
    public unsafe class ComTSLibraryTest
    {
        TransactionScope _transactionScope;
        static ComTSLibrary _lib;
        static int _connxNumber;
        static SqlConnection _connx;
        const String _paramTableName = "OutputTimeSeries";
        const String _traceTableName = "OutputTimeSeriesTraces";
        // These two constants are for temporary tables that are designed to absorb database changes
        // that are caused by SqlBulkCopy operations, and are thus not subject to transaction scope.
        const String _TestParamTableName = "#TimeSeries_TestTimeSeriesLibrary";
        const String _TestTraceTableName = "#TimeSeriesTraces_TestTimeSeriesLibrary";
        /// <summary>
        /// flag indicates whether temp tables have been created
        /// </summary>
        Boolean _createdTempTables = false;

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

        #region temporary table helper methods
        /// <summary>
        /// This is a helper method designed to be called from test methods of this class.
        /// 
        /// This method creates a two temporary tables: one for the 'parameters' table and one for the
        /// 'traces' table.  Both tables are created empty.  The method sets the flag field
        /// '_createdTempTables' to indicate that the temporary tables shall be automatically deleted
        /// when the test method is complete.
        /// 
        /// This method is useful if the test method will alter data in the database in a way that
        /// can not be rolled back via TransactionScope. For example, it has been found that
        /// SqlBulkCopy is not affected by TransactionScope.  This method allows the test methods to
        /// change the data in temporary tables instead of in the 'real' database.
        /// </summary>
        private void CreateTempTables()
        {
            DropTempTables(true);

            // FWIW, I tried to add a primary key constraint to each of these tables, and a
            // foreign key constraint on the traces table.  I thought this would provide a better
            // set of conditions for running the tests.  However, I then learned that foreign key
            // constraints are not enforced on temporary tables, so I decided it was not worth
            // adding the primary key constraints either.

            String commandText = String.Format("CREATE TABLE {0}\n(\n"
                            + "	[Id] [int] IDENTITY(1,1) NOT NULL,\n"
                            + "	[TimeSeriesType] [int] NOT NULL,\n"
                            + "	[TimeStepUnit] [int] NOT NULL,\n"
                            + "	[TimeStepQuantity] [int] NOT NULL,\n"
                            + "	[StartDate] [datetime] NOT NULL,\n"
                            + "	[Checksum] [binary](16) NOT NULL,\n"
                            + "	[RunGUID] [uniqueidentifier] NOT NULL,\n"
                            + "	[VariableType] [nvarchar](200) NULL,\n"
                            + "	[VariableName] [nvarchar](200) NULL,\n"
                            + "	[RunElementGUID] [uniqueidentifier] NOT NULL,\n"
                            + "	[CompressionCode] [int] NOT NULL,\n"
                            + "	[Unit_Id] [int] NOT NULL\n)", _TestParamTableName);
            using (var createTableCommand = new SqlCommand(commandText, _connx))
            {
                createTableCommand.ExecuteNonQuery();
            }
            commandText = String.Format("CREATE TABLE {0}\n(\n"
                            + "	[Id] [int] IDENTITY(1,1) NOT NULL,\n"
                            + "	[TraceNumber] [int] NOT NULL,\n"
                            + "	[TimeStepCount] [int] NOT NULL,\n"
                            + "	[EndDate] [datetime] NOT NULL,\n"
                            + "	[ValueBlob] [varbinary](max) NOT NULL,\n"
                            + "	[Checksum] [binary](16) NOT NULL,\n"
                            + "	[TimeSeries_Id] [int] NOT NULL\n)\n"
                            + "ALTER TABLE {0} ADD  DEFAULT ((0)) FOR [ValueBlob]",
                  _TestTraceTableName, _TestParamTableName);
            using (var createTableCommand = new SqlCommand(commandText, _connx))
            {
                createTableCommand.ExecuteNonQuery();
            }
            _createdTempTables = true;
        }
        /// <summary>
        /// This is a helper method designed to be called from test methods of this class.
        /// 
        /// This method creates a two temporary tables: one for the 'parameters' table and one for the
        /// 'traces' table.  Both tables are with an initial population copied from the permanent tables,
        /// being the 'parameters' object with the given Id number, and all of its associated traces.
        /// The method sets the flag field '_createdTempTables' to indicate that the temporary tables
        /// shall be automatically deleted when the test method is complete.
        /// 
        /// This method is useful if the test method will alter data in the database in a way that
        /// can not be rolled back via TransactionScope. For example, it has been found that
        /// SqlBulkCopy is not affected by TransactionScope.  This method allows the test methods to
        /// change the data in temporary tables instead of in the 'real' database.
        /// </summary>
        private void CreateTempTablesFromExisting(int id)
        {
            DropTempTables(true);

            // FWIW, I tried to add a primary key constraint to each of these tables, and a
            // foreign key constraint on the traces table.  I thought this would provide a better
            // set of conditions for running the tests.  However, I then learned that foreign key
            // constraints are not enforced on temporary tables, so I decided it was not worth
            // adding the primary key constraints either.

            String commandText = String.Format("SELECT * INTO {2} FROM {0} x\n"
                            + "    WHERE x.Id={4}\n"
                            + "SELECT * INTO {3} FROM {1} x\n"
                            + "    WHERE x.TimeSeries_Id={4}\n"
                            + "ALTER TABLE {3} ADD  DEFAULT ((0)) FOR [ValueBlob]",
                        _paramTableName, _traceTableName, _TestParamTableName, _TestTraceTableName, id);
            using (var createTableCommand = new SqlCommand(commandText, _connx))
            {
                createTableCommand.ExecuteNonQuery();
            }
            _createdTempTables = true;
        }
        /// <summary>
        /// This is a helper method designed to be called from MyTestCleanup.  If no temporary tables
        /// were created by this class's helper methods CreateTempTables or CreateTempTablesFromExisting,
        /// then this method does nothing.  If temporary tables were created, then this method ensures
        /// that they are deleted.
        /// </summary>
        private void DropTempTables(Boolean toForce = false)
        {
            if (_createdTempTables == false && toForce == false) return;
            _connx.DropTempTable(_TestTraceTableName);
            _connx.DropTempTable(_TestParamTableName);
            _createdTempTables = false;
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
            get { return testContextInstance; }
            set { testContextInstance = value; }
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
            _connxNumber = _lib.OpenConnection(GetSbyte("Data Source=.; Database=NYC-EF6; Trusted_Connection=yes;"));
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
            _connx.EnlistTransaction(Transaction.Current);
        }
        //Use TestCleanup to run code after each test has run
        [TestCleanup()]
        public void MyTestCleanup()
        {
            if (_transactionScope != null)
                _transactionScope.Dispose();
            
            DropTempTables();
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
            // All database changes should be directed into these temp tables, since the tested methods
            // use SqlBulkCopy, which is not subject to transaction scope.
            CreateTempTables();

            DateTime startDate = DateTime.Parse("2/10/2000");
            int timeStepCount = 40;
            short timeStepUnit = (short)TSDateCalculator.TimeStepUnitCode.Day;
            short timeStepQuantity = 2;
            int id, traceNumber = 27;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            id = _lib.WriteParametersRegular(_connxNumber, GetSbyte(_TestParamTableName), GetSbyte(_TestTraceTableName),
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
            _lib.WriteTraceRegular(_connxNumber, GetSbyte(_TestParamTableName), GetSbyte(_TestTraceTableName),
                        id, traceNumber, valArray);
            _lib.CommitTraceWrites(_connxNumber, GetSbyte(_TestParamTableName), GetSbyte(_TestTraceTableName));

            String comm = String.Format("select * from {0} where TimeSeries_Id={1} and TraceNumber={2}",
                            _TestTraceTableName, id, traceNumber);
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
            // All database changes should be directed into these temp tables, since the tested methods
            // use SqlBulkCopy, which is not subject to transaction scope.
            CreateTempTables();

            DateTime startDate = DateTime.Parse("2/10/2000");
            DateTime endDate = DateTime.Parse("2/10/2002");
            int timeStepCount = 40;
            int id, traceNumber = 27;

            String extraParamNames = "TimeSeriesType, Unit_Id, RunGUID, VariableType, VariableName, RunElementGUID";
            String extraParamValues = "0, 1, 'A0101010-AAAA-BBBB-2222-3E3E3E3E3E3E', 0, 'eraseme', '00000000-0000-0000-0000-000000000000'";
            id = _lib.WriteParametersIrregular(_connxNumber, GetSbyte(_TestParamTableName), GetSbyte(_TestTraceTableName),
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
            _lib.WriteTraceIrregular(_connxNumber, GetSbyte(_TestParamTableName), GetSbyte(_TestTraceTableName),
                        id, traceNumber, dateValArray);
            _lib.CommitTraceWrites(_connxNumber, GetSbyte(_TestParamTableName), GetSbyte(_TestTraceTableName));

            String comm = String.Format("select * from {0} where TimeSeries_Id={1} and TraceNumber={2}",
                            _TestTraceTableName, id, traceNumber);
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
        /// <summary>
        /// Test will
        ///  1) Find an existing time series that has at least one trace
        ///  2) add a new trace to the existing time series
        ///      (calling WriteTraceRegular and CommitTraceWrites)
        ///  3) verify that
        ///      a) new and old traces are indeed found in the DB table
        ///      b) checksum of in the parameters table fits the set of new and old traces
        /// </summary>
        [TestMethod()]
        public void WriteMultiTraceRegular()
        {
            int id = 0, traceNumber = 0, traceCount = 0;

            // Find an existing time series that has at least one trace and where the
            // time series type is not irregular
            String comm = "SELECT TOP(1) x.Id FROM " + _traceTableName + " t\n"
                         + "JOIN " + _paramTableName + " x ON t.TimeSeries_Id=x.Id\n"
                         + "WHERE x.TimeSeriesType != 0";
            using (var queryCommand = new SqlCommand(comm, _connx) { CommandTimeout = 0 })
            using (var reader = queryCommand.ExecuteReader())
            {
                // Loop through every record in the result set
                while (reader.Read())
                {
                    // Populate an array of Objects with the query results for this record
                    Object[] valueArray = new Object[1];
                    reader.GetValues(valueArray);
                    id = (int)valueArray[0];
                }
            }
            // Get the highest existing trace number of the existing time series
            comm = "SELECT TOP(1) t.[TraceNumber] FROM " + _traceTableName + " t\n"
                         + "where t.[TimeSeries_Id]=" + id + "\norder by t.[TraceNumber] desc";
            using (var queryCommand = new SqlCommand(comm, _connx) { CommandTimeout = 0 })
            using (var reader = queryCommand.ExecuteReader())
            {
                // Loop through every record in the result set
                while (reader.Read())
                {
                    // Populate an array of Objects with the query results for this record
                    Object[] valueArray = new Object[1];
                    reader.GetValues(valueArray);
                    traceNumber = (int)valueArray[0];
                }
            }
            // Get the highest existing trace number of the existing time series
            comm = "SELECT COUNT(*) FROM " + _traceTableName + " t\n"
                         + "where t.[TimeSeries_Id]=" + id;
            using (var queryCommand = new SqlCommand(comm, _connx) { CommandTimeout = 0 })
            using (var reader = queryCommand.ExecuteReader())
            {
                // Loop through every record in the result set
                while (reader.Read())
                {
                    // Populate an array of Objects with the query results for this record
                    Object[] valueArray = new Object[1];
                    reader.GetValues(valueArray);
                    traceCount = (int)valueArray[0];
                }
            }
            // Read the values of the existing trace that has the highest trace number
            int ArrayDim = 5000;
            var valArray = new Double[ArrayDim];
            int nVals = _lib.ReadValuesRegular(_connxNumber,
                        GetSbyte(_paramTableName), GetSbyte(_traceTableName),
                        id, traceNumber, ArrayDim, valArray, DateTime.MinValue, DateTime.MaxValue);
            var newValArray = valArray.Take(nVals).ToArray();

            // Create temporary tables that will absorb all data changes.  We do this because
            // the tested methods use SqlBulkCopy, which is not subject to TransactionScope.
            CreateTempTablesFromExisting(id);

            // The method being tested--we add another trace that is identical to the one we just
            // read, but give it a new trace number.
            _lib.WriteTraceRegular(_connxNumber, GetSbyte(_TestParamTableName), GetSbyte(_TestTraceTableName),
                        id, traceNumber + 1, newValArray);
            _lib.CommitTraceWrites(_connxNumber, GetSbyte(_TestParamTableName), GetSbyte(_TestTraceTableName));

            // Now verify that the new trace is written to the DB,
            // and that the checksum of the time series includes all traces including the new one
            comm = "SELECT c.[Checksum], c.[TimeStepUnit], c.[TimeStepQuantity], "
                          + "c.[StartDate],\n    t.[TraceNumber], t.[Checksum]\n"
                          + "FROM [" + _TestParamTableName + "] c\n"
                          + "LEFT JOIN [" + _TestTraceTableName + "] t ON c.[Id]=t.[TimeSeries_Id]\n"
                          + "WHERE c.[Id]=" + id;

            using (var queryCommand = new SqlCommand(comm, _connx) { CommandTimeout = 0 })
            using (var reader = queryCommand.ExecuteReader())
            {
                Boolean initialized = false;
                var traceList = new List<ITimeSeriesTrace>();
                Byte[] checksum = new Byte[0];
                var timeStepUnit = TSDateCalculator.TimeStepUnitCode.Day;
                short timeStepQuantity = 1;
                DateTime startDate = DateTime.MinValue;
                // Loop through every record in the result set
                while (reader.Read())
                {
                    // Populate an array of Objects with the query results for this record
                    Object[] valueArray = new Object[6];
                    reader.GetValues(valueArray);
                    if (!initialized)
                    {
                        checksum = (Byte[])valueArray[0];
                        timeStepUnit = (TSDateCalculator.TimeStepUnitCode)(int)valueArray[1];
                        timeStepQuantity = (short)(int)valueArray[2];
                        startDate = (DateTime)valueArray[3];
                        initialized = true;
                    }
                    // Add to the collection a container object for this trace's properties
                    traceList.Add(new TSTrace
                    {
                        TraceNumber = (int)valueArray[4],
                        Checksum = (Byte[])valueArray[5]
                    });
                }
                // Is the new trace in the DB?
                Assert.IsTrue(traceList.Any(o => o.TraceNumber == traceNumber + 1));
                // Is the actual number of traces incremented by one?
                Assert.AreEqual(traceCount + 1, traceList.Count);
                // Do an independent computation of the checksum
                var correctChecksum = TSBlobCoder.ComputeChecksum(timeStepUnit, timeStepQuantity,
                                                    startDate, traceList);
                // was the correct checksum stored in the DB?
                Assert.IsTrue(NumericExtensions.ByteArraysAreEqual(correctChecksum, checksum));
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
            _lib.CommitTraceWrites(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName));

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
            _lib.CommitTraceWrites(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName));

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
            _lib.CommitTraceWrites(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName));

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
            _lib.CommitTraceWrites(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName));

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
            _lib.CommitTraceWrites(_connxNumber, GetSbyte(_paramTableName), GetSbyte(_traceTableName));

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

        #region Test error handling
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
        #endregion

    }
}
