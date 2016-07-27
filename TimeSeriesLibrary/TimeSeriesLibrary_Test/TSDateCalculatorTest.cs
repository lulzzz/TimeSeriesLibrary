using TimeSeriesLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using Oasis.Foundation.Infrastructure;

namespace TimeSeriesLibrary_Test
{
    
    
    /// <summary>
    ///This is a test class for TSDateCalculator and is intended
    ///to contain all TSDateCalculator Unit Tests
    ///</summary>
    [TestClass()]
    public class TSDateCalculatorTest
    {
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
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}

        ////Use TestInitialize to run code before running each test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}

        ////Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}

        #endregion


        #region Test Methods for speed tests of FillDateArray method
        /// <summary>
        /// Test method for testing the speed of the FillDateArray method when the unit is Day.
        /// The n and nCycles variables may be adjusted for the needs of the speed test.
        /// nCycles of 400 has been useful in the past.
        /// </summary>
        [TestMethod()]
        public void FillDateArray_Speed_Day()
        {
            int nCycles = 0;
            int n = 365 * 85;
            FillDataArray_SpeedCore(nCycles, n, TSDateCalculator.TimeStepUnitCode.Day);
        }
        /// <summary>
        /// Test method for testing the speed of the FillDateArray method when the unit is Hour.
        /// The n and nCycles variables may be adjusted for the needs of the speed test.
        /// nCycles of 400 has been useful in the past.
        /// </summary>
        [TestMethod()]
        public void FillDateArray_Speed_Hour()
        {
            int nCycles = 0;
            int n = 365 * 85;
            FillDataArray_SpeedCore(nCycles, n, TSDateCalculator.TimeStepUnitCode.Hour);
        }
        /// <summary>
        /// Test method for testing the speed of the FillDateArray method when the unit is Minute.
        /// The n and nCycles variables may be adjusted for the needs of the speed test.
        /// nCycles of 400 has been useful in the past.
        /// </summary>
        [TestMethod()]
        public void FillDateArray_Speed_Minute()
        {
            int nCycles = 0;
            int n = 365 * 85;
            FillDataArray_SpeedCore(nCycles, n, TSDateCalculator.TimeStepUnitCode.Minute);
        }
        /// <summary>
        /// Test method for testing the speed of the FillDateArray method when the unit is Month.
        /// The n and nCycles variables may be adjusted for the needs of the speed test.
        /// nCycles of 400 has been useful in the past.
        /// </summary>
        [TestMethod()]
        public void FillDateArray_Speed_Month()
        {
            int nCycles = 0;
            int n = 365 * 85;
            FillDataArray_SpeedCore(nCycles, n, TSDateCalculator.TimeStepUnitCode.Month);
        }
        /// <summary>
        /// This method is to be used as a common utility function for testing the speed of the 
        /// FillDateArray method.
        /// </summary>
        public void FillDataArray_SpeedCore(int nCycles, int n, TSDateCalculator.TimeStepUnitCode unit)
        {
            var array = new DateTime[n + 2];
            DateTime start = new DateTime(1920, 1, 1);
            for (int i = 0; i < nCycles; i++)
                TSDateCalculator.FillDateArray(unit, 1, n, array, start);
        }
        #endregion

        #region Test methods: FillDateArray
        [TestMethod()]
        public void FillDateArray_1Minute()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Minute;
            short quantity = 1;
            Func<DateTime, DateTime> func = d => d.AddMinutes(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_12Minute()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Minute;
            short quantity = 12;
            Func<DateTime, DateTime> func = d => d.AddMinutes(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_1Hour()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Hour;
            short quantity = 1;
            Func<DateTime, DateTime> func = d => d.AddHours(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_5Hour()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Hour;
            short quantity = 5;
            Func<DateTime, DateTime> func = d => d.AddHours(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_1Day()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Day;
            short quantity = 1;
            Func<DateTime, DateTime> func = d => d.AddDays(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_4Day()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Day;
            short quantity = 4;
            Func<DateTime, DateTime> func = d => d.AddDays(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_1Week()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Week;
            short quantity = 1;
            Func<DateTime, DateTime> func = d => d.AddDays(quantity * 7);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_4Week()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Week;
            short quantity = 4;
            Func<DateTime, DateTime> func = d => d.AddDays(quantity * 7);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_1Month()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Month;
            short quantity = 1;
            Func<DateTime, DateTime> func = d => d.AddMonths(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_3Month()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Month;
            short quantity = 3;
            Func<DateTime, DateTime> func = d => d.AddMonths(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_1Year()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Year;
            short quantity = 1;
            Func<DateTime, DateTime> func = d => d.AddYears(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_2Year()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Year;
            short quantity = 2;
            Func<DateTime, DateTime> func = d => d.AddYears(quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        /// <summary>
        /// This method is to be used as a common utility function for testing the speed of the 
        /// FillDateArray method.
        /// </summary>
        public void FillDataArray_Core(TSDateCalculator.TimeStepUnitCode unit, short quantity,
                        Func<DateTime, DateTime> func)
        {
            int n = 20;
            var array = new DateTime[n + 2];
            DateTime start = new DateTime(1920, 1, 1);
            TSDateCalculator.FillDateArray(unit, quantity, n, array, start);

            for (int i = 1; i < n; i++)
                Assert.AreEqual(func(array[i - 1]), array[i]);
        }
        #endregion

    }
}
