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
            Func<DateTime, DateTime> func = d => d.AddMonthsByEnd(0, quantity);

            FillDataArray_Core(unit, quantity, func);
        }
        [TestMethod()]
        public void FillDateArray_3Month()
        {
            TSDateCalculator.TimeStepUnitCode unit = TSDateCalculator.TimeStepUnitCode.Month;
            short quantity = 3;
            Func<DateTime, DateTime> func = d => d.AddMonthsByEnd(0, quantity);

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

        #region Test methods: CountSteps
        [TestMethod()]
        public void CountSteps_5Min()
        {
            TSDateCalculator.TimeStepUnitCode u = TSDateCalculator.TimeStepUnitCode.Minute;
            short q = 5;

            DateTime d1 = new DateTime(1960, 1, 15);
            DateTime d2 = new DateTime(1960, 1, 17);
            int n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(576, n);

            d1 = d1.AddSeconds(98);
            n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(576, n);

            n = TSDateCalculator.CountSteps(d1, d1, u, q);
            Assert.AreEqual(0, n);
        }
        [TestMethod()]
        public void CountSteps_2Hour()
        {
            TSDateCalculator.TimeStepUnitCode u = TSDateCalculator.TimeStepUnitCode.Hour;
            short q = 2;

            DateTime d1 = new DateTime(1960, 2, 20);
            DateTime d2 = new DateTime(1960, 3, 3);
            int n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(144, n);

            d2 = d1.AddHours(6);
            d1 = d1.AddMinutes(30);
            n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(3, n);

            n = TSDateCalculator.CountSteps(d1, d1, u, q);
            Assert.AreEqual(0, n);
        }
        [TestMethod()]
        public void CountSteps_1Day()
        {
            TSDateCalculator.TimeStepUnitCode u = TSDateCalculator.TimeStepUnitCode.Day;
            short q = 1;

            DateTime d1 = new DateTime(1960, 1, 1);
            DateTime d2 = new DateTime(1960, 12, 31);
            int n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(365, n);

            d1 = d1.AddHours(23).AddMinutes(59);
            d2 = new DateTime(1961, 12, 31);
            n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(730, n);

            n = TSDateCalculator.CountSteps(d1, d1, u, q);
            Assert.AreEqual(0, n);
        }
        [TestMethod()]
        public void CountSteps_2Day()
        {
            TSDateCalculator.TimeStepUnitCode u = TSDateCalculator.TimeStepUnitCode.Day;
            short q = 2;

            DateTime d1 = new DateTime(1960, 1, 1);
            DateTime d2 = new DateTime(1960, 12, 31);
            int n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(183, n);

            d1 = d1.AddHours(23).AddMinutes(59);
            d2 = new DateTime(1961, 12, 31);
            n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(365, n);

            n = TSDateCalculator.CountSteps(d1, d1, u, q);
            Assert.AreEqual(0, n);
        }
        [TestMethod()]
        public void CountSteps_1Week()
        {
            TSDateCalculator.TimeStepUnitCode u = TSDateCalculator.TimeStepUnitCode.Week;
            short q = 1;

            DateTime d1 = new DateTime(1960, 1, 1);
            DateTime d2 = d1.AddDays(56);
            int n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(8, n);

            n = TSDateCalculator.CountSteps(d1, d1, u, q);
            Assert.AreEqual(0, n);
        }
        [TestMethod()]
        public void CountSteps_4Week()
        {
            TSDateCalculator.TimeStepUnitCode u = TSDateCalculator.TimeStepUnitCode.Week;
            short q = 4;

            DateTime d1 = new DateTime(1960, 1, 1);
            DateTime d2 = d1.AddDays(56);
            int n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(2, n);

            n = TSDateCalculator.CountSteps(d1, d1, u, q);
            Assert.AreEqual(0, n);
        }
        [TestMethod()]
        public void CountSteps_1Month()
        {
            TSDateCalculator.TimeStepUnitCode u = TSDateCalculator.TimeStepUnitCode.Month;
            short q = 1;

            DateTime d1 = new DateTime(1960, 1, 2);
            DateTime d2 = new DateTime(1960, 12, 15);
            int n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(11, n);

            d1 = new DateTime(1960, 1, 1);
            d2 = new DateTime(1960, 12, 1);
            n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(11, n);

            n = TSDateCalculator.CountSteps(d1, d1, u, q);
            Assert.AreEqual(0, n);
        }
        [TestMethod()]
        public void CountSteps_2Month()
        {
            TSDateCalculator.TimeStepUnitCode u = TSDateCalculator.TimeStepUnitCode.Month;
            short q = 2;

            DateTime d1 = new DateTime(1960, 1, 2);
            DateTime d2 = new DateTime(1960, 12, 15);
            int n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(6, n);

            d1 = new DateTime(1960, 1, 1);
            d2 = new DateTime(1960, 12, 1);
            n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(6, n);

            n = TSDateCalculator.CountSteps(d1, d1, u, q);
            Assert.AreEqual(0, n);
        }
        [TestMethod()]
        public void CountSteps_1Year()
        {
            TSDateCalculator.TimeStepUnitCode u = TSDateCalculator.TimeStepUnitCode.Year;
            short q = 1;

            DateTime d1 = new DateTime(1960, 1, 2);
            DateTime d2 = new DateTime(1980, 12, 15);
            int n = TSDateCalculator.CountSteps(d1, d2, u, q);
            Assert.AreEqual(20, n);

            n = TSDateCalculator.CountSteps(d1, d1, u, q);
            Assert.AreEqual(0, n);
        }
        #endregion

        #region Test methods: AddMonthsByEnd
        [TestMethod()]
        public void AddMonthsByEnd_0a()
        {
            DateTime d = new DateTime(1960, 1, 31);
            Assert.AreEqual(d, d.AddMonthsByEnd(0, 0));
        
            Assert.AreEqual(new DateTime(1960, 2, 29), d.AddMonthsByEnd(0, 1));
            Assert.AreEqual(new DateTime(1960, 3, 31), d.AddMonthsByEnd(0, 2));
            Assert.AreEqual(new DateTime(1961, 1, 31), d.AddMonthsByEnd(0, 12));
            Assert.AreEqual(new DateTime(1961, 2, 28), d.AddMonthsByEnd(0, 13));
            Assert.AreEqual(new DateTime(1961, 3, 31), d.AddMonthsByEnd(0, 14));
            Assert.AreEqual(new DateTime(1961, 4, 30), d.AddMonthsByEnd(0, 15));

            Assert.AreEqual(new DateTime(1959, 12, 31), d.AddMonthsByEnd(0, -1));
            Assert.AreEqual(new DateTime(1959, 11, 30), d.AddMonthsByEnd(0, -2));
            Assert.AreEqual(new DateTime(1959, 2, 28), d.AddMonthsByEnd(0, -11));
            Assert.AreEqual(new DateTime(1959, 1, 31), d.AddMonthsByEnd(0, -12));
            Assert.AreEqual(new DateTime(1958, 12, 31), d.AddMonthsByEnd(0, -13));
        }
        [TestMethod()]
        public void AddMonthsByEnd_0b()
        {
            DateTime d = new DateTime(1960, 2, 29);
            Assert.AreEqual(d, d.AddMonthsByEnd(0, 0));

            Assert.AreEqual(new DateTime(1960, 3, 31), d.AddMonthsByEnd(0, 1));
            Assert.AreEqual(new DateTime(1960, 4, 30), d.AddMonthsByEnd(0, 2));
            Assert.AreEqual(new DateTime(1961, 2, 28), d.AddMonthsByEnd(0, 12));
            Assert.AreEqual(new DateTime(1961, 3, 31), d.AddMonthsByEnd(0, 13));
            Assert.AreEqual(new DateTime(1961, 4, 30), d.AddMonthsByEnd(0, 14));
            Assert.AreEqual(new DateTime(1961, 5, 31), d.AddMonthsByEnd(0, 15));

            Assert.AreEqual(new DateTime(1960, 1, 31), d.AddMonthsByEnd(0, -1));
            Assert.AreEqual(new DateTime(1959, 12, 31), d.AddMonthsByEnd(0, -2));
            Assert.AreEqual(new DateTime(1959, 3, 31), d.AddMonthsByEnd(0, -11));
            Assert.AreEqual(new DateTime(1959, 2, 28), d.AddMonthsByEnd(0, -12));
            Assert.AreEqual(new DateTime(1959, 1, 31), d.AddMonthsByEnd(0, -13));
        }
        [TestMethod()]
        public void AddMonthsByEnd_0c()
        {
            DateTime d = new DateTime(1959, 2, 28);
            Assert.AreEqual(d, d.AddMonthsByEnd(0, 0));

            Assert.AreEqual(new DateTime(1959, 3, 31), d.AddMonthsByEnd(0, 1));
            Assert.AreEqual(new DateTime(1959, 4, 30), d.AddMonthsByEnd(0, 2));
            Assert.AreEqual(new DateTime(1960, 2, 29), d.AddMonthsByEnd(0, 12));
            Assert.AreEqual(new DateTime(1960, 3, 31), d.AddMonthsByEnd(0, 13));
            Assert.AreEqual(new DateTime(1960, 4, 30), d.AddMonthsByEnd(0, 14));
            Assert.AreEqual(new DateTime(1960, 5, 31), d.AddMonthsByEnd(0, 15));

            Assert.AreEqual(new DateTime(1959, 1, 31), d.AddMonthsByEnd(0, -1));
            Assert.AreEqual(new DateTime(1958, 12, 31), d.AddMonthsByEnd(0, -2));
            Assert.AreEqual(new DateTime(1958, 3, 31), d.AddMonthsByEnd(0, -11));
            Assert.AreEqual(new DateTime(1958, 2, 28), d.AddMonthsByEnd(0, -12));
            Assert.AreEqual(new DateTime(1958, 1, 31), d.AddMonthsByEnd(0, -13));
        }

        [TestMethod()]
        public void AddMonthsByEnd_1a()
        {
            DateTime d = new DateTime(1960, 2, 1);
            Assert.AreEqual(d, d.AddMonthsByEnd(1, 0));

            Assert.AreEqual(new DateTime(1960, 3, 1), d.AddMonthsByEnd(1, 1));
            Assert.AreEqual(new DateTime(1960, 4, 1), d.AddMonthsByEnd(1, 2));
            Assert.AreEqual(new DateTime(1961, 2, 1), d.AddMonthsByEnd(1, 12));
            Assert.AreEqual(new DateTime(1961, 3, 1), d.AddMonthsByEnd(1, 13));
            Assert.AreEqual(new DateTime(1961, 4, 1), d.AddMonthsByEnd(1, 14));
            Assert.AreEqual(new DateTime(1961, 5, 1), d.AddMonthsByEnd(1, 15));

            Assert.AreEqual(new DateTime(1960, 1, 1), d.AddMonthsByEnd(1, -1));
            Assert.AreEqual(new DateTime(1959, 12, 1), d.AddMonthsByEnd(1, -2));
            Assert.AreEqual(new DateTime(1959, 3, 1), d.AddMonthsByEnd(1, -11));
            Assert.AreEqual(new DateTime(1959, 2, 1), d.AddMonthsByEnd(1, -12));
            Assert.AreEqual(new DateTime(1959, 1, 1), d.AddMonthsByEnd(1, -13));
        }
        [TestMethod()]
        public void AddMonthsByEnd_1b()
        {
            DateTime d = new DateTime(1960, 3, 1);
            Assert.AreEqual(d, d.AddMonthsByEnd(1, 0));

            Assert.AreEqual(new DateTime(1960, 4, 1), d.AddMonthsByEnd(1, 1));
            Assert.AreEqual(new DateTime(1960, 5, 1), d.AddMonthsByEnd(1, 2));
            Assert.AreEqual(new DateTime(1961, 3, 1), d.AddMonthsByEnd(1, 12));
            Assert.AreEqual(new DateTime(1961, 4, 1), d.AddMonthsByEnd(1, 13));
            Assert.AreEqual(new DateTime(1961, 5, 1), d.AddMonthsByEnd(1, 14));
            Assert.AreEqual(new DateTime(1961, 6, 1), d.AddMonthsByEnd(1, 15));

            Assert.AreEqual(new DateTime(1960, 2, 1), d.AddMonthsByEnd(1, -1));
            Assert.AreEqual(new DateTime(1960, 1, 1), d.AddMonthsByEnd(1, -2));
            Assert.AreEqual(new DateTime(1959, 4, 1), d.AddMonthsByEnd(1, -11));
            Assert.AreEqual(new DateTime(1959, 3, 1), d.AddMonthsByEnd(1, -12));
            Assert.AreEqual(new DateTime(1959, 2, 1), d.AddMonthsByEnd(1, -13));
        }
        [TestMethod()]
        public void AddMonthsByEnd_1c()
        {
            DateTime d = new DateTime(1959, 3, 1);
            Assert.AreEqual(d, d.AddMonthsByEnd(1, 0));

            Assert.AreEqual(new DateTime(1959, 4, 1), d.AddMonthsByEnd(1, 1));
            Assert.AreEqual(new DateTime(1959, 5, 1), d.AddMonthsByEnd(1, 2));
            Assert.AreEqual(new DateTime(1960, 3, 1), d.AddMonthsByEnd(1, 12));
            Assert.AreEqual(new DateTime(1960, 4, 1), d.AddMonthsByEnd(1, 13));
            Assert.AreEqual(new DateTime(1960, 5, 1), d.AddMonthsByEnd(1, 14));
            Assert.AreEqual(new DateTime(1960, 6, 1), d.AddMonthsByEnd(1, 15));

            Assert.AreEqual(new DateTime(1959, 2, 1), d.AddMonthsByEnd(1, -1));
            Assert.AreEqual(new DateTime(1959, 1, 1), d.AddMonthsByEnd(1, -2));
            Assert.AreEqual(new DateTime(1958, 4, 1), d.AddMonthsByEnd(1, -11));
            Assert.AreEqual(new DateTime(1958, 3, 1), d.AddMonthsByEnd(1, -12));
            Assert.AreEqual(new DateTime(1958, 2, 1), d.AddMonthsByEnd(1, -13));
        }

        [TestMethod()]
        public void AddMonthsByEnd_2a()
        {
            DateTime d = new DateTime(1960, 2, 2);
            Assert.AreEqual(d, d.AddMonthsByEnd(2, 0));

            Assert.AreEqual(new DateTime(1960, 3, 2), d.AddMonthsByEnd(2, 1));
            Assert.AreEqual(new DateTime(1960, 4, 2), d.AddMonthsByEnd(2, 2));
            Assert.AreEqual(new DateTime(1961, 2, 2), d.AddMonthsByEnd(2, 12));
            Assert.AreEqual(new DateTime(1961, 3, 2), d.AddMonthsByEnd(2, 13));
            Assert.AreEqual(new DateTime(1961, 4, 2), d.AddMonthsByEnd(2, 14));
            Assert.AreEqual(new DateTime(1961, 5, 2), d.AddMonthsByEnd(2, 15));

            Assert.AreEqual(new DateTime(1960, 1, 2), d.AddMonthsByEnd(2, -1));
            Assert.AreEqual(new DateTime(1959, 12, 2), d.AddMonthsByEnd(2, -2));
            Assert.AreEqual(new DateTime(1959, 3, 2), d.AddMonthsByEnd(2, -11));
            Assert.AreEqual(new DateTime(1959, 2, 2), d.AddMonthsByEnd(2, -12));
            Assert.AreEqual(new DateTime(1959, 1, 2), d.AddMonthsByEnd(2, -13));
        }
        [TestMethod()]
        public void AddMonthsByEnd_2b()
        {
            DateTime d = new DateTime(1960, 3, 2);
            Assert.AreEqual(d, d.AddMonthsByEnd(2, 0));

            Assert.AreEqual(new DateTime(1960, 4, 2), d.AddMonthsByEnd(2, 1));
            Assert.AreEqual(new DateTime(1960, 5, 2), d.AddMonthsByEnd(2, 2));
            Assert.AreEqual(new DateTime(1961, 3, 2), d.AddMonthsByEnd(2, 12));
            Assert.AreEqual(new DateTime(1961, 4, 2), d.AddMonthsByEnd(2, 13));
            Assert.AreEqual(new DateTime(1961, 5, 2), d.AddMonthsByEnd(2, 14));
            Assert.AreEqual(new DateTime(1961, 6, 2), d.AddMonthsByEnd(2, 15));

            Assert.AreEqual(new DateTime(1960, 2, 2), d.AddMonthsByEnd(2, -1));
            Assert.AreEqual(new DateTime(1960, 1, 2), d.AddMonthsByEnd(2, -2));
            Assert.AreEqual(new DateTime(1959, 4, 2), d.AddMonthsByEnd(2, -11));
            Assert.AreEqual(new DateTime(1959, 3, 2), d.AddMonthsByEnd(2, -12));
            Assert.AreEqual(new DateTime(1959, 2, 2), d.AddMonthsByEnd(2, -13));
        }
        [TestMethod()]
        public void AddMonthsByEnd_2c()
        {
            DateTime d = new DateTime(1959, 3, 2);
            Assert.AreEqual(d, d.AddMonthsByEnd(2, 0));

            Assert.AreEqual(new DateTime(1959, 4, 2), d.AddMonthsByEnd(2, 1));
            Assert.AreEqual(new DateTime(1959, 5, 2), d.AddMonthsByEnd(2, 2));
            Assert.AreEqual(new DateTime(1960, 3, 2), d.AddMonthsByEnd(2, 12));
            Assert.AreEqual(new DateTime(1960, 4, 2), d.AddMonthsByEnd(2, 13));
            Assert.AreEqual(new DateTime(1960, 5, 2), d.AddMonthsByEnd(2, 14));
            Assert.AreEqual(new DateTime(1960, 6, 2), d.AddMonthsByEnd(2, 15));

            Assert.AreEqual(new DateTime(1959, 2, 2), d.AddMonthsByEnd(2, -1));
            Assert.AreEqual(new DateTime(1959, 1, 2), d.AddMonthsByEnd(2, -2));
            Assert.AreEqual(new DateTime(1958, 4, 2), d.AddMonthsByEnd(2, -11));
            Assert.AreEqual(new DateTime(1958, 3, 2), d.AddMonthsByEnd(2, -12));
            Assert.AreEqual(new DateTime(1958, 2, 2), d.AddMonthsByEnd(2, -13));
        }

        [TestMethod()]
        public void AddMonthsByEnd_30a()
        {
            DateTime d = new DateTime(1960, 3, 1);
            Assert.AreEqual(d, d.AddMonthsByEnd(30, 0));

            Assert.AreEqual(new DateTime(1960, 3, 30), d.AddMonthsByEnd(30, 1));
            Assert.AreEqual(new DateTime(1960, 4, 30), d.AddMonthsByEnd(30, 2));
            Assert.AreEqual(new DateTime(1961, 3, 2), d.AddMonthsByEnd(30, 12));
            Assert.AreEqual(new DateTime(1961, 3, 30), d.AddMonthsByEnd(30, 13));
            Assert.AreEqual(new DateTime(1961, 4, 30), d.AddMonthsByEnd(30, 14));
            Assert.AreEqual(new DateTime(1961, 5, 30), d.AddMonthsByEnd(30, 15));

            Assert.AreEqual(new DateTime(1960, 1, 30), d.AddMonthsByEnd(30, -1));
            Assert.AreEqual(new DateTime(1959, 12, 30), d.AddMonthsByEnd(30, -2));
            Assert.AreEqual(new DateTime(1959, 3, 30), d.AddMonthsByEnd(30, -11));
            Assert.AreEqual(new DateTime(1959, 3, 2), d.AddMonthsByEnd(30, -12));
            Assert.AreEqual(new DateTime(1959, 1, 30), d.AddMonthsByEnd(30, -13));
        }
        [TestMethod()]
        public void AddMonthsByEnd_30b()
        {
            DateTime d = new DateTime(1960, 3, 30);
            Assert.AreEqual(d, d.AddMonthsByEnd(30, 0));

            Assert.AreEqual(new DateTime(1960, 4, 30), d.AddMonthsByEnd(30, 1));
            Assert.AreEqual(new DateTime(1960, 5, 30), d.AddMonthsByEnd(30, 2));
            Assert.AreEqual(new DateTime(1961, 3, 2), d.AddMonthsByEnd(30, 11));
            Assert.AreEqual(new DateTime(1961, 3, 30), d.AddMonthsByEnd(30, 12));
            Assert.AreEqual(new DateTime(1961, 4, 30), d.AddMonthsByEnd(30, 13));
            Assert.AreEqual(new DateTime(1961, 5, 30), d.AddMonthsByEnd(30, 14));
            Assert.AreEqual(new DateTime(1961, 6, 30), d.AddMonthsByEnd(30, 15));

            Assert.AreEqual(new DateTime(1960, 3, 1), d.AddMonthsByEnd(30, -1));
            Assert.AreEqual(new DateTime(1960, 1, 30), d.AddMonthsByEnd(30, -2));
            Assert.AreEqual(new DateTime(1959, 4, 30), d.AddMonthsByEnd(30, -11));
            Assert.AreEqual(new DateTime(1959, 3, 30), d.AddMonthsByEnd(30, -12));
            Assert.AreEqual(new DateTime(1959, 3, 2), d.AddMonthsByEnd(30, -13));
        }
        [TestMethod()]
        public void AddMonthsByEnd_30c()
        {
            DateTime d = new DateTime(1959, 3, 30);
            Assert.AreEqual(d, d.AddMonthsByEnd(30, 0));

            Assert.AreEqual(new DateTime(1959, 4, 30), d.AddMonthsByEnd(30, 1));
            Assert.AreEqual(new DateTime(1959, 5, 30), d.AddMonthsByEnd(30, 2));
            Assert.AreEqual(new DateTime(1960, 3, 1), d.AddMonthsByEnd(30, 11));
            Assert.AreEqual(new DateTime(1960, 3, 30), d.AddMonthsByEnd(30, 12));
            Assert.AreEqual(new DateTime(1960, 4, 30), d.AddMonthsByEnd(30, 13));
            Assert.AreEqual(new DateTime(1960, 5, 30), d.AddMonthsByEnd(30, 14));
            Assert.AreEqual(new DateTime(1960, 6, 30), d.AddMonthsByEnd(30, 15));

            Assert.AreEqual(new DateTime(1959, 3, 2), d.AddMonthsByEnd(30, -1));
            Assert.AreEqual(new DateTime(1959, 1, 30), d.AddMonthsByEnd(30, -2));
            Assert.AreEqual(new DateTime(1958, 4, 30), d.AddMonthsByEnd(30, -11));
            Assert.AreEqual(new DateTime(1958, 3, 30), d.AddMonthsByEnd(30, -12));
            Assert.AreEqual(new DateTime(1958, 3, 2), d.AddMonthsByEnd(30, -13));
        }
        #endregion

        #region Test methods: RoundMonthEnd
        [TestMethod()]
        public void RoundMonthEnd_Jan1()
        {
            DateTime d = new DateTime(1960, 1, 1);

            Assert.AreEqual(new DateTime(1960, 1, 31, 23, 59, 0), d.RoundMonthEnd(0));
            
            Assert.AreEqual(new DateTime(1960, 1, 1, 23, 59, 0), d.RoundMonthEnd(1));
            Assert.AreEqual(new DateTime(1960, 1, 2, 23, 59, 0), d.RoundMonthEnd(2));
            Assert.AreEqual(new DateTime(1960, 1, 29, 23, 59, 0), d.RoundMonthEnd(29));
            Assert.AreEqual(new DateTime(1960, 1, 30, 23, 59, 0), d.RoundMonthEnd(30));

            Assert.AreEqual(new DateTime(1960, 1, 30, 23, 59, 0), d.RoundMonthEnd(-1));
            Assert.AreEqual(new DateTime(1960, 1, 29, 23, 59, 0), d.RoundMonthEnd(-2));
            Assert.AreEqual(new DateTime(1960, 1, 2, 23, 59, 0), d.RoundMonthEnd(-29));
            Assert.AreEqual(new DateTime(1960, 1, 1, 23, 59, 0), d.RoundMonthEnd(-30));
        }
        [TestMethod()]
        public void RoundMonthEnd_Feb28()
        {
            DateTime d = new DateTime(1960, 2, 28);

            Assert.AreEqual(new DateTime(1960, 2, 29, 23, 59, 0), d.RoundMonthEnd(0));

            Assert.AreEqual(new DateTime(1960, 3, 1, 23, 59, 0), d.RoundMonthEnd(1));
            Assert.AreEqual(new DateTime(1960, 3, 2, 23, 59, 0), d.RoundMonthEnd(2));
            Assert.AreEqual(new DateTime(1960, 2, 29, 23, 59, 0), d.RoundMonthEnd(29));
            Assert.AreEqual(new DateTime(1960, 3, 1, 23, 59, 0), d.RoundMonthEnd(30));

            Assert.AreEqual(new DateTime(1960, 2, 28, 23, 59, 0), d.RoundMonthEnd(-1));
            Assert.AreEqual(new DateTime(1960, 3, 29, 23, 59, 0), d.RoundMonthEnd(-2));
            Assert.AreEqual(new DateTime(1960, 3, 2, 23, 59, 0), d.RoundMonthEnd(-29));
            Assert.AreEqual(new DateTime(1960, 3, 1, 23, 59, 0), d.RoundMonthEnd(-30));
        }
        [TestMethod()]
        public void RoundMonthEnd_Dec2()
        {
            DateTime d = new DateTime(1959, 12, 2);

            Assert.AreEqual(new DateTime(1959, 12, 31, 23, 59, 0), d.RoundMonthEnd(0));

            Assert.AreEqual(new DateTime(1960, 1, 1, 23, 59, 0), d.RoundMonthEnd(1));
            Assert.AreEqual(new DateTime(1959, 12, 2, 23, 59, 0), d.RoundMonthEnd(2));
            Assert.AreEqual(new DateTime(1959, 12, 29, 23, 59, 0), d.RoundMonthEnd(29));
            Assert.AreEqual(new DateTime(1959, 12, 30, 23, 59, 0), d.RoundMonthEnd(30));

            Assert.AreEqual(new DateTime(1959, 12, 30, 23, 59, 0), d.RoundMonthEnd(-1));
            Assert.AreEqual(new DateTime(1959, 12, 29, 23, 59, 0), d.RoundMonthEnd(-2));
            Assert.AreEqual(new DateTime(1959, 12, 2, 23, 59, 0), d.RoundMonthEnd(-29));
            Assert.AreEqual(new DateTime(1960, 1, 1, 23, 59, 0), d.RoundMonthEnd(-30));
        }
        [TestMethod()]
        public void RoundMonthEnd_Dec31()
        {
            DateTime d = new DateTime(1959, 12, 31);

            Assert.AreEqual(new DateTime(1959, 12, 31, 23, 59, 0), d.RoundMonthEnd(0));

            Assert.AreEqual(new DateTime(1960, 1, 1, 23, 59, 0), d.RoundMonthEnd(1));
            Assert.AreEqual(new DateTime(1960, 1, 2, 23, 59, 0), d.RoundMonthEnd(2));
            Assert.AreEqual(new DateTime(1960, 1, 29, 23, 59, 0), d.RoundMonthEnd(29));
            Assert.AreEqual(new DateTime(1960, 1, 30, 23, 59, 0), d.RoundMonthEnd(30));

            Assert.AreEqual(new DateTime(1960, 1, 30, 23, 59, 0), d.RoundMonthEnd(-1));
            Assert.AreEqual(new DateTime(1960, 1, 29, 23, 59, 0), d.RoundMonthEnd(-2));
            Assert.AreEqual(new DateTime(1960, 1, 2, 23, 59, 0), d.RoundMonthEnd(-29));
            Assert.AreEqual(new DateTime(1960, 1, 1, 23, 59, 0), d.RoundMonthEnd(-30));
        }
        #endregion

    }
}
