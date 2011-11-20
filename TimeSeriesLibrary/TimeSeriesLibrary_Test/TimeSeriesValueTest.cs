using TimeSeriesLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Collections.Generic;

namespace TimeSeriesLibrary_Test
{
    
    
    /// <summary>
    ///This is a test class for TimeSeriesValueTest and is intended
    ///to contain all TimeSeriesValueTest Unit Tests
    ///</summary>
    [TestClass()]
    public class TimeSeriesValueTest
    {
        DateTime date1, date2;
        double val1, val2;

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
        
        //Use TestInitialize to run code before running each test
        [TestInitialize()]
        public void MyTestInitialize()
        {
            date1 = DateTime.Parse("6/5/2010 3:00 PM");
            date2 = DateTime.Parse("3/12/1910 11:00 AM");
            val1 = 45.67;
            val2 = -127.6;
        }
        
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion


        /// <summary>
        /// test for ValueEquals method
        ///</summary>
        [TestMethod()]
        public void ValueEqualsTrueTest()
        {
            TimeSeriesValue tsv1 = new TimeSeriesValue { Date = date1, Value = val1 };
            TimeSeriesValue tsv2 = new TimeSeriesValue { Date = date1, Value = val1 };

            Assert.IsTrue(tsv1.ValueEquals(tsv2));
        }
        [TestMethod()]
        public void ValueEqualsFalseValTest()
        {
            TimeSeriesValue tsv1 = new TimeSeriesValue { Date = date1, Value = val1 };
            TimeSeriesValue tsv2 = new TimeSeriesValue { Date = date1, Value = val2 };

            Assert.IsFalse(tsv1.ValueEquals(tsv2));
        }
        [TestMethod()]
        public void ValueEqualsFalseDateTest()
        {
            TimeSeriesValue tsv1 = new TimeSeriesValue { Date = date1, Value = val1 };
            TimeSeriesValue tsv2 = new TimeSeriesValue { Date = date2, Value = val1 };

            Assert.IsFalse(tsv1.ValueEquals(tsv2));
        }
        /// <summary>
        /// tests for explicit conversion operators
        ///</summary>
        [TestMethod()]
        public void ConvertFromStruct1()
        {
            TSDateValueStruct tsdvs = new TSDateValueStruct{ Date=date1, Value=val1 };
            TimeSeriesValue tsv = new TimeSeriesValue{ Date = date1, Value = val1 };
            
            TimeSeriesValue actual = ((TimeSeriesValue)(tsdvs));
            Assert.IsTrue(tsv.ValueEquals(actual));
        }
        [TestMethod()]
        public void ConvertFromStruct2()
        {
            TSDateValueStruct tsdvs = new TSDateValueStruct { Date = date2, Value = val2 };
            TimeSeriesValue tsv = new TimeSeriesValue { Date = date2, Value = val2 };

            TimeSeriesValue actual = ((TimeSeriesValue)(tsdvs));
            Assert.IsTrue(tsv.ValueEquals(actual));
        }
        [TestMethod()]
        public void ConvertToStruct1()
        {
            TSDateValueStruct tsdvs = new TSDateValueStruct { Date = date1, Value = val1 };
            TimeSeriesValue tsv = new TimeSeriesValue { Date = date1, Value = val1 };
            
            TSDateValueStruct actual = ((TSDateValueStruct)(tsv));
            Assert.AreEqual(tsdvs, actual);
        }
        [TestMethod()]
        public void ConvertToStruct2()
        {
            TSDateValueStruct tsdvs = new TSDateValueStruct { Date = date2, Value = val2 };
            TimeSeriesValue tsv = new TimeSeriesValue { Date = date2, Value = val2 };

            TSDateValueStruct actual = ((TSDateValueStruct)(tsv));
            Assert.AreEqual(tsdvs, actual);
        }
    }
}
