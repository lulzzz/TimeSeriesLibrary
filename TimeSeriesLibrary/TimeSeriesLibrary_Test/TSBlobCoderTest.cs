using TimeSeriesLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Collections.Generic;

namespace TimeSeriesLibrary_Test
{
    
    
    /// <summary>
    ///This is a test class for TSLibraryTest and is intended
    ///to contain all TSLibraryTest Unit Tests
    ///</summary>
    [TestClass()]
    public class TSBlobCoderTest
    {
        static List<TimeSeriesValue> IrregList1 = new List<TimeSeriesValue>();
        static List<TimeSeriesValue> IrregList2 = new List<TimeSeriesValue>();
        static List<TimeSeriesValue> RegList1 = new List<TimeSeriesValue>();
        static TSDateCalculator.TimeStepUnitCode TimeStepUnit1 = TSDateCalculator.TimeStepUnitCode.Hour;
        static short TimeStepQuantity1 = 3;
        static DateTime BlobStartDate1 = DateTime.Parse("1/1/1920 12:00 PM");
        static int TimeStepCount1 =500;
        static List<TimeSeriesValue> RegList2 = new List<TimeSeriesValue>();
        static TSDateCalculator.TimeStepUnitCode TimeStepUnit2 = TSDateCalculator.TimeStepUnitCode.Month;
        static short TimeStepQuantity2 = 1;
        static DateTime BlobStartDate2 = DateTime.Parse("3/3/1933 12:11 PM");
        static int TimeStepCount2 = 1144;

        #region TestContext
        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get { return testContextInstance; }
            set { testContextInstance = value; }
        }
        private TestContext testContextInstance;
        #endregion


        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        
        //Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            int i;

            IrregList1.Add(new TimeSeriesValue { Date = BlobStartDate1, Value = 1.1 });
            RegList1.Add(new TimeSeriesValue { Date = BlobStartDate1, Value = -500.0 });

            for (i = 1; i < TimeStepCount1; i++)
            {
                IrregList1.Add(new TimeSeriesValue
                {
                    Date = IrregList1[i - 1].Date.AddHours(Math.Sqrt(i)),
                    Value = IrregList1[i - 1].Value + i * 2 / 3
                });
                RegList1.Add(new TimeSeriesValue
                {
                    Date = RegList1[i - 1].Date.AddHours(3),
                    Value = i * 10
                });
            }

            IrregList2.Add(new TimeSeriesValue { Date = BlobStartDate2, Value = 3.5 });
            RegList2.Add(new TimeSeriesValue { Date = BlobStartDate2.RoundMonthEnd(0), Value = -20.0 });

            for (i = 1; i < TimeStepCount2; i++)
            {
                IrregList2.Add(new TimeSeriesValue
                {
                    Date = IrregList2[i - 1].Date.AddDays(i / 2),
                    Value = IrregList2[i - 1].Value * 1.222,
                });
                RegList2.Add(new TimeSeriesValue
                {
                    Date = RegList2[i - 1].Date.AddMonthsByEnd(0, 1),
                    Value = 3.0
                });
            }
        }

        #endregion

        // Note--this test class was created primarily b/c we wanted to individually test the different
        // compression methods.  Hence, we have created the test methods below.  The class TSBlobCoder
        // certainly deserves full test coverage, but that can be developed later.  In the meantime,
        // it is generally expected that TSBlobCoder has adequate test coverage via TSLibraryTest.

        #region Test Methods for ConvertBlobToListAll() and ConvertListToBlob()
        // The series of tests below is for ConvertBlobToListAll and ConvertListToBlob.
        // The tests take advantage of the fact that the methods are designed so that
        // the series that is put into the BLOB must be identical to the series that
        // comes out of the BLOB.

        // This method is re-used by the actual test methods that follow.
        public void ConvertBlobAll(List<TimeSeriesValue> inList,
                TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                DateTime blobStartDate, int compressionCode)
        {
            TSLibrary tsLib = new TSLibrary();

            var traceObject = new TSTrace
            {
                TraceNumber = 1,
                TimeStepCount = inList.Count,
                EndDate = inList.Last().Date
            };

            if (timeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular)
            {
                var inArray = inList.Select(v => (TSDateValueStruct)v).ToArray();
                var outArray = new TSDateValueStruct[inList.Count];
                byte[] blobData = TSBlobCoder.ConvertArrayToBlobIrregular(inArray,
                                                        compressionCode, traceObject);

                int ret = TSBlobCoder.ConvertBlobToArrayIrregular(inList.Count, 
                                            false, 0, blobStartDate, blobStartDate,
                                            blobData, outArray, compressionCode);

                // The return value of the function must match the number of items in the original list
                Assert.AreEqual(ret, inList.Count);
                // the count in both lists must match
                Assert.AreEqual(inArray.Length, outArray.Length);
                // now check each item in the two lists
                Boolean AreEqual = true;
                for (int i = 0; i < ret; i++)
                {
                    if (outArray[i].Date != inArray[i].Date || outArray[i].Value != inArray[i].Value)
                        AreEqual = false;
                }
                Assert.IsTrue(AreEqual);
            }
            else
            {
                var inArray = inList.Select(v => v.Value).ToArray();
                var outArray = new Double[inList.Count];
                byte[] blobData = TSBlobCoder.ConvertArrayToBlobRegular(inArray,
                                                        compressionCode, traceObject);

                int ret = TSBlobCoder.ConvertBlobToArrayRegular(timeStepUnit, timeStepQuantity,
                                            inList.Count, blobStartDate,
                                            false, 0, blobStartDate, blobStartDate,
                                            blobData, outArray, compressionCode);

                // The return value of the function must match the number of items in the original list
                Assert.AreEqual(ret, inList.Count);
                // the count in both lists must match
                Assert.AreEqual(inArray.Length, outArray.Length);
                // now check each item in the two lists
                Boolean AreEqual = true;
                for (int i = 0; i < ret; i++)
                {
                    if (outArray[i] != inArray[i])
                        AreEqual = false;
                }
                Assert.IsTrue(AreEqual);
            }

        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest1x0()
        {
            ConvertBlobAll(IrregList1, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate1, 0);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest1x1()
        {
            ConvertBlobAll(IrregList1, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate1, 1);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest1x2()
        {
            ConvertBlobAll(IrregList1, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate1, 2);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest2x0()
        {
            ConvertBlobAll(IrregList2, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate2, 0);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest2x1()
        {
            ConvertBlobAll(IrregList2, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate2, 1);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest2x2()
        {
            ConvertBlobAll(IrregList2, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate2, 2);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest1x1()
        {
            ConvertBlobAll(RegList1, TimeStepUnit1, TimeStepQuantity1, BlobStartDate1, 1);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest1x2()
        {
            ConvertBlobAll(RegList1, TimeStepUnit1, TimeStepQuantity1, BlobStartDate1, 2);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest1x3()
        {
            ConvertBlobAll(RegList1, TimeStepUnit1, TimeStepQuantity1, BlobStartDate1, 3);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest2x1()
        {
            ConvertBlobAll(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 1);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest2x2()
        {
            ConvertBlobAll(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 2);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest2x3()
        {
            ConvertBlobAll(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 3);
        }
        #endregion


    }
}
