using TimeSeriesLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace TimeSeriesLibrary_Test
{
    
    
    /// <summary>
    ///This is a test class for TSLibraryTest and is intended
    ///to contain all TSLibraryTest Unit Tests
    ///</summary>
    [TestClass()]
    public class TSLibraryTest
    {
        static List<TimeSeriesValue> IrregList1 = new List<TimeSeriesValue>();
        static List<TimeSeriesValue> IrregList2 = new List<TimeSeriesValue>();
        static List<TimeSeriesValue> RegList1 = new List<TimeSeriesValue>();
            static TSDateCalculator.TimeStepUnitCode TimeStepUnit1 = TSDateCalculator.TimeStepUnitCode.Hour;
            static short TimeStepQuantity1 = 3;
            static DateTime BlobStartDate1 = DateTime.Parse("1/1/1920 12:00 PM");
        static List<TimeSeriesValue> RegList2 = new List<TimeSeriesValue>();
            static TSDateCalculator.TimeStepUnitCode TimeStepUnit2 = TSDateCalculator.TimeStepUnitCode.Month;
            static short TimeStepQuantity2 = 1;
            static DateTime BlobStartDate2 = DateTime.Parse("3/3/1933 12:11 PM");

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
        
        //Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            int i;

            IrregList1.Add(new TimeSeriesValue { Date = BlobStartDate1, Value = 1.1 });
            RegList1.Add(new TimeSeriesValue{ Date=BlobStartDate1, Value=-500.0 });

            for(i=1; i<500; i++)
            {
                IrregList1.Add(new TimeSeriesValue{ 
                        Date=IrregList1[i-1].Date.AddHours(Math.Sqrt(i)),
                        Value=IrregList1[i-1].Value + i/4 });
                RegList1.Add(new TimeSeriesValue{
                        Date=RegList1[i-1].Date.AddHours(3),
                        Value=i * 10 });
            }

            IrregList2.Add(new TimeSeriesValue { Date = BlobStartDate2, Value = 3.5 });
            RegList2.Add(new TimeSeriesValue { Date = BlobStartDate2, Value = -20.0 });

            for (i = 1; i < 1144; i++)
            {
                IrregList2.Add(new TimeSeriesValue
                {
                    Date = IrregList2[i-1].Date.AddDays(i/2),
                    Value = IrregList2[i-1].Value * 1.222,
                });
                RegList2.Add(new TimeSeriesValue
                {
                    Date = RegList2[i-1].Date.AddMonths(1),
                    Value = 3.0
                });
            }
        }

        #endregion


        /// <summary>
        ///A test for ConvertBlobToListAll
        ///</summary>
        // This method is re-used by the actual test methods that follow.
        public void ConvertBlobAll(List<TimeSeriesValue> inList,
                TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity, 
                DateTime blobStartDate)
        {
            TSLibrary tsLib = new TSLibrary();
            List<TimeSeriesValue> outList = new List<TimeSeriesValue>();

            byte[] blobData = tsLib.ConvertListToBlob(timeStepUnit, inList);

            int ret = tsLib.ConvertBlobToListAll(timeStepUnit, timeStepQuantity,
                            blobStartDate, blobData, ref outList);

            // The return value of the function must match the number of items in the original list
            Assert.AreEqual(ret, inList.Count);
            // the count in both lists must match
            Assert.AreEqual(outList.Count, inList.Count);
            
            // now check each item in the two lists
            Boolean AreEqual = true;
            for(int i=0; i<ret; i++)
            {
                if( outList[i].ValueEquals(inList[i])==false )
                    AreEqual = false;
            }

            Assert.IsTrue(AreEqual);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest1()
        {
            ConvertBlobAll(IrregList1, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate1);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest2()
        {
            ConvertBlobAll(IrregList2, TSDateCalculator.TimeStepUnitCode.Irregular, 0, BlobStartDate2);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest1()
        {
            ConvertBlobAll(RegList1, TimeStepUnit1, TimeStepQuantity1, BlobStartDate1);
        }
        [TestMethod()]
        public void ConvertBlobToListAllRegTest2()
        {
            ConvertBlobAll(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2);
        }

    }
}
