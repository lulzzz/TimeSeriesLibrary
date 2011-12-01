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
    public class TSLibraryTest
    {
        static List<TimeSeriesValue> IrregList1 = new List<TimeSeriesValue>();
        static List<TimeSeriesValue> IrregList2 = new List<TimeSeriesValue>();
        static List<TimeSeriesValue> RegList1 = new List<TimeSeriesValue>();
            static TSDateCalculator.TimeStepUnitCode TimeStepUnit1 = TSDateCalculator.TimeStepUnitCode.HOUR;
            static short TimeStepQuantity1 = 3;
            static DateTime BlobStartDate1 = DateTime.Parse("1/1/1920 12:00 PM");
            static int TimeStepCount1 =500;
        static List<TimeSeriesValue> RegList2 = new List<TimeSeriesValue>();
            static TSDateCalculator.TimeStepUnitCode TimeStepUnit2 = TSDateCalculator.TimeStepUnitCode.MONTH;
            static short TimeStepQuantity2 = 1;
            static DateTime BlobStartDate2 = DateTime.Parse("3/3/1933 12:11 PM");
            static int TimeStepCount2 = 1144;

        private TestContext testContextInstance;

        #region TestContext
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
        
        //Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            int i;

            IrregList1.Add(new TimeSeriesValue { Date = BlobStartDate1, Value = 1.1 });
            RegList1.Add(new TimeSeriesValue{ Date=BlobStartDate1, Value=-500.0 });

            for(i=1; i<TimeStepCount1; i++)
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

            for (i = 1; i < TimeStepCount2; i++)
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


        #region Test Methods for ConvertBlobToListAll() and ConvertListToBlob()
        // The series of tests below is for ConvertBlobToListAll and ConvertListToBlob.
        // The tests take advantage of the fact that the methods are designed so that
        // the series that is put into the BLOB must be identical to the series that
        // comes out of the BLOB.

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
            for (int i = 0; i < ret; i++)
            {
                if (outList[i].ValueEquals(inList[i]) == false)
                    AreEqual = false;
            }

            Assert.IsTrue(AreEqual);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest1()
        {
            ConvertBlobAll(IrregList1, TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, BlobStartDate1);
        }
        [TestMethod()]
        public void ConvertBlobToListAllIrregTest2()
        {
            ConvertBlobAll(IrregList2, TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, BlobStartDate2);
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
        #endregion


        #region Test Methods for ConvertBlobToListLimited() and ConvertListToBlob()
        // The series of tests below is for ConvertBlobToListLimited and ConvertListToBlob.
        // The tests take advantage of the fact that the methods are designed so that
        // the series that is put into the BLOB must be identical to the series that
        // comes out of the BLOB.

        // This method is re-used by the actual test methods that follow.
        public void ConvertBlobLimited(List<TimeSeriesValue> inList,
                TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                DateTime blobStartDate,
                int nCutStart,          // the number of time steps that the test will truncate from the start of the series
                int nCutEnd,            // the number of time steps that the test will truncate from the end of the series
                int nMax)               // the maximum number of time steps to put into the series
        {
            TSLibrary tsLib = new TSLibrary();
            List<TimeSeriesValue> outList = new List<TimeSeriesValue>();

            byte[] blobData = tsLib.ConvertListToBlob(timeStepUnit, inList);

            int ret = tsLib.ConvertBlobToListLimited(timeStepUnit, timeStepQuantity,
                            blobStartDate,
                            nMax, inList[nCutStart].Date, inList[inList.Count - nCutEnd - 1].Date,
                            blobData, ref outList);

            // The return value of the function must match the number of items in the original list
            Assert.AreEqual(ret, Math.Min(nMax, inList.Count - nCutStart - nCutEnd));
            // the count in both lists must match
            Assert.AreEqual(outList.Count, Math.Min(nMax, inList.Count - nCutStart - nCutEnd));

            // now check each item in the two lists
            Boolean AreEqual = true;
            for (int i = 0; i < ret; i++)
            {
                if (outList[i].ValueEquals(inList[i + nCutStart]) == false)
                    AreEqual = false;
            }

            Assert.IsTrue(AreEqual);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedIrregTest1()
        {
            ConvertBlobLimited(IrregList1, TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, BlobStartDate1, 3, 5, TimeStepCount1);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedIrregTest2()
        {
            ConvertBlobLimited(IrregList2, TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, BlobStartDate2, 5, 13, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedIrregTest3()
        {
            ConvertBlobLimited(IrregList2, TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, BlobStartDate2, 0, 100, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedIrregTest4()
        {
            ConvertBlobLimited(IrregList2, TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, BlobStartDate2, 100, 0, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest1()
        {
            ConvertBlobLimited(RegList1, TimeStepUnit1, TimeStepQuantity1, BlobStartDate1, 4, 7, TimeStepCount1);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest2()
        {
            ConvertBlobLimited(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 7, 4, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest3()
        {
            ConvertBlobLimited(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 0, 180, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest4()
        {
            ConvertBlobLimited(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 180, 0, TimeStepCount2);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest5()
        {
            ConvertBlobLimited(RegList1, TimeStepUnit1, TimeStepQuantity1, BlobStartDate1, 0, 180, 72);
        }
        [TestMethod()]
        public void ConvertBlobToListLimitedRegTest6()
        {
            ConvertBlobLimited(RegList2, TimeStepUnit2, TimeStepQuantity2, BlobStartDate2, 180, 0, 231);
        } 
        #endregion


        #region Test Methods for ConvertListToBlobWithChecksum() without errors
        // The series of tests below is for ConvertListToBlobWithChecksum()
        //

        // This method is reused by the actual test methods that follow
        public Boolean ComputeTestChecksums(
                TSDateCalculator.TimeStepUnitCode u1, short q1, List<TimeSeriesValue> list1, ref byte[] chk1,
                TSDateCalculator.TimeStepUnitCode u2, short q2, List<TimeSeriesValue> list2, ref byte[] chk2)
        {
            TSLibrary tsLib = new TSLibrary();

            tsLib.ConvertListToBlobWithChecksum(
                u1, q1, list1.Count,
                list1[0].Date, list1[list1.Count - 1].Date, list1, ref chk1);

            tsLib.ConvertListToBlobWithChecksum(
                u2, q2, list2.Count,
                list2[0].Date, list2[list2.Count - 1].Date, list2, ref chk2);

            Assert.IsTrue(chk1.Length == 16);
            Assert.IsTrue(chk2.Length == 16);

            for (int i = 0; i < chk1.Length; i++)
                if (chk1[i] != chk2[i])
                    return false;

            return true;

        }
        // Test whether identical checksums are returned from identical timeseries.
        // Note that this test seems trivial -- equal inputs have to result in equal outputs.
        // However, it seemed important to test this behavior and I couldn't think of how else to do it.
        [TestMethod()]
        public void ChecksumIdenticalTest()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            byte[] chk1 = null, chk2 = null;

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, IrregList1, ref chk1,
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, duplicateList, ref chk2);

            Assert.IsTrue(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest1()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // Slightly alter one date in the list
            duplicateList[5].Date = duplicateList[5].Date.AddMinutes(12);

            byte[] chk1 = null, chk2 = null;

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, IrregList1, ref chk1,
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, duplicateList, ref chk2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest2()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // Slightly alter one value in the list
            duplicateList[17].Value *= 1.02;

            byte[] chk1 = null, chk2 = null;

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, IrregList1, ref chk1,
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, duplicateList, ref chk2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest2B()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // Slightly alter one value in the list
            duplicateList[177].Value *= 1.2;

            byte[] chk1 = null, chk2 = null;

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, IrregList1, ref chk1,
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, duplicateList, ref chk2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest3()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // remove the last time step in the list
            duplicateList.RemoveAt(duplicateList.Count - 1);

            byte[] chk1 = null, chk2 = null;

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, IrregList1, ref chk1,
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, duplicateList, ref chk2);

            Assert.IsFalse(ret);
        }
        [TestMethod()]
        public void ChecksumDiffTest4()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in IrregList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            // remove a value from the middle of the list
            duplicateList.RemoveAt(150);

            byte[] chk1 = null, chk2 = null;

            Boolean ret = ComputeTestChecksums(
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, IrregList1, ref chk1,
                    TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0, duplicateList, ref chk2);

            Assert.IsFalse(ret);
        }
        // This compound test method checks that the meta parameters are properly included in the
        // Checksum.  In the tests, the BLOB should be identical, but the meta parameters are different.
        [TestMethod()]
        public void ChecksumDiffParamTest()
        {
            List<TimeSeriesValue> duplicateList = new List<TimeSeriesValue>();

            // Create an identical array by deep copy
            foreach (TimeSeriesValue tsv in RegList1)
            {
                duplicateList.Add(new TimeSeriesValue { Date = tsv.Date, Value = tsv.Value });
            }
            byte[] chk1 = null, chk2 = null;

            Boolean ret;
            // The only difference should be the "TimeStepQuantity" parameter
            ret = ComputeTestChecksums(
                    TimeStepUnit1, TimeStepQuantity1, RegList1, ref chk1,
                    TimeStepUnit1, (short)(TimeStepQuantity1 + 1), duplicateList, ref chk2);

            Assert.IsFalse(ret);

            // The only difference should be the "TimeStepUnit" parameter
            ret = ComputeTestChecksums(
                    TimeStepUnit1, TimeStepQuantity1, RegList1, ref chk1,
                    TSDateCalculator.TimeStepUnitCode.DAY, TimeStepQuantity1, duplicateList, ref chk2);

            Assert.IsFalse(ret);

            // The only difference should be the "BlobStartDate" parameter
            duplicateList[0].Date = duplicateList[0].Date.AddMinutes(12);
            ret = ComputeTestChecksums(
                    TimeStepUnit1, TimeStepQuantity1, RegList1, ref chk1,
                    TimeStepUnit1, TimeStepQuantity1, duplicateList, ref chk2);

            Assert.IsFalse(ret);

            // undo the change from above
            duplicateList[0].Date = RegList1[0].Date;
            // The only difference should be the "BlobEndDate" parameter
            duplicateList[duplicateList.Count - 1].Date = duplicateList[duplicateList.Count - 1].Date.AddMinutes(-24);
            ret = ComputeTestChecksums(
                    TimeStepUnit1, TimeStepQuantity1, RegList1, ref chk1,
                    TimeStepUnit1, TimeStepQuantity1, duplicateList, ref chk2);

            Assert.IsFalse(ret);

            // undo the change from above
            duplicateList[duplicateList.Count - 1].Date = RegList1[duplicateList.Count - 1].Date;
            // Should be identical, to ensure that the above tests were what they were supposed to be
            ret = ComputeTestChecksums(
                    TimeStepUnit1, TimeStepQuantity1, RegList1, ref chk1,
                    TimeStepUnit1, TimeStepQuantity1, duplicateList, ref chk2);

            Assert.IsTrue(ret);
        } 
        #endregion


        #region Test Methods for error handling of ConvertListToBlobWithChecksum()

        // Method should not throw any exceptions.
        [TestMethod()]
        public void ConvertListToBlobWithChecksum_Err0()
        {
            TSLibrary tsLib = new TSLibrary();
            byte[] checksum = null;

            try
            {
                byte[] blobData = tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0,
                        IrregList1.Count, IrregList1.First().Date, IrregList1.Last().Date,
                        IrregList1, ref checksum);
                Assert.IsTrue(true);
            }
            catch (TSLibraryException e)
            {
                Assert.Fail("Should not throw any exceptions");
            }
        }
        // Method should throw an exception b/c the TimeStepQuantity must
        // be zero when the TimeStepUnit==Irregular.
        [TestMethod()]
        public void ConvertListToBlobWithChecksum_Err1()
        {
            TSLibrary tsLib = new TSLibrary();
            byte[] checksum = null;

            try
            {
                byte[] blobData = tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.IRREGULAR, 3,
                        IrregList1.Count, IrregList1.First().Date, IrregList1.Last().Date,
                        IrregList1, ref checksum);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Checksum_Quantity_Nonzero, e.ErrCode);
            }
        }
        // Method should throw an exception b/c the TimeStepCount is wrong
        [TestMethod()]
        public void ConvertListToBlobWithChecksum_Err2()
        {
            TSLibrary tsLib = new TSLibrary();
            byte[] checksum = null;

            try
            {
                byte[] blobData = tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0,
                        IrregList1.Count+3, IrregList1.First().Date, IrregList1.Last().Date,
                        IrregList1, ref checksum);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Checksum_Improper_Count, e.ErrCode);
            }
        }
        // Method should throw an exception b/c the StartDate is wrong
        [TestMethod()]
        public void ConvertListToBlobWithChecksum_Err3()
        {
            TSLibrary tsLib = new TSLibrary();
            byte[] checksum = null;

            try
            {
                byte[] blobData = tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0,
                        IrregList1.Count, IrregList1.First().Date.AddDays(2), IrregList1.Last().Date,
                        IrregList1, ref checksum);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Checksum_Improper_StartDate, e.ErrCode);
            }
        }
        // Method should throw an exception b/c the end date is wrong
        [TestMethod()]
        public void ConvertListToBlobWithChecksum_Err4()
        {
            TSLibrary tsLib = new TSLibrary();
            byte[] checksum = null;

            try
            {
                byte[] blobData = tsLib.ConvertListToBlobWithChecksum(
                        TSDateCalculator.TimeStepUnitCode.IRREGULAR, 0,
                        IrregList1.Count, IrregList1.First().Date, IrregList1.Last().Date.AddDays(2),
                        IrregList1, ref checksum);
                Assert.Fail("Should have thrown exception");
            }
            catch (TSLibraryException e)
            {
                Assert.AreEqual(ErrCode.Enum.Checksum_Improper_EndDate, e.ErrCode);
            }
        }

        #endregion

    }
}
